def get_user_basic(conn, schema: str, user_id: int):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                u.id,
                u.first_name,
                u.last_name,
                u.desired_income,
                u.city_id,
                u.desired_schedule_id,
                u.desired_work_format_id,
                u.specialty_id,
                sp.name AS specialty_name
            FROM {schema}.users u
            LEFT JOIN {schema}.specialties sp ON sp.id = u.specialty_id
            WHERE u.id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "first_name": row[1],
        "last_name": row[2],
        "desired_income": float(row[3]) if row[3] is not None else None,
        "city_id": row[4],
        "desired_schedule_id": row[5],
        "desired_work_format_id": row[6],
        "specialty_id": row[7],
        "specialty_name": row[8],
    }


def recommend_for_user(conn, schema: str, user_id: int, limit: int = 30):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH u AS (
                SELECT
                    id, desired_income, city_id, desired_schedule_id,
                    desired_work_format_id, specialty_id
                FROM {schema}.users
                WHERE id = %s
            )
            SELECT
                v.id,
                v.title,
                v.income_from,
                v.income_to,
                v.company_id,
                c.name AS company_name,
                v.address_id,
                ct.name AS city_name,
                v.schedule_id,
                sc.name AS schedule_name,
                v.work_format_id,
                wf.name AS work_format_name,
                v.skills_id,
                v.published_at,
                COALESCE(ms.matched_skills_count, 0) AS matched_skills_count,
                (
                    CASE WHEN u.city_id IS NOT NULL AND v.address_id = u.city_id THEN 35 ELSE 0 END +
                    CASE
                        WHEN u.desired_schedule_id IS NOT NULL
                         AND v.schedule_id IS NOT NULL
                         AND v.schedule_id::text = u.desired_schedule_id::text
                        THEN 20 ELSE 0
                    END +
                    CASE
                        WHEN u.desired_work_format_id IS NOT NULL
                         AND v.work_format_id IS NOT NULL
                         AND v.work_format_id::text = u.desired_work_format_id::text
                        THEN 20 ELSE 0
                    END +
                    CASE
                        WHEN u.desired_income IS NULL THEN 0
                        WHEN COALESCE(v.income_to, v.income_from, 0) >= u.desired_income THEN 25
                        WHEN COALESCE(v.income_from, 0) >= u.desired_income THEN 25
                        ELSE 0
                    END +
                    CASE
                        WHEN sp.name IS NOT NULL AND lower(v.title) LIKE '%%' || lower(sp.name) || '%%'
                        THEN 15
                        ELSE 0
                    END +
                    LEAST(COALESCE(ms.matched_skills_count, 0), 5) * 8
                ) AS match_score
            FROM {schema}.vacancies v
            CROSS JOIN u
            LEFT JOIN {schema}.companies c ON c.id = v.company_id
            LEFT JOIN {schema}.cities ct ON ct.id = v.address_id
            LEFT JOIN {schema}.schedules sc ON sc.id = v.schedule_id
            LEFT JOIN {schema}.work_formats wf ON wf.id = v.work_format_id
            LEFT JOIN {schema}.specialties sp ON sp.id = u.specialty_id
            LEFT JOIN LATERAL (
                SELECT COUNT(*)::int AS matched_skills_count
                FROM {schema}.vacancy_skills vs
                JOIN {schema}.user_skills us
                  ON us.skill_id = vs.skill_id
                 AND us.user_id = u.id
                WHERE vs.vacancy_id = v.id
            ) ms ON TRUE
            ORDER BY match_score DESC, v.published_at DESC NULLS LAST
            LIMIT %s
            """,
            (user_id, limit),
        )
        rows = cur.fetchall()

    recs = []
    for r in rows:
        recs.append(
            {
                "vacancy_id": r[0],
                "vacancy_url": f"https://hh.ru/vacancy/{r[0]}",
                "title": r[1],
                "income_from": float(r[2]) if r[2] is not None else None,
                "income_to": float(r[3]) if r[3] is not None else None,
                "company_id": r[4],
                "company_name": r[5],
                "city_id": r[6],
                "city_name": r[7],
                "schedule_id": r[8],
                "schedule_name": r[9],
                "work_format_id": r[10],
                "work_format_name": r[11],
                "skills_id": r[12],
                "published_at": r[13],
                "matched_skills_count": int(r[14]) if r[14] is not None else 0,
                "match_score": int(r[15]) if r[15] is not None else 0,
            }
        )
    return recs


def top_skills_for_recommendations(conn, schema: str, vacancy_ids):
    if not vacancy_ids:
        return []

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT s.id, s.name, COUNT(*) AS freq
            FROM {schema}.vacancy_skills vs
            JOIN {schema}.skills s ON s.id = vs.skill_id
            WHERE vs.vacancy_id = ANY(%s)
            GROUP BY s.id, s.name
            ORDER BY freq DESC, s.name
            LIMIT 15
            """,
            (vacancy_ids,),
        )
        rows = cur.fetchall()

    return [{"id": r[0], "name": r[1], "count": int(r[2])} for r in rows]

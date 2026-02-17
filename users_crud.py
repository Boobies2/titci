from decimal import Decimal, InvalidOperation


def _to_int_or_none(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_decimal_or_none(value):
    if value is None or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return None


def _extract_skill_ids(data):
    raw_values = []
    if hasattr(data, "getlist"):
        raw_values = data.getlist("skills_ids")
        if not raw_values:
            single = data.get("skills_id")
            if single:
                raw_values = [single]
    else:
        multi = data.get("skills_ids")
        if isinstance(multi, list):
            raw_values = multi
        elif isinstance(multi, str):
            raw_values = [x.strip() for x in multi.split(",")]
        else:
            single = data.get("skills_id")
            if single:
                raw_values = [single]

    skill_ids = []
    for v in raw_values:
        iv = _to_int_or_none(v)
        if iv is not None:
            skill_ids.append(iv)
    return sorted(set(skill_ids))


def _replace_user_skills(conn, schema: str, user_id: int, skill_ids):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.user_skills WHERE user_id = %s", (user_id,))
        if skill_ids:
            rows = [(user_id, sid) for sid in skill_ids]
            args = ",".join(cur.mogrify("(%s,%s)", r).decode("utf-8") for r in rows)
            cur.execute(
                f"""
                INSERT INTO {schema}.user_skills (user_id, skill_id)
                VALUES {args}
                ON CONFLICT (user_id, skill_id) DO NOTHING
                """
            )


def list_users(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                u.id,
                u.first_name,
                u.last_name,
                u.work_experience,
                u.desired_income,
                u.city_id,
                c.name AS city_name,
                u.desired_schedule_id,
                s.name AS schedule_name,
                u.desired_work_format_id,
                wf.name AS work_format_name,
                u.desired_employment_id,
                et.name AS employment_name,
                u.desired_experience_id,
                ex.name AS experience_name,
                u.education_id,
                e.name AS education_name,
                u.specialty_id,
                sp.name AS specialty_name,
                u.skills_id,
                COALESCE(ARRAY_AGG(DISTINCT us.skill_id) FILTER (WHERE us.skill_id IS NOT NULL), '{{}}'::int[]) AS skill_ids,
                COALESCE(string_agg(DISTINCT sk.name, ', ') FILTER (WHERE sk.name IS NOT NULL), '') AS skill_names
            FROM {schema}.users u
            LEFT JOIN {schema}.cities c ON c.id = u.city_id
            LEFT JOIN {schema}.schedules s ON s.id = u.desired_schedule_id
            LEFT JOIN {schema}.work_formats wf ON wf.id = u.desired_work_format_id
            LEFT JOIN {schema}.employment_types et ON et.id = u.desired_employment_id
            LEFT JOIN {schema}.experience_levels ex ON ex.id = u.desired_experience_id
            LEFT JOIN {schema}.education e ON e.id = u.education_id
            LEFT JOIN {schema}.specialties sp ON sp.id = u.specialty_id
            LEFT JOIN {schema}.user_skills us ON us.user_id = u.id
            LEFT JOIN {schema}.skills sk ON sk.id = us.skill_id
            GROUP BY
                u.id, u.first_name, u.last_name, u.work_experience, u.desired_income,
                u.city_id, c.name, u.desired_schedule_id, s.name, u.desired_work_format_id, wf.name,
                u.desired_employment_id, et.name,
                u.desired_experience_id, ex.name,
                u.education_id, e.name, u.specialty_id, sp.name, u.skills_id
            ORDER BY u.id DESC
            """
        )
        rows = cur.fetchall()

    users = []
    for r in rows:
        users.append(
            {
                "id": r[0],
                "first_name": r[1],
                "last_name": r[2],
                "work_experience": r[3],
                "desired_income": float(r[4]) if r[4] is not None else None,
                "city_id": r[5],
                "city_name": r[6],
                "desired_schedule_id": r[7],
                "schedule_name": r[8],
                "desired_work_format_id": r[9],
                "work_format_name": r[10],
                "desired_employment_id": r[11],
                "employment_name": r[12],
                "desired_experience_id": r[13],
                "experience_name": r[14],
                "education_id": r[15],
                "education_name": r[16],
                "specialty_id": r[17],
                "specialty_name": r[18],
                "skills_id": r[19],
                "skill_ids": list(r[20] or []),
                "skill_names": r[21],
            }
        )
    return users


def get_lookups(conn, schema: str):
    lookups = {}
    with conn.cursor() as cur:
        for table, key in [
            ("cities", "cities"),
            ("schedules", "schedules"),
            ("work_formats", "work_formats"),
            ("employment_types", "employment_types"),
            ("experience_levels", "experience_levels"),
            ("education", "education"),
            ("specialties", "specialties"),
            ("skills", "skills"),
        ]:
            cur.execute(f"SELECT id, name FROM {schema}.{table} ORDER BY name")
            lookups[key] = [{"id": row[0], "name": row[1]} for row in cur.fetchall()]
    return lookups


def create_user(conn, schema: str, data: dict):
    skill_ids = _extract_skill_ids(data)
    primary_skill_id = skill_ids[0] if skill_ids else _to_int_or_none(data.get("skills_id"))
    payload = (
        data.get("first_name", "").strip() or None,
        data.get("last_name", "").strip() or None,
        _to_int_or_none(data.get("education_id")),
        _to_int_or_none(data.get("specialty_id")),
        primary_skill_id,
        data.get("desired_experience_id", "").strip() or None,
        data.get("desired_employment_id", "").strip() or None,
        data.get("work_experience", "").strip() or None,
        _to_decimal_or_none(data.get("desired_income")),
        data.get("desired_work_format_id", "").strip() or None,
        _to_int_or_none(data.get("city_id")),
        data.get("desired_schedule_id", "").strip() or None,
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.users
            (first_name, last_name, education_id, specialty_id, skills_id, desired_experience_id, desired_employment_id,
             work_experience, desired_income, desired_work_format_id, city_id, desired_schedule_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            payload,
        )
        user_id = cur.fetchone()[0]
    _replace_user_skills(conn, schema, user_id, skill_ids)
    conn.commit()
    return user_id


def update_user(conn, schema: str, user_id: int, data: dict):
    skill_ids = _extract_skill_ids(data)
    primary_skill_id = skill_ids[0] if skill_ids else _to_int_or_none(data.get("skills_id"))
    payload = (
        data.get("first_name", "").strip() or None,
        data.get("last_name", "").strip() or None,
        _to_int_or_none(data.get("education_id")),
        _to_int_or_none(data.get("specialty_id")),
        primary_skill_id,
        data.get("desired_experience_id", "").strip() or None,
        data.get("desired_employment_id", "").strip() or None,
        data.get("work_experience", "").strip() or None,
        _to_decimal_or_none(data.get("desired_income")),
        data.get("desired_work_format_id", "").strip() or None,
        _to_int_or_none(data.get("city_id")),
        data.get("desired_schedule_id", "").strip() or None,
        user_id,
    )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {schema}.users
            SET first_name = %s,
                last_name = %s,
                education_id = %s,
                specialty_id = %s,
                skills_id = %s,
                desired_experience_id = %s,
                desired_employment_id = %s,
                work_experience = %s,
                desired_income = %s,
                desired_work_format_id = %s,
                city_id = %s,
                desired_schedule_id = %s
            WHERE id = %s
            """,
            payload,
        )
    _replace_user_skills(conn, schema, user_id, skill_ids)
    conn.commit()


def delete_user(conn, schema: str, user_id: int):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.users WHERE id = %s", (user_id,))
    conn.commit()

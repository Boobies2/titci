import asyncio
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
import psycopg2
from psycopg2.extras import execute_values

# ==============================
# SETTINGS
# ==============================

CONFIG_PATH = Path("analizador_config.json")

DEFAULT_CONFIG = {
    "hh_api_base": "https://api.hh.ru",
    "request_timeout": 20,
    "retries": 3,
    "delay": 3,
    "async_concurrency": 20,
    "http_headers": {
        "User-Agent": "MSOD-Analizador/1.0 (+https://hh.ru)",
        "Accept": "application/json",
    },
    "areas_priority": [22, 113],
    "min_vlad_count": 30,
    "currency_rates": {
        "RUR": 1,
        "RUB": 1,
        "USD": 90,
        "EUR": 100,
        "KZT": 0.2
    },
    "skill_keywords": [
        "python", "sql", "pandas", "numpy", "excel", "power bi",
        "tableau", "1c", "spark", "hadoop", "docker",
        "kubernetes", "linux", "git", "airflow"
    ],
    "search_queries": ["Аналитик"],
    "per_page": 50,
    "pages_to_parse": 2,
    "pg": {
        "host": "89.223.65.229",
        "port": 5432,
        "database": "msod_database",
        "schema": "msod19",
        "user": "airflow_orchestrator",
        "password": "D4g_Run_0p3rat0r!"
    }
}


def load_config(path: Path = CONFIG_PATH):
    if not path.exists():
        path.write_text(json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2), encoding="utf-8")
        return DEFAULT_CONFIG.copy()
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


CONFIG = load_config()
PG_CONFIG = CONFIG.get("pg", {})
PROGRESS_PATH = Path(CONFIG.get("progress_file", "analizador_progress.json"))

HH_API_BASE = CONFIG.get("hh_api_base", DEFAULT_CONFIG["hh_api_base"])
REQUEST_TIMEOUT = int(CONFIG.get("request_timeout", DEFAULT_CONFIG["request_timeout"]))
RETRIES = int(CONFIG.get("retries", DEFAULT_CONFIG["retries"]))
DELAY = int(CONFIG.get("delay", DEFAULT_CONFIG["delay"]))
ASYNC_CONCURRENCY = int(CONFIG.get("async_concurrency", DEFAULT_CONFIG["async_concurrency"]))
AREAS_PRIORITY = CONFIG.get("areas_priority", DEFAULT_CONFIG["areas_priority"])
MIN_VLAD_COUNT = int(CONFIG.get("min_vlad_count", DEFAULT_CONFIG["min_vlad_count"]))
CURRENCY_RATES = CONFIG.get("currency_rates", DEFAULT_CONFIG["currency_rates"])
SKILL_KEYWORDS = CONFIG.get("skill_keywords", DEFAULT_CONFIG["skill_keywords"])
SEARCH_QUERIES = CONFIG.get("search_queries", DEFAULT_CONFIG["search_queries"])
PER_PAGE = int(CONFIG.get("per_page", DEFAULT_CONFIG["per_page"]))
PAGES_TO_PARSE = int(CONFIG.get("pages_to_parse", DEFAULT_CONFIG["pages_to_parse"]))
HTTP_HEADERS = CONFIG.get("http_headers", DEFAULT_CONFIG["http_headers"])

PG_HOST = PG_CONFIG.get("host", DEFAULT_CONFIG["pg"]["host"])
PG_PORT = int(PG_CONFIG.get("port", DEFAULT_CONFIG["pg"]["port"]))
PG_DATABASE = PG_CONFIG.get("database", DEFAULT_CONFIG["pg"]["database"])
PG_SCHEMA = PG_CONFIG.get("schema", DEFAULT_CONFIG["pg"]["schema"])
PG_USER = PG_CONFIG.get("user", DEFAULT_CONFIG["pg"]["user"])
PG_PASSWORD = PG_CONFIG.get("password", DEFAULT_CONFIG["pg"]["password"])


def write_progress(status: str, stage: str, current: int, total: int, message: str):
    total = max(1, int(total))
    current = max(0, min(int(current), total))
    percent = int((current / total) * 100)
    payload = {
        "status": status,
        "stage": stage,
        "current": current,
        "total": total,
        "percent": percent,
        "message": message,
        "updated_at": dt.datetime.utcnow().isoformat() + "Z",
    }
    PROGRESS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

# ==============================
# LOGGING
# ==============================

logging.basicConfig(
    filename="parser.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ==============================
# HELPERS
# ==============================

def convert_to_rub(amount, currency):
    if amount is None:
        return None
    rate = CURRENCY_RATES.get(currency, 1)
    return amount * rate


def parse_hh_datetime(date_str: Optional[str]):
    if not date_str:
        return None
    return dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def detect_skills(description: Optional[str]) -> Optional[str]:
    if not description:
        return None
    description_lower = description.lower()
    found = [skill for skill in SKILL_KEYWORDS if skill in description_lower]
    return ", ".join(found) if found else None


def detect_education_levels(description: Optional[str]) -> List[str]:
    if not description:
        return []
    text = description.lower()
    levels = []

    checks = [
        ("\u0412\u044b\u0441\u0448\u0435\u0435", ["\u0432\u044b\u0441\u0448", "higher education", "university degree"]),
        ("\u0421\u0440\u0435\u0434\u043d\u0435\u0435", ["\u0441\u0440\u0435\u0434\u043d", "secondary education"]),
        ("\u0421\u0440\u0435\u0434\u043d\u0435\u0435 \u0441\u043f\u0435\u0446\u0438\u0430\u043b\u044c\u043d\u043e\u0435", ["\u0441\u0440\u0435\u0434\u043d\u0435\u0435 \u0441\u043f\u0435\u0446\u0438\u0430\u043b\u044c\u043d\u043e\u0435", "special secondary"]),
        ("\u0411\u0430\u043a\u0430\u043b\u0430\u0432\u0440", ["\u0431\u0430\u043a\u0430\u043b\u0430\u0432\u0440", "bachelor"]),
        ("\u041c\u0430\u0433\u0438\u0441\u0442\u0440", ["\u043c\u0430\u0433\u0438\u0441\u0442\u0440", "master degree", "masters degree"]),
        ("\u041d\u0435\u043e\u043a\u043e\u043d\u0447\u0435\u043d\u043d\u043e\u0435 \u0432\u044b\u0441\u0448\u0435\u0435", ["\u043d\u0435\u043e\u043a\u043e\u043d\u0447\u0435\u043d", "incomplete higher"]),
    ]
    for level_name, patterns in checks:
        if any(p in text for p in patterns):
            levels.append(level_name)

    return list(dict.fromkeys(levels))
def extract_education_from_hh(vacancy: dict) -> List[str]:
    values: List[str] = []
    if not vacancy:
        return values

    mapping = {
        "not_required": "\u041d\u0435 \u0442\u0440\u0435\u0431\u0443\u0435\u0442\u0441\u044f",
        "secondary": "\u0421\u0440\u0435\u0434\u043d\u0435\u0435",
        "special_secondary": "\u0421\u0440\u0435\u0434\u043d\u0435\u0435 \u0441\u043f\u0435\u0446\u0438\u0430\u043b\u044c\u043d\u043e\u0435",
        "unfinished_higher": "\u041d\u0435\u043e\u043a\u043e\u043d\u0447\u0435\u043d\u043d\u043e\u0435 \u0432\u044b\u0441\u0448\u0435\u0435",
        "higher": "\u0412\u044b\u0441\u0448\u0435\u0435",
        "bachelor": "\u0411\u0430\u043a\u0430\u043b\u0430\u0432\u0440",
        "master": "\u041c\u0430\u0433\u0438\u0441\u0442\u0440",
    }

    education = vacancy.get("education")
    if isinstance(education, dict):
        edu_id = education.get("id")
        edu_name = education.get("name")
        if edu_name:
            values.append(edu_name.strip())
        elif edu_id and edu_id in mapping:
            values.append(mapping[edu_id])
    elif isinstance(education, list):
        for edu in education:
            if isinstance(edu, dict):
                edu_id = edu.get("id")
                edu_name = edu.get("name")
                if edu_name:
                    values.append(edu_name.strip())
                elif edu_id and edu_id in mapping:
                    values.append(mapping[edu_id])

    education_level = vacancy.get("education_level")
    if isinstance(education_level, dict):
        lvl_id = education_level.get("id")
        lvl_name = education_level.get("name")
        if lvl_name:
            values.append(lvl_name.strip())
        elif lvl_id and lvl_id in mapping:
            values.append(mapping[lvl_id])

    return [x for x in dict.fromkeys(values) if x]
def split_skill_string(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [s.strip() for s in value.split(",") if s and s.strip()]


def extract_professional_roles(vacancy_obj: dict, item_obj: dict):
    role_pairs = []

    def add_from_obj(obj):
        if not isinstance(obj, dict):
            return
        roles = obj.get("professional_roles")
        if isinstance(roles, list):
            for r in roles:
                if not isinstance(r, dict):
                    continue
                rid = r.get("id")
                rname = r.get("name")
                if rid or rname:
                    role_pairs.append((str(rid).strip() if rid is not None else None, (rname or "").strip() or None))

        # Fallback for flattened/string variants
        role_ids_raw = obj.get("professional_roles_id")
        role_names_raw = obj.get("professional_roles_name")
        if isinstance(role_ids_raw, str) and isinstance(role_names_raw, str):
            ids = [x.strip() for x in role_ids_raw.split(",")]
            names = [x.strip() for x in role_names_raw.split(",")]
            for i, n in zip(ids, names):
                role_pairs.append((i or None, n or None))

    add_from_obj(vacancy_obj)
    add_from_obj(item_obj)

    # Keep order, remove duplicates.
    unique = []
    seen = set()
    for rid, rname in role_pairs:
        key = (rid, rname)
        if key in seen:
            continue
        seen.add(key)
        unique.append(key)

    role_ids = [rid for rid, _ in unique if rid]
    role_names = [rname for _, rname in unique if rname]
    return role_ids, role_names


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def to_int_or_none(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ==============================
# RETRY REQUEST
# ==============================

async def retry_request(session, url, params=None):
    for attempt in range(RETRIES):
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logging.error("Request error %s: %s", url, e)
            if attempt < RETRIES - 1:
                await asyncio.sleep(DELAY)
    return None


async def fetch_employer_industries(session, employer_id: Optional[int], cache: Dict[int, List[str]]) -> List[str]:
    if not employer_id:
        return []
    if employer_id in cache:
        return cache[employer_id]

    employer_url = f"{HH_API_BASE}/employers/{employer_id}"
    data = await retry_request(session, employer_url)
    if not data:
        cache[employer_id] = []
        return []

    industries = [x.get("name") for x in (data.get("industries") or []) if x.get("name")]
    cache[employer_id] = industries
    return industries


# ==============================
# FETCH FULL VACANCY
# ==============================

async def fetch_vacancy(session, item, search_query, semaphore, employer_cache):
    async with semaphore:
        vacancy_id = item["id"]
        vacancy_url = f"{HH_API_BASE}/vacancies/{vacancy_id}"

        vacancy = await retry_request(session, vacancy_url)
        if not vacancy:
            # Fallback to list payload when detailed vacancy endpoint returns 403/other errors.
            vacancy = item

        salary = vacancy.get("salary") or {}
        employer = vacancy.get("employer") or {}
        area = vacancy.get("area") or {}
        schedule = vacancy.get("schedule") or {}
        experience = vacancy.get("experience") or {}
        employment = vacancy.get("employment") or {}
        employment_form = vacancy.get("employment_form") or {}
        key_skills = vacancy.get("key_skills") or []
        key_skills_names = [s.get("name") for s in key_skills if s.get("name")]

        # Keep role parsing behavior close to test2.py:
        # take professional_roles from detailed vacancy, fallback to list item.
        professional_roles = vacancy.get("professional_roles") or item.get("professional_roles") or []
        role_ids = [str(r.get("id")).strip() for r in professional_roles if isinstance(r, dict) and r.get("id") is not None]
        role_names = [str(r.get("name")).strip() for r in professional_roles if isinstance(r, dict) and r.get("name")]
        roles_ids_csv = ", ".join(role_ids)
        roles_names_csv = ", ".join(role_names)

        employer_industries = employer.get("industries") or []
        industry_names = [i.get("name") for i in employer_industries if i.get("name")]
        if not industry_names:
            industry_names = await fetch_employer_industries(session, to_int_or_none(employer.get("id")), employer_cache)

        education_source = " ".join(
            [
                vacancy.get("description") or "",
                ((item.get("snippet") or {}).get("requirement") or ""),
                ((item.get("snippet") or {}).get("responsibility") or ""),
            ]
        )
        hh_education = extract_education_from_hh(vacancy)
        text_education = detect_education_levels(education_source)
        detected_education = list(dict.fromkeys(hh_education + text_education))

        salary_from = salary.get("from")
        salary_to = salary.get("to")
        currency = salary.get("currency")

        is_archived = bool(vacancy.get("archived"))
        status_name = "archived" if is_archived else "active"

        return {
            "id": to_int_or_none(vacancy.get("id")),
            "name": vacancy.get("name"),

            "salary_from": salary_from,
            "salary_to": salary_to,
            "salary_currency": currency,
            "salary_from_rub": convert_to_rub(salary_from, currency),
            "salary_to_rub": convert_to_rub(salary_to, currency),

            "published_at": parse_hh_datetime(vacancy.get("published_at")),
            "created_at": parse_hh_datetime(vacancy.get("created_at")),

            "status_name": status_name,

            "url": vacancy.get("url"),
            "alternate_url": vacancy.get("alternate_url"),

            "area_id": to_int_or_none(area.get("id")),
            "area_name": area.get("name"),

            "employer_id": to_int_or_none(employer.get("id")),
            "employer_name": employer.get("name"),

            "schedule_id": schedule.get("id"),
            "schedule_name": schedule.get("name"),

            "experience_id": experience.get("id"),
            "experience_name": experience.get("name"),

            "employment_id": employment.get("id"),
            "employment_name": employment.get("name"),

            "work_format_id": employment_form.get("id"),
            "work_format_name": employment_form.get("name"),

            "key_skills_list": key_skills_names,
            "auto_detected_skills": detect_skills(vacancy.get("description")),
            "role_ids": role_ids,
            "role_names": role_names,
            "professional_roles_id": roles_ids_csv,
            "professional_roles_name": roles_names_csv,
            "industry_names": industry_names,
            "education_names": detected_education,

            "search_query": search_query,
        }


# ==============================
# MAIN PARSING
# ==============================

async def query(search_queries, per_page=50, pages_to_parse=2):
    frames = []
    semaphore = asyncio.Semaphore(ASYNC_CONCURRENCY)
    employer_cache: Dict[int, List[str]] = {}
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    total_steps = max(1, len(search_queries) * len(AREAS_PRIORITY) * pages_to_parse)
    done_steps = 0

    write_progress("running", "parsing", done_steps, total_steps, "\u041f\u0430\u0440\u0441\u0435\u0440 \u0437\u0430\u043f\u0443\u0449\u0435\u043d.")

    async with aiohttp.ClientSession(timeout=timeout, trust_env=False, headers=HTTP_HEADERS) as session:
        for search_query in search_queries:
            total_vlad = 0

            for area in AREAS_PRIORITY:
                print(f"Region {area}, query: {search_query}")

                for page in range(pages_to_parse):
                    done_steps += 1
                    write_progress(
                        "running",
                        "parsing",
                        done_steps,
                        total_steps,
                        f"\u0417\u0430\u043f\u0440\u043e\u0441: {search_query}, \u0440\u0435\u0433\u0438\u043e\u043d: {area}, \u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0430: {page + 1}/{pages_to_parse}",
                    )
                    url = f"{HH_API_BASE}/vacancies"
                    params = {
                        "page": page,
                        "per_page": per_page,
                        "text": search_query,
                        "area": area,
                    }

                    data_json = await retry_request(session, url, params=params)

                    if not data_json or not data_json.get("items"):
                        continue

                    tasks = [
                        fetch_vacancy(session, item, search_query, semaphore, employer_cache)
                        for item in data_json["items"]
                    ]

                    results = await asyncio.gather(*tasks)
                    results = [r for r in results if r and r.get("id")]

                    frames.extend(results)

                    if area == 22:
                        total_vlad += len(results)

                if area == 22 and total_vlad >= MIN_VLAD_COUNT:
                    break

    write_progress(
        "running",
        "parsing",
        total_steps,
        total_steps,
        f"\u041f\u0430\u0440\u0441\u0438\u043d\u0433 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d. \u0421\u043e\u0431\u0440\u0430\u043d\u043e \u0432\u0430\u043a\u0430\u043d\u0441\u0438\u0439: {len(frames)}",
    )
    return frames


# ==============================
# DATABASE DDL + UPSERTS
# ==============================

def validate_required_tables(conn):
    required = {
        "statuses",
        "skills",
        "vacancy_skills",
        "cities",
        "employment_types",
        "experience_levels",
        "schedules",
        "work_formats",
        "specialties",
        "education",
        "company_industries",
        "companies",
        "users",
        "vacancies",
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            """,
            (PG_SCHEMA,),
        )
        existing = {row[0] for row in cur.fetchall()}

    missing = sorted(required - existing)
    if missing:
        raise RuntimeError(
            f"Missing tables in schema '{PG_SCHEMA}': {', '.join(missing)}. "
            "Create them with create_tables.py first."
        )


def upsert_statuses(conn, names: List[str]) -> Dict[str, int]:
    mapping = {}
    if not names:
        return mapping

    unique_names = sorted(set(n for n in names if n))
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {PG_SCHEMA}.statuses (name) VALUES %s ON CONFLICT (name) DO NOTHING",
            [(n,) for n in unique_names],
        )
        cur.execute(f"SELECT id, name FROM {PG_SCHEMA}.statuses WHERE name = ANY(%s)", (unique_names,))
        for sid, name in cur.fetchall():
            mapping[name] = sid
    conn.commit()
    return mapping


def upsert_named_id_table(conn, table: str, rows: List[tuple]):
    if not rows:
        return
    by_id = {}
    for rid, name in rows:
        if rid and name:
            by_id[rid] = name
    if not by_id:
        return
    unique_rows = [(rid, by_id[rid]) for rid in by_id]
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {PG_SCHEMA}.{table} (id, name)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """,
            unique_rows,
        )
    conn.commit()


def upsert_cities(conn, rows: List[tuple]):
    if not rows:
        return
    unique_rows = list({(r[0], r[1]) for r in rows if r[0] is not None and r[1]})
    if not unique_rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {PG_SCHEMA}.cities (id, name)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """,
            unique_rows,
        )
    conn.commit()


def upsert_companies(conn, rows: List[tuple]):
    if not rows:
        return
    by_id = {}
    for cid, name, industry_id in rows:
        if cid is None or not name:
            continue
        if cid not in by_id:
            by_id[cid] = (name, industry_id)
            continue
        prev_name, prev_industry = by_id[cid]
        # Keep known industry_id if any (prefer non-null).
        chosen_industry = prev_industry if prev_industry is not None else industry_id
        by_id[cid] = (name or prev_name, chosen_industry)
    if not by_id:
        return
    unique_rows = [(cid, vals[0], vals[1]) for cid, vals in by_id.items()]
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {PG_SCHEMA}.companies (id, name, industry_id)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                industry_id = COALESCE(EXCLUDED.industry_id, {PG_SCHEMA}.companies.industry_id)
            """,
            unique_rows,
        )
    conn.commit()


def upsert_skills(conn, skill_names: List[str]) -> Dict[str, int]:
    mapping = {}
    unique_names = sorted(set(s for s in skill_names if s))
    if not unique_names:
        return mapping

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {PG_SCHEMA}.skills (name) VALUES %s ON CONFLICT (name) DO NOTHING",
            [(s,) for s in unique_names],
        )
        cur.execute(f"SELECT id, name FROM {PG_SCHEMA}.skills WHERE name = ANY(%s)", (unique_names,))
        for sid, name in cur.fetchall():
            mapping[name] = sid
    conn.commit()
    return mapping


def upsert_lookup_by_name(conn, table: str, names: List[str]) -> Dict[str, int]:
    mapping = {}
    unique_names = sorted(set(n for n in names if n))
    if not unique_names:
        return mapping
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {PG_SCHEMA}.{table} (name) VALUES %s ON CONFLICT (name) DO NOTHING",
            [(n,) for n in unique_names],
        )
        cur.execute(f"SELECT id, name FROM {PG_SCHEMA}.{table} WHERE name = ANY(%s)", (unique_names,))
        for rid, name in cur.fetchall():
            mapping[name] = rid
    conn.commit()
    return mapping


def upsert_vacancies(conn, rows: List[tuple]):
    if not rows:
        return
    # Deduplicate by vacancy id to avoid "ON CONFLICT ... cannot affect row a second time".
    by_id = {}
    for row in rows:
        if not row:
            continue
        vacancy_id = row[0]
        if vacancy_id is None:
            continue
        by_id[vacancy_id] = row
    rows = list(by_id.values())
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {PG_SCHEMA}.vacancies (
                id, title, income_from, income_to, address_id,
                employment_id, experience_id, schedule_id, work_format_id,
                education_id, specialty_id, skills_id, company_id, status_id, published_at
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                income_from = EXCLUDED.income_from,
                income_to = EXCLUDED.income_to,
                address_id = EXCLUDED.address_id,
                employment_id = EXCLUDED.employment_id,
                experience_id = EXCLUDED.experience_id,
                schedule_id = EXCLUDED.schedule_id,
                work_format_id = EXCLUDED.work_format_id,
                education_id = EXCLUDED.education_id,
                specialty_id = EXCLUDED.specialty_id,
                skills_id = EXCLUDED.skills_id,
                company_id = EXCLUDED.company_id,
                status_id = EXCLUDED.status_id,
                published_at = EXCLUDED.published_at
            """,
            rows,
        )
    conn.commit()


def upsert_vacancy_skills(conn, rows: List[tuple]):
    if not rows:
        return
    unique_rows = sorted(set((r[0], r[1]) for r in rows if r and r[0] is not None and r[1] is not None))
    if not unique_rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {PG_SCHEMA}.vacancy_skills (vacancy_id, skill_id)
            VALUES %s
            ON CONFLICT (vacancy_id, skill_id) DO NOTHING
            """,
            unique_rows,
        )
    conn.commit()


def save_to_postgres(frames: List[dict]):
    if not frames:
        print("No data to save in PostgreSQL")
        return

    conn = get_pg_connection()
    try:
        validate_required_tables(conn)

        upsert_cities(conn, [(r.get("area_id"), r.get("area_name")) for r in frames])
        upsert_named_id_table(conn, "employment_types", [(r.get("employment_id"), r.get("employment_name")) for r in frames])
        upsert_named_id_table(conn, "experience_levels", [(r.get("experience_id"), r.get("experience_name")) for r in frames])
        upsert_named_id_table(conn, "schedules", [(r.get("schedule_id"), r.get("schedule_name")) for r in frames])
        upsert_named_id_table(conn, "work_formats", [(r.get("work_format_id"), r.get("work_format_name")) for r in frames])
        industry_map = upsert_lookup_by_name(
            conn,
            "company_industries",
            [name for r in frames for name in (r.get("industry_names") or [])],
        )
        specialty_map = upsert_lookup_by_name(
            conn,
            "specialties",
            [name for r in frames for name in (r.get("role_names") or [])] + [r.get("search_query") for r in frames if r.get("search_query")],
        )
        education_map = upsert_lookup_by_name(
            conn,
            "education",
            [name for r in frames for name in (r.get("education_names") or [])] + ["Не указано"],
        )
        upsert_companies(
            conn,
            [
                (
                    r.get("employer_id"),
                    r.get("employer_name"),
                    industry_map.get((r.get("industry_names") or [None])[0]),
                )
                for r in frames
            ],
        )

        status_map = upsert_statuses(conn, [r.get("status_name") for r in frames])

        all_skills = []
        for r in frames:
            all_skills.extend(r.get("key_skills_list") or [])
            all_skills.extend(split_skill_string(r.get("auto_detected_skills")))
        skill_map = upsert_skills(conn, all_skills)

        vacancy_rows = []
        vacancy_skill_rows = []
        for r in frames:
            combined_skills = []
            combined_skills.extend(r.get("key_skills_list") or [])
            combined_skills.extend(split_skill_string(r.get("auto_detected_skills")))
            combined_skills = list(dict.fromkeys(combined_skills))

            skill_ids = [skill_map[s] for s in combined_skills if s in skill_map]
            primary_skill_id = skill_ids[0] if skill_ids else None
            primary_education_name = (r.get("education_names") or [None])[0]
            primary_specialty_name = (r.get("role_names") or [r.get("search_query") or None])[0]
            vacancy_id = r.get("id")

            if vacancy_id is not None:
                for sid in skill_ids:
                    vacancy_skill_rows.append((vacancy_id, sid))

            vacancy_rows.append(
                (
                    vacancy_id,
                    r.get("name"),
                    r.get("salary_from"),
                    r.get("salary_to"),
                    r.get("area_id"),
                    r.get("employment_id"),
                    r.get("experience_id"),
                    r.get("schedule_id"),
                    r.get("work_format_id"),
                    education_map.get(primary_education_name) if primary_education_name else None,
                    specialty_map.get(primary_specialty_name) if primary_specialty_name else None,
                    primary_skill_id,
                    r.get("employer_id"),
                    status_map.get(r.get("status_name")),
                    r.get("published_at"),
                )
            )

        upsert_vacancies(conn, vacancy_rows)
        upsert_vacancy_skills(conn, vacancy_skill_rows)
        print(f"Saved to PostgreSQL: {len(vacancy_rows)} vacancies")
    finally:
        conn.close()


# ==============================
# RUN
# ==============================

if __name__ == "__main__":
    try:
        write_progress("running", "startup", 0, 1, "\u041f\u043e\u0434\u0433\u043e\u0442\u043e\u0432\u043a\u0430 \u043a \u0437\u0430\u043f\u0443\u0441\u043a\u0443 \u043f\u0430\u0440\u0441\u0435\u0440\u0430.")
        results = asyncio.run(query(SEARCH_QUERIES, per_page=PER_PAGE, pages_to_parse=PAGES_TO_PARSE))
        write_progress("running", "db_save", 0, 1, f"\u0421\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u0438\u0435 \u0432 \u0411\u0414: {len(results)} \u0432\u0430\u043a\u0430\u043d\u0441\u0438\u0439.")
        save_to_postgres(results)
        write_progress("done", "done", 1, 1, f"\u0413\u043e\u0442\u043e\u0432\u043e. \u0421\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u043e \u0432 \u0411\u0414: {len(results)} \u0432\u0430\u043a\u0430\u043d\u0441\u0438\u0439.")
        print("Done.")
    except Exception as e:
        write_progress("error", "error", 0, 1, f"\u041e\u0448\u0438\u0431\u043a\u0430: {e}")
        raise


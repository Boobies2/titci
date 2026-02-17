import atexit
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql
from flask import Flask, flash, jsonify, redirect, render_template, request, url_for
from user_recommendations import get_user_basic, recommend_for_user, top_skills_for_recommendations
from users_crud import create_user, delete_user, get_lookups, list_users, update_user


BASE_DIR = Path(__file__).resolve().parent
ANALYZADOR_PATH = BASE_DIR / "analizador.py"
LOG_PATH = BASE_DIR / "parser.log"
ANALIZADOR_CONFIG_PATH = BASE_DIR / "analizador_config.json"
ANALIZADOR_PROGRESS_PATH = BASE_DIR / "analizador_progress.json"
OUTPUT_PATTERN = "vacancies_full_*.xlsx"
TABLE_PREVIEW_LIMIT = 200
PG_HOST = "89.223.65.229"
PG_PORT = 5432
PG_DATABASE = "msod_database"
PG_SCHEMA = "msod19"
PG_USER = "airflow_orchestrator"
PG_PASSWORD = "D4g_Run_0p3rat0r!"

app = Flask(__name__)
app.config["SECRET_KEY"] = "analizador-local-secret"

_process = None
UNKNOWN_ROLE = "\u0414\u0440\u0443\u0433\u043e\u0435"


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def role_case_sql():
    return """
        CASE
            WHEN lower(v.title) LIKE '%%\u043f\u0440\u043e\u0433\u0440\u0430\u043c\u043c\u0438\u0441\u0442 1\u0441%%'
              OR lower(v.title) LIKE '%%\u0440\u0430\u0437\u0440\u0430\u0431\u043e\u0442\u0447\u0438\u043a 1\u0441%%'
              OR lower(v.title) LIKE '%%1\u0441%%'
              OR lower(v.title) LIKE '%%1c%%'
            THEN '\u043f\u0440\u043e\u0433\u0440\u0430\u043c\u043c\u0438\u0441\u0442 1\u0441'
            WHEN lower(v.title) LIKE '%%data engineer%%'
              OR lower(v.title) LIKE '%%\u0438\u043d\u0436\u0435\u043d\u0435\u0440 \u0434\u0430\u043d\u043d\u044b\u0445%%'
              OR lower(v.title) LIKE '%%data eng%%'
            THEN 'Data engineer'
            WHEN lower(v.title) LIKE '%%\u0432\u0435\u0431 \u0440\u0430\u0437\u0440\u0430\u0431\u043e\u0442\u043a\u0430%%'
              OR lower(v.title) LIKE '%%\u0432\u0435\u0431-\u0440\u0430\u0437\u0440\u0430\u0431\u043e\u0442\u043a\u0430%%'
              OR lower(v.title) LIKE '%%web developer%%'
              OR lower(v.title) LIKE '%%frontend%%'
              OR lower(v.title) LIKE '%%backend%%'
              OR lower(v.title) LIKE '%%fullstack%%'
              OR lower(v.title) LIKE '%%full stack%%'
            THEN '\u0432\u0435\u0431 \u0440\u0430\u0437\u0440\u0430\u0431\u043e\u0442\u043a\u0430'
            WHEN lower(v.title) LIKE '%%java%%'
              OR lower(v.title) LIKE '%%\u0434\u0436\u0430\u0432\u0430%%'
              OR lower(v.title) LIKE '%%spring%%'
            THEN 'java'
            WHEN lower(v.title) LIKE '%%gamedev%%'
              OR lower(v.title) LIKE '%%game dev%%'
              OR lower(v.title) LIKE '%%\u0433\u0435\u0439\u043c\u0434\u0435\u0432%%'
              OR lower(v.title) LIKE '%%unity%%'
              OR lower(v.title) LIKE '%%unreal%%'
              OR lower(v.title) LIKE '%%\u0440\u0430\u0437\u0440\u0430\u0431\u043e\u0442\u043a\u0430 \u0438\u0433\u0440%%'
            THEN 'gamedev'
            WHEN lower(v.title) LIKE '%%\u0434\u0438\u0437\u0430\u0439\u043d\u0435\u0440%%'
              OR lower(v.title) LIKE '%%designer%%'
              OR lower(v.title) LIKE '%%ux/ui%%'
              OR lower(v.title) LIKE '%%ui/ux%%'
            THEN '\u0434\u0438\u0437\u0430\u0439\u043d\u0435\u0440'
            WHEN lower(v.title) LIKE '%%\u043c\u0430\u0440\u043a\u0435\u0442\u043e\u043b\u043e\u0433%%'
              OR lower(v.title) LIKE '%%marketing%%'
              OR lower(v.title) LIKE '%%marketer%%'
              OR lower(v.title) LIKE '%%smm%%'
              OR lower(v.title) LIKE '%%seo%%'
            THEN '\u043c\u0430\u0440\u043a\u0435\u0442\u043e\u043b\u043e\u0433'
            WHEN lower(v.title) LIKE '%%\u0444\u043e\u0442\u043e\u0433\u0440\u0430\u0444%%'
              OR lower(v.title) LIKE '%%photographer%%'
              OR lower(v.title) LIKE '%%retoucher%%'
            THEN '\u0444\u043e\u0442\u043e\u0433\u0440\u0430\u0444'
            ELSE '\u0414\u0440\u0443\u0433\u043e\u0435'
        END
    """


def load_chart_data(filters=None):
    filters = filters or {}
    role_expr = role_case_sql()

    vacancies_by_role = []
    salary_by_role = []
    companies_by_role = []
    skills_by_role = []
    where_parts = []
    params = []

    if filters.get("city_id"):
        where_parts.append("v.address_id = %s")
        params.append(filters["city_id"])
    if filters.get("employment_id"):
        where_parts.append("v.employment_id = %s")
        params.append(filters["employment_id"])
    if filters.get("work_format_id"):
        where_parts.append("v.work_format_id = %s")
        params.append(filters["work_format_id"])
    if filters.get("experience_id"):
        where_parts.append("v.experience_id = %s")
        params.append(filters["experience_id"])
    if filters.get("salary_min") is not None:
        where_parts.append("COALESCE(v.income_to, v.income_from, 0) >= %s")
        params.append(filters["salary_min"])
    if filters.get("salary_max") is not None:
        where_parts.append("COALESCE(v.income_from, v.income_to, 0) <= %s")
        params.append(filters["salary_max"])
    if filters.get("skill_id"):
        where_parts.append(
            f"""EXISTS (
                    SELECT 1
                    FROM {PG_SCHEMA}.vacancy_skills vsf
                    WHERE vsf.vacancy_id = v.id
                      AND vsf.skill_id = %s
                )"""
        )
        params.append(filters["skill_id"])
    if filters.get("date_from"):
        where_parts.append("v.published_at::date >= %s")
        params.append(filters["date_from"])
    if filters.get("date_to"):
        where_parts.append("v.published_at::date <= %s")
        params.append(filters["date_to"])

    def where_for(specialty_filter):
        parts = list(where_parts)
        local_params = list(params)
        if specialty_filter:
            if isinstance(specialty_filter, (list, tuple)):
                values = [int(x) for x in specialty_filter if x is not None]
                if values:
                    parts.append("v.specialty_id = ANY(%s)")
                    local_params.append(values)
            else:
                parts.append("v.specialty_id = %s")
                local_params.append(specialty_filter)
        return (f"WHERE {' AND '.join(parts)}" if parts else ""), local_params

    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            vacancies_where_sql, vacancies_params = where_for(filters.get("specialty_vacancies_ids"))
            cur.execute(
                f"""
                SELECT role_name, COUNT(*) AS vacancies_count
                FROM (
                    SELECT {role_expr} AS role_name
                    FROM {PG_SCHEMA}.vacancies v
                    {vacancies_where_sql}
                ) q
                WHERE role_name <> %s
                GROUP BY role_name
                ORDER BY vacancies_count DESC, role_name;
                """,
                tuple(vacancies_params + [UNKNOWN_ROLE]),
            )
            vacancies_by_role = [{"role": r[0], "count": int(r[1])} for r in cur.fetchall()]

            salary_where_sql, salary_params = where_for(filters.get("specialty_salary_id"))
            cur.execute(
                f"""
                SELECT role_name,
                       ROUND(AVG(v.income_from)::numeric, 2) AS avg_income_from,
                       ROUND(AVG(v.income_to)::numeric, 2) AS avg_income_to
                FROM (
                    SELECT {role_expr} AS role_name, v.income_from, v.income_to
                    FROM {PG_SCHEMA}.vacancies v
                    {salary_where_sql}
                ) v
                WHERE role_name <> %s
                GROUP BY role_name
                ORDER BY role_name;
                """,
                tuple(salary_params + [UNKNOWN_ROLE]),
            )
            salary_by_role = [
                {
                    "role": r[0],
                    "avg_income_from": float(r[1]) if r[1] is not None else None,
                    "avg_income_to": float(r[2]) if r[2] is not None else None,
                }
                for r in cur.fetchall()
            ]

            companies_where_sql, companies_params = where_for(filters.get("specialty_companies_id"))
            cur.execute(
                f"""
                SELECT role_name, COUNT(DISTINCT company_id) AS companies_count
                FROM (
                    SELECT {role_expr} AS role_name, v.company_id
                    FROM {PG_SCHEMA}.vacancies v
                    {companies_where_sql}
                ) q
                WHERE role_name <> %s
                GROUP BY role_name
                ORDER BY companies_count DESC, role_name;
                """,
                tuple(companies_params + [UNKNOWN_ROLE]),
            )
            companies_by_role = [{"role": r[0], "count": int(r[1])} for r in cur.fetchall()]

            skills_where_sql, skills_params = where_for(filters.get("specialty_skills_id"))
            cur.execute(
                f"""
                WITH expanded AS (
                    SELECT {role_expr} AS role_name,
                           vs.skill_id
                    FROM {PG_SCHEMA}.vacancies v
                    JOIN {PG_SCHEMA}.vacancy_skills vs
                      ON vs.vacancy_id = v.id
                    {skills_where_sql}
                ),
                ranked AS (
                    SELECT e.role_name,
                           s.name AS skill_name,
                           COUNT(*) AS skill_count,
                           ROW_NUMBER() OVER (PARTITION BY e.role_name ORDER BY COUNT(*) DESC, s.name) AS rn
                    FROM expanded e
                    JOIN {PG_SCHEMA}.skills s ON s.id = e.skill_id
                    GROUP BY e.role_name, s.name
                )
                SELECT role_name, skill_name, skill_count
                FROM ranked
                WHERE rn <= 10
                  AND role_name <> %s
                ORDER BY role_name, skill_count DESC, skill_name;
                """,
                tuple(skills_params + [UNKNOWN_ROLE]),
            )
            skills_by_role = [{"role": r[0], "skill": r[1], "count": int(r[2])} for r in cur.fetchall()]

    return {
        "vacancies_by_role": vacancies_by_role,
        "salary_by_role": salary_by_role,
        "companies_by_role": companies_by_role,
        "skills_by_role": skills_by_role,
    }


def load_chart_filter_options():
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, name FROM {PG_SCHEMA}.cities ORDER BY name")
            cities = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
            cur.execute(f"SELECT id, name FROM {PG_SCHEMA}.employment_types ORDER BY name")
            employment_types = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
            cur.execute(f"SELECT id, name FROM {PG_SCHEMA}.specialties ORDER BY name")
            specialties = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
            cur.execute(f"SELECT id, name FROM {PG_SCHEMA}.skills ORDER BY name")
            skills = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
            cur.execute(f"SELECT id, name FROM {PG_SCHEMA}.work_formats ORDER BY name")
            work_formats = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
            cur.execute(f"SELECT id, name FROM {PG_SCHEMA}.experience_levels ORDER BY name")
            experience_levels = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
    return {
        "cities": cities,
        "employment_types": employment_types,
        "specialties": specialties,
        "skills": skills,
        "work_formats": work_formats,
        "experience_levels": experience_levels,
    }


def load_database_data(selected_table=None, limit=100):
    limit = max(1, min(int(limit), 500))
    tables = []
    table_counts = {}
    columns_by_table = {}
    fk_links = []
    preview_columns = []
    preview_rows = []
    preview_html = None

    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (PG_SCHEMA,),
            )
            tables = [row[0] for row in cur.fetchall()]

            for t in tables:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(PG_SCHEMA),
                        sql.Identifier(t),
                    )
                )
                table_counts[t] = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT table_name, column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY table_name, ordinal_position
                """,
                (PG_SCHEMA,),
            )
            for table_name, col_name, data_type, nullable in cur.fetchall():
                columns_by_table.setdefault(table_name, []).append(
                    {
                        "name": col_name,
                        "type": data_type,
                        "nullable": nullable == "YES",
                    }
                )

            cur.execute(
                """
                SELECT
                    tc.table_name AS child_table,
                    kcu.column_name AS child_column,
                    ccu.table_name AS parent_table,
                    ccu.column_name AS parent_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON ccu.constraint_name = tc.constraint_name
                 AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = %s
                ORDER BY tc.table_name, kcu.column_name
                """,
                (PG_SCHEMA,),
            )
            fk_links = [
                {
                    "child_table": r[0],
                    "child_column": r[1],
                    "parent_table": r[2],
                    "parent_column": r[3],
                }
                for r in cur.fetchall()
            ]

            if selected_table in tables:
                cur.execute(
                    sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
                        sql.Identifier(PG_SCHEMA),
                        sql.Identifier(selected_table),
                    ),
                    (limit,),
                )
                preview_rows = cur.fetchall()
                preview_columns = [desc[0] for desc in cur.description]

    if preview_columns:
        preview_df = pd.DataFrame(preview_rows, columns=preview_columns)
        preview_html = preview_df.to_html(classes="data-table", index=False, border=0)

    def _safe_mermaid_token(value: str) -> str:
        token = (value or "").strip().replace(" ", "_")
        token = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in token)
        if not token:
            token = "unknown"
        if token[0].isdigit():
            token = f"t_{token}"
        return token

    er_lines = ["erDiagram"]
    for t in tables:
        safe_table = _safe_mermaid_token(t)
        er_lines.append(f"  {safe_table} {{")
        for c in columns_by_table.get(t, []):
            safe_type = _safe_mermaid_token(c["type"])
            safe_col = _safe_mermaid_token(c["name"])
            er_lines.append(f"    {safe_type} {safe_col}")
        er_lines.append("  }")
    for link in fk_links:
        safe_parent = _safe_mermaid_token(link["parent_table"])
        safe_child = _safe_mermaid_token(link["child_table"])
        safe_col = _safe_mermaid_token(link["child_column"])
        er_lines.append(
            f"  {safe_parent} ||--o{{ {safe_child} : {safe_col}"
        )

    er_diagram = "\n".join(er_lines)

    return {
        "tables": tables,
        "table_counts": table_counts,
        "columns_by_table": columns_by_table,
        "fk_links": fk_links,
        "selected_table": selected_table if selected_table in tables else None,
        "preview_columns": preview_columns,
        "preview_rows": preview_rows,
        "preview_html": preview_html,
        "limit": limit,
        "er_diagram": er_diagram,
    }


def get_output_files():
    return sorted(BASE_DIR.glob(OUTPUT_PATTERN), key=lambda p: p.stat().st_mtime, reverse=True)


def get_latest_output():
    files = get_output_files()
    return files[0] if files else None


def process_is_running():
    global _process
    return _process is not None and _process.poll() is None


def read_last_log_lines(lines=50):
    if not LOG_PATH.exists():
        return []
    content = LOG_PATH.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    return content[-lines:]


def _fix_mojibake(text):
    if not isinstance(text, str) or not text:
        return text
    markers = ("\u0420", "\u0421", "\u00d0", "\u00d1")
    if not any(m in text for m in markers):
        return text

    def _score(value: str) -> int:
        good = sum(1 for ch in value if "\u0400" <= ch <= "\u04FF")
        bad = value.count("\u0420") + value.count("\u0421") + value.count("\u00d0") + value.count("\u00d1")
        return good - bad

    current = text
    for _ in range(2):
        best = current
        best_score = _score(current)
        for src_encoding in ("cp1251", "latin1"):
            try:
                candidate = current.encode(src_encoding, errors="strict").decode("utf-8", errors="strict")
                score = _score(candidate)
                if score > best_score:
                    best = candidate
                    best_score = score
            except Exception:
                continue
        if best == current:
            break
        current = best
    return current


def _fix_mojibake_obj(value):
    if isinstance(value, str):
        return _fix_mojibake(value)
    if isinstance(value, list):
        return [_fix_mojibake_obj(v) for v in value]
    if isinstance(value, dict):
        return {k: _fix_mojibake_obj(v) for k, v in value.items()}
    return value


@app.template_filter("fix_text")
def fix_text_filter(value):
    return _fix_mojibake(value) if isinstance(value, str) else value


def read_progress():
    if not ANALIZADOR_PROGRESS_PATH.exists():
        return {
            "status": "idle",
            "stage": "idle",
            "current": 0,
            "total": 1,
            "percent": 0,
            "message": "\u041f\u0430\u0440\u0441\u0435\u0440 \u043d\u0435 \u0437\u0430\u043f\u0443\u0449\u0435\u043d.",
            "updated_at": None,
        }
    try:
        raw = ANALIZADOR_PROGRESS_PATH.read_text(encoding="utf-8-sig", errors="replace")
        data = json.loads(raw)
        return {
            "status": data.get("status", "idle"),
            "stage": data.get("stage", "idle"),
            "current": int(data.get("current", 0)),
            "total": max(1, int(data.get("total", 1))),
            "percent": int(data.get("percent", 0)),
            "message": _fix_mojibake(data.get("message", "")),
            "updated_at": data.get("updated_at"),
        }
    except Exception:
        return {
            "status": "error",
            "stage": "error",
            "current": 0,
            "total": 1,
            "percent": 0,
            "message": "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043f\u0440\u043e\u0447\u0438\u0442\u0430\u0442\u044c \u0441\u043e\u0441\u0442\u043e\u044f\u043d\u0438\u0435 \u043f\u0440\u043e\u0433\u0440\u0435\u0441\u0441\u0430.",
            "updated_at": None,
        }


def load_analizador_config_text():
    if not ANALIZADOR_CONFIG_PATH.exists():
        return "{}"
    return ANALIZADOR_CONFIG_PATH.read_text(encoding="utf-8-sig", errors="replace")


def save_analizador_config_text(raw_text: str):
    parsed = json.loads(raw_text)
    ANALIZADOR_CONFIG_PATH.write_text(
        json.dumps(parsed, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


@app.route("/")
def index():
    files = get_output_files()
    latest = files[0].name if files else None
    return render_template(
        "index.html",
        is_running=process_is_running(),
        latest_file=latest,
        files=[f.name for f in files[:30]],
        logs=read_last_log_lines(40),
        progress=read_progress(),
    )


@app.get("/progress")
def progress():
    return jsonify(read_progress())


@app.route("/parser-config", methods=["GET", "POST"])
def parser_config():
    if request.method == "POST":
        raw = request.form.get("config_json", "")
        try:
            save_analizador_config_text(raw)
            flash("\u041a\u043e\u043d\u0444\u0438\u0433 analizador \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d.", "success")
            return redirect(url_for("parser_config"))
        except Exception as e:
            flash(f"\u041e\u0448\u0438\u0431\u043a\u0430 \u0441\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u0438\u044f \u043a\u043e\u043d\u0444\u0438\u0433\u0430: {e}", "danger")
            return render_template("parser_config.html", config_text=raw)

    config_text = load_analizador_config_text()
    return render_template("parser_config.html", config_text=config_text)


@app.route("/charts")
def charts():
    specialty_vacancies_ids = []
    for raw in request.args.getlist("specialty_vacancies_ids"):
        try:
            specialty_vacancies_ids.append(int(raw))
        except (TypeError, ValueError):
            continue

    filters = {
        "city_id": request.args.get("city_id", type=int),
        "employment_id": (request.args.get("employment_id") or "").strip() or None,
        "skill_id": request.args.get("skill_id", type=int),
        "work_format_id": (request.args.get("work_format_id") or "").strip() or None,
        "experience_id": (request.args.get("experience_id") or "").strip() or None,
        "salary_min": request.args.get("salary_min", type=float),
        "salary_max": request.args.get("salary_max", type=float),
        "date_from": (request.args.get("date_from") or "").strip() or None,
        "date_to": (request.args.get("date_to") or "").strip() or None,
        "specialty_vacancies_ids": specialty_vacancies_ids,
        "specialty_companies_id": request.args.get("specialty_companies_id", type=int),
        "specialty_salary_id": request.args.get("specialty_salary_id", type=int),
        "specialty_skills_id": request.args.get("specialty_skills_id", type=int),
    }
    filter_options = _fix_mojibake_obj(load_chart_filter_options())
    try:
        chart_data = load_chart_data(filters=filters)
        chart_data = _fix_mojibake_obj(chart_data)
    except Exception as e:
        flash(f"\u041e\u0448\u0438\u0431\u043a\u0430 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0438 \u0434\u0430\u043d\u043d\u044b\u0445 \u0438\u0437 PostgreSQL: {e}", "danger")
        chart_data = {
            "vacancies_by_role": [],
            "salary_by_role": [],
            "companies_by_role": [],
            "skills_by_role": [],
        }
    return render_template(
        "charts.html",
        chart_data_json=json.dumps(chart_data, ensure_ascii=False),
        filters=filters,
        filter_options=filter_options,
    )


@app.route("/database")
def database_page():
    selected_table = request.args.get("table", "").strip() or None
    limit = request.args.get("limit", 100, type=int)
    try:
        db_data = load_database_data(selected_table=selected_table, limit=limit)
    except Exception as e:
        flash(f"\u041e\u0448\u0438\u0431\u043a\u0430 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0438 \u0434\u0430\u043d\u043d\u044b\u0445 \u0411\u0414: {e}", "danger")
        db_data = {
            "tables": [],
            "table_counts": {},
            "columns_by_table": {},
            "fk_links": [],
            "selected_table": None,
            "preview_columns": [],
            "preview_rows": [],
            "preview_html": None,
            "limit": limit,
            "er_diagram": "erDiagram",
        }
    return render_template("database.html", **db_data)


@app.route("/users")
def users_page():
    edit_user_id = request.args.get("edit_user_id", type=int)
    with get_pg_connection() as conn:
        users = list_users(conn, PG_SCHEMA)
        lookups = get_lookups(conn, PG_SCHEMA)

    edit_user = None
    if edit_user_id:
        edit_user = next((u for u in users if u["id"] == edit_user_id), None)

    return render_template(
        "users.html",
        users=users,
        lookups=lookups,
        edit_user=edit_user,
    )


@app.post("/users/create")
def users_create():
    with get_pg_connection() as conn:
        user_id = create_user(conn, PG_SCHEMA, request.form)
    flash(f"\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c \u0441\u043e\u0437\u0434\u0430\u043d (id={user_id}).", "success")
    return redirect(url_for("users_page"))


@app.post("/users/<int:user_id>/update")
def users_update(user_id):
    with get_pg_connection() as conn:
        update_user(conn, PG_SCHEMA, user_id, request.form)
    flash(f"\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c {user_id} \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d.", "success")
    return redirect(url_for("users_page"))


@app.post("/users/<int:user_id>/delete")
def users_delete(user_id):
    with get_pg_connection() as conn:
        delete_user(conn, PG_SCHEMA, user_id)
    flash(f"\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044c {user_id} \u0443\u0434\u0430\u043b\u0435\u043d.", "success")
    return redirect(url_for("users_page"))


@app.route("/recommendations")
def recommendations():
    user_id = request.args.get("user_id", type=int)
    with get_pg_connection() as conn:
        users = list_users(conn, PG_SCHEMA)
        selected_user = get_user_basic(conn, PG_SCHEMA, user_id) if user_id else None
        recommendations_data = recommend_for_user(conn, PG_SCHEMA, user_id, limit=30) if user_id else []
        top_skills = top_skills_for_recommendations(
            conn,
            PG_SCHEMA,
            [r["vacancy_id"] for r in recommendations_data],
        ) if user_id else []

    return render_template(
        "recommendations.html",
        users=users,
        selected_user=selected_user,
        recommendations_data=recommendations_data,
        top_skills=top_skills,
    )


@app.post("/run")
def run_analizador():
    global _process
    if process_is_running():
        flash("analizador.py \u0443\u0436\u0435 \u0437\u0430\u043f\u0443\u0449\u0435\u043d.", "warning")
        return redirect(url_for("index"))

    if not ANALYZADOR_PATH.exists():
        flash("\u0424\u0430\u0439\u043b analizador.py \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.", "danger")
        return redirect(url_for("index"))

    ANALIZADOR_PROGRESS_PATH.write_text(
        json.dumps(
            {
                "status": "running",
                "stage": "startup",
                "current": 0,
                "total": 1,
                "percent": 0,
                "message": "\u0417\u0430\u043f\u0443\u0441\u043a \u043f\u0440\u043e\u0446\u0435\u0441\u0441\u0430...",
                "updated_at": None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    _process = subprocess.Popen(
        [sys.executable, str(ANALYZADOR_PATH)],
        cwd=str(BASE_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    flash("\u0417\u0430\u043f\u0443\u0441\u043a analizador.py \u0432\u044b\u043f\u043e\u043b\u043d\u0435\u043d.", "success")
    return redirect(url_for("index"))


@app.post("/stop")
def stop_analizador():
    global _process
    if not process_is_running():
        flash("\u041f\u0440\u043e\u0446\u0435\u0441\u0441 \u043d\u0435 \u0437\u0430\u043f\u0443\u0449\u0435\u043d.", "warning")
        return redirect(url_for("index"))

    _process.terminate()
    ANALIZADOR_PROGRESS_PATH.write_text(
        json.dumps(
            {
                "status": "stopped",
                "stage": "stopped",
                "current": 0,
                "total": 1,
                "percent": 0,
                "message": "\u041f\u0440\u043e\u0446\u0435\u0441\u0441 \u043e\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u043c.",
                "updated_at": None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    flash("\u041f\u0440\u043e\u0446\u0435\u0441\u0441 \u043e\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d.", "success")
    return redirect(url_for("index"))


@app.route("/table")
def table_view():
    flash("\u0421\u0442\u0440\u0430\u043d\u0438\u0446\u0430 \u0442\u0430\u0431\u043b\u0438\u0446\u044b \u043e\u0442\u043a\u043b\u044e\u0447\u0435\u043d\u0430.", "warning")
    return redirect(url_for("index"))


def _cleanup():
    global _process
    if process_is_running():
        _process.terminate()


atexit.register(_cleanup)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)

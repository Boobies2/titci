# Analizador (Flask + PostgreSQL)

Веб-сервис для управления парсером вакансий, просмотра данных из PostgreSQL и рекомендаций пользователям.

## Что в проекте
- `analizador.py` — парсер вакансий и сохранение в PostgreSQL
- `app.py` — Flask веб-сервис
- `user_recommendations.py` — логика рекомендаций
- `users_crud.py` — CRUD пользователей
- `templates/` — HTML шаблоны страниц
- `analizador_config.json` — конфиг парсера
- `analizador_progress.json` — статус/прогресс парсера

## Requirements
- Python 3.10+
- PostgreSQL

Python-зависимости в `requirements.txt`:
- Flask
- aiohttp
- pandas
- psycopg2-binary

## Установка
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Запуск
```bash
python app.py
```

Открой в браузере:
`http://127.0.0.1:5000`

## Загрузка в GitHub (только нужные файлы)
Если репозиторий уже создан на GitHub, выполни:

```bash
git init
git branch -M main
git remote add origin https://github.com/<USERNAME>/<REPO>.git

git add analizador_config.json analizador_progress.json app.py analizador.py user_recommendations.py users_crud.py templates README.md requirements.txt
git commit -m "Add parser/web files, templates, README and requirements"
git push -u origin main
```

Если `remote origin` уже есть, пропусти строку `git remote add origin ...`.

## Важно по безопасности
- В проекте могут быть чувствительные данные (логины/пароли БД) в коде или json.
- Перед публикацией лучше вынести их в переменные окружения и убрать из коммитов.

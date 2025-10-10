#!/usr/bin/env bash
set -euo pipefail

/app/.venv/bin/python -c 'import psycopg2'

superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin || true
superset db upgrade
superset init
superset set-database-uri -d PostgreSQL -u 'postgresql+psycopg2://user:password@postgres:5432/dbt_db'
exec /app/.venv/bin/gunicorn --bind 0.0.0.0:8088 'superset.app:create_app()'



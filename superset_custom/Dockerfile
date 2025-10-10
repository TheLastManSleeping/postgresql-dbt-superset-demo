# superset_custom/Dockerfile

FROM apache/superset:latest

USER root

# Устанавливаем системные зависимости для компиляции psycopg2
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3-dev \
        python3-pip \
        gcc \
        libpq-dev \
        libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-local.txt /app/requirements-local.txt

COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Устанавливаем psycopg2-binary в виртуальное окружение Superset на этапе сборки
RUN python3 -m ensurepip --upgrade && \
    python3 -m pip install --no-cache-dir --target /app/.venv/lib/python3.10/site-packages psycopg2-binary || \
    python3 -m pip install --no-cache-dir psycopg2-binary

USER superset
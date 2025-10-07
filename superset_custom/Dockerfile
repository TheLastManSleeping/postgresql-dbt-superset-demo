# Берем за основу официальный образ
FROM apache/superset:latest

# Устанавливаем драйвер для PostgreSQL
USER root
RUN pip install psycopg2-binary

# --- НОВАЯ СТРОКА ---
# Копируем наши ассеты для импорта внутрь образа
COPY assets/ /app/assets/

# Возвращаемся к стандартному пользователю
USER superset
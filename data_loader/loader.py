import os
import pandas as pd
from sqlalchemy import create_engine, text
import time

# --- Настройки ---
DATA_SIZE = os.environ.get("DATA_SIZE", "small")
PG_HOST = os.environ.get("PG_HOST")
PG_DATABASE = os.environ.get("PG_DATABASE")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
TABLE_NAME = "raw_taxi_trips"

DATA_URLS = {
    "small": ["https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"],
    "medium": [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet"
    ]
}

# --- Подключение к БД с ретраями ---
engine = None
for _ in range(5):
    try:
        engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DATABASE}")
        with engine.connect() as connection:
            print("Successfully connected to PostgreSQL!")
            break
    except Exception as e:
        print(f"Connection failed: {e}. Retrying in 5 seconds...")
        time.sleep(5)

if not engine:
    print("Could not connect to PostgreSQL after several retries. Exiting.")
    exit(1)

# --- Проверка, существуют ли уже данные ---
with engine.connect() as connection:
    result = connection.execute(text(f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '{TABLE_NAME}');"))
    table_exists = result.scalar()

if table_exists:
    print(f"Table '{TABLE_NAME}' already exists. Skipping data loading.")
    exit(0)

# --- Загрузка и запись данных ---
print(f"Data size selected: {DATA_SIZE}. Starting download...")
urls_to_load = DATA_URLS.get(DATA_SIZE)
if not urls_to_load:
    print(f"Invalid DATA_SIZE: {DATA_SIZE}. Exiting.")
    exit(1)

all_data = []
for url in urls_to_load:
    print(f"Downloading from {url}...")
    df = pd.read_parquet(url)
    all_data.append(df)

final_df = pd.concat(all_data, ignore_index=True)
print("Download complete. Writing to PostgreSQL...")

# Оптимизация колонок
final_df = final_df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'tip_amount', 'total_amount']]

final_df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False, chunksize=10000)
print(f"Successfully loaded {len(final_df)} records into '{TABLE_NAME}'.")
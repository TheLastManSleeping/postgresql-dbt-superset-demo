-- models/staging/stg_yellow_tripdata.sql

{{
  config(
    materialized='table'
  )
}}

select
    tpep_pickup_datetime::timestamp as pickup_datetime,
    tpep_dropoff_datetime::timestamp as dropoff_datetime,
    passenger_count::int,
    trip_distance,
    "PULocationID" as pickup_location_id,
    "DOLocationID" as dropoff_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
from {{ source('raw_taxi_data', 'raw_taxi_trips') }}
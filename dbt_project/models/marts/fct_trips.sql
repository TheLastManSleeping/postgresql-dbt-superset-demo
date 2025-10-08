-- models/marts/fct_trips.sql

{{
  config(
    materialized='table'
  )
}}

select
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description

from {{ ref('stg_yellow_tripdata') }}
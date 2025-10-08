-- tests/assert_pickup_before_dropoff.sql

select *
from {{ ref('stg_yellow_tripdata') }}
where dropoff_datetime < pickup_datetime
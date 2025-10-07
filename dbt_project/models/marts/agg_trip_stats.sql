SELECT
    payment_type,
    COUNT(*) AS total_trips,
    AVG(trip_distance) AS average_distance,
    SUM(total_amount) AS total_revenue
FROM
    {{ source('public', 'raw_taxi_trips') }}
GROUP BY
    payment_type
ORDER BY
    total_trips DESC
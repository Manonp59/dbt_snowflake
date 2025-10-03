{% set target_schema='FINAL' %}

{{
    config(
        materialized = 'view',
        database = 'NYC_TAXI_DB',
        schema = 'FINAL',
        warehouse = 'NYC_TAXI_WH',
    ) 
}}

SELECT
    EXTRACT(hour FROM tpep_pickup_datetime) AS trip_hour,
    COUNT(*) AS total_trips,
    AVG(trip_distance / NULLIF(trip_duration, 0) * 60) AS avg_speed_mph,
    SUM(total_amount) AS total_revenue
FROM {{ ref('staging_nyc_taxis') }} AS request
GROUP BY 
    EXTRACT(hour FROM tpep_pickup_datetime) 
ORDER BY 
    EXTRACT(hour FROM tpep_pickup_datetime) 
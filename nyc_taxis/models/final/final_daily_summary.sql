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
    DATE_TRUNC('day', tpep_pickup_datetime) AS trip_date,
    -- nombre de trajets par jour
    COUNT(*) AS total_trips,    
    -- distance moyenne par jour    
    AVG(trip_distance) AS avg_trip_distance,
    -- revenus totaux par jour
    SUM(total_amount) AS total_revenue,
FROM {{ ref('staging_nyc_taxis') }} AS REQUEST
GROUP BY 
    DATE_TRUNC('day', tpep_pickup_datetime) 
ORDER BY 
    trip_date


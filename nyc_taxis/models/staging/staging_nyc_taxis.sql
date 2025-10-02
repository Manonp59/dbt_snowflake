{% set target_schema='STAGING' %}

{{
    config(
        materialized = 'incremental',
        database = 'NYC_TAXI_DB',
        schema = 'STAGING',
        warehouse = 'NYC_TAXI_WH',
        incremental_strategy = 'merge',
    ) 
}}

WITH raw_data AS (
    SELECT
        *, 
       DATEDIFF(second, 'tpep_pickup_datetime', 'tpep_dropoff_datetime') AS trip_duration,
       'trip_distance' / NULLIF(DATEDIFF(second, 'tpep_pickup_datetime', 'tpep_dropoff_datetime') / 3600.0, 0) AS trip_speed,
       NULLIF('tip_amount' / NULLIF('fare_amount', 0), 0) AS tip_ratio
    FROM {{ source('raw_nyc_taxis', 'YELLOW_TAXI_TRIPS') }}
    -- WHERE 
    --     'fare_amount' >= 0
        -- AND 'total_amount' >= 0
        -- AND 'pickup_datetime' < 'dropoff_datetime'
        -- AND 'trip_distance' >= 0.1 
        -- AND 'trip_distance' <= 100
        -- AND 'PULocationID' IS NOT NULL
        -- AND 'DOLocationID' IS NOT NULL
    -- {% if is_incremental() %}
    --     AND _ETL_EXTRACT_DT >= (select max(_ETL_EXTRACT_DT) from {{ 'NYC_TAXI_DB.STAGING.clean_trips' }})
    -- {% endif %}
)
SELECT * FROM raw_data

 
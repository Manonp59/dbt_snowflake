{% set target_schema='STAGING' %}

{{
    config(
        materialized = 'incremental',
        database = 'NYC_TAXI_DB',
        schema = 'STAGING',
        warehouse = 'NYC_TAXI_WH',
        incremental_strategy = 'merge',
        unique_key=['vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    ) 
}}

WITH raw_data AS (
    SELECT 
        * 
    FROM {{ source('raw_nyc_taxis', 'YELLOW_TAXI_TRIPS') }}
    {% if is_incremental() %}
        WHERE _ETL_EXTRACT_DT > (select max(_ETL_EXTRACT_DT) from {{ this }})
    {% endif %}
),

ENFORCE_SCHEMA AS (
    SELECT 
        CAST("Airport_fee" AS FLOAT) AS airport_fee,
        CAST("DOLocationID" AS INT) AS do_location_id,
        CAST("PULocationID" AS INT) AS pu_location_id,
        CAST("RatecodeID" AS INT) AS ratecode_id,
        CAST("VendorID" AS INT) AS vendor_id,
        CAST("_ETL_EXTRACT_DT" AS TIMESTAMP_NTZ(0)) AS _etl_extract_dt,
        CAST("cbd_congestion_fee" AS FLOAT) AS cbd_congestion_fee,
        CAST("congestion_surcharge" AS FLOAT) AS congestion_surcharge,
        CAST("extra" AS FLOAT) AS extra,
        CAST("fare_amount" AS FLOAT) AS fare_amount,    
        CAST("improvement_surcharge" AS FLOAT) AS improvement_surcharge,
        CAST("mta_tax" AS FLOAT) AS mta_tax,
        CAST("passenger_count" AS INT) AS passenger_count,
        CAST("payment_type" AS INT) AS payment_type,
        CAST("store_and_fwd_flag" AS STRING) AS store_and_fwd_flag,
        CAST("tip_amount" AS FLOAT) AS tip_amount,
        CAST("tolls_amount" AS FLOAT) AS tolls_amount,
        CAST("total_amount" AS FLOAT) AS total_amount,
        TRY_TO_TIMESTAMP(TO_VARCHAR("tpep_pickup_datetime")) AS tpep_pickup_datetime,
        TRY_TO_TIMESTAMP(TO_VARCHAR("tpep_dropoff_datetime")) AS tpep_dropoff_datetime,
        CAST("trip_distance" AS FLOAT) AS trip_distance
    FROM raw_data
    WHERE 
        "fare_amount" >= 0
        AND "total_amount" >= 0
        AND TRY_TO_TIMESTAMP(TO_VARCHAR("tpep_pickup_datetime")) < TRY_TO_TIMESTAMP(TO_VARCHAR("tpep_dropoff_datetime"))
        AND "trip_distance" >= 0.1 
        AND "trip_distance" <= 100
        AND "PULocationID" IS NOT NULL
        AND "DOLocationID" IS NOT NULL
),

ADD_FIELDS AS (
    SELECT 
        *,
        ROUND(DATEDIFF(second, tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0, 2) AS trip_duration,
        CAST(
            trip_distance / NULLIF(DATEDIFF(second, tpep_pickup_datetime, tpep_dropoff_datetime) / 3600.0, 0)
        AS INT) AS trip_speed,
        ROUND(
            NULLIF(tip_amount / NULLIF(fare_amount, 0), 0) * 100,
            2
        ) AS tip_ratio_pct
    FROM ENFORCE_SCHEMA
)


SELECT * FROM ADD_FIELDS

 
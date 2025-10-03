USE WAREHOUSE NYC_TAXI_WH;
USE DATABASE NYC_TAXI_DB;

CREATE or replace STAGE load_data_from_azure
  URL = 'azure://adlsplatteau.blob.core.windows.net/nyc-taxi'
  CREDENTIALS = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-10-30T23:15:06Z&st=2025-10-02T14:00:06Z&spr=https&sig=78Qnl8h9fOpnItNwOjVgyTGdIjJG5sNTdSk45xBnVG4%3D')
  FILE_FORMAT = (TYPE = 'PARQUET');

LIST @load_data_from_azure;

CREATE OR REPLACE FILE FORMAT parquet_data
  TYPE = 'PARQUET';

CREATE TABLE IF NOT EXISTS raw.yellow_taxi_trips
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
       INFER_SCHEMA(
          LOCATION=>'@load_data_from_azure',
          FILE_FORMAT=>'parquet_data'
       )
    )
  );

-- Table temporaire pour l'ingestion brute
CREATE TABLE IF NOT EXISTS raw.yellow_taxi_trips_stage LIKE raw.yellow_taxi_trips;

-- Copier les données
COPY INTO raw.yellow_taxi_trips_stage
  FROM @load_data_from_azure
  FILE_FORMAT = (FORMAT_NAME = 'parquet_data')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


ALTER TABLE raw.yellow_taxi_trips ADD COLUMN IF NOT EXISTS _ETL_EXTRACT_DT TIMESTAMP_NTZ(0);

MERGE INTO raw.yellow_taxi_trips t
USING (
  SELECT 
        "Airport_fee",
      "congestion_surcharge",
      "PULocationID",
      "DOLocationID",
      "extra",
      "fare_amount",
      "improvement_surcharge",
      "mta_tax",
      "passenger_count",
      "payment_type",
      "RatecodeID",
      "store_and_fwd_flag",
      "tip_amount",
      "tolls_amount",
      "total_amount",
      "tpep_dropoff_datetime",
      "tpep_pickup_datetime",
      "trip_distance",
      "VendorID",
      "cbd_congestion_fee",
      CURRENT_TIMESTAMP() AS _etl_extract_dt
  FROM raw.yellow_taxi_trips_stage
) s
ON t."VendorID" = s."VendorID"
  AND t."tpep_pickup_datetime" = s."tpep_pickup_datetime"
  AND t."tpep_dropoff_datetime" = s."tpep_dropoff_datetime"
WHEN NOT MATCHED THEN
  INSERT (
      "Airport_fee",
      "congestion_surcharge",
      "PULocationID",
      "DOLocationID",
      "extra",
      "fare_amount",
      "improvement_surcharge",
      "mta_tax",
      "passenger_count",
      "payment_type",
      "RatecodeID",
      "store_and_fwd_flag",
      "tip_amount",
      "tolls_amount",
      "total_amount",
      "tpep_dropoff_datetime",
      "tpep_pickup_datetime",
      "trip_distance",
      "VendorID",
      "cbd_congestion_fee",
      _ETL_EXTRACT_DT
  )
  VALUES (
      s."Airport_fee",
      s."congestion_surcharge",
      s."PULocationID",
      s."DOLocationID",
      s."extra",
      s."fare_amount",
      s."improvement_surcharge",
      s."mta_tax",
      s."passenger_count",
      s."payment_type",
      s."RatecodeID",
      s."store_and_fwd_flag",
      s."tip_amount",
      s."tolls_amount",
      s."total_amount",
      s."tpep_dropoff_datetime",
      s."tpep_pickup_datetime",
      s."trip_distance",
      s."VendorID",
      s."cbd_congestion_fee",
      s._ETL_EXTRACT_DT
  );


-- Suppression de la table temporaire
DROP TABLE raw.yellow_taxi_trips_stage;
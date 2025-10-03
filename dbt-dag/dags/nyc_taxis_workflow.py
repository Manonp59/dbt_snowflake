from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import requests
from azure.storage.blob import BlobServiceClient
from datetime import date
import os
import urllib3
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Désactiver les warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration DAG
default_args = {
    "owner": "manon",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=20),
}

def extract_and_upload_to_blob(**kwargs):
    # Paramètres Azure Blob
    hook = WasbHook(wasb_conn_id="azure_storage_conn")
    blob_service_client = hook.blob_service_client
    container_client = blob_service_client.get_container_client("nyc-taxi")

    # Paramètres fichiers
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    file_prefix = "yellow_tripdata_"
    file_suffix = ".parquet"
    output_dir = "data/"

    os.makedirs(output_dir, exist_ok=True)

    # Définir la période à télécharger
    start_year = 2024
    start_month = 1

    today = date.today()
    current_year = today.year
    current_month = today.month

    year = start_year
    month = start_month

    while (year < current_year) or (year == current_year and month <= current_month):
        month_str = f"{month:02d}"
        filename = f"{file_prefix}{year}-{month_str}{file_suffix}"
        url = base_url + filename
        output_path = os.path.join(output_dir, filename)

        print(f"Téléchargement de {url}...")
        r = requests.get(url, verify=False)
        if r.status_code == 200:
            print(f"Téléversement de {filename} vers Azure Blob Storage...")
            with open(output_path, "wb") as f:
                f.write(r.content)
            blob_client = container_client.get_blob_client(blob=filename)
            blob_client.upload_blob(data=open(output_path, "rb"), overwrite=True, max_concurrency=4, timeout=3600, connection_timeout=3600)
            print(f"{filename} téléversé avec succès sur Azure.")
        else:
            print(f"Échec pour {filename}. Code retour : {r.status_code}")

        # Passer au mois suivant
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1

# Définition du DAG
with DAG(
    dag_id="nyc_taxi_workflow",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",  # Exécution mensuelle
    catchup=False,
    tags=["nyc_taxi"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload_to_blob,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_raw_table",
        conn_id="snowflake_conn",
        sql="sql/create_raw_table.sql"
    )
    
    profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "NYC_TAXI_DB", "schema": "RAW"},
    )
    )
    
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt/nyc_taxis",),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),

    )
    
    extract_task >> create_table >> transform_data


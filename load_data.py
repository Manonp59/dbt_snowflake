import requests
from datetime import date
from azure.storage.blob import BlobServiceClient

connection_string = "DefaultEndpointsProtocol=https;AccountName=adlsplatteau;AccountKey=9lvtDM+FkxTXwvBHYjDoPu9e9ABulG/WHDYW6Imt6d5OZKqj5mYx3hDDhrQF79lOU4tHXEaJYuWm+AStUbo4Qg==;EndpointSuffix=core.windows.net"
container_name = "nyc-taxi"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
file_prefix = "yellow_tripdata_"
file_suffix = ".parquet"
output_dir = "data/"

# Définir la période
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
    output_path = output_dir + filename

    print(f"Téléchargement de {url}...")
    r = requests.get(url, verify=False)
    if r.status_code == 200:
        # UPLOAD TO AZURE BLOB
        blob_name = filename
        print(f"Téléversement de {filename} vers Azure Blob Storage...")
        with open(output_path, "rb") as data:
            print(f"Upload {filename} (taille : {len(r.content) / 1_048_576:.2f} Mio)")
            blob_client = container_client.get_blob_client(blob=filename)
            blob_client.upload_blob(data=data, overwrite=True, max_concurrency=4, timeout=3600, connection_timeout=3600)
        print(f"{filename} téléversé avec succès sur Azure.")

    else:
        print(f"Échec pour {filename}. Code retour : {r.status_code}")

    # Passer au mois suivant
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
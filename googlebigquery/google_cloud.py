import os
from google.cloud import bigquery

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\rodrigo.gil\\Downloads\\key.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = ""
# client = bigquery.Client()

client = bigquery.Client.from_service_account_json("key.json") # type: bigquery.Client
query_results = client.run_sync_query("SELECT trip_start_timestamp, trip_miles FROM `erudite-bonbon-168216.TESTE_DATA.Y` LIMIT 10")
query_results.use_legacy_sql = False
query_results.run()

fetch_result = query_results.fetch_data(
    max_results=10
)

for x in fetch_result:
    print(x[1])
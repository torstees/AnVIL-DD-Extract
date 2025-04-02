#############################################
## Imports
#############################################

import data_repo_client
import google.auth
from google.cloud import bigquery

# Not sure which version Nate was using, but I had to specifically import the 
# transport.requests to get the code below to work. 
import google.auth.transport.requests
import requests
import pandas as pd
import datetime

import pdb

#############################################
## Functions
#############################################

# Function to refresh TDR API client
def refresh_tdr_api_client():
    creds, project = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    config = data_repo_client.Configuration()
    config.host = "https://data.terra.bio"
    config.access_token = creds.token
    api_client = data_repo_client.ApiClient(configuration=config)
    api_client.client_side_validation = False
    return api_client

def extract_query_items(object_type, object_id_list, output_path):
   
    if object_type in ["dataset", "snapshot"]:
        print(f"Start time: {datetime.datetime.now()}")
        schema_results = []
        query_items = []
         # Loop through and process listed objects
        for object_id in object_id_list:

            # Establish API client
            api_client = refresh_tdr_api_client()
            datasets_api = data_repo_client.DatasetsApi(api_client=api_client)
            snapshots_api = data_repo_client.SnapshotsApi(api_client=api_client)

            # Retrieve dataset details
            print(f"Processing {object_type} = '{object_id}'...")
            try:
                if object_type == "dataset":
                    object_details = datasets_api.retrieve_dataset(id=object_id, include=["SCHEMA"]).to_dict()
                    object_name = object_details["name"]
                    object_schema = object_details["schema"]["tables"]
                else:
                    object_details = snapshots_api.retrieve_snapshot(id=object_id).to_dict()
                    object_name = object_details["name"]
                    object_schema = object_details["tables"]
                    object_project = object_details["data_project"]
                    table_names = []
                    for table in object_schema:
                        table_names.append(table["name"])
                    query_items.append({"table_names": table_names,"dataset_name":object_name,"data_project":object_project})

            except Exception as e:
                print(f"Error retrieving object from TDR: {str(e)}")
                print("Continuing to next object.")
                continue
        return query_items
                             
    else:
        print("Invalid object_type provided. Please specified 'dataset' or 'snapshot' and try again.")

## Function to query dataset tables with BigQuery
def query_dataset_tables(query_items, output_path):
    # Initialize BigQuery client
    bq_client = bigquery.Client()

    # Loop through each query item
    for item in query_items:
        table_names = item["table_names"]
        dataset_name = item["dataset_name"]
        data_project = item["data_project"]

        # Loop through each table name and run a query
        for table_name in table_names:
            query = f"SELECT * FROM `{data_project}.{dataset_name}.{table_name}` LIMIT 10"
            try:
                df = bq_client.query(query).to_dataframe()
                output_file = f"{output_path}/{dataset_name}/{table_name}.csv"
                df.to_csv(output_file, index=False)
                print(f"Query results saved to {output_file}")
            except Exception as e:
                print(f"Error querying table {table_name}: {str(e)}")
    
def main():

    
    # Object type (valid values are 'dataset' or 'snapshot')
    object_type = "snapshot"
    
    # List objects to extract the schema from
    object_id_list = [
        "05a9e369-0011-48d9-ab2e-af334973bdb5"
    ]
    # "aa6b58c2-6eb3-4b4d-9e73-89cbb323ee26"
    
    # Specify the output GCS path for the results file
    # output_path = "gs://fc-96e29e51-79cf-4213-a2ad-26f84a89aa25/data"
    output_path = "./query_results"
    
    # Specify whether to include the schema in the results
    query_items = extract_query_items(object_type, object_id_list, output_path)
    query_dataset_tables(query_items, output_path)
    
    


if __name__ == "__main__":
    main()

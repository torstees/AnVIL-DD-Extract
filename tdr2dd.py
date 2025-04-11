#############################################
## Imports
#############################################

import data_repo_client
import google.auth
import google.auth.transport.requests
from google.cloud import bigquery
import os
import datetime
import pandas as pd
import argparse

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
                    tables = object_schema
                    for table in object_schema:
                        table_names.append(table["name"])
                    query_items.append({"table_names": table_names,"dataset_name":object_name,"data_project":object_project})

            except Exception as e:
                print(f"Error retrieving object from TDR: {str(e)}")
                print("Continuing to next object.")
                continue
        return {"query_items":query_items, "tables":tables}
                             
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
        output_files = []

        # Loop through each table name and run a query
        for table_name in table_names:
            query = f"SELECT * FROM `{data_project}.{dataset_name}.{table_name}`"
            try:
                df = bq_client.query(query).to_dataframe()
                # Create output directory if it doesn't exist
                output_dir = f"{output_path}/orginal_data"
                os.makedirs(output_dir, exist_ok=True)
                # Save the DataFrame to a CSV file
                output_file = f"{output_dir}/{table_name}.csv"
                df.to_csv(output_file, index=False)
                print(f"Query results saved to {output_file}")
                output_files.append(output_file)
            except Exception as e:
                print(f"Error querying table {table_name}: {str(e)}")
        return output_files

# Function infer data types
def infer_data_types(csv_file, tables, enumeration_threshold):
    df = pd.read_csv(csv_file)
    data_dictionary = []
    
    # Create a mapping of column names to their schema details from `tables`
    schema_mapping = {}
    for table in tables:
        for column in table["columns"]:
            schema_mapping[column["name"]] = {
                "datatype": column["datatype"],
                "is_array": column["array_of"],
                "is_required": column["required"],
                "description": column.get("description", None),

            }

    for col in df.columns:
        # Basic info
        col_name = col
        col_dtype = df[col].dtype  # Pandas-inferred data type
        schema_info = schema_mapping.get(col_name, {})
        schema_dtype = schema_info.get("datatype", "Unknown")
        is_array = schema_info.get("is_array", False)
        is_required = schema_info.get("is_required", False)
        description = schema_info.get("description", None)
        type = schema_dtype  # Default to schema data type
        enumerated_values = schema_info.get("enumerated_values", None)
        min = ''
        max = ''
        units = ''
        
        if is_array:
            if schema_dtype == 'string':
                type = 'array of strings'
            elif schema_dtype == 'integer':
                type = 'array of integers'
            elif schema_dtype == 'float':
                type = 'array of floats'
        
        # Count of non-null entries
        non_null_count = df[col].count()
        # Count of distinct values
        unique_count = df[col].nunique(dropna=True)
        # Check if unique count is 50% or less of the total non-null count        
        if unique_count < len(df[col]) and not is_array and type == 'string' and unique_count <= non_null_count * (enumeration_threshold / 100):
            enumerated_values = df[col].unique()
            enumerated_values = ";".join([f"{item}" for item in enumerate(enumerated_values)])
        elif type == 'boolean':
            enumerated_values = 'T=True;F=False'
        else:
            enumerated_values = None
        if type == 'integer' or type == 'float':
            # Get min and max values
            if df[col].dtype == 'int64':
                min = int(df[col].min())
                max = int(df[col].max())
            elif df[col].dtype == 'float64':
                min = float(df[col].min())
                max = float(df[col].max())
        
        # Construct a row for this column
        col_info = {
            'variable_name': col_name,
            'description': description,
            'type': type,
            'min': min,
            'max': max,
            'units': units,
            'enumerated_values': enumerated_values,
        }
        
        data_dictionary.append(col_info)
    # Create DataFrame with this metadata
    data_dict_df = pd.DataFrame(data_dictionary)
    return data_dict_df  # Return the DataFrame instead of printing it

def main(object_id_list, study_dir, enumeration_threshold):
    object_type = "snapshot"
    output_path = study_dir
    dataset_items = extract_query_items(object_type, object_id_list, output_path)
    query_items = dataset_items["query_items"]
    tables = dataset_items["tables"]
    working_csvs = query_dataset_tables(query_items, output_path)
    
    for csv_file in working_csvs:
        print(f"Inferring data types for {csv_file}...")
        base_name = os.path.splitext(os.path.basename(csv_file))[0]
        dict_file = os.path.join(output_path, f"{base_name}_data_dict.csv")
        data_dict_df = infer_data_types(csv_file, tables, enumeration_threshold)
        if data_dict_df is not None:
            data_dict_df.to_csv(dict_file, index=False)
            print(f"Data dictionary saved to {dict_file}")
        else:
            print(f"Failed to infer data types for {csv_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process TDR objects.')
    parser.add_argument('--object_ids', nargs='+', required=True, help='List of object IDs to process')
    parser.add_argument('--study_dir', required=True, help='Directory to save the study files')
    parser.add_argument('--enumeration_threshold', required=True, help='percentage of unique values to be considered enumerated')
    args = parser.parse_args()
    object_id_list = args.object_ids
    study_dir = args.study_dir
    enumeration_threshold = args.enumeration_threshold or 30
    main(object_id_list, study_dir, enumeration_threshold)


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

"""
The purpose of this script will be to extract all TDR snapshots into the 
map-dragon data-dictionary format. The purpose for this is two-fold: 
    1) to enable loading of data-dictionaries into MD for mapping. 
    2) to host someplace as an informational resource for researchers

Multiple sources of truth: 
    1) Terra Data Repository (TDR) - This is the database/api that 
       houses the data as snapshots. 
    2) dbGAP - This is the original source for sharing research data funded
       by the NIH. There is a lot of variation in terms of completeness
       in this data
    3) Data Use Oversight System (DUOS) - This is where the official consent 
       details are found. We have a single JSON file that contains the data
       that is currently available to us. 

Issues: 
* There is some degree of human involvement for prepping these for use in 
map-dragon. To better serve the data team, we will extract data in 2 forms:
First, enumerations will be identified based on a fractional unique value ratio
(and possibly an integer minimum distinct value). This will be the "official"
data dictionary. For mapping, we'll set the ratio to 100% and let the data
team delete the rows that aren't appropriate for mapping. 


Output:
We will segregate the data that is in DUOS from the data in TDR Only. I'm 
imagining the output directories will look something like: 

duos-output/[snapshat title]/tdr-all/[various DD files]
duos-output/[snapshat title]/tdr-enums/[various DD files]
duos-output/[snapshat title]/dbGAP/[various DD files]
tdr-only-output/[snapshat title]/tdr-all/[various DD files]
tdr-only-output/[snapshat title]/tdr-enums/[various DD files]
tdr-only-output/[snapshat title]/dbGAP/[various DD files]

TODO: 

1) Extract all snapshot details using "get_snapshot_ids" function with an 
   empty string as the search value. We should pull them in chunks, maybe
   250 at a time or something. We want to store these in a single dictionary
   keyed by the study "name". 
2) Traverse the DUOS file and update the Snapshot Details with consent 
   information
3) for each snapshot pulled from #1:
    1) if there is a phsID, generate a phs based data-dictionary (see phs.py)
    2) generate the TDR based data dictionary (thresholds 100, ??)

At a later date, we will extract summary information (counts for different 
values, etc)

"""

#############################################
## Functions
#############################################


class SnapshotDetail:
    def __init__(self, item):
        self.id = item['id']
        self.name = item['name']
        self.description = item['description']
        self.phs_id = item['phsId']
        self.duos_id = item['duosId']
        self.in_duos = False 
        self.data_use = None 
    
    def add_duos(self, duosId, dataUse):
        """Set the duos ID """
        self.duos_id = duosId 
        self.in_duos = True 
        self.data_use = dataUse 

def get_snapshot_ids(api, search_value, offset=0, limit=2000):
    """Return a list of all snapshot details that match search_value"""
    snapshot_details = {}

    ss_enums = api.enumerate_snapshots(offset=offset, 
                                       limit=limit, 
                                       filter=search_value).to_dict()
    for item in ss_enums['items']:
        ss = SnapshotDetail(item)

        assert(ss.name not in snapshot_details)
        snapshot_details[ss.name] = ss 

    return snapshot_details
    


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
                output_dir = f"{output_path}/{dataset_name}/orginal_data"
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
            enumerated_values = df[col].unique().tolist()
            enumerated_values = ";".join([f"{item}" for item in enumerated_values])
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
    parser.add_argument('--object_id', type=str,  help='Object ID to be extracted (may appear multiple times)')
    parser.add_argument('--filter', type=str, help='String to match on for snapshots to be extracted')
    parser.add_argument('--study_dir', required=True, help='Directory to save the study files')
    parser.add_argument('--enumeration_threshold',default=30, type=int, help='percentage (integer, so x 100) of unique values to be considered enumerated')
    args = parser.parse_args()
    object_id_list = args.object_ids
    study_dir = args.study_dir
    enumeration_threshold = args.enumeration_threshold
    main(object_id_list, study_dir, enumeration_threshold)


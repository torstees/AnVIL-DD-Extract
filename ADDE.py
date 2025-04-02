"""
resources
DUOS: https://drive.google.com/file/d/1WUGBQ3nrQOyBCaeeG6uMDNpyd_fG_vKK/view"""

import json
import re
from typing import List, Dict, Any
import pandas as pd

import pdb
from ddscrape import extract_table_schenas
# import datetime



def load_duos_index(index_file_path: str) -> List[Dict[str, Any]]:
    """
    Load the DUOS index JSON file and return a list of study objects.
    """
    with open(index_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def search_studies_by_title(
    studies: List[Dict[str, Any]], 
    title_query: str
) -> List[Dict[str, Any]]:
    """
    Given a list of study objects and a partial title query,
    return only those studies whose 'Study Title' matches the query
    (case-insensitive).
    """
    pattern = re.compile(f".*{re.escape(title_query)}*", re.IGNORECASE)
    matches = []
    for study in studies:
        # Adjust key references to match your actual JSON keys
        study_title = study.get("datasetName", "")
        if pattern.search(study_title):
            matches.append(study)
    return matches

def extract_study_details(study_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    From a single DUOS study JSON record, extract relevant fields needed
    for your dictionary-building process.
    Example JSON keys to parse:
      - datasetIdentifier (e.g. 'DUOS-000234')
      - datasetName (e.g. 'ANVIL_ALS_FTD_DEMENTIA_SEQ_GRU_v1')
      - dataUse (dict with 'primary' and 'secondary' arrays)
      - url (TDR snapshot URL, from which we can parse a TDR ID)
      - study --> description, studyName, phsId, phenotype, species, piName
    """
    # Attempt to parse out TDR snapshot ID from the URL
    # e.g. 'https://data.terra.bio/snapshots/85b0b351-cd0a-4efe-95a4-e39273c42831'
    tdr_url = study_obj.get('url', '')
    tdr_id = None
    if 'snapshots/' in tdr_url:
        tdr_id = tdr_url.split('snapshots/')[-1]

    study_nested = study_obj.get('study', {})

    details = {
        "dataset_identifier": study_obj.get('datasetIdentifier', ''),
        "dataset_name": study_obj.get('datasetName', ''),
        "tdr_url": tdr_url,
        "tdr_id": tdr_id,
        "access_management": study_obj.get('accessManagement', ''),
        # dataUse might have a 'primary' list of dicts, 'secondary' list of dicts
        "data_use": study_obj.get('dataUse', {}),
        # nested study fields
        "study_name": study_nested.get('studyName', ''),
        "study_description": study_nested.get('description', ''),
        "phs_id": study_nested.get('phsId', ''),
        "phenotype": study_nested.get('phenotype', ''),
        "species": study_nested.get('species', ''),
        "pi_name": study_nested.get('piName', ''),
        "data_types": study_nested.get('dataTypes', []),
    }
    return details

def main(index_file_path: str, user_query: str):
    """
    Example workflow:
    1) Load the DUOS index
    2) Find relevant studies by partial title match (study['study']['studyName'])
    3) For each matching study, build:
       - Basic study details
       - TDR-based dictionary (if TDR ID) (parsed from the 'url')
       - dbGaP-based dictionary (if phsID exists)
    4) Compare or export the results.
    """
    # Load the DUOS study index
    all_studies = load_duos_index(index_file_path)
    
    # Find matching studies
    matching_studies = search_studies_by_title(all_studies, user_query)
    print(f"Found {len(matching_studies)} matching studies for query '{user_query}'.")
    
    results = []
    for st in matching_studies:
        # Basic details
        details = extract_study_details(st)

        # TDR ID
        tdr_id = details["tdr_id"]
        # tdr_dict = build_tdr_data_dictionary(tdr_id) if tdr_id else None
        
        # If we have a phsID, build the dbGaP dictionary
        phs_id = details["phs_id"]
        # dbgap_dict = build_dbgap_data_dictionary(phs_id) if phs_id else None
        
        # Combine everything
        study_package = {
            "basic_info": details,
            # "tdr_dict": tdr_dict,
            # "dbgap_dict": dbgap_dict,
        }
        
        results.append(study_package)
    
    # Here, do whatever you like with `results`: 
    #   - Save to file
    #   - Perform dictionary comparison
    #   - Print to screen, etc.
    for r in results:
        print("=== Study ===")
        print(f"Study Name: {r['basic_info']['study_name']}")
        print(f"Dataset Name: {r['basic_info']['dataset_name']}")
        print(f"TDR ID: {r['basic_info']['tdr_id']}")
        print(f"dbGaP ID: {r['basic_info']['phs_id']}")
        # print(f"TDR Dictionary: {r['tdr_dict']}")
        # print(f"dbGaP Dictionary: {r['dbgap_dict']}\n")


if __name__ == "__main__":
    # Example usage:
    duos_index_path = "./AnVIL_All_Studies.json"  # Path to your DUOS index file
    user_search_string = "ANVIL"  # or something the user typed
    
    main(duos_index_path, user_search_string)
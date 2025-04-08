#!/usr/bin/env python3

import json
import re
from typing import List, Dict, Any
import pandas as pd
import argparse

from phs2dd import core as phs2dd

from ddscrape import main as ddscrape

import pdb
import os
# from ddscrape import extract_table_schenas
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
    pattern = re.compile(f".*{re.escape(title_query)}*", re.IGNORECASE)
    matches = []
    for study in studies:
        study_title = study.get("study", {}).get("studyName", "")
        if pattern.search(study_title):
            matches.append(study)
    return matches

def extract_study_details(study_obj: Dict[str, Any]) -> Dict[str, Any]:

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
    
    phs_id_list = []
    object_id_list = []
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
    
    #make to csv
    # Create DataFrame from results
    rows = []
    for r in results:
        rows.append({
            'study_name': r['basic_info']['study_name'],
            'dataset_name': r['basic_info']['dataset_name'],
            'tdr_id': r['basic_info']['tdr_id'],
            'phs_id': r['basic_info']['phs_id'],
            'access_management': r['basic_info']['access_management'],
            'data_use': json.dumps(r['basic_info']['data_use']),  # Convert dict to string
            'phenotype': r['basic_info']['phenotype'],
            'species': r['basic_info']['species'],
            'pi_name': r['basic_info']['pi_name']
        })
        phs_id_list.append(r['basic_info']['phs_id'])
        object_id_list.append(r['basic_info']['tdr_id'])
    
    # Create and save DataFrame
    df = pd.DataFrame(rows)
    phs2dd.main(phs_id_list)
    ddscrape(object_id_list)
    # Find the working directory name based on study name from first result
    if rows:
        study_dir = rows[0]['dataset_name'].lower()
        # Create directory if it doesn't exist
        if not os.path.exists(study_dir + '*'):
            os.makedirs(study_dir)
        # Save CSV in that directory
        filename = os.path.join(study_dir, 'study_results.csv')
        df.to_csv(filename, index=False)


        
        
if __name__ == "__main__":
    # Example usage:
    duos_index_path = "./AnVIL_All_Studies.json"  # Path to DUOS index file
    user_search_string = ""  # Example search string
    parser = argparse.ArgumentParser(description="Search DUOS index for studies.")
    parser.add_argument(
        "--query", 
        type=str, 
        required=True,
        help=" use --query Search string for study title."
    )
    args = parser.parse_args()
    user_search_string = args.query
    main(duos_index_path, user_search_string)
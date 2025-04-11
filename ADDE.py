#!/usr/bin/env python3

import json
import re
from typing import List, Dict, Any
import pandas as pd
import argparse
from phs2dd import main as phs2dd
from tdr2dd import main as tdr2dd
import os

def load_duos_index(index_file_path: str) -> List[Dict[str, Any]]:
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
        "data_use": study_obj.get('dataUse', {}),
        "study_name": study_nested.get('studyName', ''),
        "study_description": study_nested.get('description', ''),
        "phs_id": study_nested.get('phsId', ''),
        "phenotype": study_nested.get('phenotype', ''),
        "species": study_nested.get('species', ''),
        "pi_name": study_nested.get('piName', ''),
        "data_types": study_nested.get('dataTypes', []),
    }
    return details

def sanitize_directory_name(name: str) -> str:
    sanitized_name = re.sub(r'[^\w\-]', '_', name)
    return sanitized_name

def main(index_file_path: str, user_query: str,enumeration_threshold):
    all_studies = load_duos_index(index_file_path)
    matching_studies = search_studies_by_title(all_studies, user_query)
    print(f"Found {len(matching_studies)} matching studies for query '{user_query}'.")
    
    phs_id_list = []
    object_id_list = []
    results = []
    for st in matching_studies:
        details = extract_study_details(st)
        tdr_id = details["tdr_id"]
        phs_id = details["phs_id"]
        study_package = {"basic_info": details}
        results.append(study_package)
    
    rows = []
    for r in results:
        rows.append({
            'study_name': r['basic_info']['study_name'],
            'dataset_name': r['basic_info']['dataset_name'],
            'tdr_id': r['basic_info']['tdr_id'],
            'phs_id': r['basic_info']['phs_id'],
            'access_management': r['basic_info']['access_management'],
            'data_use': json.dumps(r['basic_info']['data_use']),
            'phenotype': r['basic_info']['phenotype'],
            'species': r['basic_info']['species'],
            'pi_name': r['basic_info']['pi_name']
        })
        phs_id_list.append(r['basic_info']['phs_id'])
        object_id_list.append(r['basic_info']['tdr_id'])
    
    df = pd.DataFrame(rows)
    if rows:
        study_name = rows[0]['study_name']
        sanitized_study_name = sanitize_directory_name(study_name)
        study_dir = f"query_results/{sanitized_study_name}"
        print(f"Saving results to directory: {study_dir}")
        os.makedirs(study_dir, exist_ok=True)
        filename = os.path.join(study_dir, 'study_results.csv')
        df.to_csv(filename, index=False)
        phs2dd(phs_id_list, study_dir)
        tdr2dd(object_id_list, study_dir, enumeration_threshold)

if __name__ == "__main__":
    duos_index_path = "./AnVIL_All_Studies.json"
    user_search_string = ""
    parser = argparse.ArgumentParser(description="Search DUOS index for studies.")
    parser.add_argument(
        "--query", 
        type=str, 
        required=True,
        help=" use --query Search string for study title."
    )
    parser.add_argument('--enumeration_threshold', required=False, help='percentage of unique values to be considered enumerated')
    args = parser.parse_args()
    user_search_string = args.query
    enumeration_threshold = args.enumeration_threshold or 30
    main(duos_index_path, user_search_string,enumeration_threshold)
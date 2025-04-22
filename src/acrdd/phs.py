import requests
import os
import argparse
from bs4 import BeautifulSoup
import csv
import logging
from lxml import etree

def configure_logging(study_dir):
    log_file = os.path.join(study_dir, 'phs2dd.log')
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def get_lastest_version(study_url, phs_id):
    try:
        response = requests.get(study_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
        versions = [link.get('href').strip('/') for link in links if link.get('href').startswith(phs_id)]
        
        if not versions:
            logging.error(f"No data dictionaries found for PHS ID: {phs_id} at {study_url}")
            return None
        
        latest_version = max(versions, key=lambda x: int(x.split('.')[1][1:]))
        return latest_version
    
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

def get_data_dict_str(pheno_var_sums_url):
        response = requests.get(pheno_var_sums_url)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
        data_dicts = [link.get('href') for link in links if link.get('href').endswith('data_dict.xml')]
        if data_dicts == []:
            logging.error(f"{pheno_var_sums_url}: No data_dict.xml found")
            
        return data_dicts


def convert_xml_urls_to_csv(xml_urls, study_dir):
    try:
        output_folder = os.path.join(study_dir, "dbgap_csvs")
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        for url in xml_urls:
            response = requests.get(url)
            response.raise_for_status() 

            root = etree.fromstring(response.content)

            base_name = os.path.basename(url)
            csv_name = os.path.join(output_folder, base_name.replace(".xml", ".csv"))

            variables = root.findall(".//variable")

            with open(csv_name, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "variable_name", 
                    "description", 
                    "type", 
                    "min", 
                    "max", 
                    "units", 
                    "enumerations", 
                    "comment",
                    "dbgap_id"
                ])

                for var in variables:
                    var_id = var.get("id", "")
                    name = var.findtext("name", default="")
                    description = var.findtext("description", default="")
                    var_type = var.findtext("type", default="")
                    unit = var.findtext("unit", default="")
                    logical_min = var.findtext("logical_min", default="")
                    logical_max = var.findtext("logical_max", default="")
                    
                    coded_value_list = []
                    for val in var.findall("value"):
                        code = val.get("code", "")
                        val_text = val.text or ""
                        if code:
                            coded_value_list.append(f"{code}={val_text}")
                        else:
                            coded_value_list.append(val_text)
                    coded_values = "; ".join(coded_value_list)

                    comment = var.findtext("comment", default="")

                    writer.writerow([
                        name, 
                        description, 
                        var_type, 
                        logical_min, 
                        logical_max, 
                        unit, 
                        coded_values,
                        comment,
                        var_id 
                    ])

            print(f"Saved CSV: {csv_name}")
            logging.info(f"Saved CSV: {csv_name}")
            
        csv_names = [os.path.basename(url).replace('.xml', '.csv') for url in xml_urls]
        if csv_names:
            prefix = csv_names[0].split('.data_dict')[0] if '.data_dict' in csv_names[0] else ''
            if prefix:
                new_folder = os.path.join(study_dir, prefix)
                if os.path.exists(output_folder):
                    os.rename(output_folder, new_folder)
                    logging.info(f"Renamed output folder to: {new_folder}")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")


def main(phs_ids, study_dir):
    configure_logging(study_dir)
    for phs_id in phs_ids:
        study_url = f"https://ftp.ncbi.nlm.nih.gov/dbgap/studies/{phs_id}/"
        latest_version = get_lastest_version(study_url, phs_id)
        if latest_version is None:
            print("No latest version found for PHS ID: {phs_id}")     
        pheno_var_sums_url = f"{study_url}{latest_version}/pheno_variable_summaries/"
        data_dict_urls = []
        for data_dict_url in get_data_dict_str(pheno_var_sums_url):
            data_dict_urls.append(f"{pheno_var_sums_url}{data_dict_url}")
        convert_xml_urls_to_csv(data_dict_urls, study_dir)
        logging.info(f"Processed PHS ID: {phs_id}, Data Dict URLs: {data_dict_urls}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make a GET request to a specified URL.")
    parser.add_argument("-phs", "--phs_ids", type=str, nargs='+', required=True, help="PHS ids to scrape data dict from dbgap.")
    parser.add_argument("-dir", "--study_dir", type=str, required=True, help="Directory to save logs and data dictionaries.")
    args = parser.parse_args()
    phs_ids = args.phs_ids
    study_dir = args.study_dir
    main(phs_ids, study_dir)

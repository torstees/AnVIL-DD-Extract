# AnVIL-DD-Extract

AnVIL-DD-Extract is a Python-based tool designed to extract and process study metadata and data dictionaries from the AnVIL platform and dbGaP. It allows users to search for studies by title, retrieve relevant metadata, and convert XML-based data dictionaries into CSV format for easier analysis. Additionally, it supports creating data dictionaries from the Terra Data Repository (TDR), which is part of the AnVIL platform.

## Features

- Search studies in the DUOS index by title.
- Extract detailed metadata for matching studies.
- Retrieve and process data dictionaries from dbGaP.
- Create data dictionaries from the TDR.
- Save results in structured CSV files for further analysis.

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd AnVIL-DD-Extract
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Search for Studies

Run the `ADDE.py` script to search for studies by title and extract metadata:

```bash
python ADDE.py --query "<search-string>"
```

- **Arguments**:
  - `--query`: The search string to match study titles.

- **Output**:
  - Results are saved in the `query_results/<sanitized_study_name>` directory as `study_results.csv`.



## File Structure

- `ADDE.py`: Main script for searching studies and extracting metadata.
- `phs2dd.py`: Script for retrieving and converting data dictionaries from dbGaP.
- `tdr2dd.py`: Script for creating data dictionaries from the TDR.
- `requirements.txt`: List of Python dependencies.
- `query_results`: Directory where query results are saved.

## Example Workflow

1. Search for studies with a specific title:
   ```bash
   python ADDE.py --query "Cancer"
   ```

## Dependencies

- Python 3.7+
- pandas
- requests
- BeautifulSoup4
- lxml

Install dependencies using:
```bash
pip install -r requirements.txt
```

## Logging

Logs for data dictionary processing are saved in the specified output directory as `phs2dd.log`

## License

This project is licensed under the MIT License. See the LICENSE file for details.




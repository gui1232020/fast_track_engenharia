import pandas as pd
from pathlib import Path

# Identify the project root directory relative to the current script's location
# Navigating upwards to reach the base directory where the 'data' folder resides
project_root = Path(__file__).resolve().parent.parent.parent

# Define absolute paths for input (Raw) and output (Bronze layer) artifacts
file_path = project_root / "data" / "raw" / "jira_issues_raw.json"
output_path = project_root / "data" / "bronze_layer" / "ingest_bronze.parquet"

# Perform a file existence check to ensure robust pipeline execution and facilitate debugging
if not file_path.exists():
    print(f"Error: The source file was not found at: {file_path}")
else:
    # Read the raw JSON data as a pandas Series
    read_file = pd.read_json(file_path, typ='series')
    
    # Flatten the nested JSON structure into a normalized tabular format
    df_bronze = pd.json_normalize(read_file.loc['issues'])
    
    # Ensure the target directory exists before saving; creating it if necessary
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Persist the processed data to the Bronze layer in Parquet format
    df_bronze.to_parquet(output_path, index=False)
    print("File processed and successfully saved to the Bronze layer.")
import pandas as pd
from pathlib import Path

# Identify the project root relative to the script location
# Assuming this script is located in 'src/2.silver/'
project_root = Path(__file__).resolve().parent.parent.parent

# Define input path (Bronze layer) and output path (Silver layer)
input_path = project_root / "data" / "bronze_layer" / "ingest_bronze.parquet"
output_dir = project_root / "data" / "silver_layer"

# Ensure the output directory exists
output_dir.mkdir(parents=True, exist_ok=True)
output_path = output_dir / "transform_silver.parquet"

# Load the source Parquet file
df_file = pd.read_parquet(input_path)
df = pd.DataFrame(df_file)

# Explode nested columns to normalize data structures
df_step = df.copy()
df_step = df_step.explode('assignee')
df_step = df_step.explode('timestamps')

# Flatten JSON columns
assignee_columns = pd.json_normalize(df_step['assignee'])
timestamps_columns = pd.json_normalize(df_step['timestamps'])

# Consolidate normalized columns into a single DataFrame
df = pd.concat(
    [
        df_step.drop(columns=['assignee', 'timestamps']).reset_index(drop=True),
        assignee_columns.reset_index(drop=True),
        timestamps_columns.reset_index(drop=True)
    ],
    axis=1
)

# Clean date fields and handle invalid formats
df['created_at'] = df['created_at'].replace('2026-02-30T25:61:00Z', '2026-02-28T23:59:59Z')
df['resolved_at'] = df['resolved_at'].replace('not_a_date', None)

# Convert to proper datetime objects
df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
df['resolved_at'] = pd.to_datetime(df['resolved_at'], utc=True)

# Standardize column names
df.columns = [
    'issue_id', 'issue_type', 'issue_status', 'issue_priority', 
    'assignee_email', 'assignee_id', 'assignee_name', 
    'created_at', 'resolved_at'
]

# Persist the cleaned data to the Silver layer
df.to_parquet(output_path, index=False)
print(f"Data successfully transformed and saved to: {output_path}")
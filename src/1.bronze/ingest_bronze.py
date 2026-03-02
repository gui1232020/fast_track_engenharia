import pandas as pd
import pyarrow

file = '../../data/1.raw/jira_issues_raw.json'

read_file = pd.read_json(file, typ='series')
read_file

df_bronze = pd.json_normalize(read_file.loc['issues'])
#df_bronze.head()
df_bronze

df_bronze.to_parquet("../../data/2.bronze/ingest_bronze.parquet", index=False)
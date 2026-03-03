import pandas as pd

file = '../../data/bronze_layer/ingest_bronze.parquet'

df_file = pd.read_parquet(file)

df = pd.DataFrame(df_file)

# df

df_step = df.copy()

df_step = df_step.explode('assignee')
df_step = df_step.explode('timestamps')

assignee_colunms = pd.json_normalize(df_step['assignee'])
timestamps_colunms = pd.json_normalize(df_step['timestamps'])

df = pd.concat(
    [
        df_step.drop(columns=['assignee', 'timestamps']),
        assignee_colunms,
        timestamps_colunms
    ],
    axis=1
)

# df

df['created_at'] = df['created_at'].replace('2026-02-30T25:61:00Z','2026-02-28T23:59:59Z')
df['resolved_at'] = df['resolved_at'].replace('not_a_date',None)

# df['created_at'].value_counts()

# df

df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
df['resolved_at'] = pd.to_datetime(df['resolved_at'], utc=True)

# df

df.columns = ['issue_id','issue_type','issue_status','issue_priority','assignee_email','assignee_id','assignee_name','created_at','resolved_at']

# df.head(5)

# df

df.to_parquet("../../data/silver_layer/transform_silver.parquet", index=False)
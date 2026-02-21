import pandas as pd
import json
from pathlib import Path

# Pasta raiz do projeto (fast_track_engenharia)
SCRIPT_DIR = Path(__file__).resolve().parent

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_JSON = PROJECT_ROOT / "data" / "1.raw" / "jira_issues_raw.json"

print("Abrindo:", RAW_JSON)

with open(RAW_JSON, encoding="utf-8") as f:
    data = json.load(f)

#df_bronze = pd.DataFrame(data["issues"])

#OUTPUT_CSV = SCRIPT_DIR / "ingest_bronze.csv"
#df_bronze.to_csv(OUTPUT_CSV, index=False)

#print("CSV salvo em:", OUTPUT_CSV)

df_bronze = pd.DataFrame(data["issues"])

OUTPUT_CSV = PROJECT_ROOT / "data" / "2.bronze" / "ingest_bronze.csv"
df_bronze.to_csv(OUTPUT_CSV, index=False)

print("CSV salvo em:", OUTPUT_CSV)
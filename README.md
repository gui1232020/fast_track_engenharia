# SLA Data Pipeline – Jira Issues

This project implements a Medallion Architecture (Bronze, Silver, Gold) data pipeline to calculate and analyze SLA (Service Level Agreement) metrics for Jira issues.

The pipeline performs:

1. Raw data ingestion (JSON)
2. Data transformation and normalization
3. Business calendar generation with Brazilian holidays
4. SLA calculation based on business days
5. Analytical table generation (Gold Layer)

---

# Project Architecture

```
data/
│
├── raw/
│   └── jira_issues_raw.json
│
├── bronze_layer/
│   └── ingest_bronze.parquet
│
├── silver_layer/
│   ├── transform_silver.parquet
│   └── silver_holidays.parquet
│
└── gold_layer/
    ├── gold_sla_issues.csv
    ├── gold_sla_by_analyst.csv
    └── gold_sla_by_issue_type.csv
```

---

# Bronze Layer – Data Ingestion

## Objective
- Read raw Jira JSON data
- Normalize nested structures
- Persist structured data in Parquet format

## Input
`data/raw/jira_issues_raw.json`

## Output
`data/bronze_layer/ingest_bronze.parquet`

## Key Steps
- File existence validation
- `pd.read_json()`
- `pd.json_normalize()`
- Parquet persistence

---

# Silver Layer – Data Transformation

## Objective
- Normalize nested columns (`assignee`, `timestamps`)
- Flatten JSON structures
- Clean and standardize date fields
- Enforce structured schema

## Input
`data/bronze_layer/ingest_bronze.parquet`

## Output
`data/silver_layer/transform_silver.parquet`

## Transformations Applied
- `explode()` for nested columns
- `pd.json_normalize()`
- Invalid date correction
- UTC datetime conversion
- Column renaming and standardization

### Final Schema

| Column           | Type      |
|------------------|-----------|
| issue_id         | string    |
| issue_type       | string    |
| issue_status     | string    |
| issue_priority   | string    |
| assignee_email   | string    |
| assignee_id      | string    |
| assignee_name    | string    |
| created_at       | datetime  |
| resolved_at      | datetime  |

---

# Business Calendar – Silver Layer

## Objective
Generate a business calendar dimension including:

- Day of week
- Brazilian national holidays (2025–2026)
- Business day flag

## Business Logic
A date is considered a business day if:
- It is not Saturday
- It is not Sunday
- It is not a national holiday

## Output
`data/silver_layer/silver_holidays.parquet`

### Columns

| Column        | Description |
|--------------|------------|
| date         | Calendar date |
| day_of_week  | Day name |
| holiday      | 1 = Holiday |
| business_day | 1 = Business day |

---

# SLA Calculation – Gold Business Logic

## SLA Rules by Priority

| Priority | Expected SLA |
|----------|--------------|
| High     | 24 hours     |
| Medium   | 72 hours     |
| Low      | 120 hours    |

## Core Functions

### `get_sla_expected_hours(priority)`
Returns expected SLA hours based on issue priority.

### `calculate_business_hours(...)`
Calculates resolution time considering:
- Only business days
- Configurable working hours window
- UTC timezone consistency

### `check_sla_compliance(...)`
Returns:

```python
{
    "resolution_hours": float,
    "sla_expected_hours": int,
    "is_sla_met": bool
}
```

---

# Gold Layer – Analytical Build

## Objective
Generate analytical tables for BI and reporting consumption.

## Processing Steps
1. Load Silver layer data
2. Filter issues with status:
   - Done
   - Resolved
3. Apply SLA calculation logic
4. Generate aggregated analytical tables

---

## Generated Outputs

### SLA per Issue
`gold_sla_issues.csv`

Includes:
- issue_id
- issue_type
- assignee_name
- issue_priority
- created_at
- resolved_at
- resolution_hours
- sla_expected_hours
- is_sla_met

---

### Average SLA per Analyst
`gold_sla_by_analyst.csv`

- issue_count
- avg_sla_hours

---

### Average SLA per Issue Type
`gold_sla_by_issue_type.csv`

- issue_count
- avg_sla_hours

---

# Pipeline Execution Order

```bash
python bronze_layer.py
python silver_layer.py
python holidays_layer.py
python build_gold_layer.py
```

---

# Concepts Applied

- Medallion Architecture
- Data Lake structure
- JSON normalization
- Business calendar logic
- SLA calculation based on working days
- Data Engineering with Pandas
- Parquet optimization
- Analytical aggregations

---

# Possible Future Improvements

- Parameterized business hours (e.g., 08:00–18:00)
- SLA segmentation by service type
- Incremental processing
- Orchestration (Airflow / Prefect)
- Unit testing for SLA logic
- CI/CD integration

---

# Author

Developed for Data Engineering and SLA analytics practice.
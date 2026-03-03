import pandas as pd
from pathlib import Path
from datetime import datetime
import holidays

# Define the timeframe for the calendar dimension table
start_date = "2025-01-01"
end_date = datetime.now().strftime("%Y-%m-%d")
dates = pd.date_range(start=start_date, end=end_date, freq="D")

# Initialize the DataFrame with the generated time series
df = pd.DataFrame({"date": dates})

# Add the day name for easier filtering and time-series analysis
df["day_of_week"] = df["date"].dt.day_name()

# Load Brazilian national holidays for 2025 and 2026
br_holidays = holidays.Brazil(years=[2025, 2026])

# Flag holidays: 1 if the date is a holiday, 0 otherwise
holiday_list = []
for date in df["date"]:
    if date in br_holidays:
        holiday_list.append(1)
    else:
        holiday_list.append(0)

df["holiday"] = holiday_list

# Define business logic for working days
# A day is considered a business day (1) only if it is not a weekend 
# (Saturday/Sunday) and not a holiday. This is critical for SLA calculations.
def is_business_day(row):
    if row["date"].weekday() >= 5:  # Saturday (5) or Sunday (6)
        return 0
    if row["holiday"] == 1:
        return 0
    return 1

df["business_day"] = df.apply(is_business_day, axis=1)

# Configure the output path for the Silver layer of the Data Lake
# Ensures the directory structure exists before persisting the file
dir = Path(__file__).resolve().parent
out_path = dir.parent.parent / "data" / "silver_layer"
out_path.mkdir(parents=True, exist_ok=True)
file_path = out_path / "silver_holidays.parquet"

# Export the processed DataFrame to Parquet format for optimized storage and retrieval
df.to_parquet(file_path, index=False)
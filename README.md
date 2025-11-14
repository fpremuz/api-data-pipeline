# API Data Pipeline

A Data Engineering practice project that extracts, transforms, and stores data from the **Alpha Vantage API** using **Python**, **pandas**, and **Delta Lake** (locally or in MinIO).

It demonstrates **incremental (dynamic)** and **full-overwrite (static)** ingestion, **schema evolution**, **Z-Ordering**, **compaction**, **time travel** and a full **Medallion Architecture (Bronze → Silver → Gold)**.

---
## Project Structure
```
project/
│
├── main.py # Main Python script
├── requirements.txt # Project dependencies
├── .gitignore # Files ignored by Git
├── venv/ # Virtual environment (not included in Git)
├── config/
│   ├── api.conf      # Alpha Vantage API configuration
│   └── storage.conf  # MinIO storage configuration
│
├── data/                # Local Delta Lake (if MinIO not configured)
│   └── bronze/
│   ├── silver/
│   └── gold/
|
├── maintenance/             # NEW — maintenance scripts
│   ├── vacuum_tables.py
│   └── optimize_tables.py
│
└── README.md # Documentation
```

## Setup Instructions

### 1. Create and activate the virtual environment

```bash
python -m venv venv
.\venv\Scripts\activate     # Windows
source venv/bin/activate    # Linux/macOS
```

### 2. Install dependencies
```
python -m pip install -r requirements.txt
```

### 3. Configure API and storage
Create a file named api.conf in a config folder:
config/api.conf
```
[alphavantage]
base_url = https://www.alphavantage.co/query
api_key = YOUR_API_KEY
```
config/storage.conf:
```
[minio]
AWS_ENDPOINT_URL = http://31.97.241.212:9002
AWS_ACCESS_KEY_ID = YOUR_ACCESS_KEY
AWS_SECRET_ACCESS_KEY = YOUR_SECRET_KEY
AWS_ALLOW_HTTP = true
aws_conditional_put = etag
AWS_S3_ALLOW_UNSAFE_RENAME = true
bucket_name = YOUR_BUCKET
```
Make sure the config folder is listed in .gitignore to avoid exposing your keys.
If config/storage.conf is not provided, the script will store Delta tables locally in ./data/bronze, ./data/silver, ./data/gold.

### 4. Run the main script
```
python main.py
```

The script performs:

**Dynamic extraction (incremental) – Bitcoin daily prices (BTC/USD)**

Loads historical prices for Bitcoin

Updates incrementally using Delta Lake upsert.

Automatically creates the table schema if it doesn’t exist

Adds constraint: close > 0

Stored partitioned by date.

**Static extraction (full overwrite) – USD/EUR exchange rate**

Automatically creates the table schema if it doesn’t exist

Removes duplicated columns before writing


🧱 **Delta Lake Operations**

Each ingestion step performs:

Schema evolution: automatically adapts to new columns

Compaction: merges small files for better performance

Z-Ordering: optimizes data skipping by sorting on the date column

Version tracking & time travel

💾 **Data Lake Tiers**
Tier	Description	Example
```
Bronze	Raw extracted data (incremental updates)	crypto_daily/
Silver	Cleaned and normalized data	crypto_daily_clean/
Gold	Aggregated or analytical data	crypto_daily_summary/
```

🧩 **Silver Layer (Data Cleansing)**

Removes null values and invalid records

Keeps only valid prices (close > 0)

Deduplicates columns for static data

🪙 **Gold Layer (Aggregation)**

Aggregates monthly Bitcoin statistics (avg, max, min close)

Keeps the latest USD/EUR exchange rate snapshot

🧠 **Notes on Extraction Types**

Full Extraction: The entire dataset is replaced every run (used for static data).

Incremental Extraction: Only new or updated records are merged (used for dynamic data).

🔧 **Maintenance Scripts (VACUUM / OPTIMIZE)**

Delta Lake maintenance must run separately from data ingestion.
This is why we store them in:
```
maintenance/
  vacuum_tables.py
  optimize_tables.py
```
Why separate these scripts?

Because:

ETL should not trigger heavy operations like VACUUM or OPTIMIZE

These tasks are often scheduled (e.g., weekly, nightly)

They may run on different hardware

They rewrite files and could interfere with ingestion

Script 1 — vacuum_tables.py

Performs:

Remove old files beyond retention threshold

Cleanup unused files after merges

Improve lake hygiene
Script 2 — optimize_tables.py

Performs:

File compaction

Z-Ordering (date) for skipping optimization

🧩 **Features**

Extracts two API endpoints:

Dynamic (incremental) → Bitcoin daily prices (DIGITAL_CURRENCY_DAILY)

Static (full overwrite) → USD/EUR exchange rate (CURRENCY_EXCHANGE_RATE)

Cleans and normalizes data automatically.

Writes in Delta Lake format with partitioning.

Supports both local and MinIO S3-compatible storage.

Ensures idempotency — re-running the pipeline won’t duplicate records.

Adds data constraints (e.g. close > 0 for valid prices).

Implements Bronze → Silver → Gold pipeline

🧰 **Technologies Used**
```
Python 3.12+
pandas
requests
pyarrow
deltalake
configparser
```

🧠 **Concepts Practiced**

Data Extraction (API)

Incremental vs Full Loads

Medallion Architecture (Bronze / Silver / Gold)

Delta Lake and ACID tables

Data validation and constraints

Configuration-driven pipelines
## Extracted Endpoints
### 1. Dynamic Endpoint (Incremental Extraction)

Function: TIME_SERIES_DAILY

Description: Retrieves daily stock or cryptocurrency price data (e.g., BTC/USD). This data updates every day — therefore, only new data since the last extraction would be added in a real pipeline.

Extraction Type: Incremental

Output Example:
```
datetime       open       high        low      close     volume
2025-10-29  112906.75  112917.79  112646.71  112766.46   85.47
```

### 2. Static Endpoint (Full Extraction)

Function: CURRENCY_EXCHANGE_RATE

Description: Returns the current exchange rate between two currencies (e.g., USD/EUR). This data represents the current state and is always replaced in each run.

Extraction Type: Full

Output Example:
```
Code  Name                  Code  Name  Rate      Refreshed             Zone
USD   United States Dollar  EUR   Euro  0.8616    2025-10-29 21:08:20   UTC
```

Notes on Extraction Types

Full Extraction: The entire dataset is replaced each time, regardless of previous runs.
→ Used for static or low-volume data (e.g., metadata, current prices).

Incremental Extraction: Only new or updated records are added since the last extraction.
→ Used for frequently updated data (e.g., time series, logs, transactions).

Example Output

When you run main.py, you’ll see two formatted tables printed in the console:
```
Dynamic Extraction: Bitcoin Daily Prices (BTC/USD)
| datetime   | open   | high   | low   | close  | volume  |
|------------|--------|--------|-------|--------|---------|
| 2025-10-29 | ...    | ...    | ...   | ...    | ...     |
============================================================
Static Extraction: USD/EUR Exchange Rate
| Code | Name | Code | Name | Rate | Refreshed | Zone |
|------|------|------|------|------|-----------|------|
| USD  | ...  | EUR  | ...  | ...  | ...       | UTC  |
```

🧹 .gitignore

Make sure sensitive files and virtual environments are excluded:
```
venv/
config/
__pycache__/
*.pyc
*.log
*.tmp
```

# API Data Pipeline

A Data Engineering practice project that extracts, transforms, and stores data from the **Alpha Vantage API** using **Python**, **pandas**, and **Delta Lake** (locally or in MinIO).

This version demonstrates both **incremental** and **full** extractions, as well as Delta Lake concepts such as **upserts**, **partitions**, **constraints**, and multi-tier data organization (**Bronze / Silver / Gold**).

---
## Project Structure
```
project/
│
├── main.py # Main Python script
├── requirements.txt # Project dependencies
├── .gitignore # Files ignored by Git
├── venv/ # Virtual environment (not included in Git)
└── README.md # Documentation
```

## Setup Instructions

### 1. Activate the virtual environment

```bash
.\venv\Scripts\activate
```

### 2. Install dependencies
```
python -m pip install -r requirements.txt
```

### 3. Configure API and storage
Create a file named pipeline.conf in the project root:
```
[alphavantage]
base_url = https://www.alphavantage.co/query
api_key = YOUR_API_KEY

[minio]
AWS_ENDPOINT_URL = http://31.97.241.212:9002
AWS_ACCESS_KEY_ID = YOUR_ACCESS_KEY
AWS_SECRET_ACCESS_KEY = YOUR_SECRET_KEY
AWS_ALLOW_HTTP = true
aws_conditional_put = etag
AWS_S3_ALLOW_UNSAFE_RENAME = true
bucket_name = YOUR_BUCKET
```
Make sure pipeline.conf is listed in .gitignore to avoid exposing your key.
If [minio] is not provided, the script will store Delta tables locally in ./data/bronze.

### 4. Run the main script
```
python main.py
```

The script performs:

Dynamic extraction (incremental) – Bitcoin daily prices (BTC/USD)

Updates incrementally using Delta Lake upsert.

Stored partitioned by date.

Bronze → Silver → Gold data tiers.

Static extraction (full) – USD/EUR exchange rate

Full overwrite on each run.

💾 Data Lake Tiers
Tier	Description	Example
Bronze	Raw extracted data (incremental updates)	crypto_daily/
Silver	Cleaned and normalized data	crypto_daily_clean/
Gold	Aggregated or analytical data	crypto_daily_summary/

🧠 Notes on Extraction Types

Full Extraction: The entire dataset is replaced every run (used for static data).

Incremental Extraction: Only new or updated records are merged (used for dynamic data).

🧰 Technologies Used
```
Python 3.12+
pandas
requests
pyarrow
deltalake
configparser
```
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

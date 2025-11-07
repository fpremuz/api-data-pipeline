# API Data Pipeline

A small Data Engineering practice project that extracts and transforms data from the **Alpha Vantage API**, using Python and pandas.

This version focuses on **data extraction only**, demonstrating both **incremental** and **full** extractions from two different endpoints — one dynamic and one static.

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

### 3. Set your API key
Create a file named .env in the project root and add your Alpha Vantage API key:
```
ALPHAVANTAGE_API_KEY=your_api_key_here
```
Make sure .env is listed in .gitignore to avoid exposing your key.

### 4. Run the main script
```
python main.py
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

Technologies Used:
```
Python 3.12+
pandas
requests
configparser
```

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

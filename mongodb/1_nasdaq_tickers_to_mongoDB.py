"""
This code imports all NASDAQ tickers from EODHD, we use this as we can get the entire exchange and then use different brokers, if needed at a later date.
This imports into MongoDB.
It uses re as we use regex.
Requests is for the URL fetch.
pymongo is for the insert to the collection.


url = 'https://eodhd.com/api/exchange-symbol-list/NASDAQ?api_token=&fmt=json'

insert your API token in this line

I excluded a load of things I'm not interested in, preferred stock doesn't have the same liquidity and ETFs aren't my bag. Blocking funds
is obvious.

"""
import re
import requests
from pymongo import MongoClient

# Any of these words (case-insensitive) in the name => skip the ticker
EXCLUDED_NAME_KEYWORDS = [
    "Direxion",
    "GraniteShares",
    "ProShares",
    "iShares",
    "Leverage Shares",
    "Invesco"
    "Warrants"
    # "velocityshares",
]

# Any of these words (case-insensitive) in the type => skip the ticker
EXCLUDED_TYPE_KEYWORDS = [
    "Fund",
    "ETF",
    "Preferred Stock",
    # add more here, e.g.:
    # "ETF",
    # "ETN",
]

# Precompile regex patterns for performance, case-insensitive
EXCLUDED_NAME_PATTERN = re.compile(
    "|".join(re.escape(kw) for kw in EXCLUDED_NAME_KEYWORDS),
    re.IGNORECASE,
)
EXCLUDED_TYPE_PATTERN = re.compile(
    "|".join(re.escape(kw) for kw in EXCLUDED_TYPE_KEYWORDS),
    re.IGNORECASE,
)

# Step 1: Fetch data from EODHD
url = 'https://eodhd.com/api/exchange-symbol-list/NASDAQ?api_token=&fmt=json'
data = requests.get(url).json()

# Step 2: Convert all dictionary keys to lowercase
lowered_data = [{k.lower(): v for k, v in item.items()} for item in data]

print(f"Fetched {len(lowered_data)} records from EODHD.")

# Step 2b: Filter out records whose 'name' or 'type' matches excluded keywords
filtered_data = []
excluded_count = 0

for item in lowered_data:
    name = item.get("name", "") or ""
    typ = item.get("type", "") or ""

    if EXCLUDED_NAME_PATTERN.search(name) or EXCLUDED_TYPE_PATTERN.search(typ):
        excluded_count += 1
        continue

    filtered_data.append(item)

print(f"Excluded {excluded_count} records based on name/type keywords.")
print(f"Preparing to insert {len(filtered_data)} records.")

# Step 3: Connect to MongoDB
client = MongoClient("mongodb://192.168.1.126:27017/")
db = client["trading_data"]
collection = db["nasdaq_production_tickers"]

# Step 4: Clear existing contents
delete_result = collection.delete_many({})
print(f"Cleared {delete_result.deleted_count} existing documents.")

# Step 5: Insert filtered data
if filtered_data:
    collection.insert_many(filtered_data)
    print(f"Inserted {len(filtered_data)} records into MongoDB.")
else:
    print("No data to insert after filtering.")

#IMPORTING

import os, json
import requests
from dotenv import load_dotenv

#laoding .env
load_dotenv()

TOKEN = os.getenv("VENDOR_1_TOKEN")
API_URL = os.getenv("VENDOR_1_URL", "http://127.0.0.1:8000/Vendor1/Insurance")
OUT_FILE = os.getenv("OUT_NDJSON", "vendor1_insurance.ndjson")

# getting url and verifying headers for correct token
resp = requests.get(
    API_URL,
    headers={"Authorization": f"Bearer {TOKEN}"},
    timeout=60
)
resp.raise_for_status()

payload = resp.json()
#dict list
records = payload["records"] 

with open(OUT_FILE, "w", encoding="utf-8") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")

print(f"Wrote {len(records)} rows to {OUT_FILE}")

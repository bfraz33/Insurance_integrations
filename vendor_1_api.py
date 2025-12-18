#imports

import os
from fastapi import FastAPI, Header, HTTPException, Query
from dotenv import load_dotenv
import pandas as pd

#loading .env
load_dotenv()


app = FastAPI(title="Vendor 1 data (JSON)", version="1.0")

# secuirty token in env variables
TOKEN = os.getenv("VENDOR_1_TOKEN")
if not TOKEN:
    raise RuntimeError("VENDOR_1_TOKEN is not set")

# file path and directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, os.getenv("DATA_PATH", "data/insurance.csv"))

#script checking the token with error handling
def check_token(authorization: str | None):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Use: Authorization: Bearer <token>")

    if parts[1] != TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")

#api home
@app.get("/")
def home():
    return {
        "message": "Vendor1 API is running. Go to /Vendor1/Insurance for data"
    }

#api data 
@app.get("/Vendor1/Insurance")
def get_insurance_data(
    authorization: str | None = Header(default=None)
):
    check_token(authorization)

    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=500, detail=f"CSV not found at {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)

    return {
        "vendor": "Vendor1",
        "count": len(df),
        "records": df.to_dict(orient="records"),
    }

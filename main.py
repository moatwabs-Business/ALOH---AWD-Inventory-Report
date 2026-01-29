import os
import json
import time
import requests
import pandas as pd
import numpy as np
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone, timedelta

# ================= CONFIG =================

SPREADSHEET_NAME = "Inventory Analysis-ALOH-v1" 
WORKSHEET_NAME = "TEST"      

MAX_GSPREAD_RETRIES = 5

# ================= AMAZON SECRETS =================

AMAZON_REFRESH_TOKEN = os.environ["AMAZON_REFRESH_TOKEN"]
AMAZON_LWA_CLIENT_ID = os.environ["AMAZON_LWA_CLIENT_ID"]
AMAZON_LWA_CLIENT_SECRET = os.environ["AMAZON_LWA_CLIENT_SECRET"]

# ================= GOOGLE AUTH =================

service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(
    service_account_info, scopes=SCOPES
)

gs_client = gspread.authorize(creds)
print("✅ Google authenticated")

# ================= STEP 1 — AMAZON TOKEN =================

token_response = requests.post(
    "https://api.amazon.com/auth/o2/token",
    data={
        "grant_type": "refresh_token",
        "refresh_token": AMAZON_REFRESH_TOKEN,
        "client_id": AMAZON_LWA_CLIENT_ID,
        "client_secret": AMAZON_LWA_CLIENT_SECRET,
    },
    timeout=30
)

token_response.raise_for_status()
access_token = token_response.json()["access_token"]

print("✅ Amazon access token received")

# ================= STEP 2 — AMAZON AWD INVENTORY =================

inventory_response = requests.get(
    "https://sellingpartnerapi-na.amazon.com/awd/2024-05-09/inventory",
    headers={"x-amz-access-token": access_token},
    timeout=60
)

inventory_response.raise_for_status()

inventory = inventory_response.json()["inventory"]

df = pd.DataFrame(inventory)

print(f"✅ Amazon data loaded: {df.shape[0]} rows, {df.shape[1]} columns")

# ================= ADD EXTRACTION TIMESTAMP (EST UTC-5) =================

EST_TZ = timezone(timedelta(hours=-5))
extracted_at = datetime.now(EST_TZ).strftime("%Y-%m-%d %H:%M:%S")
df["Extracted At (EST)"] = extracted_at

print(f"🕒 Extraction timestamp added (EST): {extracted_at}")

# ================= STEP 3 — CLEAN DATA =================

df = df.replace([np.inf, -np.inf], "")
df = df.fillna("")

print("✅ Cleaned NaN and Inf values")

# ================= STEP 4 — GOOGLE SHEET UPDATE (FORMATTING SAFE + 503 SAFE) =================

data = [df.columns.tolist()] + df.values.tolist()

for attempt in range(MAX_GSPREAD_RETRIES):
    try:
        spreadsheet = gs_client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

        # Clear ONLY values (not formatting/formulas)
        print("🧹 Clearing old values (preserving formatting & formulas)...")
        worksheet.batch_clear(["A2:Z100000"])

        print("⬆️ Uploading data to Google Sheets...")
        worksheet.update(
            values=data,
            range_name="A1",
            value_input_option="USER_ENTERED"
        )

        print(f"🎉 Google Sheet updated with {len(df)} rows")
        break

    except APIError as e:
        status = getattr(e.response, "status_code", None)

        if status == 503:
            wait = 2 ** attempt
            print(f"⚠️ Google API 503. Retrying in {wait} seconds... (attempt {attempt+1}/{MAX_GSPREAD_RETRIES})")
            time.sleep(wait)
        else:
            print(f"❌ Google API error: {e}")
            raise

else:
    raise Exception("❌ Failed to update Google Sheet after multiple retries (503 errors)")

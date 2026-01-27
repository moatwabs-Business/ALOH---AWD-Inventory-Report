import os
import json
import requests
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials

# ================= CONFIG =================
SPREADSHEET_NAME = "Inventory Analysis-ALOH-v1"  
WORKSHEET_NAME = "TEST"      

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

# ================= STEP 2 — AMAZON INVENTORY =================
inventory_response = requests.get(
    "https://sellingpartnerapi-na.amazon.com/awd/2024-05-09/inventory",
    headers={"x-amz-access-token": access_token},
    timeout=60
)

inventory_response.raise_for_status()

inventory = inventory_response.json()['inventory']

# 🔴 FLATTEN NESTED JSON (CRITICAL)
df = pd.DataFrame(inventory)


# ================= STEP 3 — CLEAN DATA =================
df = df.replace([np.inf, -np.inf], "")
df = df.fillna("")

print("✅ Cleaned NaN/Inf")

# ================= STEP 4 — GOOGLE SHEET OVERWRITE =================
spreadsheet = gs_client.open(SPREADSHEET_NAME)
worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

worksheet.clear()

data = [df.columns.tolist()] + df.values.tolist()

# ✅ New gspread signature (no warning)
worksheet.update(
    values=data,
    range_name="A1",
    value_input_option="USER_ENTERED"
)

print(f"🎉 Google Sheet updated with {len(df)} rows")

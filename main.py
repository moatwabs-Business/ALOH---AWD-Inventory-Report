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

AWD_WORKSHEET_NAME = "AWD Data API Request"
FBA_WORKSHEET_NAME = "Amazon Data  API"

MAX_GSPREAD_RETRIES = 5


# ================= AMAZON SECRETS =================

AMAZON_REFRESH_TOKEN = os.environ["AMAZON_REFRESH_TOKEN"]
AMAZON_LWA_CLIENT_ID = os.environ["AMAZON_LWA_CLIENT_ID"]
AMAZON_LWA_CLIENT_SECRET = os.environ["AMAZON_LWA_CLIENT_SECRET"]


# ================= GOOGLE AUTH =================

service_account_info = json.loads(
    os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=SCOPES
)

gs_client = gspread.authorize(creds)

print("✅ Google authenticated")


# ================= STEP 1 — GET AMAZON ACCESS TOKEN =================

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


# ================= STEP 2 — GET AWD INVENTORY =================

awd_response = requests.get(
    "https://sellingpartnerapi-na.amazon.com/awd/2024-05-09/inventory",
    headers={"x-amz-access-token": access_token},
    timeout=60
)

awd_response.raise_for_status()

awd_inventory = awd_response.json()["inventory"]

df_awd = pd.DataFrame(awd_inventory)

print(f"✅ AWD data loaded: {df_awd.shape[0]} rows")


# ================= STEP 3 — GET FBA INVENTORY =================

fba_response = requests.get(
    "https://sellingpartnerapi-na.amazon.com/fba/inventory/v1/summaries",
    headers={"x-amz-access-token": access_token},
    params={
        "details": "true",
        "granularityType": "Marketplace",
        "granularityId": "ATVPDKIKX0DER",
        "marketplaceIds": "ATVPDKIKX0DER"
    },
    timeout=60
)

fba_response.raise_for_status()

fba_inventory = fba_response.json()['payload']["inventorySummaries"]

fba_records = []

for item in fba_inventory:

    inventory = item.get("inventoryDetails", {})
    reserved = inventory.get("reservedQuantity", {})

    record = {

        "sellerSku": item.get("sellerSku", ""),

        "asin": item.get("asin", ""),

        "Inventory Supply at FBA":
            inventory.get("fulfillableQuantity", 0),

        "Reserved FC Processing":
            reserved.get("fcProcessingQuantity", 0),

        "Reserved Customer Order":
            reserved.get("customerOrderQuantity", 0)

    }

    fba_records.append(record)

df_fba = pd.DataFrame(fba_records)

print(f"✅ FBA data loaded: {df_fba.shape[0]} rows")


# ================= STEP 4 — ADD EXTRACTION TIMESTAMP =================

EST_TZ = timezone(timedelta(hours=-5))

extracted_at = datetime.now(EST_TZ).strftime("%Y-%m-%d %H:%M:%S")

df_awd["Extracted At (EST)"] = extracted_at
df_fba["Extracted At (EST)"] = extracted_at

print(f"🕒 Extraction timestamp added: {extracted_at} EST")


# ================= STEP 5 — CLEAN DATA =================

df_awd = df_awd.replace([np.inf, -np.inf], "")
df_awd = df_awd.fillna("")

df_fba = df_fba.replace([np.inf, -np.inf], "")
df_fba = df_fba.fillna("")

print("✅ Data cleaned")


# ================= STEP 6 — UPLOAD AWD DATA =================

awd_data = [df_awd.columns.tolist()] + df_awd.values.tolist()

for attempt in range(MAX_GSPREAD_RETRIES):

    try:

        spreadsheet = gs_client.open(SPREADSHEET_NAME)

        worksheet = spreadsheet.worksheet(AWD_WORKSHEET_NAME)

        print("🧹 Clearing AWD sheet...")

        worksheet.batch_clear(["A1:Z100000"])

        print("⬆️ Uploading AWD data...")

        worksheet.update(
            values=awd_data,
            range_name="A1",
            value_input_option="USER_ENTERED"
        )

        worksheet.update(
            "H1",
            f"Last Extracted At: {extracted_at} EST"
        )

        print(f"🎉 AWD sheet updated ({len(df_awd)} rows)")

        break

    except APIError as e:

        status = getattr(e.response, "status_code", None)

        if status == 503:

            wait = 2 ** attempt

            print(f"⚠️ Retry AWD upload in {wait}s")

            time.sleep(wait)

        else:
            raise

else:
    raise Exception("❌ Failed AWD upload")


# ================= STEP 7 — UPLOAD FBA DATA =================

fba_data = [df_fba.columns.tolist()] + df_fba.values.tolist()

for attempt in range(MAX_GSPREAD_RETRIES):

    try:

        spreadsheet = gs_client.open(SPREADSHEET_NAME)

        worksheet = spreadsheet.worksheet(FBA_WORKSHEET_NAME)

        print("🧹 Clearing Amazon Data API sheet...")

        worksheet.batch_clear(["A1:Z100000"])

        print("⬆️ Uploading FBA data...")

        worksheet.update(
            values=fba_data,
            range_name="A1",
            value_input_option="USER_ENTERED"
        )

        worksheet.update(
            "H1",
            f"Last Extracted At: {extracted_at} EST"
        )

        print(f"🎉 Amazon Data API sheet updated ({len(df_fba)} rows)")

        break

    except APIError as e:

        status = getattr(e.response, "status_code", None)

        if status == 503:

            wait = 2 ** attempt

            print(f"⚠️ Retry FBA upload in {wait}s")

            time.sleep(wait)

        else:
            raise

else:
    raise Exception("❌ Failed FBA upload")


print("🚀 FULL AMAZON PIPELINE COMPLETED SUCCESSFULLY")

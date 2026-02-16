import os
import json
import time
import gzip
import io
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

MARKETPLACE_ID = "ATVPDKIKX0DER"


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


# ================= GET AMAZON ACCESS TOKEN =================

def get_access_token():

    response = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": AMAZON_REFRESH_TOKEN,
            "client_id": AMAZON_LWA_CLIENT_ID,
            "client_secret": AMAZON_LWA_CLIENT_SECRET,
        },
        timeout=30
    )

    response.raise_for_status()

    return response.json()["access_token"]


access_token = get_access_token()

print("✅ Amazon access token received")


# ================= GET AWD INVENTORY =================

def get_awd_inventory():

    print("📦 Getting AWD inventory...")

    response = requests.get(
        "https://sellingpartnerapi-na.amazon.com/awd/2024-05-09/inventory",
        headers={"x-amz-access-token": access_token},
        timeout=60
    )

    response.raise_for_status()

    df = pd.DataFrame(response.json()["inventory"])

    print(f"✅ AWD rows: {len(df)}")

    return df


# ================= GET FBA INVENTORY =================

def get_fba_inventory():

    print("📦 Getting FBA inventory...")

    response = requests.get(
        "https://sellingpartnerapi-na.amazon.com/fba/inventory/v1/summaries",
        headers={"x-amz-access-token": access_token},
        params={
            "details": "true",
            "granularityType": "Marketplace",
            "granularityId": MARKETPLACE_ID,
            "marketplaceIds": MARKETPLACE_ID
        },
        timeout=60
    )

    response.raise_for_status()

    records = []

    for item in response.json()['payload']["inventorySummaries"]:

        inventory = item.get("inventoryDetails", {})
        reserved = inventory.get("reservedQuantity", {})

        records.append({

            "sellerSku": item.get("sellerSku", ""),
            "asin": item.get("asin", ""),

            "Inventory Supply at FBA":
                inventory.get("fulfillableQuantity", 0),

            "Reserved FC Processing":
                reserved.get("fcProcessingQuantity", 0),

            "Reserved Customer Order":
                reserved.get("customerOrderQuantity", 0)
        })

    df = pd.DataFrame(records)

    print(f"✅ FBA rows: {len(df)}")

    return df


# ================= GET UNITS SHIPPED T30 =================

def get_units_shipped_t30():

    print("📊 Requesting Sales and Traffic report...")

    create_report = requests.post(
        "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports",
        headers={
            "x-amz-access-token": access_token,
            "Content-Type": "application/json"
        },
        json={
            "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
            "marketplaceIds": [MARKETPLACE_ID],
            "reportOptions": {
                "reportPeriod": "DAY",
                "asinGranularity": "SKU"
            }
        }
    )

    create_report.raise_for_status()

    report_id = create_report.json()["reportId"]

    print(f"📄 Report ID: {report_id}")

    while True:

        status_response = requests.get(
            f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}",
            headers={"x-amz-access-token": access_token}
        )

        status_response.raise_for_status()

        status_data = status_response.json()

        status = status_data["processingStatus"]

        print(f"⏳ Status: {status}")

        if status == "DONE":

            document_id = status_data["reportDocumentId"]
            break

        elif status in ["FATAL", "CANCELLED"]:
            raise Exception("Report failed")

        time.sleep(10)

    doc_response = requests.get(
        f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{document_id}",
        headers={"x-amz-access-token": access_token}
    )

    doc_response.raise_for_status()

    download_url = doc_response.json()["url"]

    print("⬇️ Downloading report...")

    compressed = requests.get(download_url)

    compressed.raise_for_status()

    buffer = io.BytesIO(compressed.content)

    with gzip.GzipFile(fileobj=buffer) as gz:
        content = gz.read().decode("utf-8")

    df = pd.read_csv(io.StringIO(content), sep="\t")

    df_units = df[[
        "seller-sku",
        "units-shipped-t30"
    ]].copy()

    df_units.rename(columns={
        "seller-sku": "sellerSku",
        "units-shipped-t30": "Units Shipped T30"
    }, inplace=True)

    print(f"✅ Units Shipped rows: {len(df_units)}")

    return df_units


# ================= EXTRACTION DATE =================

EST_TZ = timezone(timedelta(hours=-5))

extracted_at = datetime.now(EST_TZ).strftime("%Y-%m-%d")


# ================= GET DATA =================

df_awd = get_awd_inventory()

df_fba = get_fba_inventory()

df_units = get_units_shipped_t30()


# ================= MERGE =================

df_fba = df_fba.merge(
    df_units,
    on="sellerSku",
    how="left"
)

df_fba["Units Shipped T30"] = df_fba["Units Shipped T30"].fillna(0)

df_fba["Extracted At"] = extracted_at

df_awd["Extracted At"] = extracted_at


# ================= CLEAN =================

df_fba = df_fba.replace([np.inf, -np.inf], "").fillna("")

df_awd = df_awd.replace([np.inf, -np.inf], "").fillna("")


# ================= UPLOAD FUNCTION =================

def upload_to_sheet(name, df):

    data = [df.columns.tolist()] + df.values.tolist()

    for attempt in range(MAX_GSPREAD_RETRIES):

        try:

            sheet = gs_client.open(SPREADSHEET_NAME).worksheet(name)

            sheet.batch_clear(["A1:Z100000"])

            sheet.update(
                values=data,
                range_name="A1",
                value_input_option="USER_ENTERED"
            )

            print(f"🎉 Uploaded {len(df)} rows to {name}")

            return

        except APIError as e:

            if getattr(e.response, "status_code", None) == 503:

                wait = 2 ** attempt
                print(f"⚠️ Retry in {wait}s")
                time.sleep(wait)

            else:
                raise

    raise Exception("❌ Upload failed")


# ================= UPLOAD =================

upload_to_sheet(AWD_WORKSHEET_NAME, df_awd)

upload_to_sheet(FBA_WORKSHEET_NAME, df_fba)


# ================= DONE =================

print("🚀 PIPELINE COMPLETED SUCCESSFULLY")

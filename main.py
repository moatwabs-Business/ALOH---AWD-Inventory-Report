import os
import json
import time
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

MARKETPLACE_ID = "ATVPDKIKX0DER"

MAX_GSPREAD_RETRIES = 5


# ================= AMAZON SECRETS =================

REFRESH_TOKEN = os.environ["AMAZON_REFRESH_TOKEN"]
CLIENT_ID = os.environ["AMAZON_LWA_CLIENT_ID"]
CLIENT_SECRET = os.environ["AMAZON_LWA_CLIENT_SECRET"]


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
            "refresh_token": REFRESH_TOKEN,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=30
    )

    response.raise_for_status()

    return response.json()["access_token"]


access_token = get_access_token()

print("✅ Amazon access token received")


# ================= GET AWD INVENTORY =================

def get_awd_inventory():

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

    data = response.json()

    for item in data['payload']["inventorySummaries"]:

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

    print("📊 Requesting Inventory Planning report...")

    create_report = requests.post(
        "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports",
        headers={
            "x-amz-access-token": access_token,
            "Content-Type": "application/json"
        },
        json={
            "reportType": "GET_FBA_INVENTORY_PLANNING_DATA",
            "marketplaceIds": [MARKETPLACE_ID]
        }
    )

    create_report.raise_for_status()

    report_id = create_report.json()["reportId"]

    print(f"Report ID: {report_id}")

    while True:

        status = requests.get(
            f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}",
            headers={"x-amz-access-token": access_token}
        ).json()

        processing_status = status["processingStatus"]

        print("Status:", processing_status)

        if processing_status == "DONE":

            document_id = status["reportDocumentId"]
            break

        elif processing_status in ["FATAL", "CANCELLED"]:
            raise Exception("Report failed")

        time.sleep(10)

    doc = requests.get(
        f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{document_id}",
        headers={"x-amz-access-token": access_token}
    ).json()

    download_url = doc["url"]

    response = requests.get(download_url)

    response.raise_for_status()

    df = pd.read_csv(
        io.StringIO(response.text),
        sep="\t"
    )

    df_units = df[["sku", "units-shipped-t30"]].copy()

    df_units.rename(columns={
        "sku": "sellerSku",
        "units-shipped-t30": "Units Shipped T30"
    }, inplace=True)

    df_units["Units Shipped T30"] = pd.to_numeric(
        df_units["Units Shipped T30"],
        errors="coerce"
    ).fillna(0).astype(int)

    print(f"✅ Units shipped rows: {len(df_units)}")

    return df_units


# ================= EXTRACTION DATE =================

EST_TZ = timezone(timedelta(hours=-5))
extracted_at = datetime.now(EST_TZ).strftime("%Y-%m-%d")


# ================= GET DATA =================

df_awd = get_awd_inventory()
df_fba = get_fba_inventory()
df_units = get_units_shipped_t30()


# ================= FULL OUTER JOIN =================

df_final = pd.merge(
    df_fba,
    df_units,
    on="sellerSku",
    how="outer"
)


# ================= FIX NUMERIC COLUMNS =================

numeric_columns = [
    "Units Shipped T30",
    "Inventory Supply at FBA",
    "Reserved FC Processing",
    "Reserved Customer Order"
]

for col in numeric_columns:

    if col in df_final.columns:

        df_final[col] = pd.to_numeric(
            df_final[col],
            errors="coerce"
        ).fillna(0).astype(int)


# Fix text columns
df_final["sellerSku"] = df_final["sellerSku"].fillna("")
df_final["asin"] = df_final.get("asin", "").fillna("")

df_final["Extracted At"] = extracted_at
df_awd["Extracted At"] = extracted_at


# ================= CLEAN =================

df_final = df_final.replace([np.inf, -np.inf], "").fillna("")
df_awd = df_awd.replace([np.inf, -np.inf], "").fillna("")


# ================= UPLOAD FUNCTION =================

def upload_to_sheet(name, df):

    data = [df.columns.tolist()] + df.values.tolist()

    for attempt in range(MAX_GSPREAD_RETRIES):

        try:

            worksheet = gs_client.open(SPREADSHEET_NAME).worksheet(name)

            worksheet.batch_clear(["A1:Z100000"])

            worksheet.update(
                values=data,
                range_name="A1",
                value_input_option="RAW"
            )

            print(f"Uploaded {len(df)} rows to {name}")

            return

        except APIError as e:

            if getattr(e.response, "status_code", None) == 503:

                wait = 2 ** attempt
                time.sleep(wait)

            else:
                raise

    raise Exception("Upload failed")


# ================= UPLOAD =================

upload_to_sheet(AWD_WORKSHEET_NAME, df_awd)
upload_to_sheet(FBA_WORKSHEET_NAME, df_final)


print("🚀 PIPELINE COMPLETED SUCCESSFULLY")

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

FBA_WORKSHEET_NAME = "Amazon Data  API"
AWD_WORKSHEET_NAME = "AWD Data API Request"

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

    print("🔑 Requesting Amazon access token...")

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

    token = response.json()["access_token"]

    print("✅ Amazon access token received")

    return token


# ================= GET INVENTORY PLANNING REPORT =================

def get_inventory_planning_data(access_token):

    print("📊 Requesting Inventory Planning Report...")

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


    # Wait for report completion
    while True:

        status = requests.get(
            f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}",
            headers={"x-amz-access-token": access_token}
        ).json()

        processing_status = status["processingStatus"]

        print("Report status:", processing_status)

        if processing_status == "DONE":

            document_id = status["reportDocumentId"]
            break

        elif processing_status in ["FATAL", "CANCELLED"]:
            raise Exception("Report failed")

        time.sleep(10)


    # Download report
    doc = requests.get(
        f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{document_id}",
        headers={"x-amz-access-token": access_token}
    ).json()

    download_url = doc["url"]

    file = requests.get(download_url)

    file.raise_for_status()

    df = pd.read_csv(io.StringIO(file.text), sep="\t")


    # Extract required columns
    df = df[[
        "sku",
        "asin",
        "Inventory Supply at FBA",
        "Reserved FC Processing",
        "Reserved Customer Order",
        "units-shipped-t30"
    ]].copy()


    df.rename(columns={
        "sku": "sellerSku",
        "units-shipped-t30": "Units Shipped T30"
    }, inplace=True)


    # Convert numeric safely
    numeric_cols = [
        "Inventory Supply at FBA",
        "Reserved FC Processing",
        "Reserved Customer Order",
        "Units Shipped T30"
    ]

    for col in numeric_cols:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        ).fillna(0).astype(int)


    # Add Extracted At column
    EST_TZ = timezone(timedelta(hours=-5))
    df["Extracted At"] = datetime.now(EST_TZ).strftime("%Y-%m-%d")


    print(f"✅ FBA rows extracted: {len(df)}")

    return df


# ================= GET AWD INVENTORY =================

def get_awd_inventory(access_token):

    print("📦 Requesting AWD inventory...")

    url = "https://sellingpartnerapi-na.amazon.com/awd/2024-05-09/inventory"

    headers = {
        "x-amz-access-token": access_token
    }

    response = requests.get(url, headers=headers, timeout=60)

    response.raise_for_status()

    data = response.json()["inventory"]

    df = pd.DataFrame(data)


    # Keep only required columns
    df = df[[
        "sku",
        "totalInboundQuantity",
        "totalOnhandQuantity"
    ]].copy()


    # Convert numeric safely
    df["totalInboundQuantity"] = pd.to_numeric(
        df["totalInboundQuantity"],
        errors="coerce"
    ).fillna(0).astype(int)

    df["totalOnhandQuantity"] = pd.to_numeric(
        df["totalOnhandQuantity"],
        errors="coerce"
    ).fillna(0).astype(int)


    # Add Extracted At column
    EST_TZ = timezone(timedelta(hours=-5))
    df["Extracted At"] = datetime.now(EST_TZ).strftime("%Y-%m-%d")


    print(f"✅ AWD rows extracted: {len(df)}")

    return df


# ================= GOOGLE SHEETS UPLOAD =================

def upload_to_sheet(sheet_name, df):

    print(f"⬆️ Uploading to sheet: {sheet_name}")

    data = [df.columns.tolist()] + df.values.tolist()

    for attempt in range(MAX_GSPREAD_RETRIES):

        try:

            worksheet = gs_client.open(SPREADSHEET_NAME).worksheet(sheet_name)

            worksheet.batch_clear(["A1:Z100000"])

            worksheet.update(
                values=data,
                range_name="A1",
                value_input_option="RAW"
            )

            print(f"✅ Uploaded {len(df)} rows to {sheet_name}")

            return

        except APIError as e:

            if getattr(e.response, "status_code", None) == 503:

                wait = 2 ** attempt
                print(f"Retrying in {wait} seconds...")
                time.sleep(wait)

            else:
                raise


    raise Exception(f"Failed to upload to {sheet_name}")


# ================= MAIN EXECUTION =================

def main():

    access_token = get_access_token()

    # FBA Inventory Planning Report
    df_fba = get_inventory_planning_data(access_token)

    # AWD Inventory
    df_awd = get_awd_inventory(access_token)

    # Upload both
    upload_to_sheet(FBA_WORKSHEET_NAME, df_fba)

    upload_to_sheet(AWD_WORKSHEET_NAME, df_awd)

    print("🚀 ALL DATA UPDATED SUCCESSFULLY")


if __name__ == "__main__":
    main()

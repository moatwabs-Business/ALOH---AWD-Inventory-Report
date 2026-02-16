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
WORKSHEET_NAME = "Amazon Data  API"

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


# ================= REQUEST INVENTORY PLANNING REPORT =================

def get_inventory_planning_data():

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


    # Wait for report completion
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


    # Download report
    doc = requests.get(
        f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{document_id}",
        headers={"x-amz-access-token": access_token}
    ).json()

    download_url = doc["url"]

    response = requests.get(download_url)

    response.raise_for_status()


    # Load into dataframe
    df = pd.read_csv(
        io.StringIO(response.text),
        sep="\t"
    )


    # Extract required columns ONLY
    df_final = df[[
        "sku",
        "asin",
        "Inventory Supply at FBA",
        "Reserved FC Processing",
        "Reserved Customer Order",
        "units-shipped-t30"
    ]].copy()


    # Rename columns
    df_final.rename(columns={
        "sku": "sellerSku",
        "units-shipped-t30": "Units Shipped T30"
    }, inplace=True)


    # Convert numeric columns safely
    numeric_columns = [
        "Inventory Supply at FBA",
        "Reserved FC Processing",
        "Reserved Customer Order",
        "Units Shipped T30"
    ]

    for col in numeric_columns:

        df_final[col] = pd.to_numeric(
            df_final[col],
            errors="coerce"
        ).fillna(0).astype(int)


    print(f"✅ Extracted rows: {len(df_final)}")

    return df_final


# ================= ADD EXTRACTION DATE =================

EST_TZ = timezone(timedelta(hours=-5))
extracted_at = datetime.now(EST_TZ).strftime("%Y-%m-%d")


df_final = get_inventory_planning_data()

df_final["Extracted At"] = extracted_at


# ================= CLEAN =================

df_final = df_final.replace([np.inf, -np.inf], "").fillna("")


# ================= UPLOAD TO GOOGLE SHEETS =================

def upload_to_sheet(df):

    data = [df.columns.tolist()] + df.values.tolist()

    for attempt in range(MAX_GSPREAD_RETRIES):

        try:

            worksheet = gs_client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)

            worksheet.batch_clear(["A1:Z100000"])

            worksheet.update(
                values=data,
                range_name="A1",
                value_input_option="RAW"
            )

            print(f"✅ Uploaded {len(df)} rows successfully")

            return

        except APIError as e:

            if getattr(e.response, "status_code", None) == 503:

                wait = 2 ** attempt
                time.sleep(wait)

            else:
                raise


    raise Exception("Upload failed")


# ================= RUN =================

upload_to_sheet(df_final)

print("🚀 PIPELINE COMPLETED SUCCESSFULLY")

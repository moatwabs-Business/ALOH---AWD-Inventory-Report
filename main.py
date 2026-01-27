import os
import json
import requests
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

ALERT_EMAIL_USER = os.environ.get("ALERT_EMAIL_USER")
ALERT_EMAIL_PASSWORD = os.environ.get("ALERT_EMAIL_PASSWORD")

ALERT_RECIPIENTS = [
    "moatwa.bs@gmail.com",
    "segi@aloh.com"
]


def send_error_email(subject, message):
    try:
        msg = MIMEMultipart()
        msg["From"] = ALERT_EMAIL_USER
        msg["To"] = ", ".join(ALERT_RECIPIENTS)
        msg["Subject"] = subject

        msg.attach(MIMEText(message, "plain"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(ALERT_EMAIL_USER, ALERT_EMAIL_PASSWORD)
            server.sendmail(
                ALERT_EMAIL_USER,
                ALERT_RECIPIENTS,
                msg.as_string()
            )

        print("📧 Error email sent successfully")

    except Exception as e:
        print(f"❌ Failed to send error email: {e}")




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


try:
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

    if token_response.status_code != 200:
        raise Exception(f"Token Error {token_response.status_code}: {token_response.text}")

    access_token = token_response.json()["access_token"]
    print("✅ Amazon access token received")

    # ================= STEP 2 — AMAZON INVENTORY =================
    inventory_response = requests.get(
        "https://sellingpartnerapi-na.amazon.com/awd/2024-05-09/inventory",
        headers={"x-amz-access-token": access_token},
        timeout=60
    )

    if inventory_response.status_code != 200:
        raise Exception(
            f"Inventory API Error {inventory_response.status_code}:\n{inventory_response.text}"
        )

   inventory = inventory_response.json()['inventory']

    df = pd.DataFrame(inventory)

    print(f"✅ Amazon data (flattened): {df.shape[0]} rows, {df.shape[1]} columns")

except Exception as amazon_error:
    error_message = f"""
Amazon AWD Inventory Automation Failed

Error Details:
{str(amazon_error)}

Time: Automated GitHub Run

Action Required:
Please review Amazon SP-API credentials, endpoint, or service status.
"""

    print("❌ Amazon API Error:", amazon_error)

    send_error_email(
        subject="🚨 Amazon AWD Inventory Automation Failed",
        message=error_message
    )

    # Stop execution so bad data is NOT sent to Google Sheets
    raise


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

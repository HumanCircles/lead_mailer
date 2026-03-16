import gspread
from google.oauth2.service_account import Credentials
import os, pandas as pd
from dotenv import load_dotenv

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def get_leads() -> list[dict]:
    """
    Returns list of dicts: [{name, email, linkedin_url, status}, ...]
    Skips rows where status == 'SENT' to avoid duplicates.
    """
    creds = Credentials.from_service_account_file(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"), scopes=SCOPES
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(os.getenv("GOOGLE_SHEET_ID"))
    ws = sheet.worksheet(os.getenv("GOOGLE_SHEET_TAB", "Sheet1"))

    records = ws.get_all_records()  # headers: Name, Email, LinkedIn URL, Status
    leads = [
        {
            "name":         r.get("Name", "").strip(),
            "email":        r.get("Email", "").strip(),
            "linkedin_url": r.get("LinkedIn URL", "").strip(),
            "row_index":    idx + 2,  # 1-indexed + header row
        }
        for idx, r in enumerate(records)
        if r.get("Status", "").upper() != "SENT"
        and r.get("Email", "").strip()
    ]
    return leads


def mark_sent(row_index: int):
    """Update Status column to SENT after successful send."""
    creds = Credentials.from_service_account_file(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(os.getenv("GOOGLE_SHEET_ID"))
    ws = sheet.worksheet(os.getenv("GOOGLE_SHEET_TAB", "Sheet1"))
    # Assumes column D is Status
    ws.update_cell(row_index, 4, "SENT")


import os
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

SCOPES_READONLY = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SCOPES_READWRITE = ["https://www.googleapis.com/auth/spreadsheets"]


def _get_worksheet(scopes: list[str]):
    """Shared auth + sheet open. Returns the worksheet for the configured tab."""
    creds = Credentials.from_service_account_file(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"), scopes=scopes
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(os.getenv("GOOGLE_SHEET_ID"))
    return sheet.worksheet(os.getenv("GOOGLE_SHEET_TAB", "Sheet1"))


def get_leads() -> list[dict]:
    """
    Returns list of dicts: [{name, email, linkedin_url, status}, ...]
    Skips rows where status == 'SENT' to avoid duplicates.
    """
    ws = _get_worksheet(SCOPES_READONLY)
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
    ws = _get_worksheet(SCOPES_READWRITE)
    ws.update_cell(row_index, 4, "SENT")  # column D = Status


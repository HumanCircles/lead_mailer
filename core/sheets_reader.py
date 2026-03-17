import os
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

SCOPES_RO = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SCOPES_RW = ["https://www.googleapis.com/auth/spreadsheets"]


def _get_worksheet(scopes: list[str], sheet_id: str = None, sheet_tab: str = None):
    sid = sheet_id or os.getenv("GOOGLE_SHEET_ID")
    tab = sheet_tab or os.getenv("GOOGLE_SHEET_TAB", "Sheet1")
    creds = Credentials.from_service_account_file(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"), scopes=scopes
    )
    client = gspread.authorize(creds)
    return client.open_by_key(sid).worksheet(tab)


def get_leads(sheet_id: str = None, sheet_tab: str = None) -> list[dict]:
    ws = _get_worksheet(SCOPES_RO, sheet_id, sheet_tab)
    records = ws.get_all_records()
    return [
        {
            "name":         r.get("Name", "").strip(),
            "email":        r.get("Email", "").strip(),
            "linkedin_url": r.get("LinkedIn URL", "").strip(),
            "row_index":    idx + 2,
        }
        for idx, r in enumerate(records)
        if r.get("Status", "").upper() != "SENT"
        and r.get("Email", "").strip()
    ]


def mark_sent(row_index: int, sheet_id: str = None, sheet_tab: str = None):
    ws = _get_worksheet(SCOPES_RW, sheet_id, sheet_tab)
    ws.update_cell(row_index, 4, "SENT")


# HireQuotient Lead Mailer

## Folder Structure
lead_mailer/
├── main.py                  # Entry point
├── requirements.txt
├── .env                     # Your API keys (copy from .env.example)
├── service_account.json     # Google Sheets service account
├── gmail_credentials.json   # Gmail OAuth2 client credentials
├── ui.py                    # Simple Streamlit UI to set Google Sheet
└── core/
    ├── sheets_reader.py     # Reads leads from Google Sheet
    ├── linkedin_scraper.py  # EnrichLayer LinkedIn profile fetcher
    ├── email_drafter.py     # Gemini 2.5 Pro + Search Grounding
    └── gmail_sender.py      # Gmail API sender

## Google Sheet Format (required columns)
| Name | Email | LinkedIn URL | Status |
|------|-------|--------------|--------|
| John Doe | john@acme.com | https://linkedin.com/in/johndoe | |

## Setup Steps
1. pip install -r requirements.txt
2. Copy .env.example to .env and fill all keys **except** the Google Sheet + EnrichLayer flags (these will be set via the UI)
3. Create Google Cloud project → enable Sheets API + Gmail API
4. Download service_account.json for Sheets access
5. Download gmail_credentials.json (OAuth2) for Gmail
6. Run the config UI to set Google Sheet + EnrichLayer fresh/cached mode:
   - `streamlit run ui.py`
   - Enter **Google Sheet ID**, **Sheet tab name**, and choose whether to prefer fresh EnrichLayer data, then click **Save configuration**
7. Run the mailer:
   - `python main.py`
   - (First run opens browser for Gmail OAuth consent)

## Cost Per Email (approx)
- ProxyCurl (LinkedIn): $0.010
- Gemini 2.5 Pro + Search Grounding: ~$0.047
- Gmail API: Free
- TOTAL: ~$0.057/email

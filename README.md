# HireQuotient Lead Mailer

AI-powered personalized cold email pipeline. Reads leads from Google Sheets, scrapes LinkedIn via EnrichLayer, drafts hyper-personalized emails using Gemini 2.5 Pro with live Google Search grounding, and sends via Gmail API.

***

## Folder Structure

```
lead_mailer/
├── main.py                  # CLI entry point — runs full pipeline
├── ui.py                    # Streamlit UI — configure + preview + send
├── requirements.txt
├── .env                     # API keys (copy from env.example)
├── env.example              # Environment variable template
├── service_account.json     # Google Sheets service account key
├── gmail_credentials.json   # Gmail OAuth2 client credentials
├── gmail_token.json         # Auto-created on first run (Gmail session)
└── core/
    ├── sheets_reader.py     # Reads leads + marks rows as SENT
    ├── linkedin_scraper.py  # EnrichLayer People API — LinkedIn profiles
    ├── email_drafter.py     # Gemini 2.5 Pro + Google Search grounding
    └── gmail_sender.py      # Gmail API sender (OAuth2)
```

***

## Google Sheet Format

Create a sheet with exactly these column headers (case-sensitive):

| Name | Email | LinkedIn URL | Status |
|------|-------|--------------|--------|
| John Doe | john@acme.com | https://linkedin.com/in/johndoe | |

- **Status** column is auto-updated to `SENT` by the script — re-runs are safe, already-sent rows are skipped.
- Share the sheet with the service account email: `hirequotient-lead-mailer@hq-sourcing-stag.iam.gserviceaccount.com` (Editor access)

***

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Copy `env.example` to `.env` and fill in:

```env
GEMINI_API_KEY=              # aistudio.google.com → Get API Key
ENRICHLAYER_API_KEY=         # enrichlayer.com/dashboard → Bearer token
SENDER_EMAIL=                # Gmail address to send from
ENRICHLAYER_FRESH_DATA=true  # true = fresh data (+1 credit), false = cached (free)

# Credential file paths (leave as-is if using defaults)
GOOGLE_SERVICE_ACCOUNT_JSON=service_account.json
GMAIL_OAUTH_CREDENTIALS=gmail_credentials.json
GMAIL_TOKEN_FILE=gmail_token.json
```

## Running

### Option A — Streamlit UI (recommended)

```bash
streamlit run ui.py
```

- Set Google Sheet ID or full URL and tab name (the UI will auto-extract the ID from a full URL)
- Toggle fresh vs cached EnrichLayer data
- Preview each drafted email before sending
- Send individually with one click

### Option B — CLI (bulk send)

```bash
python main.py
```

Processes all pending leads automatically, marks each row SENT, prints a summary table.

> **First run only:** A browser tab opens for Gmail OAuth consent — click Allow. Token is saved to `gmail_token.json` for all future runs.

***

## How It Works

```
Google Sheet
    ↓  (gspread + service account)
EnrichLayer API  →  LinkedIn profile data
    ↓  (name, headline, role, skills, education)
Gemini 2.5 Pro + Google Search Grounding
    ↓  (researches person/company live, drafts personalized email)
Gmail API
    ↓  (sends from SENDER_EMAIL)
Google Sheet  ←  Status marked SENT
```

***

## Cost Per Email

| Component | Cost |
|-----------|------|
| EnrichLayer LinkedIn scrape (`if-recent`) | ~$0.020 |
| Gemini 2.5 Pro + Search Grounding | ~$0.037 |
| Gmail API | Free |
| **Total** | **~$0.057 / email** |

Switch `ENRICHLAYER_FRESH_DATA=false` to use cached profiles and save ~$0.010/email.

***

# HireQuotient Lead Mailer

AI-assisted personalized BD outreach: load prospects from CSV, draft emails with OpenAI using the playbook in `MESSAGING_README.md`, and **send via SMTP** (your own mail host and sender mailboxes). Outbound mail does **not** use Google or the Gmail API.

## Folder structure

```
lead_mailer/
├── ui.py                 # Streamlit UI — upload CSV, generate, preview, send
├── agent.py              # Batch CLI — prospects CSV → OpenAI → SMTP → sent_log.csv
├── requirements.txt
├── .env                  # API keys and SMTP (copy from .env.example)
├── .env.example
├── MESSAGING_README.md   # Voice and structure rules for drafts
├── suppression.txt.example
└── core/
    ├── prospect_csv.py   # Normalise ATS-style or simple CSV columns
    ├── email_drafter.py  # OpenAI drafting from the playbook
    ├── smtp_sender.py    # SMTP send (rotating sender pool, throttling)
    └── deliverability.py # Suppression list, unsubscribe footer, List-Unsubscribe headers
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and set:

- **`OPENAI_API_KEY`** — required for drafting.
- **`SENDER_POOL`** — comma-separated `email:password` pairs for mailboxes on your SMTP host.
- **`SMTP_HOST`** / **`SMTP_PORT`** — your provider’s SMTP endpoint (e.g. `mail.yourdomain.com` and `465` for SSL).

Optional: `FROM_NAME`, `DAILY_LIMIT`, deliverability variables (`SUPPRESSION_FILE`, `UNSUBSCRIBE_MAILTO`, `UNSUBSCRIBE_FOOTER_ENABLED`, `UNSUBSCRIBE_URL`).

### 3. Suppression list (optional)

Copy `suppression.txt.example` to `suppression.txt` and add one email per line (addresses that must never receive mail). `suppression.txt` is gitignored by default.

## Running

### Streamlit UI

```bash
streamlit run ui.py
```

Upload a CSV, generate drafts, edit if needed, send individually or in bulk.

### Batch agent (CLI)

```bash
python agent.py
```

Reads `prospects.csv` (or `PROSPECTS_FILE`), skips already-logged sends, respects suppression, appends to `sent_log.csv`.

## How it works

```
CSV prospects
    ↓
OpenAI (playbook from MESSAGING_README.md)
    ↓
SMTP (rotating pool, daily limits, optional footer + List-Unsubscribe)
```

## Cost

Drafting cost depends on your OpenAI model and usage; **SMTP sending** is through your own provider (no Gmail API or OAuth in this path).

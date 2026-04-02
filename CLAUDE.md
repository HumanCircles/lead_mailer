# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

AI-assisted BD outreach tool: load prospects from CSV → generate personalized emails via OpenAI (guided by `MESSAGING_README.md`) → send via **SendGrid API** (rotating sender pool, domain-to-account routing). Two entry points: an interactive Streamlit UI and a batch CLI. A seed summary email is sent to the admin after every run.

## Commands

```bash
# Run Streamlit UI (interactive)
streamlit run ui.py

# Run batch CLI (non-interactive, reads prospects.csv → sends → appends to sent_log.csv)
python agent.py

# Deploy to EC2 (rsync + docker-compose up)
./deploy.sh

# Install dependencies
pip install -r requirements.txt
```

No test suite exists in this repo.

## Configuration

All config is via `.env` (gitignored). Copy `.env.example` to `.env` and set at minimum:
- `OPENAI_API_KEY`
- `SENDGRID_ACCOUNTS` — `domain:api_key:pool_name` triples, comma-separated (see `.env.example` for the three production accounts)
- `SENDER_POOL` — comma-separated sender email addresses (no passwords needed; SendGrid authenticates via API key)

Key optional config: `DAILY_LIMIT`, `HOURLY_LIMIT`, `PROSPECTS_MAX` (0 = all), `SUPPRESSION_FILE`, `SIGNATURE_*`, `UNSUBSCRIBE_*`, `SEED_EMAIL` (default `ashutosh@HireQuotient.com`).

## Architecture

### Email generation pipeline

```
MESSAGING_README.md (playbook)
    ↓ (loaded as LLM system prompt at module import time)
OpenAI → JSON {subject, body}
    ↓
deliverability.py: greeting check → append_signature_block → append_unsubscribe_footer
    ↓
SendGrid API (rotating sender pool, domain → API key + IP pool routing, daily/hourly throttle)
    ↓
sent_log.csv (append-only audit log)
    ↓
send_seed_email() → ashutosh@HireQuotient.com (run summary)
```

`MESSAGING_README.md` is the single source of truth for email voice and structure. It is read at startup by both `agent.py` and `core/email_drafter.py`.

### Two independent code paths

| | `agent.py` (CLI) | `core/smtp_sender.py` (UI) |
|---|---|---|
| Throttle | per-sender daily + hourly + account-level cap | per-sender daily only |
| Concurrency | `threading.Semaphore(CONCURRENT_SENDS)` | sequential with `time.sleep(1.5–4s)` |
| Log hydration | reads `sent_log.csv` at startup | reads `sent_log.csv` at module import |

**Rate-limiting logic is duplicated between `agent.py` and `core/smtp_sender.py`** — changes to throttle behavior must be made in both.

### `core/` modules

- **`prospect_csv.py`** — maps heterogeneous CSV column names (ATS exports, LinkedIn enrichment, plain CSVs) to canonical `{first_name, last_name, email, company, title, hcm_platform}`. `canonicalize_prospect_row()` is used by the CLI; `normalise_prospects_dataframe()` by the UI (pandas path). Handles duplicate column names (e.g. ATS exports with two `title` columns via `.1` suffix deduplication).

- **`email_drafter.py`** — used by the UI only. Creates a single module-level OpenAI client at import time. `draft_email()` returns `{subject, body}` and post-processes body for plain-text paragraph formatting.

- **`smtp_sender.py`** — shared by both paths (despite the legacy name). Sender pool loaded at module import; daily/hourly counters hydrated from `sent_log.csv` immediately. Routes each sender by email domain to the correct SendGrid subaccount + IP pool (via `SENDGRID_ACCOUNTS`). `send_email()` checks suppression, rotates sender, appends signature + footer, delivers via SendGrid API. `send_seed_email()` sends admin notifications bypassing pool rotation.

- **`deliverability.py`** — shared by both paths. Suppression list is loaded lazily and cached as a module-level `frozenset` (not reloaded during a session). `strip_control_chars()` sanitizes prospect data before it reaches the LLM.

### Suppression list

`suppression.txt` (gitignored) — one lowercase email per line, `#` for comments. Loaded once per process. To pick up changes in the UI, restart the Streamlit server.

### Data files (all gitignored)

- `prospects.csv` — CLI input (path overridable via `PROSPECTS_FILE`)
- `sent_log.csv` — append-only send log used by both paths to skip duplicates and hydrate rate-limit counters
- `suppression.txt` — do-not-contact list

### Deployment

`deploy.sh` rsyncs the local directory (excluding `*.csv`, `venv`, `.git`) to EC2 and runs `docker-compose up -d --build`. **The local `.env` is synced to the server**, overwriting the server's copy. The Streamlit app runs on port 8501.

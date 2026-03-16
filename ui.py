import json as _json
import os
from typing import Dict, Any, List

import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

load_dotenv()

from core.sheets_reader import get_leads, mark_sent
from core.linkedin_scraper import scrape_linkedin
from core.email_drafter import draft_email
from core.gmail_sender import send_email


ENV_PATH = ".env"


def load_current_values() -> tuple[str, str, bool]:
    load_dotenv(override=False)
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "")
    sheet_tab = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")
    fresh_flag = os.getenv("ENRICHLAYER_FRESH_DATA", "true").lower() == "true"
    return sheet_id, sheet_tab, fresh_flag


def write_env(sheet_id: str, sheet_tab: str, fresh: bool):
    """
    Update or create .env with new Google Sheet + EnrichLayer settings
    while preserving any existing lines/keys.
    """
    existing: Dict[str, str] = {}

    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, "r") as f:
            for line in f:
                raw = line.rstrip("\n")
                if not raw or raw.startswith("#") or "=" not in raw:
                    continue
                key, value = raw.split("=", 1)
                existing[key.strip()] = value.strip()

    # Overwrite the sheet-related keys
    existing["GOOGLE_SHEET_ID"] = sheet_id
    existing["GOOGLE_SHEET_TAB"] = sheet_tab
    existing["ENRICHLAYER_FRESH_DATA"] = "true" if fresh else "false"

    # Re-write file
    with open(ENV_PATH, "w") as f:
        for key, value in existing.items():
            f.write(f"{key}={value}\n")


def normalize_sheet_id(raw: str) -> str:
    """
    Accept either a bare Sheet ID or a full docs.google.com URL
    and always return just the ID part.
    """
    raw = raw.strip()
    if "docs.google.com" not in raw:
        return raw

    # Expect URLs like: https://docs.google.com/spreadsheets/d/<ID>/edit...
    try:
        parts = raw.split("/d/")[1]
        sheet_id = parts.split("/")[0]
        return sheet_id
    except Exception:
        return raw


def run_campaign_ui(fresh_data: bool):
    st.header("Run campaign")

    if st.button("Load pending leads"):
        try:
            leads = get_leads()
            if not leads:
                st.info("No pending leads found (all rows are marked SENT or missing email).")
                return
            st.session_state["leads"] = leads
            st.success(f"Loaded {len(leads)} pending leads.")
        except Exception as e:
            st.error(f"Failed to load leads: {e}")

    leads: List[Dict[str, Any]] = st.session_state.get("leads", [])
    if not leads:
        return

    for idx, lead in enumerate(leads):
        with st.expander(f"{lead['name']} — {lead['email']}"):
            st.write(f"LinkedIn: {lead['linkedin_url']}")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Preview email", key=f"preview_{idx}"):
                    try:
                        with st.spinner("Drafting email..."):
                            profile = scrape_linkedin(
                                lead["linkedin_url"],
                                fresh=fresh_data,
                            )
                            email_content = draft_email(lead, profile)
                            if isinstance(email_content, dict):
                                st.session_state[f"email_preview__{idx}"] = email_content
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to draft email: {e}")

            with col2:
                if st.button("Send email", key=f"send_{idx}"):
                    try:
                        # Use existing preview if available and valid, otherwise draft on the fly
                        email_content = st.session_state.get(f"email_preview__{idx}")
                        if not isinstance(email_content, dict):
                            with st.spinner("Drafting and sending..."):
                                profile = scrape_linkedin(
                                    lead["linkedin_url"],
                                    fresh=fresh_data,
                                )
                                email_content = draft_email(lead, profile)
                        if isinstance(email_content, dict):
                            with st.spinner("Sending..."):
                                msg_id = send_email(
                                    lead["email"],
                                    email_content["subject"],
                                    email_content["body"],
                                )
                                mark_sent(lead["row_index"])
                            st.success(f"Email sent (Gmail ID: {msg_id}). Marked as SENT in sheet.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to send email: {e}")

            preview = st.session_state.get(f"email_preview__{idx}")
            if isinstance(preview, dict):
                st.markdown("**Subject:** " + preview["subject"])
                st.markdown("**Body:**")
                st.text(preview["body"])
                draft_text = f"Subject: {preview['subject']}\n\n{preview['body']}"
                # Copy-to-clipboard button (browser only)
                copy_js = f"""
                <button id="copyBtn" style="
                    padding: 0.35rem 0.75rem;
                    margin-right: 0.5rem;
                    margin-bottom: 0.5rem;
                    cursor: pointer;
                    border: 1px solid #ccc;
                    border-radius: 4px;
                    background: #f0f2f6;
                    font-size: 14px;
                ">Copy draft</button>
                <span id="copyMsg" style="font-size: 12px; color: #0e8c80;"></span>
                <script>
                (function() {{
                    var btn = document.getElementById("copyBtn");
                    var msg = document.getElementById("copyMsg");
                    var text = {_json.dumps(draft_text)};
                    btn.onclick = function() {{
                        navigator.clipboard.writeText(text).then(function() {{
                            msg.textContent = "Copied!";
                            setTimeout(function() {{ msg.textContent = ""; }}, 2000);
                        }});
                    }};
                }})();
                </script>
                """
                components.html(copy_js, height=45)
                st.download_button(
                    "Download draft",
                    data=draft_text,
                    file_name="draft_email.txt",
                    mime="text/plain",
                    key=f"copy_draft_{idx}",
                )


def main():
    st.set_page_config(page_title="HireQuotient Lead Mailer")
    st.title("HireQuotient Lead Mailer")

    st.subheader("Configuration")
    current_id, current_tab, current_fresh = load_current_values()

    sheet_id_input = st.text_input(
        "Google Sheet ID or full URL",
        value=current_id,
        help="You can paste either the raw ID or the full URL: https://docs.google.com/spreadsheets/d/<ID>/edit",
    )
    sheet_tab = st.text_input(
        "Sheet tab name",
        value=current_tab,
        help="Tab name inside the Google Sheet (default: Sheet1).",
    )

    fresh_data = st.checkbox(
        "Prefer fresh EnrichLayer profile data (costs +1 credit per profile)",
        value=current_fresh,
    )

    if st.button("Save configuration"):
        normalized_id = normalize_sheet_id(sheet_id_input)
        if not normalized_id.strip():
            st.error("Google Sheet ID cannot be empty.")
        else:
            write_env(normalized_id.strip(), sheet_tab.strip() or "Sheet1", fresh_data)
            st.success("Saved configuration.")

    st.markdown("---")
    run_campaign_ui(fresh_data)


if __name__ == "__main__":
    main()


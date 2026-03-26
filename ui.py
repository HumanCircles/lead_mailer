import csv
import io
import json
import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from core.email_drafter import draft_email

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="BD Outreach — Email Previewer",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .block-container { padding-top: 2rem; max-width: 1100px; }
    footer { visibility: hidden; }
    .status-badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }
    .badge-pending  { background: #1e293b; color: #94a3b8; }
    .badge-done     { background: #14532d; color: #86efac; }
    .badge-failed   { background: #450a0a; color: #fca5a5; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────

REQUIRED_COLS = {"first_name", "last_name", "email", "company", "title"}
OPTIONAL_COLS = {"hcm_platform"}
ALL_COLS      = REQUIRED_COLS | OPTIONAL_COLS

# ── Helpers ───────────────────────────────────────────────────────────────────

def _validate_csv(df: pd.DataFrame) -> list[str]:
    missing = REQUIRED_COLS - set(c.lower().strip() for c in df.columns)
    return [f"Missing column: `{c}`" for c in sorted(missing)]


def _normalise_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower().strip() for c in df.columns]
    for col in OPTIONAL_COLS:
        if col not in df.columns:
            df[col] = ""
    return df


def _prospect_key(p: dict) -> str:
    return p.get("email", "").strip().lower()


def _generate_for_prospect(prospect: dict) -> dict:
    lead = {
        "name":         f"{prospect.get('first_name', '')} {prospect.get('last_name', '')}".strip(),
        "company":      prospect.get("company", ""),
        "title":        prospect.get("title", ""),
        "hcm_platform": prospect.get("hcm_platform", ""),
    }
    return draft_email(lead)


def _results_to_csv(prospects: list[dict], results: dict) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["first_name", "last_name", "email", "company", "title",
                     "hcm_platform", "subject", "body", "status", "error"])
    for p in prospects:
        key = _prospect_key(p)
        res = results.get(key, {})
        writer.writerow([
            p.get("first_name", ""),
            p.get("last_name", ""),
            p.get("email", ""),
            p.get("company", ""),
            p.get("title", ""),
            p.get("hcm_platform", ""),
            res.get("subject", ""),
            res.get("body", ""),
            res.get("status", "pending"),
            res.get("error", ""),
        ])
    return buf.getvalue().encode()

# ── Session state defaults ────────────────────────────────────────────────────

if "prospects" not in st.session_state:
    st.session_state.prospects = []
if "results" not in st.session_state:
    st.session_state.results = {}   # email → {subject, body, status, error}
if "selected_idx" not in st.session_state:
    st.session_state.selected_idx = 0

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("## BD Outreach — Email Previewer")
st.caption("Upload prospects CSV → generate emails → review & download")
st.divider()

# ── Step 1: Upload ────────────────────────────────────────────────────────────

st.markdown("### 1. Upload prospects CSV")
st.caption("Required columns: `first_name`, `last_name`, `email`, `company`, `title` · Optional: `hcm_platform`")

uploaded = st.file_uploader("Choose CSV", type=["csv"], label_visibility="collapsed")

if uploaded:
    try:
        df = pd.read_csv(uploaded)
        df = _normalise_cols(df)
        errors = _validate_csv(df)
        if errors:
            for e in errors:
                st.error(e)
        else:
            prospects = df[sorted(ALL_COLS)].fillna("").to_dict("records")
            if prospects != st.session_state.prospects:
                st.session_state.prospects  = prospects
                st.session_state.results    = {}
                st.session_state.selected_idx = 0
            st.success(f"Loaded **{len(prospects)}** prospects")
    except Exception as e:
        st.error(f"Could not parse CSV: {e}")

prospects: list[dict] = st.session_state.prospects

if not prospects:
    st.info("Upload a CSV to get started.")
    st.stop()

# ── Step 2: Preview table ─────────────────────────────────────────────────────

st.markdown("### 2. Prospects")

results: dict = st.session_state.results

def _badge(email: str) -> str:
    res = results.get(email.lower().strip(), {})
    s = res.get("status", "pending")
    cls = {"done": "badge-done", "failed": "badge-failed"}.get(s, "badge-pending")
    return f'<span class="status-badge {cls}">{s}</span>'

preview_df = pd.DataFrame([{
    "#":          i + 1,
    "Name":       f"{p['first_name']} {p['last_name']}",
    "Email":      p["email"],
    "Company":    p["company"],
    "Title":      p["title"],
    "Platform":   p.get("hcm_platform", ""),
    "Status":     results.get(_prospect_key(p), {}).get("status", "pending"),
} for i, p in enumerate(prospects)])

st.dataframe(preview_df, use_container_width=True, hide_index=True,
             column_config={"Status": st.column_config.TextColumn(width="small")})

# ── Step 3: Generate ──────────────────────────────────────────────────────────

st.markdown("### 3. Generate emails")

col_gen, col_dl = st.columns([3, 1])

done_count    = sum(1 for r in results.values() if r.get("status") == "done")
pending_count = len(prospects) - done_count

with col_gen:
    gen_label = f"Generate all ({pending_count} pending)" if pending_count else "All generated"
    if st.button(gen_label, type="primary", disabled=(pending_count == 0), use_container_width=True):
        prog = st.progress(0)
        stat = st.empty()
        to_run = [p for p in prospects if results.get(_prospect_key(p), {}).get("status") != "done"]
        for i, p in enumerate(to_run):
            name = f"{p['first_name']} {p['last_name']}"
            stat.caption(f"[{i+1}/{len(to_run)}] Drafting for {name}…")
            prog.progress(int((i + 1) / len(to_run) * 100))
            key = _prospect_key(p)
            try:
                ec = _generate_for_prospect(p)
                st.session_state.results[key] = {
                    "subject": ec["subject"],
                    "body":    ec["body"],
                    "status":  "done",
                    "error":   "",
                }
            except Exception as e:
                st.session_state.results[key] = {
                    "subject": "", "body": "",
                    "status": "failed", "error": str(e),
                }
        prog.empty()
        stat.empty()
        st.rerun()

with col_dl:
    if done_count:
        st.download_button(
            "Download CSV",
            data=_results_to_csv(prospects, results),
            file_name="outreach_emails.csv",
            mime="text/csv",
            use_container_width=True,
        )

# ── Step 4: Per-prospect preview ──────────────────────────────────────────────

generated = [p for p in prospects if results.get(_prospect_key(p), {}).get("status") == "done"]

if not generated:
    st.stop()

st.divider()
st.markdown("### 4. Email preview")

left_col, right_col = st.columns([1, 2], gap="large")

with left_col:
    st.caption("Select a prospect to preview their email")
    for i, p in enumerate(generated):
        key    = _prospect_key(p)
        name   = f"{p['first_name']} {p['last_name']}"
        subj   = results[key].get("subject", "")[:55]
        active = (st.session_state.selected_idx == i)
        label  = f"**{name}**\n{p['company']} · {p['title']}"
        btn_type = "primary" if active else "secondary"
        if st.button(label, key=f"sel_{i}", use_container_width=True, type=btn_type):
            st.session_state.selected_idx = i
            st.rerun()

with right_col:
    idx = min(st.session_state.selected_idx, len(generated) - 1)
    p   = generated[idx]
    key = _prospect_key(p)
    res = results[key]

    name = f"{p['first_name']} {p['last_name']}"
    st.markdown(f"#### {name}")
    st.caption(f"{p['title']} · {p['company']}" + (f" · {p['hcm_platform']}" if p.get("hcm_platform") else ""))
    st.caption(f"📧 {p['email']}")
    st.divider()

    st.markdown("**Subject**")
    subject_val = st.text_input(
        "subject_edit", value=res["subject"],
        label_visibility="collapsed", key=f"subj_{key}"
    )

    st.markdown("**Body**")
    body_val = st.text_area(
        "body_edit", value=res["body"], height=320,
        label_visibility="collapsed", key=f"body_{key}"
    )

    # Save edits back
    if subject_val != res["subject"] or body_val != res["body"]:
        st.session_state.results[key]["subject"] = subject_val
        st.session_state.results[key]["body"]    = body_val

    btn_a, btn_b = st.columns(2)
    with btn_a:
        full_text = f"Subject: {subject_val}\n\n{body_val}"
        st.download_button(
            "Download .txt",
            data=full_text.encode(),
            file_name=f"{name.replace(' ', '_')}.txt",
            mime="text/plain",
            use_container_width=True,
        )
    with btn_b:
        # Regenerate this one prospect
        if st.button("Regenerate", use_container_width=True, key=f"regen_{key}"):
            with st.spinner("Regenerating…"):
                try:
                    ec = _generate_for_prospect(p)
                    st.session_state.results[key] = {
                        "subject": ec["subject"],
                        "body":    ec["body"],
                        "status":  "done",
                        "error":   "",
                    }
                except Exception as e:
                    st.error(str(e))
            st.rerun()

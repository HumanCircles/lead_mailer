import csv
import io
import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from core.email_drafter import draft_email
from core.gmail_sender  import send_email

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="BD Outreach",
    page_icon="✉️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; max-width: 1200px; }
    footer { visibility: hidden; }
    div[data-testid="stMetricValue"] { font-size: 1.6rem; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────

REQUIRED_COLS = {"first_name", "last_name", "email", "company", "title"}
OPTIONAL_COLS = {"hcm_platform"}

# ── Session defaults ──────────────────────────────────────────────────────────

for k, v in [("prospects", []), ("results", {}), ("sel", 0)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Helpers ───────────────────────────────────────────────────────────────────

def _key(p: dict) -> str:
    return p.get("email", "").strip().lower()

def _validate(df: pd.DataFrame) -> list[str]:
    cols = {c.lower().strip() for c in df.columns}
    return [f"Missing column: `{c}`" for c in sorted(REQUIRED_COLS - cols)]

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower().strip() for c in df.columns]
    for c in OPTIONAL_COLS:
        if c not in df.columns:
            df[c] = ""
    return df

def _to_csv(prospects: list[dict], results: dict) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["first_name", "last_name", "email", "company", "title",
                "hcm_platform", "subject", "body", "status", "error"])
    for p in prospects:
        r = results.get(_key(p), {})
        w.writerow([p.get("first_name",""), p.get("last_name",""), p.get("email",""),
                    p.get("company",""), p.get("title",""), p.get("hcm_platform",""),
                    r.get("subject",""), r.get("body",""),
                    r.get("status","pending"), r.get("error","")])
    return buf.getvalue().encode()

def _icon(status: str) -> str:
    return {"done": "🟢", "sent": "✅", "failed": "🔴", "sending": "🟡"}.get(status, "⚪")

# ── Sidebar — status only ─────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ✉️ BD Outreach")
    st.caption("CSV → OpenAI → SMTP")
    st.divider()

    oai_ok     = "✅" if os.getenv("OPENAI_API_KEY","").startswith("sk-") else "❌ missing"
    pool_count = len([e for e in os.getenv("SENDER_POOL","").split(",") if ":" in e])
    pool_ok    = f"✅ {pool_count} senders" if pool_count else "❌ missing"

    st.markdown(f"**OpenAI key:** {oai_ok}")
    st.markdown(f"**Sender pool:** {pool_ok}")
    st.markdown(f"**Model:** `{os.getenv('OPENAI_MODEL','gpt-4.1-mini')}`")
    st.markdown(f"**SMTP:** `{os.getenv('SMTP_HOST','mail.recruitagents.net')}`")
    st.caption("Edit `.env` to change settings")
    st.divider()

    prospects = st.session_state.prospects
    results   = st.session_state.results
    n_sent    = sum(1 for r in results.values() if r.get("status") == "sent")
    n_done    = sum(1 for r in results.values() if r.get("status") in ("done","sent"))
    n_fail    = sum(1 for r in results.values() if r.get("status") == "failed")

    c1, c2, c3 = st.columns(3)
    c1.metric("Total",     len(prospects))
    c2.metric("Generated", n_done)
    c3.metric("Sent",      n_sent)
    if n_fail:
        st.warning(f"{n_fail} failed")

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("# BD Outreach")
st.caption("Upload CSV · Generate personalised emails · Preview & send")
st.divider()

# ── Step 1: Upload ────────────────────────────────────────────────────────────

with st.expander("**Step 1 — Upload prospects CSV**", expanded=not bool(st.session_state.prospects)):
    st.caption("Required: `first_name` `last_name` `email` `company` `title`  ·  Optional: `hcm_platform`")
    uploaded = st.file_uploader("CSV file", type=["csv"], label_visibility="collapsed")
    if uploaded:
        try:
            df = _normalise(pd.read_csv(uploaded))
            errs = _validate(df)
            if errs:
                for e in errs:
                    st.error(e)
            else:
                all_cols = list(REQUIRED_COLS | OPTIONAL_COLS)
                new_prospects = df[[c for c in all_cols if c in df.columns]].fillna("").to_dict("records")
                if new_prospects != st.session_state.prospects:
                    st.session_state.prospects = new_prospects
                    st.session_state.results   = {}
                    st.session_state.sel       = 0
                st.success(f"Loaded **{len(new_prospects)}** prospects")
        except Exception as e:
            st.error(f"Could not read CSV: {e}")

prospects: list[dict] = st.session_state.prospects
results:   dict       = st.session_state.results

if not prospects:
    st.info("Upload a CSV above to get started.")
    st.stop()

# ── Step 2: Prospects table ───────────────────────────────────────────────────

st.markdown("### Prospects")

st.dataframe(
    pd.DataFrame([{
        "#":       i + 1,
        "Name":    f"{p['first_name']} {p['last_name']}",
        "Email":   p["email"],
        "Company": p["company"],
        "Title":   p["title"],
        "":        _icon(results.get(_key(p), {}).get("status", "pending")),
    } for i, p in enumerate(prospects)]),
    width="stretch",
    hide_index=True,
    column_config={"": st.column_config.TextColumn(width="small")},
)

# ── Step 3: Generate ──────────────────────────────────────────────────────────

st.markdown("### Generate emails")

pending = [p for p in prospects if results.get(_key(p), {}).get("status") not in ("done","sent")]

col_gen, col_dl = st.columns([3, 1])
with col_gen:
    if st.button(
        f"Generate all  ({len(pending)} pending)" if pending else "✅ All generated",
        type="primary", disabled=not pending, width="stretch",
    ):
        prog = st.progress(0)
        stat = st.empty()
        for i, p in enumerate(pending):
            stat.caption(f"[{i+1}/{len(pending)}] Drafting for {p['first_name']} {p['last_name']}…")
            prog.progress(int((i + 1) / len(pending) * 100))
            k = _key(p)
            try:
                ec = draft_email({
                    "name":         f"{p['first_name']} {p['last_name']}",
                    "company":      p.get("company", ""),
                    "title":        p.get("title", ""),
                    "hcm_platform": p.get("hcm_platform", ""),
                })
                st.session_state.results[k] = {"subject": ec["subject"], "body": ec["body"],
                                                "status": "done", "error": ""}
            except Exception as e:
                st.session_state.results[k] = {"subject":"","body":"","status":"failed","error":str(e)}
        prog.empty(); stat.empty()
        st.rerun()

with col_dl:
    if n_done:
        st.download_button("⬇ Download CSV", data=_to_csv(prospects, results),
                           file_name="outreach_emails.csv", mime="text/csv",
                           width="stretch")

# ── Step 4: Preview & Send ────────────────────────────────────────────────────

generated = [p for p in prospects if results.get(_key(p), {}).get("status") in ("done","sent")]

if not generated:
    st.stop()

st.divider()

hdr_col, send_all_col = st.columns([3, 1])
hdr_col.markdown("### Preview & Send")

unsent = [p for p in generated if results.get(_key(p), {}).get("status") != "sent"]
with send_all_col:
    if st.button(f"Send all ({len(unsent)})", type="primary",
                 disabled=not unsent, width="stretch"):
        prog = st.progress(0)
        stat = st.empty()
        failed = 0
        for i, p in enumerate(unsent):
            k   = _key(p)
            res = results[k]
            stat.caption(f"[{i+1}/{len(unsent)}] Sending to {p['first_name']} {p['last_name']}…")
            prog.progress(int((i + 1) / len(unsent) * 100))
            try:
                send_email(p["email"], res["subject"], res["body"])
                st.session_state.results[k]["status"] = "sent"
            except Exception as e:
                st.session_state.results[k]["status"] = "failed"
                st.session_state.results[k]["error"]  = str(e)
                failed += 1
        prog.empty(); stat.empty()
        sent_n = len(unsent) - failed
        st.success(f"Sent {sent_n}" + (f", {failed} failed" if failed else ""))
        st.rerun()

left, right = st.columns([1, 2], gap="large")

with left:
    st.caption(f"{len(generated)} emails generated")
    for i, p in enumerate(generated):
        k      = _key(p)
        status = results[k].get("status", "done")
        name   = f"{p['first_name']} {p['last_name']}"
        if st.button(f"{_icon(status)} **{name}**  \n{p['company']}",
                     key=f"sel_{i}", width="stretch",
                     type="primary" if st.session_state.sel == i else "secondary"):
            st.session_state.sel = i
            st.rerun()

with right:
    idx = min(st.session_state.sel, len(generated) - 1)
    p   = generated[idx]
    k   = _key(p)
    res = results[k]

    name   = f"{p['first_name']} {p['last_name']}"
    status = res.get("status", "done")

    st.markdown(f"#### {name}  {_icon(status)}")
    st.caption(f"{p['title']} · {p['company']}" +
               (f" · {p['hcm_platform']}" if p.get("hcm_platform") else ""))
    st.caption(f"📧 {p['email']}")
    st.divider()

    subj = st.text_input("Subject", value=res.get("subject", ""), key=f"s_{k}")
    body = st.text_area("Body", value=res.get("body", ""), height=300, key=f"b_{k}")

    if subj != res.get("subject") or body != res.get("body"):
        st.session_state.results[k]["subject"] = subj
        st.session_state.results[k]["body"]    = body

    btn1, btn2, btn3 = st.columns(3)

    with btn1:
        st.download_button("⬇ .txt", data=f"Subject: {subj}\n\n{body}".encode(),
                           file_name=f"{name.replace(' ','_')}.txt",
                           width="stretch")
    with btn2:
        if st.button("↺ Regenerate", width="stretch", key=f"regen_{k}"):
            with st.spinner("Regenerating…"):
                try:
                    ec = draft_email({"name": name, "company": p.get("company",""),
                                      "title": p.get("title",""),
                                      "hcm_platform": p.get("hcm_platform","")})
                    st.session_state.results[k].update(subject=ec["subject"],
                                                        body=ec["body"], status="done", error="")
                except Exception as e:
                    st.error(str(e))
            st.rerun()
    with btn3:
        if status != "sent":
            if st.button("Send ✉️", type="primary", width="stretch", key=f"send_{k}"):
                with st.spinner("Sending…"):
                    try:
                        from_addr = send_email(p["email"], subj, body)
                        st.session_state.results[k]["status"] = "sent"
                        st.success(f"Sent via {from_addr}")
                    except Exception as e:
                        st.session_state.results[k]["status"] = "failed"
                        st.session_state.results[k]["error"]  = str(e)
                        st.error(str(e))
                st.rerun()
        else:
            st.success("Sent ✅")

    if res.get("error"):
        st.error(res["error"])

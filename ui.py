import csv
import io
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from core.deliverability import is_suppressed, reload_suppression
from core.email_drafter import draft_email
from core.logger import IST
from core.pipeline import CONCURRENT_SENDS, SENT_LOG_FILE, run_pipeline
from core.prospect_csv import detect_column_mapping, normalise_prospects_dataframe
from core.sendgrid_sender import (
    DAILY_LIMIT,
    HOURLY_LIMIT,
    _pool,
    _pool_lock,
    _sender_state,
    send_email,
    send_seed_email,
)

try:
    from fetch_inboxes import fetch_inbox as _fetch_inbox_fn
    _FETCH_INBOX_AVAILABLE = True
except Exception:
    _fetch_inbox_fn = None  # type: ignore[assignment]
    _FETCH_INBOX_AVAILABLE = False

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="BD Outreach",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .block-container { padding-top: 5rem; }
    footer { visibility: hidden; }
    div[data-testid="stMetricValue"] { font-size: 1.6rem; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────

REQUIRED_COLS = {"first_name", "last_name", "email", "company", "title"}
OPTIONAL_COLS = {"hcm_platform"}
INBOX_CSV     = "inbox_replies.csv"
INBOX_FIELDS  = ["inbox", "from", "subject", "date", "body"]

_LOG_HEADERS = [
    "timestamp", "prospect_email", "prospect_name",
    "company", "subject", "status", "error", "from_email",
]

# ── Session defaults ──────────────────────────────────────────────────────────

_DEFAULTS: dict = {
    # outreach tab
    "prospects":         [],
    "results":           {},
    "sel":               0,
    # batch tab
    "batch_prospects":   [],
    "batch_log":         [],
    "batch_running":     False,
    "batch_done":        0,
    "batch_total":       0,
    "batch_stop_event":  None,
    # inbox tab
    "inbox_running":     False,
    "inbox_done":        0,
    "inbox_total":       0,
    "inbox_rows":        [],
    "inbox_stop_event":  None,
    "inbox_acct_log":    [],
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ── Helpers ───────────────────────────────────────────────────────────────────


def _key(p: dict) -> str:
    rid = p.get("_row_id", 0)
    e   = p.get("email", "").strip().lower()
    return f"{rid}:{e}"


def _has_contact_email(p: dict) -> bool:
    return bool(str(p.get("email", "")).strip())


def _initial_outreach_status(p: dict) -> str:
    return "no_email" if not _has_contact_email(p) else "pending"


def _outreach_status_label(p: dict, results: dict) -> str:
    if not _has_contact_email(p):
        return "no_email"
    if is_suppressed(p.get("email", "")):
        return "suppressed"
    st_ = results.get(_key(p), {}).get("status", "pending")
    if st_ == "done":
        return "generated"
    if st_ in ("sent", "failed"):
        return st_
    return "pending"


def _validate(df: pd.DataFrame) -> list[str]:
    errs: list[str] = []
    if df.empty:
        return ["No rows found in uploaded CSV."]
    if not df["email"].astype(str).str.strip().any():
        errs.append("No usable email column found.")
    return errs


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    df = normalise_prospects_dataframe(df)
    for c in OPTIONAL_COLS:
        if c not in df.columns:
            df[c] = ""
    return df


def _to_csv(prospects: list[dict], results: dict) -> bytes:
    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(["first_name", "last_name", "email", "company", "title",
                "hcm_platform", "outreach_status", "subject", "body", "status", "error"])
    for p in prospects:
        r = results.get(_key(p), {})
        w.writerow([p.get("first_name", ""), p.get("last_name", ""), p.get("email", ""),
                    p.get("company", ""), p.get("title", ""), p.get("hcm_platform", ""),
                    _outreach_status_label(p, results),
                    r.get("subject", ""), r.get("body", ""),
                    r.get("status", "pending"), r.get("error", "")])
    return buf.getvalue().encode()


def _icon(status: str) -> str:
    return {"done": "✅", "sent": "📤", "failed": "❌", "sending": "⏳"}.get(status, "⏸️")


def _row_icon(p: dict, results: dict) -> str:
    if not _has_contact_email(p):
        return "📭"
    if is_suppressed(p.get("email", "")):
        return "🚫"
    return _icon(results.get(_key(p), {}).get("status", "pending"))


def _confidence_badge(conf: str) -> str:
    return {"high": "✅ high", "low": "⚠️ low", "missing": "❌ missing"}.get(conf, conf)


def _load_sent_log() -> pd.DataFrame:
    if not os.path.isfile(SENT_LOG_FILE):
        return pd.DataFrame(columns=_LOG_HEADERS)
    try:
        df = pd.read_csv(SENT_LOG_FILE, dtype=str, keep_default_na=False)
        # If the CSV has no header row (written by agent.py), the first data
        # row becomes the column names. Detect this by checking whether the
        # first column looks like a timestamp rather than "timestamp".
        if "timestamp" not in df.columns:
            df = pd.read_csv(
                SENT_LOG_FILE, names=_LOG_HEADERS, header=None,
                dtype=str, keep_default_na=False,
            )
        return df
    except Exception:
        return pd.DataFrame(columns=_LOG_HEADERS)


def _ist_display(ts_utc: str) -> str:
    try:
        dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
        return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        return ts_utc


def _load_pool_with_passwords() -> list[tuple[str, str]]:
    """Parse SENDER_POOL env var into (email, password) pairs."""
    raw   = os.getenv("SENDER_POOL", "").strip()
    pairs = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            email, pwd = entry.split(":", 1)
            pairs.append((email.strip(), pwd.strip()))
    return pairs


def _load_inbox_csv() -> pd.DataFrame:
    if not os.path.isfile(INBOX_CSV):
        return pd.DataFrame(columns=INBOX_FIELDS)
    try:
        return pd.read_csv(INBOX_CSV, dtype=str, keep_default_na=False)
    except Exception:
        return pd.DataFrame(columns=INBOX_FIELDS)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## BD Outreach")
    st.caption("CSV → OpenAI → SendGrid")
    st.divider()

    oai_ok     = "configured" if os.getenv("OPENAI_API_KEY", "").startswith("sk-") else "missing"
    pool_count = len([e for e in os.getenv("SENDER_POOL", "").split(",") if "@" in e])

    # Count distinct SendGrid domains
    _sg_raw   = os.getenv("SENDGRID_ACCOUNTS", "").strip()
    _sg_doms  = [t.split(":")[0].strip() for t in _sg_raw.split(",") if t.strip()] if _sg_raw else []
    sg_count  = len(_sg_doms)

    pool_label = f"{pool_count} senders configured" if pool_count else "missing"

    st.markdown(f"**OpenAI key:** {oai_ok}")
    st.markdown(f"**SendGrid:** {sg_count} domain(s) configured")
    st.markdown(f"**Sender pool:** {pool_label}")
    st.markdown(f"**Model:** `{os.getenv('OPENAI_MODEL', 'gpt-4.1-mini')}`")
    st.markdown(f"**Daily limit:** {DAILY_LIMIT}  |  **Hourly:** {HOURLY_LIMIT}")
    st.caption("Edit `.env` to change settings")

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_dash, tab_outreach, tab_batch, tab_inbox, tab_log = st.tabs([
    "Dashboard",
    "Outreach",
    "Batch",
    "Inbox",
    "Activity Log",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

with tab_dash:
    st.markdown("# Dashboard")

    if st.button("↺ Refresh", key="dash_refresh"):
        st.rerun()

    # ── Load data ─────────────────────────────────────────────────────────────
    log_df_d   = _load_sent_log()
    today_ist  = datetime.now(tz=IST).strftime("%Y-%m-%d")

    if not log_df_d.empty and "timestamp" in log_df_d.columns:
        log_df_d["_ts_ist"] = log_df_d["timestamp"].apply(_ist_display)
        today_rows_d = log_df_d[log_df_d["_ts_ist"].str.startswith(today_ist, na=False)]
    else:
        today_rows_d = pd.DataFrame(columns=_LOG_HEADERS)

    sent_today   = int((today_rows_d.get("status", pd.Series(dtype=str)) == "pushed").sum())
    failed_today = int((today_rows_d.get("status", pd.Series(dtype=str)) == "failed_api").sum())
    total_sent   = int((log_df_d.get("status", pd.Series(dtype=str)) == "pushed").sum()) if not log_df_d.empty else 0

    # Sender pool count
    pool_display = f"{pool_count} senders" if pool_count else "—"

    # Inbox replies count
    inbox_df_d = _load_inbox_csv()
    inbox_reply_count = len(inbox_df_d) if not inbox_df_d.empty else 0
    inbox_display = str(inbox_reply_count) if inbox_reply_count else "—"

    # ── 5 metrics ─────────────────────────────────────────────────────────────
    mc1, mc2, mc3, mc4, mc5 = st.columns(5)
    mc1.metric("Sent today",       sent_today)
    mc2.metric("Failed today",     failed_today)
    mc3.metric("Total sent",       total_sent)
    mc4.metric("Sender pool",      pool_display)
    mc5.metric("Inbox replies",    inbox_display)

    st.divider()

    # ── Sender usage today (by domain) ────────────────────────────────────────
    st.markdown("**Sender usage today**")

    with _pool_lock:
        _state_snapshot = dict(_sender_state)

    if _state_snapshot:
        # Group by domain
        _domain_counts: dict[str, int]   = {}
        _domain_senders: dict[str, int]  = {}
        for sender_email, state in _state_snapshot.items():
            domain = sender_email.split("@")[-1] if "@" in sender_email else "unknown"
            _domain_counts[domain]  = _domain_counts.get(domain, 0)  + state.get("daily", 0)
            _domain_senders[domain] = _domain_senders.get(domain, 0) + 1

        for domain, total_count in sorted(_domain_counts.items()):
            n_senders  = _domain_senders.get(domain, 1)
            cap        = DAILY_LIMIT * n_senders
            pct        = min(total_count / max(cap, 1), 1.0)
            st.markdown(f"`{domain}` — {total_count} / {cap} ({n_senders} senders)")
            st.progress(pct)
    else:
        st.caption("No sender state loaded yet.")

    st.divider()

    # ── Recent sends ──────────────────────────────────────────────────────────
    st.markdown("**Recent sends** (last 10)")
    if not log_df_d.empty:
        show_cols_d = [c for c in ["_ts_ist", "prospect_email", "prospect_name",
                                    "company", "status", "from_email", "subject"]
                       if c in log_df_d.columns]
        recent_d = log_df_d[show_cols_d].iloc[::-1].head(10).reset_index(drop=True)
        st.dataframe(recent_d, hide_index=True, use_container_width=True,
                     column_config={
                         "_ts_ist": st.column_config.TextColumn("Timestamp (IST)", width="medium"),
                     })
    else:
        st.info("No sends logged yet.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — OUTREACH
# ══════════════════════════════════════════════════════════════════════════════

with tab_outreach:
    st.markdown("# BD Outreach")
    st.caption("Upload CSV · Generate personalised emails · Preview & send")
    st.divider()

    with st.expander("**Step 1 — Upload prospects CSV**",
                     expanded=not bool(st.session_state.prospects)):
        st.caption(
            "Required: `first_name` `last_name` `email` `company` `title`  ·  Optional: `hcm_platform`"
        )
        uploaded = st.file_uploader("CSV file", type=["csv"], label_visibility="collapsed",
                                    key="tab1_upload")
        if uploaded:
            try:
                raw_df = pd.read_csv(uploaded, dtype=str, keep_default_na=False,
                                     engine="python", on_bad_lines="skip")
                df = _normalise(raw_df)
                errs = _validate(df)
                if errs:
                    for e in errs:
                        st.error(e)
                else:
                    # ── Mapping preview ──────────────────────────────────────
                    mapping = detect_column_mapping(raw_df)
                    st.markdown("**Detected column mapping:**")
                    rows_preview = []
                    any_low              = False
                    any_missing_required = False
                    email_missing        = False
                    for field, info in mapping.items():
                        conf = info["confidence"]
                        if conf == "low":
                            any_low = True
                        if conf == "missing" and info.get("required"):
                            any_missing_required = True
                        if field == "email" and conf == "missing":
                            email_missing = True
                        rows_preview.append({
                            "Field":       field,
                            "Mapped from": info["mapped_from"] or "(not found)",
                            "Confidence":  _confidence_badge(conf),
                        })
                    st.dataframe(pd.DataFrame(rows_preview), hide_index=True, width=600)

                    if email_missing:
                        st.error("No email column detected — cannot proceed.")
                    else:
                        if any_low:
                            st.warning("Some columns mapped with low confidence — review the table above.")
                        if any_missing_required:
                            st.caption("Missing optional fields will be blank.")

                        # ── Load prospects ───────────────────────────────────
                        all_cols   = list(REQUIRED_COLS | OPTIONAL_COLS)
                        raw_rows   = df[[c for c in all_cols if c in df.columns]].fillna("").to_dict("records")
                        new_prospects = []
                        for i, row in enumerate(raw_rows):
                            p = dict(row)
                            p["_row_id"]         = i
                            p["outreach_status"] = _initial_outreach_status(p)
                            new_prospects.append(p)
                        if new_prospects != st.session_state.prospects:
                            st.session_state.prospects = new_prospects
                            st.session_state.results   = {}
                            st.session_state.sel       = 0
                        st.success(f"Loaded **{len(new_prospects)}** prospects")
            except Exception as e:
                st.error(f"Could not read CSV: {e}")

    prospects: list[dict] = st.session_state.prospects
    results:   dict       = st.session_state.results

    for i, p in enumerate(prospects):
        if "_row_id" not in p:
            p["_row_id"] = i
        if "outreach_status" not in p:
            p["outreach_status"] = _initial_outreach_status(p)

    if not prospects:
        st.info("Upload a CSV above to get started.")
    else:
        # Prospects table
        st.markdown("### Prospects")

        def _err_cell(p: dict, results: dict) -> str:
            r = results.get(_key(p), {})
            if r.get("status") != "failed":
                return ""
            return str(r.get("error", "") or "")[:60]

        st.dataframe(
            pd.DataFrame([{
                "#":               i + 1,
                "Name":            f"{p['first_name']} {p['last_name']}",
                "Email":           p["email"],
                "Company":         p["company"],
                "Title":           p["title"],
                "outreach_status": _outreach_status_label(p, results),
                "Error":           _err_cell(p, results),
                "":                _row_icon(p, results),
            } for i, p in enumerate(prospects)]),
            width="stretch",
            hide_index=True,
            column_config={
                "":      st.column_config.TextColumn(width="small"),
                "Error": st.column_config.TextColumn(width="large"),
            },
        )

        # Generate + send
        st.markdown("### Generate emails")

        pending_gen = [
            p for p in prospects
            if _has_contact_email(p)
            and results.get(_key(p), {}).get("status") not in ("done", "sent")
        ]
        pending_gs = [
            p for p in prospects
            if _has_contact_email(p)
            and not is_suppressed(p.get("email", ""))
            and results.get(_key(p), {}).get("status") != "sent"
        ]

        col_gen, col_gs, col_dl = st.columns([2, 2, 1])
        with col_gen:
            if st.button(
                f"Generate all ({len(pending_gen)} pending)" if pending_gen else "All generated",
                type="primary", disabled=not pending_gen, use_container_width=True,
            ):
                prog = st.progress(0)
                stat = st.empty()
                for i, p in enumerate(pending_gen):
                    stat.caption(f"[{i+1}/{len(pending_gen)}] Drafting {p['first_name']} {p['last_name']}…")
                    prog.progress(int((i + 1) / len(pending_gen) * 100))
                    k = _key(p)
                    try:
                        ec = draft_email({
                            "name":         f"{p['first_name']} {p['last_name']}",
                            "company":      p.get("company", ""),
                            "title":        p.get("title", ""),
                            "hcm_platform": p.get("hcm_platform", ""),
                        })
                        st.session_state.results[k] = {
                            "subject": ec["subject"], "body": ec["body"],
                            "status": "done", "error": "",
                        }
                    except Exception as e:
                        st.session_state.results[k] = {
                            "subject": "", "body": "", "status": "failed", "error": str(e),
                        }
                prog.empty(); stat.empty()
                st.rerun()

        with col_gs:
            if st.button(
                f"Generate & send all ({len(pending_gs)})" if pending_gs else "Nothing to send",
                type="primary", disabled=not pending_gs, use_container_width=True,
            ):
                prog = st.progress(0)
                stat = st.empty()
                n = len(pending_gs)
                _gs_lock = threading.Lock()
                _gs_done = [0]

                def _draft_and_send(p):
                    k  = _key(p)
                    r0 = st.session_state.results.get(k, {})
                    has_draft = bool(
                        str(r0.get("subject", "")).strip() and str(r0.get("body", "")).strip()
                    )
                    if not has_draft:
                        try:
                            ec = draft_email({
                                "name":         f"{p['first_name']} {p['last_name']}",
                                "company":      p.get("company", ""),
                                "title":        p.get("title", ""),
                                "hcm_platform": p.get("hcm_platform", ""),
                            })
                            with _gs_lock:
                                st.session_state.results[k] = {
                                    "subject": ec["subject"], "body": ec["body"],
                                    "status": "done", "error": "",
                                }
                        except Exception as e:
                            with _gs_lock:
                                st.session_state.results[k] = {
                                    "subject": "", "body": "", "status": "failed", "error": str(e),
                                }
                            return k, str(e)
                    r = st.session_state.results[k]
                    try:
                        send_email(p["email"], r["subject"], r["body"], p.get("first_name", ""))
                        with _gs_lock:
                            st.session_state.results[k]["status"] = "sent"
                            st.session_state.results[k]["error"]  = ""
                        return k, None
                    except Exception as e:
                        with _gs_lock:
                            st.session_state.results[k]["status"] = "failed"
                            st.session_state.results[k]["error"]  = str(e)
                        return k, str(e)

                with ThreadPoolExecutor(max_workers=CONCURRENT_SENDS) as _ex:
                    _futs = {_ex.submit(_draft_and_send, p): p for p in pending_gs}
                    for _fut in as_completed(_futs):
                        with _gs_lock:
                            _gs_done[0] += 1
                            _i = _gs_done[0]
                        _p  = _futs[_fut]
                        stat.caption(f"[{_i}/{n}] {_p['first_name']} {_p['last_name']} done")
                        prog.progress(int(_i / n * 100))
                prog.empty(); stat.empty()
                st.rerun()

        failed_rows = [
            p for p in prospects
            if _has_contact_email(p) and results.get(_key(p), {}).get("status") == "failed"
        ]
        if failed_rows:
            if st.button(f"↺ Retry failed ({len(failed_rows)})", type="secondary", key="retry_bulk"):
                for p in failed_rows:
                    st.session_state.results[_key(p)]["status"] = "pending"
                st.rerun()

        with col_dl:
            n_done = sum(1 for r in results.values() if r.get("status") in ("done", "sent"))
            if n_done:
                st.download_button("Download CSV", data=_to_csv(prospects, results),
                                   file_name="outreach_emails.csv", mime="text/csv",
                                   use_container_width=True)

        # Preview & Send section (only shown when emails exist)
        generated = [p for p in prospects if results.get(_key(p), {}).get("status") in ("done", "sent")]
        if generated:
            st.divider()
            hdr_col, send_all_col = st.columns([3, 1])
            hdr_col.markdown("### Preview & Send")

            unsent = [
                p for p in generated
                if results.get(_key(p), {}).get("status") != "sent"
                and not is_suppressed(p.get("email", ""))
            ]
            with send_all_col:
                if st.button(f"Send all ({len(unsent)})", type="primary",
                             disabled=not unsent, use_container_width=True):
                    prog = st.progress(0)
                    stat = st.empty()
                    _sa_lock  = threading.Lock()
                    _sa_done  = [0]
                    _sa_failed = [0]

                    def _send_one(p):
                        k   = _key(p)
                        res = results[k]
                        try:
                            send_email(p["email"], res["subject"], res["body"], p.get("first_name", ""))
                            with _sa_lock:
                                st.session_state.results[k]["status"] = "sent"
                            return None
                        except Exception as e:
                            with _sa_lock:
                                st.session_state.results[k]["status"] = "failed"
                                st.session_state.results[k]["error"]  = str(e)
                            return str(e)

                    with ThreadPoolExecutor(max_workers=CONCURRENT_SENDS) as _ex:
                        _futs = {_ex.submit(_send_one, p): p for p in unsent}
                        for _fut in as_completed(_futs):
                            err = _fut.result()
                            with _sa_lock:
                                _sa_done[0] += 1
                                if err:
                                    _sa_failed[0] += 1
                                _i = _sa_done[0]
                            _p = _futs[_fut]
                            stat.caption(f"[{_i}/{len(unsent)}] {_p['first_name']} {_p['last_name']}…")
                            prog.progress(int(_i / len(unsent) * 100))
                    prog.empty(); stat.empty()
                    st.success(f"Sent {len(unsent)-_sa_failed[0]}" + (f", {_sa_failed[0]} failed" if _sa_failed[0] else ""))
                    st.rerun()

            left, right = st.columns([1, 2], gap="large")
            with left:
                st.caption(f"{len(generated)} emails generated")
                for i, p in enumerate(generated):
                    name = f"{p['first_name']} {p['last_name']}"
                    if st.button(
                        f"{_row_icon(p, results)} **{name}**  \n{p['company']}",
                        key=f"sel_{i}", use_container_width=True,
                        type="primary" if st.session_state.sel == i else "secondary",
                    ):
                        st.session_state.sel = i
                        st.rerun()

            with right:
                idx    = min(st.session_state.sel, len(generated) - 1)
                p      = generated[idx]
                k      = _key(p)
                res    = results[k]
                name   = f"{p['first_name']} {p['last_name']}"
                status = res.get("status", "done")
                st.markdown(f"#### {name}  {_icon(status)}")
                st.caption(
                    f"{p['title']} · {p['company']}"
                    + (f" · {p['hcm_platform']}" if p.get("hcm_platform") else "")
                )
                st.caption(f"Email: {p['email']}")
                st.divider()
                subj = st.text_input("Subject", value=res.get("subject", ""), key=f"s_{k}")
                body = st.text_area("Body", value=res.get("body", ""), height=300, key=f"b_{k}")
                if subj != res.get("subject") or body != res.get("body"):
                    st.session_state.results[k]["subject"] = subj
                    st.session_state.results[k]["body"]    = body

                btn1, btn2, btn3 = st.columns(3)
                with btn1:
                    st.download_button(
                        "Download .txt",
                        data=f"Subject: {subj}\n\n{body}".encode(),
                        file_name=f"{name.replace(' ', '_')}.txt",
                        use_container_width=True,
                    )
                with btn2:
                    if st.button("↺ Regenerate", use_container_width=True, key=f"regen_{k}"):
                        with st.spinner("Regenerating…"):
                            try:
                                ec = draft_email({
                                    "name":         name,
                                    "company":      p.get("company", ""),
                                    "title":        p.get("title", ""),
                                    "hcm_platform": p.get("hcm_platform", ""),
                                })
                                st.session_state.results[k].update(
                                    subject=ec["subject"], body=ec["body"],
                                    status="done", error="",
                                )
                            except Exception as e:
                                st.error(str(e))
                        st.rerun()
                with btn3:
                    if is_suppressed(p["email"]):
                        st.caption("Send disabled (suppression list)")
                    elif status != "sent":
                        if st.button("Send", type="primary", use_container_width=True, key=f"send_{k}"):
                            with st.spinner("Sending…"):
                                try:
                                    from_addr, _ = send_email(
                                        p["email"], subj, body, p.get("first_name", "")
                                    )
                                    st.session_state.results[k]["status"] = "sent"
                                    st.success(f"Sent via {from_addr}")
                                except Exception as e:
                                    st.session_state.results[k]["status"] = "failed"
                                    st.session_state.results[k]["error"]  = str(e)
                                    st.error(str(e))
                            st.rerun()
                    else:
                        st.success("Sent")
                if res.get("error"):
                    st.error(res["error"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — BATCH
# ══════════════════════════════════════════════════════════════════════════════

with tab_batch:
    st.markdown("# Batch Run")
    st.caption("Upload a CSV, click Start — the full concurrent pipeline runs in-browser.")

    uploaded_batch = st.file_uploader("Prospects CSV", type=["csv"],
                                      label_visibility="collapsed", key="batch_upload")
    if uploaded_batch:
        try:
            raw_df_b = pd.read_csv(uploaded_batch, dtype=str, keep_default_na=False,
                                   engine="python", on_bad_lines="skip")
            df_b = _normalise(raw_df_b)
            if not df_b.empty:
                mapping_b = detect_column_mapping(raw_df_b)
                with st.expander("Column mapping", expanded=False):
                    st.dataframe(pd.DataFrame([{
                        "Field":       f,
                        "Mapped from": i["mapped_from"] or "(not found)",
                        "Confidence":  _confidence_badge(i["confidence"]),
                    } for f, i in mapping_b.items()]), hide_index=True)

                all_cols_b = list(REQUIRED_COLS | OPTIONAL_COLS)
                rows_b     = df_b[[c for c in all_cols_b if c in df_b.columns]].fillna("").to_dict("records")
                new_batch  = [dict(r, _row_id=i) for i, r in enumerate(rows_b)]
                if new_batch != st.session_state.batch_prospects:
                    st.session_state.batch_prospects = new_batch
                    st.session_state.batch_log       = []
                    st.session_state.batch_done      = 0
                    st.session_state.batch_total     = len(new_batch)
                st.success(f"Loaded **{len(new_batch)}** prospects")
        except Exception as e:
            st.error(f"Could not read CSV: {e}")

    batch_prospects = st.session_state.batch_prospects
    if not batch_prospects:
        st.info("Upload a CSV above to start a batch run.")
    else:
        already_sent_b: set[str] = set()
        if os.path.isfile(SENT_LOG_FILE):
            try:
                sl = pd.read_csv(SENT_LOG_FILE, dtype=str, keep_default_na=False)
                already_sent_b = set(
                    sl[sl["status"] == "pushed"]["prospect_email"]
                    .str.strip().str.lower().tolist()
                )
            except Exception:
                pass

        pending_b = [
            p for p in batch_prospects
            if str(p.get("email", "")).strip().lower() not in already_sent_b
        ]

        col_start, col_stop, col_dryrun = st.columns([2, 1, 1])
        dry_run_b = col_dryrun.checkbox("Dry run (no send)", key="batch_dryrun")

        with col_start:
            start_disabled = st.session_state.batch_running or not pending_b
            if st.button(
                f"▶ Start batch ({len(pending_b)} pending)"
                if not st.session_state.batch_running else "⏳ Running…",
                type="primary",
                disabled=start_disabled,
                use_container_width=True,
            ):
                stop_ev = threading.Event()
                st.session_state.batch_stop_event = stop_ev
                st.session_state.batch_running    = True
                st.session_state.batch_log        = []
                st.session_state.batch_done       = 0
                st.session_state.batch_total      = len(pending_b)

                _log_lock_b = threading.Lock()

                def _on_result_b(row: dict) -> None:
                    with _log_lock_b:
                        st.session_state.batch_log.append(row)
                    try:
                        if not os.path.isfile(SENT_LOG_FILE):
                            with open(SENT_LOG_FILE, "w", newline="", encoding="utf-8") as f:
                                csv.DictWriter(f, fieldnames=_LOG_HEADERS).writeheader()
                        with open(SENT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
                            csv.DictWriter(f, fieldnames=_LOG_HEADERS, extrasaction="ignore").writerow(row)
                    except Exception:
                        pass

                def _on_progress_b(done: int, total: int) -> None:
                    st.session_state.batch_done  = done
                    st.session_state.batch_total = total

                def _run_batch() -> None:
                    run_pipeline(
                        prospects=batch_prospects,
                        already_sent=already_sent_b,
                        on_result=_on_result_b,
                        on_progress=_on_progress_b,
                        stop_event=stop_ev,
                        dry_run=dry_run_b,
                    )
                    st.session_state.batch_running = False

                    # Seed email summary to admin
                    log_rows = st.session_state.batch_log
                    pushed   = sum(1 for r in log_rows if r.get("status") == "pushed")
                    failed   = sum(1 for r in log_rows if r.get("status") == "failed_api")
                    dry_tag  = " [DRY RUN]" if dry_run_b else ""
                    ts_label = datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M IST")
                    send_seed_email(
                        subject=f"[BD Outreach UI] Batch complete{dry_tag} — {ts_label}",
                        body=(
                            f"Batch run completed via UI{dry_tag}.\n\n"
                            f"Prospects loaded: {len(batch_prospects)}\n"
                            f"Pushed: {pushed}\n"
                            f"Failed: {failed}\n"
                            f"Total processed: {len(log_rows)}\n"
                        ),
                    )

                t = threading.Thread(target=_run_batch, daemon=True)
                t.start()
                st.rerun()

        with col_stop:
            if st.button("⏹ Stop", disabled=not st.session_state.batch_running,
                         use_container_width=True):
                if st.session_state.batch_stop_event:
                    st.session_state.batch_stop_event.set()

        # Progress bar
        done_b    = st.session_state.batch_done
        total_b   = st.session_state.batch_total
        if total_b > 0:
            st.progress(done_b / total_b, text=f"{done_b}/{total_b} processed")

        # Live log table
        batch_log = st.session_state.batch_log
        if batch_log:
            st.markdown(f"**Results** ({len(batch_log)} rows so far)")
            log_df_b = pd.DataFrame(batch_log[::-1])
            if "timestamp" in log_df_b.columns:
                log_df_b["timestamp"] = log_df_b["timestamp"].apply(_ist_display)
            st.dataframe(log_df_b, hide_index=True, use_container_width=True,
                         column_config={
                             "error": st.column_config.TextColumn(width="large"),
                         })

        # Auto-rerun while running
        if st.session_state.batch_running:
            time.sleep(0.8)
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — INBOX
# ══════════════════════════════════════════════════════════════════════════════

with tab_inbox:
    st.markdown("# Inbox")
    st.caption("Fetch replies from all sender inboxes via IMAP.")

    if not _FETCH_INBOX_AVAILABLE:
        st.warning(
            "`fetch_inboxes` module could not be imported. "
            "Make sure `fetch_inboxes.py` exists and its dependencies are installed."
        )
    else:
        # ── Date range & options ──────────────────────────────────────────────
        _today_date   = datetime.now(tz=IST).date()
        _default_since = _today_date - timedelta(days=7)

        dcol1, dcol2, dcol3 = st.columns([2, 2, 2])
        with dcol1:
            since_date = st.date_input("Since", value=_default_since, key="inbox_since")
        with dcol2:
            until_date = st.date_input("Until", value=_today_date,   key="inbox_until")
        with dcol3:
            inbox_workers = st.slider("Workers", min_value=10, max_value=50,
                                      value=30, step=5, key="inbox_workers")

        since_dt = datetime(since_date.year, since_date.month, since_date.day,
                            tzinfo=timezone.utc)
        until_dt = datetime(until_date.year, until_date.month, until_date.day,
                            23, 59, 59, tzinfo=timezone.utc)

        # ── Start / Stop ──────────────────────────────────────────────────────
        ib_col1, ib_col2 = st.columns([2, 1])

        with ib_col1:
            ib_start_disabled = st.session_state.inbox_running
            if st.button(
                "▶ Fetch inboxes" if not st.session_state.inbox_running else "⏳ Fetching…",
                type="primary",
                disabled=ib_start_disabled,
                use_container_width=True,
                key="inbox_start",
            ):
                pool_pairs = _load_pool_with_passwords()
                if not pool_pairs:
                    st.error("SENDER_POOL is not configured — cannot fetch inboxes.")
                else:
                    stop_ev_ib = threading.Event()
                    st.session_state.inbox_stop_event = stop_ev_ib
                    st.session_state.inbox_running    = True
                    st.session_state.inbox_done       = 0
                    st.session_state.inbox_total      = len(pool_pairs)
                    st.session_state.inbox_rows       = []
                    st.session_state.inbox_acct_log   = []

                    _inbox_lock = threading.Lock()

                    def _run_inbox_fetch(
                        _pairs: list[tuple[str, str]],
                        _since: datetime,
                        _until: datetime,
                        _workers: int,
                        _stop: threading.Event,
                    ) -> None:
                        # Determine already-fetched inboxes if file exists (fresh run = cleared above)
                        _done_accounts: set[str] = set()

                        csv_write_lock = threading.Lock()

                        # Ensure CSV header exists
                        with csv_write_lock:
                            if not os.path.isfile(INBOX_CSV):
                                with open(INBOX_CSV, "w", newline="", encoding="utf-8") as _f:
                                    csv.DictWriter(_f, fieldnames=INBOX_FIELDS).writeheader()

                        def _fetch_one(email: str, pwd: str) -> tuple[str, list[dict]]:
                            if _stop.is_set():
                                return email, []
                            try:
                                msgs = _fetch_inbox_fn(email, pwd, _since, _until)
                                return email, msgs if msgs else []
                            except Exception:
                                return email, []

                        with ThreadPoolExecutor(max_workers=_workers) as executor:
                            futures = {
                                executor.submit(_fetch_one, email, pwd): email
                                for email, pwd in _pairs
                                if email not in _done_accounts
                            }
                            for fut in as_completed(futures):
                                if _stop.is_set():
                                    break
                                acct_email, msgs = fut.result()
                                count = len(msgs)

                                with _inbox_lock:
                                    st.session_state.inbox_rows.extend(msgs)
                                    st.session_state.inbox_done += 1
                                    st.session_state.inbox_acct_log.append(
                                        {"account": acct_email, "messages": count}
                                    )

                                if msgs:
                                    with csv_write_lock:
                                        try:
                                            with open(INBOX_CSV, "a", newline="", encoding="utf-8") as _f:
                                                w = csv.DictWriter(
                                                    _f, fieldnames=INBOX_FIELDS,
                                                    extrasaction="ignore",
                                                )
                                                w.writerows(msgs)
                                        except Exception:
                                            pass

                        st.session_state.inbox_running = False

                    t_ib = threading.Thread(
                        target=_run_inbox_fetch,
                        args=(pool_pairs, since_dt, until_dt, inbox_workers,
                              stop_ev_ib),
                        daemon=True,
                    )
                    t_ib.start()
                    st.rerun()

        with ib_col2:
            if st.button("⏹ Stop", disabled=not st.session_state.inbox_running,
                         use_container_width=True, key="inbox_stop"):
                if st.session_state.inbox_stop_event:
                    st.session_state.inbox_stop_event.set()

        # ── Progress bar ──────────────────────────────────────────────────────
        ib_done  = st.session_state.inbox_done
        ib_total = st.session_state.inbox_total
        if ib_total > 0:
            st.progress(
                ib_done / ib_total,
                text=f"{ib_done}/{ib_total} accounts processed",
            )

        # ── Live account log ──────────────────────────────────────────────────
        ib_acct_log = st.session_state.inbox_acct_log
        if ib_acct_log:
            st.markdown(f"**Accounts processed** ({len(ib_acct_log)} so far)")
            st.dataframe(
                pd.DataFrame(ib_acct_log),
                hide_index=True,
                use_container_width=True,
            )

        # ── Results table (after completion) ──────────────────────────────────
        inbox_rows = st.session_state.inbox_rows
        if inbox_rows and not st.session_state.inbox_running:
            st.divider()
            st.markdown(f"**Fetched {len(inbox_rows)} messages**")

            inbox_result_df = pd.DataFrame(inbox_rows)

            # Filters
            fc1, fc2 = st.columns(2)
            with fc1:
                if "inbox" in inbox_result_df.columns:
                    _ib_domains = sorted(
                        set(e.split("@")[-1] for e in inbox_result_df["inbox"].dropna() if "@" in e)
                    )
                    ib_domain_filter = st.selectbox(
                        "Filter by inbox domain",
                        ["all"] + _ib_domains,
                        key="inbox_domain_filter",
                    )
                else:
                    ib_domain_filter = "all"
            with fc2:
                ib_search = st.text_input("Search subject / from", key="inbox_search", value="")

            filtered_ib = inbox_result_df.copy()
            if ib_domain_filter != "all" and "inbox" in filtered_ib.columns:
                filtered_ib = filtered_ib[
                    filtered_ib["inbox"].str.endswith(f"@{ib_domain_filter}", na=False)
                ]
            if ib_search.strip():
                mask = pd.Series([False] * len(filtered_ib), index=filtered_ib.index)
                if "subject" in filtered_ib.columns:
                    mask |= filtered_ib["subject"].str.contains(ib_search, case=False, na=False)
                if "from" in filtered_ib.columns:
                    mask |= filtered_ib["from"].str.contains(ib_search, case=False, na=False)
                filtered_ib = filtered_ib[mask]

            st.dataframe(filtered_ib, hide_index=True, use_container_width=True)

            st.download_button(
                "Download CSV",
                data=filtered_ib.to_csv(index=False).encode(),
                file_name="inbox_replies_export.csv",
                mime="text/csv",
                key="inbox_download",
            )
        elif not inbox_rows and not st.session_state.inbox_running and ib_total > 0:
            st.info("No messages found in the selected date range.")

        # ── Also show existing inbox_replies.csv if no session data ──────────
        if not inbox_rows and not st.session_state.inbox_running and ib_total == 0:
            existing_inbox = _load_inbox_csv()
            if not existing_inbox.empty:
                st.divider()
                st.markdown(f"**Existing inbox_replies.csv** — {len(existing_inbox)} rows")
                st.dataframe(existing_inbox, hide_index=True, use_container_width=True)
                st.download_button(
                    "Download existing CSV",
                    data=existing_inbox.to_csv(index=False).encode(),
                    file_name="inbox_replies.csv",
                    mime="text/csv",
                    key="inbox_dl_existing",
                )

        # Auto-rerun while running
        if st.session_state.inbox_running:
            time.sleep(0.8)
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — ACTIVITY LOG
# ══════════════════════════════════════════════════════════════════════════════

with tab_log:
    st.markdown("# Activity Log")

    col_ref, col_supp, col_auto = st.columns([1, 1, 1])

    with col_ref:
        if st.button("↺ Refresh log", use_container_width=True):
            reload_suppression()
            st.rerun()

    auto_refresh = col_auto.checkbox("Auto-refresh (30s)", key="log_autorefresh")

    log_df = _load_sent_log()

    if log_df.empty:
        st.info("No sends logged yet.")
    else:
        # IST timestamps
        if "timestamp" in log_df.columns:
            log_df["timestamp_ist"] = log_df["timestamp"].apply(_ist_display)

        # ── Summary metrics ───────────────────────────────────────────────────
        today_ist_log = datetime.now(tz=IST).strftime("%Y-%m-%d")
        ts_col_log    = (log_df["timestamp_ist"]
                         if "timestamp_ist" in log_df.columns
                         else log_df.get("timestamp", pd.Series(dtype=str)))
        today_rows_log = log_df[ts_col_log.str.startswith(today_ist_log, na=False)]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Sent today",   int((today_rows_log.get("status", pd.Series(dtype=str)) == "pushed").sum()))
        c2.metric("Failed today", int((today_rows_log.get("status", pd.Series(dtype=str)) == "failed_api").sum()))
        c3.metric("Total rows",   len(log_df))
        pushed_total_log = int((log_df.get("status", pd.Series(dtype=str)) == "pushed").sum())
        c4.metric("Total sent",   pushed_total_log)

        st.divider()

        # ── Per-sender daily usage ────────────────────────────────────────────
        if "from_email" in log_df.columns:
            st.markdown("**Sender usage today**")
            sender_today_log = (
                today_rows_log[today_rows_log["status"] == "pushed"]["from_email"].value_counts()
            )
            if not sender_today_log.empty:
                for sender, count in sender_today_log.items():
                    pct = min(count / max(DAILY_LIMIT, 1), 1.0)
                    st.markdown(f"`{sender}` — {count}/{DAILY_LIMIT}")
                    st.progress(pct)
            else:
                st.caption("No sends today.")

        st.divider()

        # ── Filters ───────────────────────────────────────────────────────────
        f1, f2, f3 = st.columns(3)
        status_opts = (["all"] + sorted(log_df["status"].dropna().unique().tolist())
                       if "status" in log_df.columns else ["all"])
        sender_opts = (["all"] + sorted(log_df["from_email"].dropna().unique().tolist())
                       if "from_email" in log_df.columns else ["all"])

        sel_status = f1.selectbox("Status", status_opts, key="log_status_filter")
        sel_sender = f2.selectbox("Sender", sender_opts, key="log_sender_filter")
        sel_date   = f3.date_input("Date (IST)", value=None, key="log_date_filter")

        filtered = log_df.copy()
        if sel_status != "all" and "status" in filtered.columns:
            filtered = filtered[filtered["status"] == sel_status]
        if sel_sender != "all" and "from_email" in filtered.columns:
            filtered = filtered[filtered["from_email"] == sel_sender]
        if sel_date and "timestamp_ist" in filtered.columns:
            filtered = filtered[filtered["timestamp_ist"].str.startswith(str(sel_date), na=False)]

        # Show newest first, use IST timestamp
        show_cols_log = ["timestamp_ist", "prospect_email", "prospect_name",
                         "company", "status", "from_email", "subject", "error"]
        show_cols_log = [c for c in show_cols_log if c in filtered.columns or c == "timestamp_ist"]
        display_df    = filtered[show_cols_log].iloc[::-1].reset_index(drop=True)

        st.markdown(f"**{len(display_df)} rows** (newest first)")
        st.dataframe(display_df, hide_index=True, use_container_width=True,
                     column_config={
                         "error":         st.column_config.TextColumn(width="large"),
                         "timestamp_ist": st.column_config.TextColumn("Timestamp (IST)", width="medium"),
                     })

        # Download filtered
        st.download_button("Download filtered CSV",
                           data=display_df.to_csv(index=False).encode(),
                           file_name="activity_log_filtered.csv",
                           mime="text/csv")

    # ── Suppression list ──────────────────────────────────────────────────────
    st.divider()
    st.markdown("**Suppression list**")
    supp_file = os.getenv("SUPPRESSION_FILE", "suppression.txt")
    if os.path.isfile(supp_file):
        with open(supp_file, encoding="utf-8") as f:
            supp_contents = f.read().strip()
        supp_count = len([line for line in supp_contents.splitlines()
                          if line.strip() and not line.startswith("#")])
        st.caption(f"{supp_count} address(es) suppressed — `{supp_file}`")
        with st.expander("View suppression list"):
            st.code(supp_contents, language=None)
    else:
        st.caption(f"`{supp_file}` not found — no addresses suppressed.")

    if auto_refresh:
        time.sleep(30)
        st.rerun()

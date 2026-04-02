import csv
import io
import os
import threading
import time
from datetime import datetime

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from core.deliverability import is_suppressed, reload_suppression
from core.email_drafter import draft_email
from core.logger import IST
from core.pipeline import SENT_LOG_FILE, run_pipeline
from core.prospect_csv import detect_column_mapping, normalise_prospects_dataframe
from core.sendgrid_sender import DAILY_LIMIT, HOURLY_LIMIT, _pool_lock, _sender_state, send_email, send_seed_email

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="BD Outreach",
    page_icon="",
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

_LOG_HEADERS = [
    "timestamp", "prospect_email", "prospect_name",
    "company", "subject", "status", "error", "from_email",
]

# ── Session defaults ──────────────────────────────────────────────────────────

_DEFAULTS = {
    "prospects": [],
    "results": {},
    "sel": 0,
    # batch tab
    "batch_prospects": [],
    "batch_log": [],
    "batch_running": False,
    "batch_done": 0,
    "batch_total": 0,
    "batch_stop_event": None,
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Helpers ───────────────────────────────────────────────────────────────────


def _key(p: dict) -> str:
    rid = p.get("_row_id", 0)
    e = p.get("email", "").strip().lower()
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
    w = csv.writer(buf)
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
        return df
    except Exception:
        return pd.DataFrame(columns=_LOG_HEADERS)


def _ist_display(ts_utc: str) -> str:
    try:
        dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
        return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        return ts_utc


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## BD Outreach")
    st.caption("CSV → OpenAI → SMTP")
    st.divider()
    oai_ok     = "configured" if os.getenv("OPENAI_API_KEY", "").startswith("sk-") else "missing"
    pool_count = len([e for e in os.getenv("SENDER_POOL", "").split(",") if ":" in e])
    pool_ok    = f"{pool_count} senders configured" if pool_count else "missing"
    st.markdown(f"**OpenAI key:** {oai_ok}")
    st.markdown(f"**Sender pool:** {pool_ok}")
    st.markdown(f"**Model:** `{os.getenv('OPENAI_MODEL', 'gpt-4.1-mini')}`")
    st.markdown(f"**SMTP:** `{os.getenv('SMTP_HOST', 'mail.recruitagents.net')}`")
    st.caption("Edit `.env` to change settings")

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_outreach, tab_batch, tab_log = st.tabs(["📧 Outreach", "⚡ Batch", "📋 Activity Log"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OUTREACH (individual, existing flow + mapping preview)
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
                    any_low = False
                    any_missing_required = False
                    for field, info in mapping.items():
                        conf = info["confidence"]
                        if conf == "low":
                            any_low = True
                        if conf == "missing" and info.get("required"):
                            any_missing_required = True
                        rows_preview.append({
                            "Field": field,
                            "Mapped from": info["mapped_from"] or "(not found)",
                            "Confidence": _confidence_badge(conf),
                        })
                    st.dataframe(pd.DataFrame(rows_preview), hide_index=True, width=600)
                    if mapping.get("email", {}).get("confidence") == "missing":
                        st.error("No email column detected — cannot proceed.")
                        st.stop()
                    if any_low:
                        st.warning("Some columns mapped with low confidence — review the table above.")
                    if any_missing_required:
                        st.caption("Missing optional fields will be blank.")
                    # ── Load prospects ───────────────────────────────────────
                    all_cols = list(REQUIRED_COLS | OPTIONAL_COLS)
                    raw_rows = df[[c for c in all_cols if c in df.columns]].fillna("").to_dict("records")
                    new_prospects = []
                    for i, row in enumerate(raw_rows):
                        p = dict(row)
                        p["_row_id"] = i
                        p["outreach_status"] = _initial_outreach_status(p)
                        new_prospects.append(p)
                    if new_prospects != st.session_state.prospects:
                        st.session_state.prospects = new_prospects
                        st.session_state.results = {}
                        st.session_state.sel = 0
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
        st.stop()

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
            "": st.column_config.TextColumn(width="small"),
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
                    st.session_state.results[k] = {"subject": ec["subject"], "body": ec["body"],
                                                    "status": "done", "error": ""}
                except Exception as e:
                    st.session_state.results[k] = {"subject": "", "body": "", "status": "failed",
                                                    "error": str(e)}
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
            for i, p in enumerate(pending_gs):
                k = _key(p)
                r0 = st.session_state.results.get(k, {})
                has_draft = bool(str(r0.get("subject", "")).strip() and str(r0.get("body", "")).strip())
                stat.caption(
                    f"[{i+1}/{n}] {p['first_name']} {p['last_name']}: "
                    + ("send…" if has_draft else "draft + send…")
                )
                prog.progress(int((i + 1) / n * 100))
                if not has_draft:
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
                        st.session_state.results[k] = {"subject": "", "body": "", "status": "failed",
                                                        "error": str(e)}
                        continue
                r = st.session_state.results[k]
                try:
                    send_email(p["email"], r["subject"], r["body"], p.get("first_name", ""))
                    st.session_state.results[k]["status"] = "sent"  # noqa: unpack ignored
                    st.session_state.results[k]["error"] = ""
                except Exception as e:
                    st.session_state.results[k]["status"] = "failed"
                    st.session_state.results[k]["error"] = str(e)
            prog.empty(); stat.empty()
            st.rerun()

    failed_rows = [p for p in prospects
                   if _has_contact_email(p) and results.get(_key(p), {}).get("status") == "failed"]
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

    # Preview & Send
    generated = [p for p in prospects if results.get(_key(p), {}).get("status") in ("done", "sent")]
    if not generated:
        st.stop()

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
            failed = 0
            for i, p in enumerate(unsent):
                k   = _key(p)
                res = results[k]
                stat.caption(f"[{i+1}/{len(unsent)}] Sending to {p['first_name']} {p['last_name']}…")
                prog.progress(int((i + 1) / len(unsent) * 100))
                try:
                    send_email(p["email"], res["subject"], res["body"], p.get("first_name", ""))  # returns (addr, body)
                    st.session_state.results[k]["status"] = "sent"
                except Exception as e:
                    st.session_state.results[k]["status"] = "failed"
                    st.session_state.results[k]["error"]  = str(e)
                    failed += 1
            prog.empty(); stat.empty()
            st.success(f"Sent {len(unsent)-failed}" + (f", {failed} failed" if failed else ""))
            st.rerun()

    left, right = st.columns([1, 2], gap="large")
    with left:
        st.caption(f"{len(generated)} emails generated")
        for i, p in enumerate(generated):
            name = f"{p['first_name']} {p['last_name']}"
            if st.button(f"{_row_icon(p, results)} **{name}**  \n{p['company']}",
                         key=f"sel_{i}", use_container_width=True,
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
        st.caption(f"{p['title']} · {p['company']}"
                   + (f" · {p['hcm_platform']}" if p.get("hcm_platform") else ""))
        st.caption(f"Email: {p['email']}")
        st.divider()
        subj = st.text_input("Subject", value=res.get("subject", ""), key=f"s_{k}")
        body = st.text_area("Body", value=res.get("body", ""), height=300, key=f"b_{k}")
        if subj != res.get("subject") or body != res.get("body"):
            st.session_state.results[k]["subject"] = subj
            st.session_state.results[k]["body"]    = body

        btn1, btn2, btn3 = st.columns(3)
        with btn1:
            st.download_button("Download .txt", data=f"Subject: {subj}\n\n{body}".encode(),
                               file_name=f"{name.replace(' ', '_')}.txt",
                               use_container_width=True)
        with btn2:
            if st.button("↺ Regenerate", use_container_width=True, key=f"regen_{k}"):
                with st.spinner("Regenerating…"):
                    try:
                        ec = draft_email({"name": name, "company": p.get("company", ""),
                                          "title": p.get("title", ""),
                                          "hcm_platform": p.get("hcm_platform", "")})
                        st.session_state.results[k].update(subject=ec["subject"],
                                                            body=ec["body"], status="done", error="")
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
                            from_addr, _ = send_email(p["email"], subj, body, p.get("first_name", ""))
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
# TAB 2 — BATCH
# ══════════════════════════════════════════════════════════════════════════════

with tab_batch:
    st.markdown("# ⚡ Batch Run")
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
                        "Field": f,
                        "Mapped from": i["mapped_from"] or "(not found)",
                        "Confidence": _confidence_badge(i["confidence"]),
                    } for f, i in mapping_b.items()]), hide_index=True)

                all_cols_b = list(REQUIRED_COLS | OPTIONAL_COLS)
                rows_b = df_b[[c for c in all_cols_b if c in df_b.columns]].fillna("").to_dict("records")
                new_batch = [dict(r, _row_id=i) for i, r in enumerate(rows_b)]
                if new_batch != st.session_state.batch_prospects:
                    st.session_state.batch_prospects = new_batch
                    st.session_state.batch_log = []
                    st.session_state.batch_done = 0
                    st.session_state.batch_total = len(new_batch)
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
                f"▶ Start batch ({len(pending_b)} pending)" if not st.session_state.batch_running
                else "⏳ Running…",
                type="primary",
                disabled=start_disabled,
                use_container_width=True,
            ):
                stop_ev = threading.Event()
                st.session_state.batch_stop_event = stop_ev
                st.session_state.batch_running = True
                st.session_state.batch_log = []
                st.session_state.batch_done = 0
                st.session_state.batch_total = len(pending_b)

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
                    st.session_state.batch_done = done
                    st.session_state.batch_total = total

                def _run_batch():
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
                    pushed  = sum(1 for r in log_rows if r.get("status") == "pushed")
                    failed  = sum(1 for r in log_rows if r.get("status") == "failed_api")
                    dry_tag = " [DRY RUN]" if dry_run_b else ""
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
        done    = st.session_state.batch_done
        total_b = st.session_state.batch_total
        if total_b > 0:
            st.progress(done / total_b, text=f"{done}/{total_b} processed")

        # Live log table
        batch_log = st.session_state.batch_log
        if batch_log:
            st.markdown(f"**Results** ({len(batch_log)} rows so far)")
            log_df_b = pd.DataFrame(batch_log[::-1])  # newest first
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
# TAB 3 — ACTIVITY LOG
# ══════════════════════════════════════════════════════════════════════════════

with tab_log:
    st.markdown("# 📋 Activity Log")

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
        today_ist = datetime.now(tz=IST).strftime("%Y-%m-%d")
        ts_col = log_df["timestamp_ist"] if "timestamp_ist" in log_df.columns else log_df["timestamp"]
        today_rows = log_df[ts_col.str.startswith(today_ist, na=False)]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Sent today",   int((today_rows.get("status", pd.Series(dtype=str)) == "pushed").sum()))
        c2.metric("Failed today", int((today_rows.get("status", pd.Series(dtype=str)) == "failed_api").sum()))
        c3.metric("Total rows",   len(log_df))
        pushed_total = int((log_df.get("status", pd.Series(dtype=str)) == "pushed").sum())
        c4.metric("Total sent",   pushed_total)

        st.divider()

        # ── Per-sender daily usage ────────────────────────────────────────────
        if "from_email" in log_df.columns:
            st.markdown("**Sender usage today**")
            sender_today = today_rows[today_rows["status"] == "pushed"]["from_email"].value_counts()
            if not sender_today.empty:
                for sender, count in sender_today.items():
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
        show_cols = ["timestamp_ist", "prospect_email", "prospect_name",
                     "company", "status", "from_email", "subject", "error"]
        show_cols = [c for c in show_cols if c in filtered.columns or c == "timestamp_ist"]
        display_df = filtered[show_cols].iloc[::-1].reset_index(drop=True)

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

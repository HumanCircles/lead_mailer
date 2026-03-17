import csv
import html
import io
import json as _json
import os
from typing import Any, Dict, List

import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

load_dotenv()

from core.email_drafter    import draft_email
from core.gmail_sender     import send_email
from core.linkedin_scraper import scrape_linkedin
from core.sheets_reader    import get_leads, mark_sent

SHEET_CONFIG_PATH = "sheet_config.json"
DRAFT_CACHE_PATH = "draft_cache.json"


def _normalize_linkedin_url(url: str) -> str:
    """Normalize URL for cache key (strip, lower, no trailing slash)."""
    u = (url or "").strip().lower().rstrip("/")
    return u or ""


def _load_draft_cache() -> Dict[str, Dict[str, Any]]:
    if os.path.exists(DRAFT_CACHE_PATH):
        try:
            with open(DRAFT_CACHE_PATH) as f:
                return _json.load(f)
        except Exception:
            pass
    return {}


def _save_draft_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    with open(DRAFT_CACHE_PATH, "w") as f:
        _json.dump(cache, f, indent=2)


def _get_cached_draft(linkedin_url: str) -> Any:
    key = _normalize_linkedin_url(linkedin_url)
    if not key:
        return None
    cache = _load_draft_cache()
    return cache.get(key)


def _set_cached_draft(linkedin_url: str, data: Dict[str, Any]) -> None:
    key = _normalize_linkedin_url(linkedin_url)
    if not key:
        return
    cache = _load_draft_cache()
    cache[key] = {
        "subject": data.get("subject", ""),
        "body": data.get("body", ""),
        "name": data.get("name", ""),
        "headline": data.get("headline", ""),
        "current_role": data.get("current_role", ""),
    }
    _save_draft_cache(cache)


def load_sheet_config() -> tuple[str, str, bool]:
    """Load sheet ID, tab, and fresh from JSON; fallback to .env then defaults."""
    if os.path.exists(SHEET_CONFIG_PATH):
        try:
            with open(SHEET_CONFIG_PATH) as f:
                data = _json.load(f)
            return (
                data.get("sheet_id", ""),
                data.get("sheet_tab", "Sheet1"),
                data.get("fresh", True),
            )
        except Exception:
            pass
    return (
        os.getenv("GOOGLE_SHEET_ID", ""),
        os.getenv("GOOGLE_SHEET_TAB", "Sheet1"),
        True,
    )


def save_sheet_config(sheet_id: str, sheet_tab: str, fresh: bool) -> None:
    """Persist sheet config to JSON so it survives restarts."""
    with open(SHEET_CONFIG_PATH, "w") as f:
        _json.dump({"sheet_id": sheet_id, "sheet_tab": sheet_tab, "fresh": fresh}, f, indent=2)


st.set_page_config(
    page_title="HireQuotient Lead Mailer",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# Minimal styling: hide footer only; rest uses Streamlit defaults
st.markdown("<style>footer{visibility:hidden;}</style>", unsafe_allow_html=True)


# ── helpers ──────────────────────────────────────────────────────────────────

def normalize_sid(raw: str) -> str:
    raw = raw.strip()
    if "docs.google.com" not in raw:
        return raw
    try:
        return raw.split("/d/")[1].split("/")[0]
    except IndexError:
        return raw


def run_draft_for_lead(lead: Dict[str, Any], fresh: bool) -> Dict[str, Any]:
    """Scrape LinkedIn and draft email for one sheet lead. Uses cached draft if available."""
    row = {**lead, "subject": "", "body": "", "headline": "", "current_role": "", "error": ""}
    url = lead.get("linkedin_url", "")
    cached = _get_cached_draft(url)
    if cached:
        row.update(
            subject=cached.get("subject", ""),
            body=cached.get("body", ""),
            headline=cached.get("headline", ""),
            current_role=cached.get("current_role", ""),
        )
        return row
    try:
        profile = scrape_linkedin(url, fresh=fresh)
        ec = draft_email(lead, profile)
        row.update(
            subject=ec.get("subject", ""),
            body=ec.get("body", ""),
            headline=profile.get("headline", ""),
            current_role=profile.get("current_role", ""),
        )
        _set_cached_draft(url, row)
    except Exception as e:
        row["error"] = str(e)
    return row


def run_draft_for_url(url: str, fresh: bool) -> Dict[str, Any]:
    """Scrape LinkedIn and draft email for one URL. Uses cached draft if available."""
    row = {"linkedin_url": url, "name": "", "email": "", "headline": "", "current_role": "", "subject": "", "body": "", "error": ""}
    cached = _get_cached_draft(url)
    if cached:
        row.update(
            name=cached.get("name", ""),
            headline=cached.get("headline", ""),
            current_role=cached.get("current_role", ""),
            subject=cached.get("subject", ""),
            body=cached.get("body", ""),
        )
        return row
    try:
        profile = scrape_linkedin(url, fresh=fresh)
        row.update(
            name=profile.get("full_name", ""),
            headline=profile.get("headline", ""),
            current_role=profile.get("current_role", ""),
        )
        ec = draft_email({"name": profile.get("full_name") or url, "email": "", "linkedin_url": url}, profile)
        row.update(subject=ec.get("subject", ""), body=ec.get("body", ""))
        _set_cached_draft(url, row)
    except Exception as e:
        row["error"] = str(e)
    return row

def copy_btn(text: str, label: str, uid: str) -> None:
    # Escape for HTML attribute so quotes in draft text don't break onclick and leak as visible text
    text_attr = html.escape(_json.dumps(text), quote=True)
    label_attr = html.escape(_json.dumps(label), quote=True)
    components.html(
        f"""<button onclick="navigator.clipboard.writeText({text_attr}).then(()=>{{
            this.textContent='Copied';this.style.color='#6ee7b7';
            setTimeout(()=>{{this.textContent={label_attr};this.style.color='';}},1800);
        }})" style="background:#0f1120;border:1px solid #2e3350;color:#94a3b8;border-radius:7px;
        padding:4px 12px;font-size:12px;cursor:pointer;font-family:Inter,sans-serif;">{html.escape(label)}</button>""",
        height=34,
    )


def metric_cards(total: int, drafted: int, failed: int, labels: tuple = ("Total", "Drafted", "Failed")) -> None:
    m1, m2, m3 = st.columns(3)
    m1.metric(labels[0], total)
    m2.metric(labels[1], drafted)
    m3.metric(labels[2], failed)


# ── sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Lead Mailer")
    st.caption("HireQuotient · AI outbound pipeline")
    st.divider()

    st.markdown("**Config**")
    cur_id, cur_tab, cur_fresh = load_sheet_config()
    sheet_id_input  = st.text_input("Sheet ID or URL", value=cur_id, help="Paste full URL or just the sheet ID")
    sheet_tab_input = st.text_input("Tab name", value=cur_tab)
    fresh_data = st.checkbox("Fresh RapidAPI data", value=cur_fresh)
    if st.button("Save", use_container_width=True):
        nid = normalize_sid(sheet_id_input)
        if nid:
            save_sheet_config(nid, sheet_tab_input or "Sheet1", fresh_data)
            st.success("Saved to sheet_config.json")
        else:
            st.error("Sheet ID or URL required.")

    st.divider()
    st.markdown("**Session**")
    res_all = st.session_state.get("sheet_results", []) + st.session_state.get("url_results", [])
    ok_n = sum(1 for r in res_all if not r.get("error"))
    ca, cb = st.columns(2)
    ca.metric("Drafted", ok_n)
    cb.metric("Failed", len(res_all) - ok_n)


# ── header ────────────────────────────────────────────────────────────────────
st.markdown("# HireQuotient Lead Mailer")
st.caption("Google Sheets → RapidAPI LinkedIn → Gemini 3.1 Pro → Gmail")

tab_sheet, tab_urls = st.tabs([
    "From Google Sheet",
    "From LinkedIn URLs",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — FROM GOOGLE SHEET
# ══════════════════════════════════════════════════════════════════════════════
with tab_sheet:
    st.markdown("#### Load & Draft from Sheet")
    st.caption("Reads all non-SENT rows → scrapes LinkedIn → drafts email → download CSV or send")

    active_sid  = normalize_sid(sheet_id_input or "").strip()
    active_stab = (sheet_tab_input or "Sheet1").strip()

    col_load, col_run, col_clr = st.columns([2, 2, 1])
    load_btn = col_load.button("Load leads", key="load_sheet", use_container_width=True)
    run_btn  = col_run.button("Draft all", key="run_sheet", type="primary", use_container_width=True)
    if col_clr.button("Clear", key="clr_sheet", use_container_width=True):
        for k in ["sheet_leads", "sheet_results"]: st.session_state.pop(k, None)
        st.rerun()

    if load_btn:
        if not active_sid:
            st.error("Set Google Sheet ID in the sidebar first.")
        else:
            with st.spinner("Loading leads from sheet…"):
                try:
                    leads = get_leads(sheet_id=active_sid, sheet_tab=active_stab)
                    if not leads:
                        st.info("No pending leads (all SENT or missing email).")
                    else:
                        st.session_state["sheet_leads"] = leads
                        st.success(f"Loaded **{len(leads)}** pending leads from sheet.")
                except Exception as e:
                    st.error(f"Could not read sheet: {e}")

    leads: List[Dict] = st.session_state.get("sheet_leads", [])

    if run_btn:
        if not leads: st.warning("Load leads from sheet first.")
        else:
            results: List[Dict] = []
            prog = st.progress(0)
            stat = st.empty()
            for i, lead in enumerate(leads):
                prog.progress(int(i / len(leads) * 100))
                stat.caption(f"[{i+1}/{len(leads)}] {lead['name']} — {lead['email']}")
                results.append(run_draft_for_lead(lead, fresh_data))
            prog.progress(100)
            stat.empty()
            st.session_state["sheet_results"] = results
            st.rerun()

    sheet_results: List[Dict] = st.session_state.get("sheet_results", [])

    # pending leads preview (before drafting)
    if leads and not sheet_results:
        st.caption(f"{len(leads)} leads ready — click Draft all to run")
        for lead in leads:
            with st.container():
                st.markdown(f"**{lead['name']}** · PENDING")
                st.caption(lead["email"])
                st.caption(lead["linkedin_url"])
                st.divider()

    # drafted results
    if sheet_results:
        ok_c = sum(1 for r in sheet_results if not r.get("error"))
        metric_cards(len(sheet_results), ok_c, len(sheet_results) - ok_c)

        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=["name","email","linkedin_url","subject","body","error"], extrasaction="ignore")
        w.writeheader(); w.writerows(sheet_results)
        st.download_button("Download CSV", data=buf.getvalue().encode(),
                           file_name="hq_emails.csv", mime="text/csv", type="primary")

        for r in sheet_results:
            with st.container():
                if r.get("error"):
                    st.markdown(f"**{r['name']}** · FAILED")
                    st.caption(r["email"])
                    st.error(r["error"])
                else:
                    draft_text = f"Subject: {r['subject']}\n\n{r['body']}"
                    sent_key = f"ss_sent_{r['row_index']}"
                    is_sent = st.session_state.get(sent_key, False)
                    status = "SENT" if is_sent else "DRAFTED"
                    st.markdown(f"**{r['name']}** · {status}")
                    st.caption(f"{r['email']} · {r.get('current_role', '')}")
                    st.caption(r["linkedin_url"])
                    st.markdown(f"**Subject:** {r['subject']}")
                    st.text(r["body"])

                    if not is_sent:
                        ba, bb, bc, _ = st.columns([1.2, 1.2, 1.5, 4])
                        with ba:
                            copy_btn(draft_text, "Copy", f"cp_{r['row_index']}")
                        with bb:
                            st.download_button(".txt", data=draft_text,
                                file_name=f"{r['name'].replace(' ', '_')}.txt",
                                mime="text/plain", key=f"dl_{r['row_index']}")
                        with bc:
                            if st.button("Send", key=f"send_ss_{r['row_index']}", type="primary", use_container_width=True):
                                with st.spinner("Sending…"):
                                    try:
                                        msg_id = send_email(r["email"], r["subject"], r["body"])
                                        mark_sent(r["row_index"], sheet_id=active_sid, sheet_tab=active_stab)
                                        st.session_state[sent_key] = True
                                        st.success(f"Sent · {msg_id}")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Send failed: {e}")
                st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — FROM LINKEDIN URLs
# ══════════════════════════════════════════════════════════════════════════════
with tab_urls:
    st.markdown("#### Paste LinkedIn URLs")
    st.caption("One per line · max 100 · no sheet needed · download CSV")

    urls_raw = st.text_area("LinkedIn URLs", height=140, key="export_urls", label_visibility="collapsed",
                             placeholder="https://linkedin.com/in/johndoe\nhttps://linkedin.com/in/janedoe")

    c1, _, c2 = st.columns([3, 4, 1])
    run_url = c1.button("Draft all", key="run_urls", type="primary", use_container_width=True)
    if c2.button("Clear", key="clr_urls", use_container_width=True):
        st.session_state.pop("url_results", None); st.rerun()

    if run_url:
        urls = [u.strip() for u in urls_raw.strip().splitlines() if u.strip()][:100]
        if not urls: st.warning("Paste at least one LinkedIn URL.")
        else:
            results = []
            prog = st.progress(0)
            stat = st.empty()
            for i, url in enumerate(urls):
                prog.progress(int(i / len(urls) * 100))
                stat.caption(f"[{i+1}/{len(urls)}] {url[:75]}")
                results.append(run_draft_for_url(url, fresh_data))
            prog.progress(100)
            stat.empty()
            st.session_state["url_results"] = results; st.rerun()

    url_results: List[Dict] = st.session_state.get("url_results", [])
    if url_results:
        st.markdown("---")
        ok_c = sum(1 for r in url_results if not r.get("error"))
        metric_cards(len(url_results), ok_c, len(url_results) - ok_c, ("Processed", "Drafted", "Failed"))

        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=["name", "linkedin_url", "subject", "body", "error"], extrasaction="ignore")
        w.writeheader(); w.writerows(url_results)
        st.download_button("Download CSV", data=buf.getvalue().encode(),
                           file_name="hq_url_emails.csv", mime="text/csv", type="primary")

        for r in url_results:
            with st.container():
                if r.get("error"):
                    st.markdown("**Error** · FAILED")
                    st.caption(r["linkedin_url"])
                    st.error(r["error"])
                else:
                    draft_text = f"Subject: {r['subject']}\n\n{r['body']}"
                    st.markdown(f"**{r['name'] or r['linkedin_url']}** · DRAFTED")
                    st.caption(r["current_role"])
                    st.caption(r["linkedin_url"])
                    st.markdown(f"**Subject:** {r['subject']}")
                    st.text(r["body"])
                    ca2, cb2, _ = st.columns([1.3, 1.3, 5])
                    with ca2:
                        copy_btn(draft_text, "Copy", f"cp_{r['linkedin_url']}")
                    with cb2:
                        st.download_button(".txt", data=draft_text,
                            file_name=f"{(r['name'] or 'draft').replace(' ', '_')}.txt",
                            mime="text/plain", key=f"dl_{r['linkedin_url']}")
                st.divider()

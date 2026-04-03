"""Map various prospect CSV layouts (including ATS / LinkedIn enrichment) to canonical fields."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def _merge_row_keys_casefold(row: dict) -> dict[str, object]:
    """Lowercase keys; later columns win for duplicates (e.g. enriched Title over search title)."""
    out: dict[str, object] = {}
    for k, v in row.items():
        out[str(k).lower().strip()] = v
    return out


_EMAIL_RE = re.compile(r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", flags=re.IGNORECASE)


def _clean_scalar(value: object) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return ""
    return s


def _first_non_empty(m: dict[str, object], *keys: str) -> str:
    for k in keys:
        if k in m:
            v = _clean_scalar(m[k])
            if v:
                return v
    return ""


def _extract_first_email(raw: str) -> str:
    if not raw:
        return ""
    match = _EMAIL_RE.search(raw)
    return match.group(1).strip() if match else ""


def _split_name(full_name: str) -> tuple[str, str]:
    # Drop credentials/suffix after comma: "Charles Johnson, CSP" -> "Charles Johnson"
    base = (full_name or "").strip().split(",", 1)[0].strip()
    if not base:
        return "", ""
    parts = [p for p in re.split(r"\s+", base) if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def canonicalize_prospect_row(row: dict) -> dict[str, str]:
    """Return first_name, last_name, email, company, title, hcm_platform from one CSV row."""
    m = _merge_row_keys_casefold(row)

    first_name = _first_non_empty(m, "first_name", "first name")
    last_name = _first_non_empty(m, "last_name", "last name")
    full_name = _first_non_empty(m, "name", "full name", "candidate name")
    if full_name and (not first_name or not last_name):
        f0, l0 = _split_name(full_name)
        first_name = first_name or f0
        last_name = last_name or l0

    email_raw = _first_non_empty(
        m,
        "email",
        "preferred email",
        "email professional",
        "professional email",
        "work email",
        "business email",
        "email personal",
        "personal email",
    )
    email = _extract_first_email(email_raw)

    company = _first_non_empty(
        m,
        "company",
        "company name",
        "companyname",
        "current organization",
        "organization",
        "current company",
    )
    title = _first_non_empty(m, "title", "current position", "job title", "current title", "position")
    hcm_platform  = _first_non_empty(m, "hcm_platform", "hcm platform", "platform")
    research_note = _first_non_empty(m, "research_note", "research note", "linkedin_note", "note")
    return {
        "first_name":    first_name,
        "last_name":     last_name,
        "email":         email,
        "company":       company,
        "title":         title,
        "hcm_platform":  hcm_platform,
        "research_note": research_note,
    }


# ---------------------------------------------------------------------------
# Column-mapping preview
# ---------------------------------------------------------------------------

# Canonical field → (high-confidence aliases, low-confidence aliases)
_FIELD_ALIASES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "first_name":    (("first_name", "first name"), ("fname", "given name")),
    "last_name":     (("last_name", "last name"), ("lname", "surname", "family name")),
    "email":         (("email",), ("preferred email", "email professional", "professional email",
                                   "work email", "business email", "email personal", "personal email")),
    "company":       (("company", "company name"), ("companyname", "current organization",
                                                     "organization", "current company")),
    "title":         (("title", "job title"), ("current position", "current title", "position")),
    "hcm_platform":  (("hcm_platform", "hcm platform"), ("platform",)),
}

# Fields that are required for sending; others are enrichment-only.
_REQUIRED_FIELDS = {"first_name", "email", "company"}


def detect_column_mapping(df: "pd.DataFrame") -> dict[str, dict]:
    """Return per-canonical-field mapping info from raw DataFrame column headers.

    Returns a dict keyed by canonical field name, each value::

        {
            "mapped_from": str | None,   # actual CSV column that matched (None if missing)
            "confidence":  "high" | "low" | "missing",
            "required":    bool,
        }

    "high"    — column name is an exact or primary alias match
    "low"     — column name is a secondary alias match
    "missing" — no matching column found
    """
    cols_lower = {str(c).lower().strip() for c in df.columns}
    # Also keep a lowercase→original mapping so we can report the real column name
    lower_to_orig: dict[str, str] = {
        str(c).lower().strip(): str(c) for c in df.columns
    }

    # Full-name fallback: if first_name/last_name are both missing but a "name"
    # column exists, treat it as low-confidence for both.
    has_full_name = any(a in cols_lower for a in ("name", "full name", "candidate name"))

    result: dict[str, dict] = {}
    for field, (high_aliases, low_aliases) in _FIELD_ALIASES.items():
        matched_col: str | None = None
        confidence: str = "missing"

        for alias in high_aliases:
            if alias in cols_lower:
                matched_col = lower_to_orig[alias]
                confidence = "high"
                break

        if matched_col is None:
            for alias in low_aliases:
                if alias in cols_lower:
                    matched_col = lower_to_orig[alias]
                    confidence = "low"
                    break

        # Full-name fallback for name fields
        if matched_col is None and field in ("first_name", "last_name") and has_full_name:
            for alias in ("name", "full name", "candidate name"):
                if alias in cols_lower:
                    matched_col = lower_to_orig[alias]
                    confidence = "low"
                    break

        result[field] = {
            "mapped_from": matched_col,
            "confidence": confidence,
            "required": field in _REQUIRED_FIELDS,
        }

    return result


def _dedupe_column_names(names: list[str]) -> list[str]:
    """Make duplicate names unique (title, title.1, …) so pandas does not return a 2D DataFrame."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for c in names:
        n = seen.get(c, 0)
        seen[c] = n + 1
        out.append(c if n == 0 else f"{c}.{n}")
    return out


def normalise_prospects_dataframe(df: "pd.DataFrame") -> "pd.DataFrame":
    """Build a dataframe with canonical columns from simple or ATS-style exports."""
    import pandas as pd

    df = df.copy()
    lower = [str(c).lower().strip() for c in df.columns]
    df.columns = _dedupe_column_names(lower)
    if "title.1" in df.columns:
        t0 = df["title"] if "title" in df.columns else None
        t1 = df["title.1"]
        if t0 is not None:
            df["title"] = t1.fillna(t0)
        else:
            df["title"] = t1
        df = df.drop(columns=["title.1"])

    rows = [canonicalize_prospect_row(dict(r)) for r in df.to_dict("records")]
    return pd.DataFrame(rows)

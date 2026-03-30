"""Map various prospect CSV layouts (including ATS / LinkedIn enrichment) to canonical fields."""

from __future__ import annotations

import pandas as pd


def _merge_row_keys_casefold(row: dict) -> dict[str, object]:
    """Lowercase keys; later columns win for duplicates (e.g. enriched Title over search title)."""
    out: dict[str, object] = {}
    for k, v in row.items():
        out[str(k).lower().strip()] = v
    return out


def canonicalize_prospect_row(row: dict) -> dict[str, str]:
    """Return first_name, last_name, email, company, title, hcm_platform from one CSV row."""
    m = _merge_row_keys_casefold(row)

    def pick(*keys: str) -> str:
        for k in keys:
            if k in m and m[k] is not None and str(m[k]).strip():
                return str(m[k]).strip()
        return ""

    return {
        "first_name": pick("first_name", "first name"),
        "last_name": pick("last_name", "last name"),
        "email": pick("email"),
        "company": pick("company", "company name", "companyname"),
        "title": pick("title"),
        "hcm_platform": pick("hcm_platform", "hcm platform"),
    }


def _dedupe_column_names(names: list[str]) -> list[str]:
    """Make duplicate names unique (title, title.1, …) so pandas does not return a 2D DataFrame."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for c in names:
        n = seen.get(c, 0)
        seen[c] = n + 1
        out.append(c if n == 0 else f"{c}.{n}")
    return out


def normalise_prospects_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Build a dataframe with canonical columns from simple or ATS-style exports."""
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

    def col(*names: str) -> pd.Series:
        for n in names:
            if n in df.columns:
                return df[n].fillna("")
        return pd.Series([""] * len(df), index=df.index, dtype=object)

    return pd.DataFrame(
        {
            "first_name": col("first_name", "first name"),
            "last_name": col("last_name", "last name"),
            "email": col("email"),
            "company": col("company", "company name", "companyname"),
            "title": col("title"),
            "hcm_platform": col("hcm_platform", "hcm platform"),
        }
    )


import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Tuple, Iterable, Optional
from aclient import AsyncGongClient
# ---- configure these ----
TARGET_INTERNAL_EMAILS = [
]
INTERNAL_DOMAIN = "coderabbit.ai"

# ---------- helpers ----------

def is_external_party(party: Dict[str, Any], internal_domain: str) -> bool:
    """
    External if:
      - affiliation == 'External', OR
      - affiliation == 'Unknown' AND email domain != internal_domain
    """
    aff = (party.get("affiliation") or "").strip()
    email = (party.get("emailAddress") or "").strip().lower()
    if aff == "External":
        return True
    if aff == "Unknown" and email and "@" in email:
        domain = email.split("@")[-1]
        return domain != internal_domain.lower()
    return False

def uniq_by(items: Iterable[Dict[str, Any]], key_fields: Tuple[str, ...]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for it in items:
        k = tuple((it.get(f) or "") for f in key_fields)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out

async def get_user_ids_for_emails(gong: AsyncGongClient, emails: List[str]) -> Dict[str, str]:
    targets = {e.strip().lower() for e in emails if e and e.strip()}
    email_to_id: Dict[str, str] = {}
    users = await gong.list_users()
    for u in users:
        email = (u.get("emailAddress") or u.get("email") or "").strip().lower()
        if email in targets:
            email_to_id[email] = u.get("id")
    return email_to_id

async def fetch_calls_last_week_for_primary_users(
    gong: AsyncGongClient, primary_user_ids: List[str]
) -> List[Dict[str, Any]]:
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=30)

    calls = await gong.calls_extensive(
        filter={
            "fromDateTime": to_dt.isoformat().replace("+00:00", "Z").replace(to_dt.strftime("%H:%M:%S.%f")[:-3]+"Z", to_dt.strftime("%H:%M:%S")+"Z") if False else from_dt.isoformat().replace("+00:00", "Z"),
            "toDateTime": to_dt.isoformat().replace("+00:00", "Z"),
            "primaryUserIds": primary_user_ids,
        },
        exposed_fields={
            "parties": True  # keep payload lean; add other fields if needed
        },
        limit=200,
        order_by=[{"field": "startTime", "order": "ASC"}],
    )
    return calls

def extract_external_parties_from_calls(
    calls: List[Dict[str, Any]], internal_domain: str
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for call in calls:
        # /v2/calls/extensive (with contentSelector.exposedFields.parties) typically returns:
        # { "metaData": {...}, "parties": [...] }
        meta = call.get("metaData") or {}
        call_id = meta.get("id") or call.get("id")  # be resilient to schema variants
        call_title = meta.get("title")
        call_started = meta.get("started")
        for party in call.get("parties", []):
            if not party.get("emailAddress"):
                continue
            if is_external_party(party, internal_domain):
                rows.append({
                    "call_id": call_id,
                    "call_title": call_title,
                    "call_started": call_started,
                    "party_id": party.get("id"),
                    "party_name": party.get("name"),
                    "party_email": party.get("emailAddress"),
                    "party_title": party.get("title"),
                    "party_userId": party.get("userId"),  # will be absent for most externals
                    "affiliation": party.get("affiliation"),
                    "methods": ",".join(party.get("methods", [])),
                })

    # de-dupe per call by email
    rows = uniq_by(rows, key_fields=("call_id", "party_email"))
    return rows


# ---------- excel write --------
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
import re

def _auto_fit_columns(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame, max_width: int = 60) -> None:
    """Best-effort column width autosize (OpenPyXL engine)."""
    ws = writer.sheets[sheet_name]
    for i, col in enumerate(df.columns, start=1):
        series = df[col].astype(str)
        width = max([len(col)] + [len(s) for s in series.tolist()]) + 2
        ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(width, max_width)

def _domain(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@")[-1].lower()

def write_externals_to_excel(externals: List[Dict[str, Any]], filepath: str) -> str:
    """
    Write externals to an Excel file with multiple sheets:
      - External_Parties
      - Unique_Contacts
      - By_Company_Domain
    Returns the final path written.
    """
    if not externals:
        # still write an empty workbook with headers for consistency
        cols = [
            "call_id", "call_title", "call_started",
            "party_id", "party_name", "party_email", "party_title",
            "party_userId", "affiliation", "methods"
        ]
        df = pd.DataFrame(columns=cols)
        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="External_Parties")
            _auto_fit_columns(writer, "External_Parties", df)
        return filepath

    df = pd.DataFrame(externals)

    # External_Parties sheet
    df_sorted = df.sort_values(["call_started", "call_id", "party_email"], na_position="last")
    # Optional: keep a consistent column order
    col_order = [
        "call_started", "call_title", "call_id",
        "party_name", "party_title", "party_email",
        "affiliation", "methods", "party_id", "party_userId"
    ]
    for c in col_order:
        if c not in df_sorted.columns:
            df_sorted[c] = None
    df_sorted = df_sorted[col_order]

    # Unique_Contacts sheet (dedupe by email)
    contacts = (
        df_sorted.sort_values(["party_email", "call_started"])
        .drop_duplicates(subset=["party_email"], keep="last")
        .copy()
    )
    contacts["company_domain"] = contacts["party_email"].map(_domain)

    # By_Company_Domain sheet (simple summary)
    by_domain = (
        contacts.groupby("company_domain", dropna=False)
        .size()
        .reset_index(name="unique_contacts")
        .sort_values("unique_contacts", ascending=False)
    )

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df_sorted.to_excel(writer, index=False, sheet_name="External_Parties")
        _auto_fit_columns(writer, "External_Parties", df_sorted)

        contacts.to_excel(writer, index=False, sheet_name="Unique_Contacts")
        _auto_fit_columns(writer, "Unique_Contacts", contacts)

        by_domain.to_excel(writer, index=False, sheet_name="By_Company_Domain")
        _auto_fit_columns(writer, "By_Company_Domain", by_domain)

    return filepath

# ---------- main flow ----------

async def main():
    async with AsyncGongClient.from_env() as gong:
        # 1) Emails -> userIds
        email_to_id = await get_user_ids_for_emails(gong, TARGET_INTERNAL_EMAILS)
        primary_user_ids = list(email_to_id.values())
        if not primary_user_ids:
            print("No matching Gong users for provided emails.")
            return
        print("Primary user IDs:", primary_user_ids)

        # 2) Last 7 days of calls where these users are the primary owners
        calls = await fetch_calls_last_week_for_primary_users(gong, primary_user_ids)
        print(f"Calls found: {len(calls)}")

        # 3) Extract external parties
        externals = extract_external_parties_from_calls(calls, INTERNAL_DOMAIN)
        print(f"External parties (unique per call+email): {len(externals)}\n")

        ts = datetime.utcnow().strftime("%Y-%m-%d")
        out_path = f"gong_external_parties_{ts}.xlsx"
        final_path = write_externals_to_excel(externals, out_path)
        print(f"\nExcel written to: {final_path}")

        # Pretty print a few
        for r in externals[:10]:
            print(f"[{r['call_started']}] {r['call_title']} — {r['party_name']} <{r['party_email']}> ({r['affiliation']})")

        # If you want a flat list of unique external contacts across all calls:
        uniq_contacts = uniq_by(
            [{"party_email": r["party_email"], "party_name": r["party_name"], "party_title": r["party_title"]} for r in externals],
            key_fields=("party_email",),
        )
        print("\nUnique external contacts across these calls:", len(uniq_contacts))

if __name__ == "__main__":
    asyncio.run(main())
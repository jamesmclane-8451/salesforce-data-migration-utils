


# field_selection.py
from typing import List, Dict, Any, Optional
import pandas as pd
from oauth_login import get_salesforce_connection

"""
    Build and export a list of Salesforce fields that are safe and relevant for data migration
    between a SOURCE (Production) org and a TARGET (sandbox/test) org.

    ----------------------------------------------------------------------
    🔧 WHAT THIS FUNCTION DOES
    ----------------------------------------------------------------------
    1️⃣ Connects to two Salesforce orgs:
        - Source org (Production)
        - Target org (the `to_env` parameter, e.g., MCUAT8451)

    2️⃣ Describes the specified object (`object_name`) in both orgs using Salesforce metadata.

    3️⃣ Evaluates every field in both orgs and classifies them into:
        - ✅ Write fields: exist in both orgs, queryable in source, writable in target,
          not formula or autoNumber, type matches, not reserved.
        - 🧩 Reference fields: lookup/master-detail fields that exist in both orgs and
          are queryable and type-consistent, included for reference and backfill context.
        - 🚫 Excluded fields: do not meet the criteria above (with a reason logged).

    4️⃣ Builds an ordered field list following these rules:
        - Id always first
        - Then write fields (alphabetical)
        - Then reference fields (alphabetical)
        - RecordTypeId last (if present)

    5️⃣ Produces several CSV and Feather outputs automatically:
        ────────────────────────────────────────────────
        • `<ObjectName>_fields.csv`
          → all selected fields with category and type info
        • `<ObjectName>_fields_excluded.csv`
          → fields excluded from selection with the reason
        • `<objectname>.csv` and `<objectname>.feather`
          → simplified object–field map in lowercase form for use by
            `get_records()` or other automation (columns: Object, Field, Category, source_type, target_type)

    6️⃣ Returns:
        - An ordered Python list of selected field API names (Id first, RecordTypeId last).

    ----------------------------------------------------------------------
    📦 OUTPUT SUMMARY
    ----------------------------------------------------------------------
    ✅ `<ObjectName>_fields.csv` ........ included fields with metadata  
    ⚠️ `<ObjectName>_fields_excluded.csv` . excluded fields and why  
    📄 `<objectname>.csv` ................ flat map of object/fields  
    🪶 `<objectname>.feather` ............. same data, in fast binary Feather format  

    ----------------------------------------------------------------------
    🧠 PARAMETERS
    ----------------------------------------------------------------------
    object_name: str  
        The Salesforce object API name to analyze (e.g., 'Account', 'Opportunity', 'Contact').

    to_env: str  
        The target Salesforce environment (e.g., 'MCUAT8451') for comparison.

    restrict_fields_csv: Optional[str]  
        Optional CSV with candidate field names to restrict analysis to.

    included_outfile / excluded_outfile: Optional[str]  
        Optional custom filenames for the included/excluded CSV outputs.

    object_map_csv_outfile / object_map_feather_outfile: Optional[str]  
        Optional overrides for the lowercase object mapping file outputs.

    ----------------------------------------------------------------------
    🧾 RETURNS
    ----------------------------------------------------------------------
    List[str]: ordered list of selected field API names suitable for querying/export.
    """


# -----------------------------
# Internal helpers and constants
# -----------------------------

_RESERVED = {
    "Id", "IsDeleted", "CreatedById", "CreatedDate", "LastModifiedById", "LastModifiedDate",
    "SystemModstamp", "LastActivityDate", "LastViewedDate", "LastReferencedDate",
    "Old_Org_Id__c", "RecordTypeId",
}

def _is_reference(fd: Dict[str, Any]) -> bool:
    return fd.get("type") == "reference"

def _is_safe_target_field(tgt_fd: Dict[str, Any]) -> bool:
    if _is_reference(tgt_fd):
        return False
    if not (tgt_fd.get("createable") or tgt_fd.get("updateable")):
        return False
    if tgt_fd.get("calculated") or tgt_fd.get("autoNumber"):
        return False
    if tgt_fd.get("name") in _RESERVED:
        return False
    return True

def _load_restrict_from_csv(restrict_fields_csv: str) -> List[str]:
    """
    Accepts either:
      - single column with no header, or
      - a column named 'field' (any case)
    Preserves order and de-dupes.
    """
    df = pd.read_csv(restrict_fields_csv, dtype=str, keep_default_na=False, na_filter=False)
    col = next((c for c in df.columns if str(c).strip().lower() == "field"), df.columns[0])
    raw = [str(v).strip() for v in df[col].tolist()]
    seen = set()
    return [f for f in raw if f and not (f in seen or seen.add(f))]


# -----------------------------
# Public API
# -----------------------------

def build_source_select_fields(
    object_name: str,
    to_env: str,
    restrict_fields_csv: Optional[str] = None,
    included_outfile: Optional[str] = None,
    excluded_outfile: Optional[str] = None,
    # new optional outputs, default to object-based names
    object_map_csv_outfile: Optional[str] = None,
    object_map_feather_outfile: Optional[str] = None,
) -> List[str]:
    """
    Determine the ordered field list for SOURCE export using the same rules as the old migrate function.

    Selection rules
      1) Always include 'Id' first
      2) write_fields:
           - exists in BOTH orgs
           - source.queryable = True
           - target is writable (createable or updateable), not formula, not autoNumber
           - NOT reference
           - types match between source and target
           - not in reserved set
      3) reference_fields:
           - reference fields that exist in BOTH, source.queryable, types match
           - included AFTER write_fields for backfill and context
      4) Append 'RecordTypeId' at the end if present in source
      5) Order: Id, write_fields, reference_fields, RecordTypeId

    Outputs
      - Writes <ObjectName>_fields.csv with the final ordered selection and category
      - Writes <ObjectName>_fields_excluded.csv with excluded fields and a reason
      - Writes <objectname>.csv and <objectname>.feather with rows for the selected fields,
        including columns: Object, Field, Category, source_type, target_type

    Returns
      - The ordered list of fields selected, suitable to pass to your exporter
    """
    # 0) Connect to source and target
    src_sf = get_salesforce_connection(use_sandbox=False)  # your PROD by convention
    tgt_sf = get_salesforce_connection(to_env, requires_chatter=False)

    # 1) Describe both
    src_desc = getattr(src_sf, object_name).describe()
    tgt_desc = getattr(tgt_sf, object_name).describe()
    src_by: Dict[str, Dict[str, Any]] = {f["name"]: f for f in src_desc["fields"]}
    tgt_by: Dict[str, Dict[str, Any]] = {f["name"]: f for f in tgt_desc["fields"]}

    # 2) Candidate pool
    if restrict_fields_csv:
        candidates = [c for c in _load_restrict_from_csv(restrict_fields_csv) if c in src_by]
    else:
        candidates = sorted(set(src_by) | set(tgt_by))

    write_fields: List[str] = []
    reference_fields: List[str] = []
    excluded_rows: List[Dict[str, Any]] = []

    def reason_for_exclude(
        name: str,
        in_src: bool,
        in_tgt: bool,
        src_queryable: bool,
        tgt_writable: bool,
        types_match: bool,
        is_ref: bool,
    ) -> str:
        if not in_src or not in_tgt:
            return "missing_in_one_org"
        if name in _RESERVED:
            return "reserved_field"
        if is_ref:
            return "reference_field_logged_only"
        if not src_queryable:
            return "not_queryable_in_source"
        if not tgt_writable:
            return "not_writable_in_target"
        if not types_match:
            return "type_mismatch"
        return "excluded_other"

    # 3) Classify fields per rules
    for fname in candidates:
        if fname == "Id" or fname == "RecordTypeId":
            continue

        src_fd = src_by.get(fname)
        tgt_fd = tgt_by.get(fname)
        in_src = src_fd is not None
        in_tgt = tgt_fd is not None
        if not in_src or not in_tgt:
            excluded_rows.append({
                "field": fname,
                "in_source": in_src,
                "in_target": in_tgt,
                "source_type": src_fd.get("type") if src_fd else "",
                "target_type": tgt_fd.get("type") if tgt_fd else "",
                "reason": "missing_in_one_org",
            })
            continue

        src_type = src_fd.get("type")
        tgt_type = tgt_fd.get("type")
        src_queryable = bool(src_fd.get("queryable", True))
        tgt_writable = _is_safe_target_field(tgt_fd)
        types_match = (src_type == tgt_type)
        is_ref = _is_reference(src_fd) or _is_reference(tgt_fd)

        if is_ref:
            if src_queryable and types_match:
                reference_fields.append(fname)
            else:
                excluded_rows.append({
                    "field": fname,
                    "in_source": True,
                    "in_target": True,
                    "source_type": src_type,
                    "target_type": tgt_type,
                    "reason": reason_for_exclude(fname, True, True, src_queryable, tgt_writable, types_match, True),
                })
            continue

        if (
            src_queryable
            and tgt_writable
            and types_match
            and fname not in _RESERVED
        ):
            write_fields.append(fname)
        else:
            excluded_rows.append({
                "field": fname,
                "in_source": True,
                "in_target": True,
                "source_type": src_type,
                "target_type": tgt_type,
                "reason": reason_for_exclude(fname, True, True, src_queryable, tgt_writable, types_match, False),
            })

    # 4) Build final ordered list
    ordered: List[str] = ["Id"]
    ordered += sorted(write_fields)
    ordered += sorted(reference_fields)
    if "RecordTypeId" in src_by:
        ordered.append("RecordTypeId")

    # 5) Write field include and exclude CSVs
    included_path = included_outfile or f"{object_name}_fields.csv"
    excluded_path = excluded_outfile or f"{object_name}_fields_excluded.csv"

    included_rows = []
    for f in ordered:
        cat = "id" if f == "Id" else "recordtype" if f == "RecordTypeId" else ("write" if f in write_fields else "reference")
        fd = src_by.get(f) or {}
        included_rows.append({
            "field": f,
            "category": cat,
            "source_type": fd.get("type", ""),
            "target_type": (tgt_by.get(f) or {}).get("type", ""),
        })

    included_df = pd.DataFrame(included_rows)
    included_df.to_csv(included_path, index=False)

    if excluded_rows:
        pd.DataFrame(excluded_rows).to_csv(excluded_path, index=False)
    else:
        pd.DataFrame(columns=["field","in_source","in_target","source_type","target_type","reason"]).to_csv(excluded_path, index=False)

    # 6) Additionally, write <object>.csv and <object>.feather derived from the selected fields
    file_base = object_name.lower()
    object_map_csv = object_map_csv_outfile or f"{file_base}.csv"
    object_map_feather = object_map_feather_outfile or f"{file_base}.feather"

    # Build a tidy mapping with Object, Field, Category, plus source and target types
    object_map_rows = []
    for row in included_rows:
        object_map_rows.append({
            "Object": object_name,
            "Field": row["field"],
            "Category": row["category"],
            "source_type": row["source_type"],
            "target_type": row["target_type"],
        })
    object_map_df = pd.DataFrame(object_map_rows)

    # Persist as both CSV and Feather
    object_map_df.to_csv(object_map_csv, index=False)
    try:
        object_map_df.to_feather(object_map_feather)
    except Exception as e:
        # Feather requires pyarrow, so leave a friendly breadcrumb while still returning fields
        print(f"Warning, could not write feather '{object_map_feather}': {e}")

    return ordered


# Example CLI-like usage
if __name__ == "__main__":
    fields = build_source_select_fields(
        object_name="Account",
        to_env="MCUAT8451",
        restrict_fields_csv=None,                     # or "Account_candidate_fields.csv"
        included_outfile="Account_fields.csv",
        excluded_outfile="Account_fields_excluded.csv",
        # optional, will default to "account.csv" and "account.feather"
        object_map_csv_outfile=None,
        object_map_feather_outfile=None,
    )
    print(f"Selected {len(fields)} fields. Wrote Account_fields.csv, Account_fields_excluded.csv, account.csv, account.feather.")

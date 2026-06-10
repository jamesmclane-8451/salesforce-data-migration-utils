

# compare_records.py
import os
import pandas as pd
from typing import List, Optional


def compare_records_envs(
    object_name: str,
    source_env: str,
    target_env: str,
    *,
    source_uid: str,                       # e.g. "Email" in source env
    target_uid: str,                       # e.g. "Email" (or different) in target env
    compare_fields: Optional[List[str]] = None,  # fields to compare; default = intersection
    export_csv: bool = True,
    uid_case_insensitive: bool = True,     # normalize UID values to lowercase for matching
) -> pd.DataFrame:
    """
    Compare record-level data between two environments using pre-extracted Feather files,
    aligning rows by a UID column (which can differ between environments). Includes the
    Salesforce record IDs where available.

    Auto-reads (from current working directory):
      - get_records_<sourceEnv>_<object>.feather
      - get_records_<targetEnv>_<object>.feather

    Output (single CSV in CWD):
      - compare_records_<sourceEnv>_vs_<targetEnv>_<object>.csv

    CSV columns:
      UID, SourceId, TargetId, Field, SourceValue, TargetValue, ChangeType
      where ChangeType ∈ {'CHANGED', 'SOURCE_ONLY', 'TARGET_ONLY'}
    """
    # -----------------------
    # Helpers
    # -----------------------
    def _clean(s: str) -> str:
        return str(s).lower().replace(" ", "_")

    def _resolve_col(df: pd.DataFrame, desired: str) -> str:
        """
        Resolve a column by exact, case-insensitive, or trimmed-case-insensitive match.
        Raises a helpful error if not found.
        """
        cols = list(df.columns)
        # exact
        if desired in df.columns:
            return desired
        # case-insensitive
        low_map = {c.lower(): c for c in cols}
        if desired.lower() in low_map:
            return low_map[desired.lower()]
        # trimmed + lowercase
        trim_low_map = {str(c).strip().lower(): c for c in cols}
        key = str(desired).strip().lower()
        if key in trim_low_map:
            return trim_low_map[key]
        sample = ", ".join(cols[:20]) + ("..." if len(cols) > 20 else "")
        raise ValueError(f"Column '{desired}' not found. Available columns (first 20): {sample}")

    def _norm_scalar(v):
        """Stringify complex types; keep simple scalars/NaN as-is."""
        if pd.isna(v):
            return v
        if isinstance(v, (str, int, float, bool, pd.Timestamp)):
            return v
        return str(v)

    def _norm_uid_series(s: pd.Series, case_insensitive: bool) -> pd.Series:
        s2 = s.astype("string").str.strip()
        return s2.str.lower() if case_insensitive else s2

    # -----------------------
    # Build paths
    # -----------------------
    obj_clean = _clean(object_name)
    src_clean = _clean(source_env)
    tgt_clean = _clean(target_env)

    source_path = f"get_records_{src_clean}_{obj_clean}.feather"
    target_path = f"get_records_{tgt_clean}_{obj_clean}.feather"
    out_path    = f"compare_records_{src_clean}_vs_{tgt_clean}_{obj_clean}.csv"

    for p in (source_path, target_path):
        if not os.path.exists(p):
            raise FileNotFoundError(f"Required file not found: {p}")

    # -----------------------
    # Load datasets
    # -----------------------
    left_raw  = pd.read_feather(source_path)
    right_raw = pd.read_feather(target_path)

    # Resolve UID columns robustly
    source_uid_col = _resolve_col(left_raw, source_uid)
    target_uid_col = _resolve_col(right_raw, target_uid)

    # Prefer a real Salesforce Id column if present; otherwise we’ll fill NA
    source_id_col = "Id" if "Id" in left_raw.columns else None
    target_id_col = "Id" if "Id" in right_raw.columns else None

    # Determine value columns to compare
    if compare_fields is not None:
        value_cols = sorted([c for c in compare_fields if c in left_raw.columns and c in right_raw.columns])
    else:
        common_cols = set(left_raw.columns).intersection(set(right_raw.columns))
        # Exclude the UID columns (which may differ in name) and Id
        exclude = {source_uid_col, target_uid_col}
        if source_id_col is not None:
            exclude.add(source_id_col)
        if target_id_col is not None:
            exclude.add(target_id_col)
        value_cols = sorted([c for c in common_cols if c not in exclude])

    # -----------------------
    # Slice & copy; normalize
    # -----------------------
    left_cols  = [source_uid_col] + ([source_id_col] if source_id_col else []) + value_cols
    right_cols = [target_uid_col] + ([target_id_col] if target_id_col else []) + value_cols

    left  = left_raw.loc[:, left_cols].copy()
    right = right_raw.loc[:, right_cols].copy()

    # Normalize value columns for stable equality
    if value_cols:
        left.loc[:, value_cols]  = left.loc[:, value_cols].apply(lambda s: s.map(_norm_scalar))
        right.loc[:, value_cols] = right.loc[:, value_cols].apply(lambda s: s.map(_norm_scalar))

    # Normalize UIDs and attach as a canonical join key
    left_uid_series  = _norm_uid_series(left[source_uid_col], uid_case_insensitive)
    right_uid_series = _norm_uid_series(right[target_uid_col], uid_case_insensitive)

    left  = left.assign(__UID__=left_uid_series)
    right = right.assign(__UID__=right_uid_series)

    # -----------------------
    # Index & presence-only sets
    # -----------------------
    l_idx = left.set_index("__UID__", drop=True)
    r_idx = right.set_index("__UID__", drop=True)

    left_uids_only  = l_idx.index.difference(r_idx.index)
    right_uids_only = r_idx.index.difference(l_idx.index)

    # -----------------------
    # Compare matched UIDs (inner join)
    # -----------------------
    merged = l_idx.join(r_idx, how="inner", lsuffix="_left", rsuffix="_right")

    rows = []

    # Field-level CHANGED rows, include SourceId/TargetId where possible
    for c in value_cols:
        lcol, rcol = f"{c}_left", f"{c}_right"
        diff_mask = ~((merged[lcol].isna() & merged[rcol].isna()) | (merged[lcol] == merged[rcol]))
        if diff_mask.any():
            sel_cols = [lcol, rcol]
            # bring IDs if we have them
            if source_id_col:
                sel_cols.append(f"{source_id_col}_left")
            if target_id_col:
                sel_cols.append(f"{target_id_col}_right")

            tmp = merged.loc[diff_mask, sel_cols].reset_index(names="UID")
            tmp["Field"] = c
            # rename values
            rename_map = {lcol: "SourceValue", rcol: "TargetValue"}
            if source_id_col:
                rename_map[f"{source_id_col}_left"] = "SourceId"
            if target_id_col:
                rename_map[f"{target_id_col}_right"] = "TargetId"
            tmp = tmp.rename(columns=rename_map)

            # ensure ID columns exist even if one side lacked "Id"
            if "SourceId" not in tmp.columns:
                tmp["SourceId"] = pd.NA
            if "TargetId" not in tmp.columns:
                tmp["TargetId"] = pd.NA

            tmp["ChangeType"] = "CHANGED"
            rows.append(tmp[["UID", "SourceId", "TargetId", "Field", "SourceValue", "TargetValue", "ChangeType"]])

    # SOURCE_ONLY rows — include SourceId if available
    if len(left_uids_only) > 0:
        cols = [source_uid_col]
        if source_id_col:
            cols.append(source_id_col)
        tmp = left.reset_index()[left.reset_index()["__UID__"].isin(left_uids_only)][["__UID__"] + cols].copy()
        tmp = tmp.rename(columns={"__UID__": "UID"})
        if source_id_col:
            tmp = tmp.rename(columns={source_id_col: "SourceId"})
        else:
            tmp["SourceId"] = pd.NA
        tmp["TargetId"] = pd.NA
        tmp["Field"] = pd.NA
        tmp["SourceValue"] = pd.NA
        tmp["TargetValue"] = pd.NA
        tmp["ChangeType"] = "SOURCE_ONLY"
        rows.append(tmp[["UID", "SourceId", "TargetId", "Field", "SourceValue", "TargetValue", "ChangeType"]])

    # TARGET_ONLY rows — include TargetId if available
    if len(right_uids_only) > 0:
        cols = [target_uid_col]
        if target_id_col:
            cols.append(target_id_col)
        tmp = right.reset_index()[right.reset_index()["__UID__"].isin(right_uids_only)][["__UID__"] + cols].copy()
        tmp = tmp.rename(columns={"__UID__": "UID"})
        if target_id_col:
            tmp = tmp.rename(columns={target_id_col: "TargetId"})
        else:
            tmp["TargetId"] = pd.NA
        tmp["SourceId"] = pd.NA
        tmp["Field"] = pd.NA
        tmp["SourceValue"] = pd.NA
        tmp["TargetValue"] = pd.NA
        tmp["ChangeType"] = "TARGET_ONLY"
        rows.append(tmp[["UID", "SourceId", "TargetId", "Field", "SourceValue", "TargetValue", "ChangeType"]])

    # -----------------------
    # Final delta & output
    # -----------------------
    if rows:
        delta = pd.concat(rows, ignore_index=True)
        delta = delta.sort_values(by=["ChangeType", "UID", "Field"], kind="stable", na_position="last")
    else:
        delta = pd.DataFrame(columns=["UID", "SourceId", "TargetId", "Field", "SourceValue", "TargetValue", "ChangeType"])

    if export_csv:
        delta.to_csv(out_path, index=False)
        print(f"✅ Wrote diff CSV → {out_path}")

    return delta

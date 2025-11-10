

# compare_fields.py
import os
import pandas as pd
from typing import Optional, Dict


def compare_object_fields(
    object_name: str,
    env_left: str,
    env_right: str,
    *,
    feather_dir: str = ".",
    output_dir: str = ".",
    output_prefix: str = "field_delta",
) -> pd.DataFrame:
    """
    Compare field definitions for a Salesforce object across two environments using ONLY
    the local field-map feathers produced by your get_fields exporter:
        get_fields_<env>_<object>.feather

    This function DOES NOT query Salesforce. It reads the two Feather files,
    aligns their schemas, and reports per-field deltas:
        - presence in each env
        - field type (if available)
        - simple status classification (SAME, TYPE_MISMATCH, ONLY_IN_LEFT, ONLY_IN_RIGHT)

    Parameters
    ----------
    object_name : str
        Salesforce object API name (e.g., "Account").
    env_left : str
        Name/label of the "left" environment (e.g., "PROD").
    env_right : str
        Name/label of the "right" environment (e.g., "MCUAT8451").
    feather_dir : str, default "."
        Directory where get_fields_<env>_<object>.feather files live.
    output_dir : str, default "."
        Directory to write the CSV output.
    output_prefix : str, default "field_delta"
        Prefix for the output CSV filename.

    Returns
    -------
    pd.DataFrame
        The field-delta table with columns:
          - Field
          - InLeft, InRight (bool)
          - TypeLeft, TypeRight (if available)
          - LabelLeft, LabelRight (if available)
          - QueryableLeft/Right, CreatableLeft/Right, UpdateableLeft/Right (if available)
          - Status (SAME, TYPE_MISMATCH, ONLY_IN_LEFT, ONLY_IN_RIGHT)

    Output
    ------
    Writes:
        {output_dir}/{output_prefix}_{envLeft}_vs_{envRight}_{object}.csv

    Notes
    -----
    • This function expects your field export files to be named:
        get_fields_<env>_<object>.feather  (lowercased, spaces → underscores)
      which matches the `build_field_list_single_env` convention provided earlier.
    • It gracefully handles both schemas we've used before:
        - columns like: Environment, Object, Field, Type, Label, Queryable, Creatable, Updateable
        - or older: Field, Category, source_type/target_type
      Missing columns are filled with NA.
    """

    # ---------- Build paths based on naming convention ----------
    def _clean(s: str) -> str:
        return s.lower().replace(" ", "_")

    obj_clean = _clean(object_name)
    left_clean = _clean(env_left)
    right_clean = _clean(env_right)

    left_path = os.path.join(feather_dir, f"get_fields_{left_clean}_{obj_clean}.feather")
    right_path = os.path.join(feather_dir, f"get_fields_{right_clean}_{obj_clean}.feather")

    if not os.path.exists(left_path):
        raise FileNotFoundError(f"Left env feather not found: {left_path}")
    if not os.path.exists(right_path):
        raise FileNotFoundError(f"Right env feather not found: {right_path}")

    # ---------- Load feathers ----------
    left_raw = pd.read_feather(left_path)
    right_raw = pd.read_feather(right_path)

    # ---------- Normalize to a common schema ----------
    # We map a variety of possible column names to a canonical set.
    # Preferred columns (if present): Field, Type, Label, Queryable, Creatable, Updateable
    def _normalize(df: pd.DataFrame, side: str) -> pd.DataFrame:
        # Find a "field name" column
        field_col = None
        for cand in ["Field", "field", "field_name", "name"]:
            if cand in df.columns:
                field_col = cand
                break
        if field_col is None:
            raise ValueError(f"No field-name column found in {side} dataset. "
                             f"Expected one of: Field, field, field_name, name")

        # Find a "type" column
        type_col = None
        for cand in ["Type", "type", "source_type", "target_type", "source_Type", "target_Type"]:
            if cand in df.columns:
                type_col = cand
                break

        # Optional metadata columns
        def _pick(*names) -> Optional[str]:
            for n in names:
                if n in df.columns:
                    return n
            return None

        label_col = _pick("Label", "label")
        queryable_col = _pick("Queryable", "queryable")
        creatable_col = _pick("Creatable", "createable", "Createable")
        updateable_col = _pick("Updateable", "updateable")

        cols: Dict[str, Optional[str]] = {
            "Field": field_col,
            "Type": type_col,
            "Label": label_col,
            "Queryable": queryable_col,
            "Creatable": creatable_col,
            "Updateable": updateable_col,
        }

        # Build normalized frame
        norm = pd.DataFrame({
            "Field": df[cols["Field"]].astype(str).str.strip()
        })

        for k in ["Type", "Label", "Queryable", "Creatable", "Updateable"]:
            c = cols[k]
            if c is not None:
                norm[k] = df[c]
            else:
                # Fill missing columns with NA so downstream joins work
                norm[k] = pd.NA

        # De-dup in case upstream produced duplicates
        norm = norm.drop_duplicates(subset=["Field"], keep="first").reset_index(drop=True)
        return norm

    left = _normalize(left_raw, "left")
    right = _normalize(right_raw, "right")

    # ---------- Build union of fields and compare ----------
    all_fields = sorted(set(left["Field"]).union(set(right["Field"])))

    # Join helpers (to bring Type/Label/etc. from each side)
    left_keyed = left.set_index("Field")
    right_keyed = right.set_index("Field")

    rows = []
    for f in all_fields:
        l = left_keyed.loc[f] if f in left_keyed.index else None
        r = right_keyed.loc[f] if f in right_keyed.index else None

        in_left = l is not None
        in_right = r is not None

        type_left = (None if not in_left else (None if pd.isna(l["Type"]) else str(l["Type"])))
        type_right = (None if not in_right else (None if pd.isna(r["Type"]) else str(r["Type"])))

        # Status classification
        if in_left and in_right:
            if type_left is not None and type_right is not None and type_left != type_right:
                status = "TYPE_MISMATCH"
            else:
                status = "SAME"
        elif in_left and not in_right:
            status = "ONLY_IN_LEFT"
        else:
            status = "ONLY_IN_RIGHT"

        rows.append({
            "Field": f,
            "InLeft": in_left,
            "InRight": in_right,
            "TypeLeft": type_left,
            "TypeRight": type_right,
            "LabelLeft": (None if not in_left else (None if pd.isna(l["Label"]) else str(l["Label"]))),
            "LabelRight": (None if not in_right else (None if pd.isna(r["Label"]) else str(r["Label"]))),
            "QueryableLeft": (None if not in_left else (None if pd.isna(l["Queryable"]) else bool(l["Queryable"]))),
            "QueryableRight": (None if not in_right else (None if pd.isna(r["Queryable"]) else bool(r["Queryable"]))),
            "CreatableLeft": (None if not in_left else (None if pd.isna(l["Creatable"]) else bool(l["Creatable"]))),
            "CreatableRight": (None if not in_right else (None if pd.isna(r["Creatable"]) else bool(r["Creatable"]))),
            "UpdateableLeft": (None if not in_left else (None if pd.isna(l["Updateable"]) else bool(l["Updateable"]))),
            "UpdateableRight": (None if not in_right else (None if pd.isna(r["Updateable"]) else bool(r["Updateable"]))),
            "Status": status,
        })

    delta = pd.DataFrame(rows).sort_values(
        by=["Status", "Field"],
        key=lambda s: s.map({"ONLY_IN_LEFT": 0, "ONLY_IN_RIGHT": 1, "TYPE_MISMATCH": 2, "SAME": 3}).fillna(99)
        if s.name == "Status" else s
    )

    # ---------- Write CSV ----------
    os.makedirs(output_dir, exist_ok=True)
    out_name = f"{output_prefix}_{left_clean}_vs_{right_clean}_{obj_clean}.csv"
    out_path = os.path.join(output_dir, out_name)
    delta.to_csv(out_path, index=False)

    print(f"✅ Field comparison complete for {object_name}: {env_left} vs {env_right}")
    print(f"💾 Wrote: {out_path}")
    print(f"📊 Summary → ONLY_IN_LEFT: {(delta['Status']=='ONLY_IN_LEFT').sum()}, "
          f"ONLY_IN_RIGHT: {(delta['Status']=='ONLY_IN_RIGHT').sum()}, "
          f"TYPE_MISMATCH: {(delta['Status']=='TYPE_MISMATCH').sum()}, "
          f"SAME: {(delta['Status']=='SAME').sum()}")

    return delta

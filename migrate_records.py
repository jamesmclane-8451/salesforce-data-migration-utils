# migrate_records.py
import re
import math
import time
from typing import List, Dict, Any, Optional, Set, Tuple
from functools import lru_cache
from pathlib import Path

import pandas as pd
from simple_salesforce import SalesforceGeneralError, SalesforceMalformedRequest  # type: ignore

from oauth_login import get_salesforce_connection

# =========================
# Metadata helpers
# =========================

@lru_cache(maxsize=2048)
def _probe_field_queryable_from_describe(field_def: Dict[str, Any]) -> bool:
    return bool(field_def.get("queryable", True))

def _is_reference(fd: Dict[str, Any]) -> bool:
    return fd.get("type") == "reference"

def _collect_src_recordtype_ids_from_df(df: pd.DataFrame) -> Set[str]:
    ids: Set[str] = set()
    if "RecordTypeId" not in df.columns:
        return ids
    for v in df["RecordTypeId"].astype(str).tolist():
        s = str(v).strip()
        if not s or s.lower() == "nan":
            continue
        if re.fullmatch(r"[A-Za-z0-9]{15,18}", s):
            ids.add(s)
    return ids

def _target_devname_to_id(tgt_sf, object_name: str) -> Dict[str, str]:
    soql_tgt = f"SELECT Id, DeveloperName, IsActive FROM RecordType WHERE SobjectType = '{object_name}'"
    rows = tgt_sf.query_all(soql_tgt).get("records", [])
    mapping: Dict[str, str] = {}
    for r in rows:
        dev = r["DeveloperName"]
        if dev not in mapping or r.get("IsActive", True):
            mapping[dev] = r["Id"]
    return mapping

def _load_recordtype_mapping_csv(
    csv_path: str, object_name: str
) -> Tuple[Dict[str, str], Dict[str, str]]:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_filter=False).copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "sobjecttype" in df.columns:
        df = df[df["sobjecttype"].str.strip().str.lower().isin(["", object_name.lower()])].copy()

    def clean(v: Any) -> str:
        return (v or "").strip()

    srcDev_to_tgtId: Dict[str, str] = {}
    srcId_to_tgtId: Dict[str, str] = {}

    for _, row in df.iterrows():
        s_dev = clean(row.get("source_developername"))
        s_id = clean(row.get("source_recordtypeid"))
        t_dev = clean(row.get("target_developername"))
        t_id = clean(row.get("target_recordtypeid"))

        if s_dev and t_id:
            srcDev_to_tgtId[s_dev] = t_id
        if s_id and t_id:
            srcId_to_tgtId[s_id] = t_id
        if s_dev and t_dev and not t_id:
            srcDev_to_tgtId[s_dev] = f"__DEVNAME__:{t_dev}"

    return srcDev_to_tgtId, srcId_to_tgtId


# =========================
# Main migration
# =========================

def migrate_salesforce_data(
    object_name: str,
    from_env: str,
    to_env: str,
    *,
    source_feather_path: str,
    limit: Optional[int] = None,
    suppress_chatter_check: bool = True,
    record_type_mapping_csv: Optional[str] = None,
    on_unmapped_record_type: str = "omit",   # 'omit' | 'use_target_default' | 'error'
    batch_size: Optional[int] = None,        # NEW: user-configurable batch size
) -> pd.DataFrame:
    """
    Migrate records from a Feather file into the TARGET Salesforce org.
    Prints progress as batches are imported.
    """

    # ----- connect target only -----
    print(f"\n🎯 TARGET (to_env) → {to_env}")
    tgt_sf = get_salesforce_connection(to_env, requires_chatter=not suppress_chatter_check)
    tgt_obj = getattr(tgt_sf, object_name)

    # ----- target describe -----
    tgt_desc = tgt_obj.describe()
    tgt_fields_by_name: Dict[str, Dict[str, Any]] = {f["name"]: f for f in tgt_desc["fields"]}

    # ----- read source feather -----
    source_path = Path(source_feather_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source Feather not found at {source_feather_path}")

    df_src = pd.read_feather(source_path)
    df_src.columns = [str(c) for c in df_src.columns]

    if isinstance(limit, int) and limit > 0 and len(df_src) > limit:
        df_src = df_src.head(limit).copy()

    if "Id" not in df_src.columns:
        df_src = df_src.copy()
        df_src["Id"] = [f"ROW_{i}" for i in range(len(df_src))]

    source_csv = f"source_pull_{object_name}_{from_env}.csv"
    df_src.to_csv(source_csv, index=False)

    # ----- field selection using target metadata only -----
    def _is_safe_target_field(fd: Dict[str, Any]) -> bool:
        if _is_reference(fd):
            return False
        if not (fd.get("createable") or fd.get("updateable")):
            return False
        if fd.get("calculated") or fd.get("autoNumber"):
            return False
        if fd["name"] in {
            "Id", "IsDeleted", "CreatedById", "CreatedDate", "LastModifiedById", "LastModifiedDate",
            "SystemModstamp", "LastActivityDate", "LastViewedDate", "LastReferencedDate",
            "Old_Org_Id__c", "RecordTypeId"
        }:
            return False
        return True

    write_fields: List[str] = []
    reference_fields: List[str] = []
    non_matching_rows: List[Dict[str, Any]] = []

    src_field_names = set(df_src.columns)
    tgt_field_names = set(tgt_fields_by_name.keys())
    all_field_names = src_field_names | tgt_field_names

    for fname in sorted(all_field_names):
        if fname == "RecordTypeId":
            non_matching_rows.append({
                "field_name": fname,
                "in_source": fname in src_field_names,
                "in_target": fname in tgt_field_names,
                "source_type": "unknown_from_feather",
                "target_type": tgt_fields_by_name.get(fname, {}).get("type"),
                "reason": "reserved_field",
            })
            continue

        in_src = fname in src_field_names
        tgt_fd = tgt_fields_by_name.get(fname)
        in_tgt = tgt_fd is not None
        tgt_type = tgt_fd.get("type") if in_tgt else None
        is_ref = bool(tgt_fd and _is_reference(tgt_fd))

        if in_src and in_tgt:
            src_readable = True
            tgt_writable = _is_safe_target_field(tgt_fd)
            types_match = True  # let SF validate on write

            if is_ref:
                reference_fields.append(fname)
                non_matching_rows.append({
                    "field_name": fname,
                    "in_source": in_src,
                    "in_target": in_tgt,
                    "source_type": "unknown_from_feather",
                    "target_type": tgt_type,
                    "reason": "lookup_field_excluded",
                })
            elif fname not in {"Id", "Old_Org_Id__c"} and src_readable and tgt_writable and types_match:
                write_fields.append(fname)
            else:
                non_matching_rows.append({
                    "field_name": fname,
                    "in_source": in_src,
                    "in_target": in_tgt,
                    "source_type": "unknown_from_feather",
                    "target_type": tgt_type,
                    "reason": ("not_writable_in_target" if not tgt_writable else "excluded_other"),
                })
        else:
            non_matching_rows.append({
                "field_name": fname,
                "in_source": in_src,
                "in_target": in_tgt,
                "source_type": "unknown_from_feather",
                "target_type": tgt_type,
                "reason": "missing_in_one_side",
            })

    pd.DataFrame(non_matching_rows).to_csv("field_delta.csv", index=False)

    if not write_fields:
        return pd.DataFrame([{
            "object": object_name,
            "total_source": len(df_src),
            "attempted": 0,
            "success": 0,
            "failed": 0,
            "results_csv": None,
            "field_delta_csv": "field_delta.csv",
            "source_csv": source_csv,
            "omitted_lookups_csv": "",
            "matched_non_lookup_fields": 0,
            "lookup_fields_detected": len(reference_fields),
            "limit_applied": limit if limit is not None else "None",
        }])

    # ----- build records from DataFrame -----
    src_records: List[Dict[str, Any]] = df_src.to_dict(orient="records")

    # ----- RecordType mapping setup -----
    tgt_dev_to_id = _target_devname_to_id(tgt_sf, object_name)

    csv_srcDev_to_tgtId: Dict[str, str] = {}
    csv_srcId_to_tgtId: Dict[str, str] = {}
    if record_type_mapping_csv:
        csv_srcDev_to_tgtId, csv_srcId_to_tgtId = _load_recordtype_mapping_csv(record_type_mapping_csv, object_name)
        for k, v in list(csv_srcDev_to_tgtId.items()):
            if isinstance(v, str) and v.startswith("__DEVNAME__:"):
                t_dev = v.split(":", 1)[1]
                t_id = tgt_dev_to_id.get(t_dev)
                if t_id:
                    csv_srcDev_to_tgtId[k] = t_id

    has_rt_dev_col = "RecordTypeDeveloperName" in df_src.columns

    payload_to_send: List[Dict[str, Any]] = []
    attempt_rows: List[Dict[str, Any]] = []
    omitted_lookup_rows: List[Dict[str, Any]] = []

    for r in src_records:
        row_key = str(r.get("Id", "") or "").strip() or ""
        base_row: Dict[str, Any] = {
            "Old_Org_Id__c": row_key,
            "result": "pending",
            "target_id": "",
            "created": "",
            "errors": "",
        }
        new_rec: Dict[str, Any] = {f: r.get(f, None) for f in write_fields}
        sendable = True

        # RecordType mapping
        if "RecordTypeId" in df_src.columns:
            src_rt_id = r.get("RecordTypeId")
            if src_rt_id:
                tgt_rt_id = csv_srcId_to_tgtId.get(str(src_rt_id))
                if not tgt_rt_id and has_rt_dev_col:
                    src_dev = str(r.get("RecordTypeDeveloperName") or "").strip()
                    if src_dev:
                        tgt_rt_id = csv_srcDev_to_tgtId.get(src_dev)
                if tgt_rt_id:
                    new_rec["RecordTypeId"] = tgt_rt_id
                elif on_unmapped_record_type == "error":
                    base_row["result"] = "precheck_error"
                    base_row["errors"] = f"No RecordType mapping for source id '{src_rt_id}'"
                    sendable = False
        elif has_rt_dev_col:
            src_dev = str(r.get("RecordTypeDeveloperName") or "").strip()
            if src_dev:
                tgt_rt_id = csv_srcDev_to_tgtId.get(src_dev)
                if tgt_rt_id:
                    new_rec["RecordTypeId"] = tgt_rt_id
                elif on_unmapped_record_type == "error":
                    base_row["result"] = "precheck_error"
                    base_row["errors"] = f"No RecordType mapping for source devname '{src_dev}'"
                    sendable = False

        # log omitted lookups
        for lf in reference_fields:
            if lf in r and r.get(lf) not in (None, ""):
                ref_to = tgt_fields_by_name.get(lf, {}).get("referenceTo", [])
                omitted_lookup_rows.append({
                    "Old_Org_Id__c": row_key,
                    "field": lf,
                    "source_value": r.get(lf),
                    "reference_to": ";".join(ref_to) if ref_to else "",
                })

        new_rec["Old_Org_Id__c"] = row_key or ""
        attempt_rows.append({**base_row, **new_rec})

        if sendable:
            payload_to_send.append(new_rec)

    # Always define omitted_csv
    omitted_csv = ""
    if omitted_lookup_rows:
        omitted_csv = f"omitted_lookups_{object_name}_{from_env}_to_{to_env}.csv"
        pd.DataFrame(omitted_lookup_rows).to_csv(omitted_csv, index=False)

    # ----- upsert into TARGET (progress + custom batch size) -----
    has_bulk = hasattr(tgt_sf, "bulk")
    tgt_obj_rest = getattr(tgt_sf, object_name)
    tgt_obj_bulk = getattr(tgt_sf.bulk, object_name) if has_bulk else None

    # Resolve effective batch size
    if batch_size is not None:
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer when provided.")
        effective_batch_size = batch_size
    else:
        effective_batch_size = 10000 if has_bulk else 200

    results_csv = f"migration_results_{object_name}_{from_env}_to_{to_env}.csv"
    total = len(payload_to_send)
    num_batches = max(1, math.ceil(total / effective_batch_size))

    # Build attempt_index as a dict (not a list!)
    attempt_index: Dict[str, Dict[str, Any]] = {row["Old_Org_Id__c"]: row for row in attempt_rows}

    def _pct(n, d):
        return (100.0 * n / d) if d else 100.0

    mode = "Bulk" if has_bulk else "REST"
    print(f"\n📦 Preparing import: {total:,} records → {to_env} "
          f"({num_batches} batch{'es' if num_batches!=1 else ''}, size={effective_batch_size}, mode={mode})")

    t0 = time.time()
    cumulative_attempted = cumulative_success = cumulative_error = 0

    for i in range(num_batches):
        start = i * effective_batch_size
        end = min((i + 1) * effective_batch_size, total)
        chunk = payload_to_send[start:end]
        batch_label = f"Batch {i+1}/{num_batches} (records {start+1:,}-{end:,})"

        batch_attempted = batch_success = batch_error = 0
        batch_t0 = time.time()

        if has_bulk:
            try:
                bulk_results = tgt_obj_bulk.upsert(chunk, external_id_field="Old_Org_Id__c")
                for rec, res in zip(chunk, bulk_results):
                    key = rec.get("Old_Org_Id__c", "")
                    row = attempt_index.get(key)
                    if not row:
                        continue
                    batch_attempted += 1
                    if res.get("success"):
                        row["result"] = "success"
                        row["target_id"] = res.get("id") or ""
                        row["created"] = "" if res.get("created") is None else str(res.get("created"))
                        row["errors"] = ""
                        batch_success += 1
                    else:
                        row["result"] = "error"
                        row["errors"] = str(res.get("errors"))
                        batch_error += 1
            except Exception:
                # Fallback to REST per record
                for rec in chunk:
                    key = rec.get("Old_Org_Id__c", "")
                    row = attempt_index.get(key)
                    if not row:
                        continue
                    try:
                        body = {k: v for k, v in rec.items() if k != "Old_Org_Id__c"}
                        resp = tgt_obj_rest.upsert(f"Old_Org_Id__c/{key}", body)
                        row["result"] = "success"
                        row["target_id"] = (resp or {}).get("id", "")
                        row["created"] = "" if (resp or {}).get("created") is None else str((resp or {}).get("created"))
                        row["errors"] = ""
                        batch_success += 1
                    except Exception as inner_e:
                        row["result"] = "error"
                        row["target_id"] = ""
                        row["created"] = ""
                        row["errors"] = str(inner_e)
                        batch_error += 1
                    finally:
                        batch_attempted += 1
        else:
            # REST-only path
            for rec in chunk:
                key = rec.get("Old_Org_Id__c", "")
                row = attempt_index.get(key)
                if not row:
                    continue
                try:
                    body = {k: v for k, v in rec.items() if k != "Old_Org_Id__c"}
                    resp = tgt_obj_rest.upsert(f"Old_Org_Id__c/{key}", body)
                    row["result"] = "success"
                    row["target_id"] = (resp or {}).get("id", "")
                    row["created"] = "" if (resp or {}).get("created") is None else str((resp or {}).get("created"))
                    row["errors"] = ""
                    batch_success += 1
                except (SalesforceMalformedRequest, SalesforceGeneralError) as e:
                    row["result"] = "error"
                    row["target_id"] = ""
                    row["created"] = ""
                    row["errors"] = str(e)
                    batch_error += 1
                except Exception as e:
                    row["result"] = "error"
                    row["target_id"] = ""
                    row["created"] = ""
                    row["errors"] = str(e)
                    batch_error += 1
                finally:
                    batch_attempted += 1

        # progress line
        cumulative_attempted += batch_attempted
        cumulative_success += batch_success
        cumulative_error += batch_error

        elapsed = time.time() - t0
        batch_elapsed = time.time() - batch_t0
        pct = _pct(cumulative_attempted, total)
        rate = (cumulative_attempted / elapsed) if elapsed > 0 else 0
        remaining = max(0, total - cumulative_attempted)
        eta_sec = (remaining / rate) if rate > 0 else 0

        print(
            f"⏩ {batch_label}: "
            f"batch ok={batch_success:,}, err={batch_error:,}, "
            f"total ok={cumulative_success:,}, err={cumulative_error:,}, "
            f"prog={pct:5.1f}% | {cumulative_attempted:,}/{total:,} | "
            f"batch {batch_elapsed:0.1f}s | ETA ~{eta_sec:0.0f}s"
        )

    # finalize
    for row in attempt_rows:
        if row["result"] == "pending":
            row["result"] = "unknown"
            if not row["errors"]:
                row["errors"] = "No status reported"

    results_csv = f"migration_results_{object_name}_{from_env}_to_{to_env}.csv"
    pd.DataFrame(attempt_rows).to_csv(results_csv, index=False)

    attempted = sum(1 for r in attempt_rows if r["result"] in {"success", "error"})
    failed = sum(1 for r in attempt_rows if r["result"] == "error")
    succeeded = sum(1 for r in attempt_rows if r["result"] == "success")

    print(f"\n✅ Import finished: {succeeded:,} succeeded, {failed:,} failed, out of {attempted:,} attempted.")

    return pd.DataFrame([{
        "object": object_name,
        "total_source": len(df_src),
        "attempted": attempted,
        "success": succeeded,
        "failed": failed,
        "results_csv": results_csv,
        "field_delta_csv": "field_delta.csv",
        "source_csv": source_csv,
        "omitted_lookups_csv": omitted_csv,
        "matched_non_lookup_fields": len(write_fields),
        "lookup_fields_detected": len(reference_fields),
        "limit_applied": limit if limit is not None else "None",
        "batch_size_used": effective_batch_size,
    }])

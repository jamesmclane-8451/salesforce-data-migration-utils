from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
import csv
import json
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from auth import get_salesforce_connection


DEFAULT_FALLBACK_OWNER_ID = "005ao000007Br4zAAC"
DEFAULT_OWNER_TARGET_FIELDS = ("OwnerId", "UserId")


def replace_inactive_owner_ids_in_diff_checkpoints(
    target_env: str,
    source_env: str,
    diff_checkpoint_dir: str,
    metadata_scope_csv_path: str,
    output_diff_checkpoint_dir: str = "diff_diff_parts_prerun_cleaned",
    fallback_owner_id: str = DEFAULT_FALLBACK_OWNER_ID,
    owner_target_fields: Sequence[str] = DEFAULT_OWNER_TARGET_FIELDS,
    replacement_audit_csv_path: str = "pre_run_inactive_owner_replacements.csv",
    overwrite_output: bool = True,
    batch_size: int = 50000,
) -> pd.DataFrame:
    """
    Pre-load cleanup for known safe owner/user substitutions.

    The load step resolves reference fields through External_Id__c, so this
    function replaces inactive owner/user source values with the fallback user's
    External_Id__c before load. It writes a cleaned copy of the diff checkpoint
    directory and a CSV audit of every replacement.
    """

    source_root = Path(diff_checkpoint_dir)
    output_root = Path(output_diff_checkpoint_dir)
    metadata_path = Path(metadata_scope_csv_path)

    if not source_root.exists():
        raise FileNotFoundError(f"Diff checkpoint directory not found: {source_root}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata scope CSV not found: {metadata_path}")
    if output_root.exists():
        if not overwrite_output:
            raise FileExistsError(f"Output diff checkpoint directory already exists: {output_root}")
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    print(f"\nPre-run cleanup: replacing inactive owner/user references for [{target_env}]")
    print(f"Source diff checkpoint dir: {source_root}")
    print(f"Cleaned diff checkpoint dir: {output_root}")
    print(f"Fallback owner/user Id: {fallback_owner_id}")

    sf = get_salesforce_connection(env=target_env)
    fallback_external_id = get_fallback_user_external_id(
        sf=sf,
        fallback_owner_id=fallback_owner_id,
    )
    inactive_owner_values = get_inactive_user_owner_values(sf)
    owner_source_fields_by_object = get_owner_source_fields_by_object(
        metadata_path=metadata_path,
        owner_target_fields=owner_target_fields,
    )

    print(f"Fallback owner/user External_Id__c used in cleaned payloads: {fallback_external_id}")
    print(f"Inactive owner/user id values detected: {len(inactive_owner_values)}")
    print(f"Objects with owner/user fields in metadata: {len(owner_source_fields_by_object)}")

    source_value_col = f"{source_env}_value"
    source_load_value_col = f"{source_env}_load_value"
    audit_rows: List[Dict[str, Any]] = []
    parquet_paths = sorted(source_root.glob("Obj=*/JoinBucket=*/record_diff.parquet"))

    for index, source_path in enumerate(parquet_paths, start=1):
        relative_path = source_path.relative_to(source_root)
        output_path = output_root / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        replacements = rewrite_diff_parquet_file(
            source_path=source_path,
            output_path=output_path,
            source_record_id_col=f"{source_env.lower()}_recordid",
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            owner_source_fields_by_object=owner_source_fields_by_object,
            inactive_owner_values=inactive_owner_values,
            fallback_external_id=fallback_external_id,
            fallback_owner_id=fallback_owner_id,
            batch_size=batch_size,
        )
        audit_rows.extend(replacements)

        if replacements:
            object_name = source_path.parts[-3].replace("Obj=", "")
            print(
                f"Cleaned {object_name} {source_path.parts[-2]}: "
                f"{len(replacements)} inactive owner/user replacement(s)"
            )
        elif index % 100 == 0:
            print(f"Checked {index}/{len(parquet_paths)} diff checkpoint file(s)")

    audit_df = pd.DataFrame(
        audit_rows,
        columns=[
            "Obj",
            "Source_RecordId",
            "Payload_Column",
            "Field",
            "Original_Value",
            "Replacement_External_Id__c",
            "Fallback_Target_User_Id",
            "Diff_File",
        ],
    )
    audit_df.to_csv(
        replacement_audit_csv_path,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
    )

    print(
        f"Pre-run cleanup complete: {len(audit_df)} replacement(s). "
        f"Audit CSV written: {replacement_audit_csv_path}"
    )
    return audit_df


def rewrite_diff_parquet_file(
    source_path: Path,
    output_path: Path,
    source_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    owner_source_fields_by_object: Dict[str, Set[str]],
    inactive_owner_values: Set[str],
    fallback_external_id: str,
    fallback_owner_id: str,
    batch_size: int,
) -> List[Dict[str, Any]]:
    parquet_file = pq.ParquetFile(source_path)
    writer: Optional[pq.ParquetWriter] = None
    audit_rows: List[Dict[str, Any]] = []

    try:
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            batch_df = batch.to_pandas()
            batch_replacements = clean_batch_owner_values(
                batch_df=batch_df,
                source_value_col=source_value_col,
                source_load_value_col=source_load_value_col,
                source_record_id_col=source_record_id_col,
                owner_source_fields_by_object=owner_source_fields_by_object,
                inactive_owner_values=inactive_owner_values,
                fallback_external_id=fallback_external_id,
                fallback_owner_id=fallback_owner_id,
                diff_file=str(source_path),
            )
            audit_rows.extend(batch_replacements)
            table = pa.Table.from_pandas(batch_df, schema=batch.schema, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        pq.write_table(parquet_file.read(), output_path)

    return audit_rows


def clean_batch_owner_values(
    batch_df: pd.DataFrame,
    source_value_col: str,
    source_load_value_col: str,
    source_record_id_col: str,
    owner_source_fields_by_object: Dict[str, Set[str]],
    inactive_owner_values: Set[str],
    fallback_external_id: str,
    fallback_owner_id: str,
    diff_file: str,
) -> List[Dict[str, Any]]:
    audit_rows: List[Dict[str, Any]] = []
    payload_columns = [
        column
        for column in (source_value_col, source_load_value_col)
        if column in batch_df.columns
    ]

    for row_index, row in batch_df.iterrows():
        object_name = normalize_blank(row.get("Obj"))
        if not object_name:
            continue

        owner_fields = owner_source_fields_by_object.get(object_name, set()) | {
            "OwnerId",
            "Owner.External_Id__c",
            "UserId",
            "User.External_Id__c",
        }
        if not owner_fields:
            continue

        source_record_id = normalize_blank(row.get(source_record_id_col)) or ""

        for payload_column in payload_columns:
            payload = parse_payload(row.get(payload_column))
            if not payload:
                continue

            replacements = replace_owner_values_in_payload(
                payload=payload,
                object_name=object_name,
                source_record_id=source_record_id,
                payload_column=payload_column,
                owner_fields=owner_fields,
                inactive_owner_values=inactive_owner_values,
                fallback_external_id=fallback_external_id,
                fallback_owner_id=fallback_owner_id,
                diff_file=diff_file,
            )
            if not replacements:
                continue

            batch_df.at[row_index, payload_column] = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
            audit_rows.extend(replacements)

    return audit_rows


def replace_owner_values_in_payload(
    payload: Dict[str, Any],
    object_name: str,
    source_record_id: str,
    payload_column: str,
    owner_fields: Set[str],
    inactive_owner_values: Set[str],
    fallback_external_id: str,
    fallback_owner_id: str,
    diff_file: str,
) -> List[Dict[str, Any]]:
    audit_rows: List[Dict[str, Any]] = []

    for field_name in sorted(owner_fields):
        if field_name not in payload:
            continue

        original_value = normalize_blank(payload.get(field_name))
        if original_value not in inactive_owner_values:
            continue

        payload[field_name] = fallback_external_id
        audit_rows.append(
            build_audit_row(
                object_name=object_name,
                source_record_id=source_record_id,
                payload_column=payload_column,
                field_name=field_name,
                original_value=original_value,
                fallback_external_id=fallback_external_id,
                fallback_owner_id=fallback_owner_id,
                diff_file=diff_file,
            )
        )

    for relationship_name in ("Owner", "User"):
        relationship_value = payload.get(relationship_name)
        if not isinstance(relationship_value, dict):
            continue

        original_value = normalize_blank(relationship_value.get("External_Id__c"))
        if original_value not in inactive_owner_values:
            continue

        relationship_value["External_Id__c"] = fallback_external_id
        audit_rows.append(
            build_audit_row(
                object_name=object_name,
                source_record_id=source_record_id,
                payload_column=payload_column,
                field_name=f"{relationship_name}.External_Id__c",
                original_value=original_value,
                fallback_external_id=fallback_external_id,
                fallback_owner_id=fallback_owner_id,
                diff_file=diff_file,
            )
        )

    return audit_rows


def get_owner_source_fields_by_object(
    metadata_path: Path,
    owner_target_fields: Sequence[str],
) -> Dict[str, Set[str]]:
    metadata_df = pd.read_csv(metadata_path, dtype=str, encoding="utf-8-sig").fillna("")
    owner_target_field_set = {str(field).strip() for field in owner_target_fields if str(field).strip()}
    owner_fields_by_object: Dict[str, Set[str]] = {}

    for _, row in metadata_df.iterrows():
        if should_ignore_row(row.get("Ignore")):
            continue

        source_object = normalize_blank(row.get("SF 1.0 Object"))
        source_field = normalize_blank(row.get("SF 1.0 Field"))
        target_field = normalize_blank(row.get("SF 2.0 Field"))
        if not source_object or not source_field or target_field not in owner_target_field_set:
            continue

        owner_fields_by_object.setdefault(source_object, set()).add(source_field)

    return owner_fields_by_object


def get_fallback_user_external_id(sf, fallback_owner_id: str) -> str:
    escaped_id = escape_soql_string(fallback_owner_id)
    result = sf.query(
        "SELECT Id, External_Id__c, IsActive FROM User "
        f"WHERE Id = '{escaped_id}' "
        "LIMIT 1"
    )
    records = result.get("records", []) if isinstance(result, dict) else []
    if not records:
        raise ValueError(f"Fallback owner/user not found in target Salesforce: {fallback_owner_id}")

    record = records[0]
    if record.get("IsActive") is False:
        raise ValueError(f"Fallback owner/user is inactive: {fallback_owner_id}")

    return normalize_blank(record.get("External_Id__c")) or fallback_owner_id


def get_inactive_user_owner_values(sf) -> Set[str]:
    inactive_values: Set[str] = set()
    soql = "SELECT Id, External_Id__c FROM User WHERE IsActive = false"

    for record in query_salesforce_all(sf, soql):
        user_id = normalize_blank(record.get("Id"))
        external_id = normalize_blank(record.get("External_Id__c"))
        if user_id:
            inactive_values.add(user_id)
        if external_id:
            inactive_values.add(external_id)

    return inactive_values


def query_salesforce_all(sf, soql: str) -> Iterable[Dict[str, Any]]:
    result = sf.query(soql)
    while True:
        records = result.get("records", []) if isinstance(result, dict) else []
        for record in records:
            yield record

        if not result.get("done") or result.get("nextRecordsUrl"):
            result = sf.query_more(result["nextRecordsUrl"], True)
        else:
            break


def build_audit_row(
    object_name: str,
    source_record_id: str,
    payload_column: str,
    field_name: str,
    original_value: str,
    fallback_external_id: str,
    fallback_owner_id: str,
    diff_file: str,
) -> Dict[str, Any]:
    return {
        "Obj": object_name,
        "Source_RecordId": source_record_id,
        "Payload_Column": payload_column,
        "Field": field_name,
        "Original_Value": original_value,
        "Replacement_External_Id__c": fallback_external_id,
        "Fallback_Target_User_Id": fallback_owner_id,
        "Diff_File": diff_file,
    }


def parse_payload(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value

    text = normalize_blank(value)
    if not text:
        return {}

    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        return {}
    return parsed


def should_ignore_row(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "t", "yes", "y", "1"}


def normalize_blank(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return None
    return text


def escape_soql_string(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")

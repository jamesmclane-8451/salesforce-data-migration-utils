

from __future__ import annotations
from datetime import datetime, timezone
import json
import re
import shutil
import unicodedata
import zlib
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import pandas as pd
from auth import get_salesforce_connection


def extract_salesforce_records_to_parquet_for_two_envs(
    env_1: str,
    env_1_output_dir: str,
    env_2: str,
    env_2_output_dir: str,
    csv_path: str,
    limit_per_object: Optional[int] = None,
    field_error_csv_path: str = "field_extract_errors.csv",
    resume_from_checkpoint: bool = False,
    checkpoint_batch_size: int = 2000,
    parquet_bucket_count: int = 32,
) -> Dict[str, Path]:
    """
    TL;DR
    This function pulls Salesforce records for two environments, flattens them into
    one row per Obj + RecordId + Field, and writes partitioned Parquet extract datasets.

    Output format
    -------------
    Each extract is written as a partitioned Parquet directory. Rows are split by
    Obj and JoinBucket so downstream diffing can read smaller slices instead of
    one giant extract file.

    Parameters
    ----------
    env_1 : str
        First Salesforce environment name.

    env_1_output_dir : str
        Output partitioned Parquet dataset directory for env_1.

    env_2 : str
        Second Salesforce environment name.

    env_2_output_dir : str
        Output partitioned Parquet dataset directory for env_2.

    csv_path : str
        Metadata scope CSV path.

    limit_per_object : Optional[int]
        Optional SOQL LIMIT for testing.

    field_error_csv_path : str
        Output path for skipped field errors.

    resume_from_checkpoint : bool
        When True, reuse the most recent saved Parquet files and
        continue from the last saved RecordId for any incomplete object.
        When False, existing output directories are removed
        and all objects are queried from the beginning.

    checkpoint_batch_size : int
        Number of Salesforce records to save per Parquet batch.

    parquet_bucket_count : int
        Number of JoinRecordId buckets per object. Higher values make very large
        object diffs more memory-friendly at the cost of more output files.

    Returns
    -------
    Dict[str, Path]
        Mapping of environment name to partitioned Parquet dataset directory.
    """

    output_columns = ["Env", "Obj", "RecordId", "Field", "Value", "External_Id__c"]

    if checkpoint_batch_size < 1:
        raise ValueError("checkpoint_batch_size must be at least 1")

    if parquet_bucket_count < 1:
        raise ValueError("parquet_bucket_count must be at least 1")

    def repair_text_value(value: str) -> str:
        """
        Normalize text and repair common mojibake artifacts.

        Parameters
        ----------
        value : str
            Raw text value from Salesforce.

        Returns
        -------
        str
            Cleaned text value.
        """
        if value is None:
            return value

        text = unicodedata.normalize("NFC", value)

        replacements = {
            "‚Äì": "–",
            "Äì": "–",
            "â€“": "–",
            "â€”": "—",
            "â€˜": "‘",
            "â€™": "’",
            "â€œ": "“",
            "â€�": "”",
            "Â ": " ",
            "\u00a0": " ",
        }

        for bad, good in replacements.items():
            text = text.replace(bad, good)

        return text

    def normalize_value(value: Any) -> Any:
        """
        Normalize values before they are written to the flattened output.

        Behavior
        --------
        - str -> repaired / normalized string
        - dict/list -> JSON string
        - all other scalar types are returned as-is here
          and later coerced to pandas string dtype before Parquet output

        Parameters
        ----------
        value : Any
            Raw Salesforce field value.

        Returns
        -------
        Any
            Normalized value.
        """
        if isinstance(value, str):
            return repair_text_value(value)

        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)

        return value

    def query_all(sf, soql: str) -> List[Dict[str, Any]]:
        """
        Run a SOQL query and page through all results.

        Parameters
        ----------
        sf
            Authenticated Salesforce client.

        soql : str
            SOQL query string.

        Returns
        -------
        List[Dict[str, Any]]
            All returned records.
        """
        result = sf.query(soql)
        records = result["records"]

        while not result["done"]:
            result = sf.query_more(result["nextRecordsUrl"], True)
            records.extend(result["records"])

        return records

    def coerce_output_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Force the extract output into the stable string schema used by diff.py.
        """
        if df.empty:
            df = pd.DataFrame(columns=output_columns)

        for col in output_columns:
            if col not in df.columns:
                df[col] = None

        df = df[output_columns].copy()

        # Fix PyArrow write issues caused by mixed object types in output columns.
        # This converts bool/int/float/None/etc. into a consistent nullable string dtype.
        for col in output_columns:
            df[col] = df[col].astype("string")

        return df

    def safe_path_part(value: str) -> str:
        """
        Create a filesystem-safe folder/file segment while preserving readability.
        """
        safe_value = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value).strip())
        return safe_value.strip("._") or "blank"

    def get_env_output_dir(output_dir: str) -> Path:
        """
        Resolve the extract dataset directory.
        """
        return Path(output_dir)

    def get_object_output_dir(env_output_dir: Path, obj: str) -> Path:
        return env_output_dir / f"Obj={safe_path_part(obj)}"

    def get_chunk_metadata_dir(object_output_dir: Path) -> Path:
        return object_output_dir / "_chunks"

    def get_join_bucket(join_id: Any) -> str:
        text = "" if join_id is None else str(join_id).strip()

        if not text:
            return "missing"

        bucket_number = zlib.crc32(text.encode("utf-8")) % parquet_bucket_count
        return f"{bucket_number:03d}"

    def write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        tmp_path.replace(path)

    def write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        df.to_parquet(tmp_path, index=False)
        tmp_path.replace(path)

    def write_csv_atomic(df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
        tmp_path.replace(path)

    def get_chunk_index(path: Path) -> int:
        match = re.search(r"chunk_(\d+)\.(?:parquet|json)$", path.name)
        return int(match.group(1)) if match else 0

    def read_chunk_metadata(meta_path: Path) -> Dict[str, Any]:
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            return {
                "chunk_index": get_chunk_index(meta_path),
                "record_count": 0,
                "row_count": 0,
                "last_record_id": None,
            }

    def get_resume_state(object_output_dir: Path) -> Dict[str, Any]:
        """
        Determine whether an object is complete or where to continue it.
        """
        complete_path = object_output_dir / "_complete.json"
        chunk_metadata_dir = get_chunk_metadata_dir(object_output_dir)
        chunk_paths = sorted(chunk_metadata_dir.glob("chunk_*.json"))
        chunk_metadata = [
            {**read_chunk_metadata(path), "path": path}
            for path in chunk_paths
        ]
        valid_chunk_metadata = [
            meta for meta in chunk_metadata if meta.get("last_record_id")
        ]

        latest_chunk = None
        if valid_chunk_metadata:
            latest_chunk = max(
                valid_chunk_metadata,
                key=lambda meta: meta["path"].stat().st_mtime,
            )

        return {
            "is_complete": complete_path.exists(),
            "chunk_count": len(chunk_paths),
            "next_chunk_index": (
                max((get_chunk_index(path) for path in chunk_paths), default=0) + 1
            ),
            "record_count": sum(int(meta.get("record_count") or 0) for meta in chunk_metadata),
            "row_count": sum(int(meta.get("row_count") or 0) for meta in chunk_metadata),
            "last_record_id": latest_chunk.get("last_record_id") if latest_chunk else None,
            "latest_chunk_path": latest_chunk.get("path") if latest_chunk else None,
        }

    def mark_object_complete(
        object_output_dir: Path,
        env_name: str,
        obj: str,
        record_count: int,
        row_count: int,
        chunk_count: int,
    ) -> None:
        write_json_atomic(
            object_output_dir / "_complete.json",
            {
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "env": env_name,
                "obj": obj,
                "record_count": record_count,
                "row_count": row_count,
                "chunk_count": chunk_count,
            },
        )

    def write_object_chunk(
        object_output_dir: Path,
        chunk_index: int,
        env_name: str,
        obj: str,
        last_record_id: Optional[str],
        record_count: int,
        chunk_df: pd.DataFrame,
    ) -> Path:
        for stale_part_path in object_output_dir.glob(f"JoinBucket=*/chunk_{chunk_index:06d}.parquet"):
            stale_part_path.unlink()

        chunk_part_paths: List[str] = []

        for bucket, bucket_df in chunk_df.groupby("JoinBucket", dropna=False):
            bucket_text = safe_path_part(str(bucket))
            chunk_path = (
                object_output_dir
                / f"JoinBucket={bucket_text}"
                / f"chunk_{chunk_index:06d}.parquet"
            )
            write_parquet_atomic(bucket_df, chunk_path)
            chunk_part_paths.append(str(chunk_path))

        chunk_metadata_path = (
            get_chunk_metadata_dir(object_output_dir)
            / f"chunk_{chunk_index:06d}.json"
        )
        write_json_atomic(
            chunk_metadata_path,
            {
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "env": env_name,
                "obj": obj,
                "chunk_index": chunk_index,
                "record_count": record_count,
                "row_count": int(len(chunk_df)),
                "last_record_id": last_record_id,
                "part_paths": chunk_part_paths,
            },
        )
        return chunk_metadata_path

    def write_extract_manifest(env_output_dir: Path, env_name: str) -> None:
        write_json_atomic(
            env_output_dir / "_extract_manifest.json",
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "env": env_name,
                "format": "partitioned_parquet",
                "partition_columns": ["Obj", "JoinBucket"],
                "parquet_bucket_count": parquet_bucket_count,
                "columns": output_columns + ["JoinBucket"],
            },
        )

    def records_to_output_dataframe(
        env_name: str,
        obj: str,
        records: List[Dict[str, Any]],
        extract_fields: List[str],
        external_id_field_api: Optional[str],
    ) -> pd.DataFrame:
        """
        Flatten one Salesforce record batch into the extract output schema.
        """
        rows: List[Dict[str, Any]] = []

        for r in records:
            record_id = r.get("Id")
            external_id_value = None

            if external_id_field_api:
                external_id_value = normalize_value(r.get(external_id_field_api))

            join_id_value = external_id_value if is_mcuat_env(env_name) else record_id
            join_bucket = get_join_bucket(join_id_value)

            for field in extract_fields:
                rows.append(
                    {
                        "Env": env_name,
                        "Obj": obj,
                        "RecordId": record_id,
                        "Field": field,
                        "Value": normalize_value(get_nested_field_value(r, field)),
                        "External_Id__c": external_id_value,
                        "JoinBucket": join_bucket,
                    }
                )

        df = pd.DataFrame(rows, columns=output_columns + ["JoinBucket"])
        df = coerce_output_dataframe(df)
        df["JoinBucket"] = pd.Series(
            [row.get("JoinBucket") for row in rows],
            dtype="string",
        )
        return df

    def escape_soql_literal(value: str) -> str:
        return str(value).replace("\\", "\\\\").replace("'", "\\'")

    def simplify_error(exc: Exception) -> Tuple[str, str]:
        """
        Build a compact error code/message pair for the skipped-field report.
        """
        content = getattr(exc, "content", None)

        if isinstance(content, list) and content:
            first_error = content[0]
            if isinstance(first_error, dict):
                error_code = str(first_error.get("errorCode") or type(exc).__name__)
                message = str(first_error.get("message") or exc)
                return error_code, simplify_error_message(message)

        if isinstance(content, dict):
            error_code = str(content.get("errorCode") or type(exc).__name__)
            message = str(content.get("message") or exc)
            return error_code, simplify_error_message(message)

        return type(exc).__name__, simplify_error_message(str(exc))

    def simplify_error_message(message: str) -> str:
        """
        Keep the Salesforce error message short enough to scan in PyCharm.
        """
        compact_message = " ".join(message.split())

        missing_field_match = re.search(
            r"No such column '([^']+)' on entity '([^']+)'",
            compact_message,
        )
        if missing_field_match:
            field_name, object_name = missing_field_match.groups()
            return f"No such column '{field_name}' on entity '{object_name}'."

        relationship_match = re.search(
            r"Didn't understand relationship '([^']+)'",
            compact_message,
        )
        if relationship_match:
            return f"Didn't understand relationship '{relationship_match.group(1)}'."

        return compact_message

    def get_failed_query_field(exc: Exception, query_fields: List[str]) -> Optional[str]:
        """
        Extract the field Salesforce rejected from a query exception.
        """
        _, simplified_message = simplify_error(exc)

        quoted_field_match = re.search(r"No such column '([^']+)'", simplified_message)
        if quoted_field_match:
            failed_name = quoted_field_match.group(1)
            for field in query_fields:
                if field == failed_name or field.endswith(f".{failed_name}"):
                    return field
            return failed_name

        relationship_match = re.search(
            r"Didn't understand relationship '([^']+)'",
            simplified_message,
        )
        if relationship_match:
            failed_relationship = relationship_match.group(1)
            for field in query_fields:
                if field == failed_relationship or field.startswith(f"{failed_relationship}."):
                    return field
            return failed_relationship

        return None

    def record_field_error(
        error_rows: List[Dict[str, str]],
        env_name: str,
        obj: str,
        field: str,
        exc: Exception,
    ) -> None:
        """
        Print and store a skipped field error.
        """
        error_code, simplified_message = simplify_error(exc)
        print(f"SKIPPED FIELD: {env_name}.{obj}.{field} - {simplified_message}")
        error_rows.append(
            {
                "Env": env_name,
                "Obj": obj,
                "Field": field,
                "Error": simplified_message,
                "ErrorCode": error_code,
            }
        )

    def record_object_query_error(
        error_rows: List[Dict[str, str]],
        env_name: str,
        obj: str,
        exc: Exception,
    ) -> None:
        """
        Print and store an object-level query error.
        """
        error_code, simplified_message = simplify_error(exc)
        print(f"SKIPPED OBJECT: {env_name}.{obj} - {simplified_message}")
        error_rows.append(
            {
                "Env": env_name,
                "Obj": obj,
                "Field": "",
                "Error": simplified_message,
                "ErrorCode": error_code,
            }
        )

    def query_batch_skipping_bad_fields(
        sf,
        env_name: str,
        obj: str,
        fields: List[str],
        error_rows: List[Dict[str, str]],
        last_record_id: Optional[str],
        batch_limit: int,
    ) -> Tuple[Optional[List[Dict[str, Any]]], List[str], bool]:
        """
        Query one resumable object batch, dropping fields Salesforce reports as invalid.
        """
        active_fields = list(fields)

        while active_fields:
            soql = f"SELECT {', '.join(active_fields)} FROM {obj}"
            where_clauses: List[str] = []

            if last_record_id:
                where_clauses.append(f"Id > '{escape_soql_literal(last_record_id)}'")

            if where_clauses:
                soql += f" WHERE {' AND '.join(where_clauses)}"

            soql += f" ORDER BY Id ASC LIMIT {batch_limit}"

            print(f"\nQuerying {env_name}.{obj}")
            print(soql)

            try:
                return query_all(sf, soql), active_fields, False
            except Exception as exc:
                failed_field = get_failed_query_field(exc, active_fields)

                if not failed_field or failed_field not in active_fields:
                    record_object_query_error(
                        error_rows=error_rows,
                        env_name=env_name,
                        obj=obj,
                        exc=exc,
                    )
                    return None, [], True

                record_field_error(
                    error_rows=error_rows,
                    env_name=env_name,
                    obj=obj,
                    field=failed_field,
                    exc=exc,
                )
                active_fields = [field for field in active_fields if field != failed_field]

        return [], [], False

    def get_field_name_map(sf, object_name: str) -> Dict[str, str]:
        """
        Describe an object and return a lowercase field name map.

        Parameters
        ----------
        sf
            Authenticated Salesforce client.

        object_name : str
            Salesforce object API name.

        Returns
        -------
        Dict[str, str]
            lowercase field api name -> actual field api name
        """
        describe_result = getattr(sf, object_name).describe()
        return {
            field_def["name"].strip().lower(): field_def["name"].strip()
            for field_def in describe_result.get("fields", [])
            if field_def.get("name")
        }

    def is_mcuat_env(env_name: str) -> bool:
        """
        Determine whether the environment should use SF 2.0 columns.

        Parameters
        ----------
        env_name : str
            Environment name.

        Returns
        -------
        bool
            True when env_name represents MCUAT / SF 2.0.
        """
        return "MCUAT" in env_name.upper()

    def get_nested_field_value(record: Dict[str, Any], field_path: str) -> Any:
        """
        Safely read a direct field or dotted relationship path from a Salesforce record.

        Examples
        --------
        Name
        Contact.External_Id__c

        Parameters
        ----------
        record : Dict[str, Any]
            Salesforce record payload.

        field_path : str
            Field API name or dotted relationship path.

        Returns
        -------
        Any
            Field value or None if not found.
        """
        if not field_path:
            return None

        if "." not in field_path:
            return record.get(field_path)

        current_value: Any = record

        for path_part in field_path.split("."):
            if current_value is None:
                return None
            if not isinstance(current_value, dict):
                return None
            current_value = current_value.get(path_part)

        return current_value

    def build_env_object_field_map_from_metadata_scope(
        mapping_df: pd.DataFrame,
        env_names: List[str],
    ) -> Dict[str, Dict[str, List[str]]]:
        """
        Build env -> object -> field map from the metadata scope CSV.

        Logic
        -----
        - MCUAT envs use SF 2.0 Object / Field
        - non-MCUAT envs use SF 1.0 Object / Field
        - rows with Ignore = TRUE are skipped entirely

        Filter Logic column is intentionally ignored.

        Parameters
        ----------
        mapping_df : pd.DataFrame
            Metadata scope DataFrame.

        env_names : List[str]
            Environment names to prepare instructions for.

        Returns
        -------
        Dict[str, Dict[str, List[str]]]
            Nested extraction map.
        """
        required_columns = [
            "SF 1.0 Object",
            "SF 1.0 Field",
            "SF 2.0 Object",
            "SF 2.0 Field",
        ]

        missing_columns = [c for c in required_columns if c not in mapping_df.columns]
        if missing_columns:
            raise ValueError(
                f"{csv_path} is missing required columns: {missing_columns}"
            )

        def should_ignore_row(value: Any) -> bool:
            return str(value).strip().lower() in {"true", "t", "yes", "y", "1"}

        working_df = mapping_df.copy()

        for col in required_columns:
            working_df[col] = working_df[col].fillna("").astype(str).str.strip()

        if "Ignore" in working_df.columns:
            ignore_mask = working_df["Ignore"].fillna("").map(should_ignore_row)
            ignored_count = int(ignore_mask.sum())
            if ignored_count:
                print(f"Ignoring {ignored_count} metadata_scope.csv row(s) where Ignore is TRUE")
            working_df = working_df.loc[~ignore_mask].copy()

        env_object_field_map: Dict[str, Dict[str, List[str]]] = {
            env_name: {} for env_name in env_names
        }

        for env_name in env_names:
            use_sf2_columns = is_mcuat_env(env_name)

            object_col = "SF 2.0 Object" if use_sf2_columns else "SF 1.0 Object"
            field_col = "SF 2.0 Field" if use_sf2_columns else "SF 1.0 Field"

            for _, row in working_df.iterrows():
                obj = row[object_col]
                field = row[field_col]

                if not obj or not field:
                    continue

                env_object_field_map.setdefault(env_name, {})
                env_object_field_map[env_name].setdefault(obj, [])

                if field not in env_object_field_map[env_name][obj]:
                    env_object_field_map[env_name][obj].append(field)

        return env_object_field_map

    csv_df = pd.read_csv(csv_path, dtype=str).fillna("")

    env_object_field_map = build_env_object_field_map_from_metadata_scope(
        mapping_df=csv_df,
        env_names=[env_1, env_2],
    )

    env_configs = {
        env_1: env_1_output_dir,
        env_2: env_2_output_dir,
    }

    outputs: Dict[str, Path] = {}
    field_error_columns = ["Env", "Obj", "Field", "Error", "ErrorCode"]
    field_error_rows: List[Dict[str, str]] = []

    if resume_from_checkpoint and Path(field_error_csv_path).exists():
        existing_field_errors_df = pd.read_csv(
            field_error_csv_path,
            dtype=str,
            encoding="utf-8-sig",
        ).fillna("")

        for col in field_error_columns:
            if col not in existing_field_errors_df.columns:
                existing_field_errors_df[col] = ""

        field_error_rows = existing_field_errors_df[field_error_columns].to_dict("records")
        print(
            f"Loaded existing field error CSV for resume: "
            f"{field_error_csv_path} ({len(field_error_rows)} rows)"
        )

    for env_name, output_dir in env_configs.items():
        env_output_dir = get_env_output_dir(output_dir)

        if resume_from_checkpoint:
            env_output_dir.mkdir(parents=True, exist_ok=True)
            saved_checkpoint_paths = list(env_output_dir.glob("Obj=*/JoinBucket=*/chunk_*.parquet"))

            if saved_checkpoint_paths:
                latest_checkpoint_path = max(
                    saved_checkpoint_paths,
                    key=lambda path: path.stat().st_mtime,
                )
                print(
                    f"\nResume enabled for {env_name}; latest checkpoint found: "
                    f"{latest_checkpoint_path}"
                )
            else:
                print(f"\nResume enabled for {env_name}; no checkpoint files found")
        else:
            if env_output_dir.exists():
                shutil.rmtree(env_output_dir)
            env_output_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nConnecting to Salesforce env [{env_name}]")

        sf = get_salesforce_connection(env=env_name)

        object_map = env_object_field_map.get(env_name, {})

        for obj, fields in object_map.items():
            object_output_dir = get_object_output_dir(env_output_dir, obj)
            resume_state = (
                get_resume_state(object_output_dir)
                if resume_from_checkpoint
                else {
                    "is_complete": False,
                    "chunk_count": 0,
                    "next_chunk_index": 1,
                    "record_count": 0,
                    "row_count": 0,
                    "last_record_id": None,
                    "latest_chunk_path": None,
                }
            )

            if resume_from_checkpoint and resume_state["is_complete"]:
                print(f"\nSkipping {env_name}.{obj}; checkpoint already complete")
                continue

            if resume_from_checkpoint and resume_state.get("latest_chunk_path"):
                print(
                    f"\nResuming {env_name}.{obj} from checkpoint "
                    f"{resume_state['latest_chunk_path']}"
                )

            query_fields = ["Id"] + [f for f in fields if f != "Id"]

            external_id_field_api: Optional[str] = None

            if is_mcuat_env(env_name):
                try:
                    field_name_map = get_field_name_map(sf, obj)
                except Exception as exc:
                    record_object_query_error(
                        error_rows=field_error_rows,
                        env_name=env_name,
                        obj=obj,
                        exc=exc,
                    )
                    print(f"Retrieved 0 records")
                    mark_object_complete(
                        object_output_dir=object_output_dir,
                        env_name=env_name,
                        obj=obj,
                        record_count=int(resume_state["record_count"]),
                        row_count=int(resume_state["row_count"]),
                        chunk_count=int(resume_state["chunk_count"]),
                    )
                    continue

                external_id_field_api = field_name_map.get("external_id__c")

                if external_id_field_api and external_id_field_api not in query_fields:
                    query_fields.append(external_id_field_api)

            object_record_count = int(resume_state["record_count"])
            object_row_count = int(resume_state["row_count"])
            chunk_index = int(resume_state["next_chunk_index"])
            last_record_id = (
                str(resume_state["last_record_id"])
                if resume_state.get("last_record_id")
                else None
            )
            object_query_failed = False

            while True:
                if limit_per_object is not None:
                    remaining_limit = limit_per_object - object_record_count

                    if remaining_limit <= 0:
                        break

                    batch_limit = min(checkpoint_batch_size, remaining_limit)
                else:
                    batch_limit = checkpoint_batch_size

                records, successful_query_fields, query_failed = query_batch_skipping_bad_fields(
                    sf=sf,
                    env_name=env_name,
                    obj=obj,
                    fields=query_fields,
                    error_rows=field_error_rows,
                    last_record_id=last_record_id,
                    batch_limit=batch_limit,
                )

                if query_failed:
                    object_query_failed = True
                    print(f"Retrieved 0 records")
                    break

                if successful_query_fields:
                    query_fields = successful_query_fields

                successful_field_set = set(successful_query_fields)
                extract_fields = [
                    field for field in fields if field in successful_field_set
                ]

                print(f"Retrieved {len(records)} records")

                if not records:
                    break

                last_record_id = str(records[-1].get("Id") or "")
                chunk_df = records_to_output_dataframe(
                    env_name=env_name,
                    obj=obj,
                    records=records,
                    extract_fields=extract_fields,
                    external_id_field_api=external_id_field_api,
                )
                chunk_path = write_object_chunk(
                    object_output_dir=object_output_dir,
                    chunk_index=chunk_index,
                    env_name=env_name,
                    obj=obj,
                    last_record_id=last_record_id,
                    record_count=len(records),
                    chunk_df=chunk_df,
                )

                object_record_count += len(records)
                object_row_count += len(chunk_df)
                print(f"Checkpoint written: {chunk_path} ({len(chunk_df)} rows)")

                chunk_index += 1

                if len(records) < batch_limit:
                    break

            if object_query_failed and object_record_count:
                print(
                    f"Checkpoint incomplete for {env_name}.{obj}; "
                    "it will retry from the last saved checkpoint on the next resume run"
                )
            else:
                mark_object_complete(
                    object_output_dir=object_output_dir,
                    env_name=env_name,
                    obj=obj,
                    record_count=object_record_count,
                    row_count=object_row_count,
                    chunk_count=chunk_index - 1,
                )

        write_extract_manifest(env_output_dir, env_name)
        print(f"\nParquet extract directory written: {env_output_dir}")

        outputs[env_name] = env_output_dir

    field_error_df = pd.DataFrame(
        field_error_rows,
        columns=field_error_columns,
    )
    field_error_df = field_error_df.drop_duplicates(ignore_index=True)
    write_csv_atomic(field_error_df, Path(field_error_csv_path))
    print(f"\nField error CSV written: {field_error_csv_path} ({len(field_error_df)} rows)")

    return outputs

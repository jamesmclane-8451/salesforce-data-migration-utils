from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple
import csv
from datetime import datetime, timezone
from fnmatch import fnmatchcase
import html
import json
import re
import shutil
import unicodedata

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq


def diff_salesforce_parquet_extracts(
    source_extract_dir: str,
    target_extract_dir: str,
    output_feather_path: str,
    output_csv: bool = True,
    exclusions_csv_path: str | None = None,
    resume_from_checkpoint: bool = True,
    diff_checkpoint_dir: str | None = None,
    return_dataframe: bool = False,
    stream_batch_size: int = 50000,
    comparison_excluded_fields: List[str] | None = None,
) -> pd.DataFrame:
    """
    Compare partitioned Parquet extracts from SF 2.0/source vs SF 1.0/target.

    The extract directories are expected to use this shape:

        extract_dir/
          _extract_manifest.json
          Obj=Account/
            _complete.json
            _chunks/chunk_000001.json
            JoinBucket=000/chunk_000001.parquet

    The diff is processed object-by-object and JoinBucket-by-JoinBucket. Each
    completed bucket writes checkpoint Parquet files so interrupted runs can
    resume without redoing completed buckets. Final outputs are streamed from
    those checkpoint files to avoid loading the full diff into memory.

    comparison_excluded_fields removes fields from diff comparison/output only.
    Entries can be exact field names ("Id"), object-qualified names
    ("Opportunity.AccountId"), or shell-style wildcards ("*Id").
    """

    required_columns: List[str] = [
        "Env",
        "Obj",
        "RecordId",
        "Field",
        "Value",
        "External_Id__c",
    ]

    source_root = Path(source_extract_dir)
    target_root = Path(target_extract_dir)

    if not source_root.exists():
        raise FileNotFoundError(f"Source extract directory not found: {source_root}")
    if not target_root.exists():
        raise FileNotFoundError(f"Target extract directory not found: {target_root}")

    def normalize_text(series: pd.Series) -> pd.Series:
        series = series.where(pd.notna(series), None)
        return series.map(
            lambda x: None
            if x is None or str(x).strip().lower() in {"", "nan", "none", "null"}
            else str(x).strip()
        )

    def canonicalize_string(value: str) -> str:
        text = unicodedata.normalize("NFKC", value)
        text = " ".join(text.split())
        return text.strip()

    def normalize_compare_value(value: Any) -> Any:
        if pd.isna(value) or value is None:
            return None
        return canonicalize_string(str(value))

    def is_missing_value(value: Any) -> bool:
        if value is None:
            return True
        try:
            is_missing = pd.isna(value)
        except (TypeError, ValueError):
            return False
        return bool(is_missing) if isinstance(is_missing, bool) else False

    def to_json_safe_value(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: to_json_safe_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [to_json_safe_value(v) for v in value]
        if is_missing_value(value):
            return None
        return value

    def to_json_string(value: Any) -> str | None:
        if value is None:
            return None
        return json.dumps(
            to_json_safe_value(value),
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            allow_nan=False,
        )

    def to_csv_value(value: Any) -> str:
        safe_value = to_json_safe_value(value)
        if safe_value is None:
            return ""
        if isinstance(safe_value, (dict, list)):
            text = json.dumps(
                safe_value,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
                allow_nan=False,
            )
        else:
            text = str(safe_value)
        return text.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")

    invalid_markup_pattern = re.compile(
        r"(?is)"
        r"mso-|mso-level-text|mso-level-tab-stop|mso-level-number-position|"
        r"@list|MsoNormal|"
        r"<\s*(?:html|body|table|tbody|tr|td|div|span|p|a|ul|ol|li|strong|br|style)\b|"
        r"</\s*(?:html|body|table|tbody|tr|td|div|span|p|a|ul|ol|li|strong|style)\s*>|"
        r"&nbsp;|"
        r"(?:font-family|font-size|line-height|margin-|padding-|list-style-type)\s*:"
    )

    def clean_markup_value(value: Any) -> Tuple[Any, bool]:
        if not isinstance(value, str) or not invalid_markup_pattern.search(value):
            return value, False

        original_value = value
        text = value.replace("\r\n", "\n").replace("\r", "\n")

        office_body_match = re.search(r"(?is)\bBody:\s*<!--", text)
        if office_body_match and re.search(r"(?is)mso-|MsoNormal|@list|/\*\s*Font Definitions\s*\*/", text):
            text = text[: office_body_match.start()] + "Body:"

        text = re.sub(r"(?is)<!--.*?-->", " ", text)
        text = re.sub(r"(?is)<!--.*", " ", text)
        text = re.sub(r"(?is)<\s*(script|style|head|meta)[^>]*>.*?</\s*\1\s*>", " ", text)
        text = re.sub(
            r"(?is)/\*\s*(?:Font|Style|List) Definitions\s*\*/.*?(?=(?:<body|<div|<p|Body:|From:|To:|Subject:)|$)",
            " ",
            text,
        )
        text = re.sub(r"(?is)@(?:font-face|list)[^{]*\{.*?\}", " ", text)
        text = re.sub(r"(?im)^\s*(?:p|li|div|span|\.)?[\w. #,-]*Mso[\w. #,-]*(?:\{|$).*?$", " ", text)
        text = re.sub(r"(?im)^\s*(?:@page|div\.WordSection|p\.xmso|li\.xmso|span\.xemailstyle|span\.emailstyle).*?$", " ", text)
        text = re.sub(r"(?is)\{[^{}]*(?:mso-|font-family|font-size|list-style-type)[^{}]*\}", " ", text)
        text = re.sub(
            r"(?is)(?:mso-[\w-]+|font-family|font-size|margin(?:-[\w-]+)?|"
            r"padding(?:-[\w-]+)?|text-indent|line-height|color|background|"
            r"list-style-type)\s*:[^;{}]+;?",
            " ",
            text,
        )

        text = re.sub(r"(?is)<\s*br\s*/?\s*>", "\n", text)
        text = re.sub(r"(?is)<\s*li[^>]*>", "\n- ", text)
        text = re.sub(r"(?is)</\s*(p|div|li|tr|h[1-6]|ul|ol|table)\s*>", "\n", text)
        text = re.sub(
            r"(?is)<\s*/?\s*(?:html|body|table|tbody|tr|td|div|span|p|a|ul|ol|li|strong|style)\b[^>\n]*(?:>|$)",
            " ",
            text,
        )
        text = re.sub(r"(?is)<[^>]+>", " ", text)

        text = html.unescape(text)
        text = unicodedata.normalize("NFC", text)
        text = re.sub(r"[ \t\f\v]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        return text, text != original_value

    def load_manifest_env(root: Path) -> str:
        manifest_path = root / "_extract_manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            env = str(manifest.get("env", "")).strip()
            if env:
                return env

        first_part = next(root.glob("Obj=*/JoinBucket=*/chunk_*.parquet"), None)
        if first_part is None:
            return root.name

        sample_df = pd.read_parquet(first_part, columns=["Env"])
        env_values = normalize_text(sample_df["Env"]).dropna().unique()
        return str(env_values[0]) if len(env_values) else root.name

    def safe_path_part(value: str) -> str:
        safe_value = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value).strip())
        return safe_value.strip("._") or "blank"

    def get_chunk_index(path: Path) -> int:
        match = re.search(r"chunk_(\d+)\.(?:parquet|json)$", path.name)
        return int(match.group(1)) if match else 0

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

    def write_feather_atomic(df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        df.to_feather(tmp_path)
        tmp_path.replace(path)

    def coerce_output_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        if df.empty:
            df = pd.DataFrame(columns=columns)

        for col in columns:
            if col not in df.columns:
                df[col] = None

        df = df[columns].copy()

        for col in columns:
            df[col] = df[col].astype("string")

        return df

    def output_schema(columns: List[str]) -> pa.Schema:
        return pa.schema([(col, pa.string()) for col in columns])

    def iter_parquet_batches(
        part_paths: List[Path],
        columns: List[str],
    ):
        schema = output_schema(columns)

        for part_path in part_paths:
            parquet_file = pq.ParquetFile(part_path)
            for batch in parquet_file.iter_batches(
                columns=columns,
                batch_size=stream_batch_size,
            ):
                if batch.num_rows == 0:
                    continue

                table = pa.Table.from_batches([batch])
                table = table.cast(schema)

                for out_batch in table.to_batches(max_chunksize=stream_batch_size):
                    yield out_batch

    def parquet_row_count(part_paths: List[Path]) -> int:
        row_count = 0
        for part_path in part_paths:
            row_count += pq.ParquetFile(part_path).metadata.num_rows
        return row_count

    def stream_parquet_parts_to_feather(
        part_paths: List[Path],
        path: Path,
        columns: List[str],
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")

        with pa_ipc.new_file(tmp_path, output_schema(columns)) as writer:
            for batch in iter_parquet_batches(part_paths, columns):
                writer.write_batch(batch)

        tmp_path.replace(path)

    def stream_parquet_parts_to_csv(
        part_paths: List[Path],
        path: Path,
        columns: List[str],
        atomic: bool = True,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_path = path.with_suffix(path.suffix + ".tmp") if atomic else path

        with write_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
            writer = csv.writer(
                csv_file,
                quoting=csv.QUOTE_ALL,
                lineterminator="\n",
            )
            writer.writerow(columns)

            for batch in iter_parquet_batches(part_paths, columns):
                column_values = [batch.column(i).to_pylist() for i in range(len(columns))]
                for row in zip(*column_values):
                    writer.writerow([to_csv_value(value) for value in row])

        if atomic:
            write_path.replace(path)

    def summarize_excluded_audit_parts(
        part_paths: List[Path],
    ) -> pd.DataFrame:
        audit_counts: Dict[Tuple[str, str, str, str], int] = {}

        for part_path in part_paths:
            audit_df = pd.read_parquet(part_path, columns=excluded_audit_columns)
            audit_df["Excluded Row Count"] = pd.to_numeric(
                audit_df["Excluded Row Count"],
                errors="coerce",
            ).fillna(0).astype(int)

            for _, row in audit_df.iterrows():
                key = (
                    str(row["Env"]),
                    str(row["Obj"]),
                    str(row["Field"]),
                    str(row["Reason"]),
                )
                audit_counts[key] = audit_counts.get(key, 0) + int(row["Excluded Row Count"])

        rows = [
            {
                "Env": env,
                "Obj": obj,
                "Field": field,
                "Reason": reason,
                "Excluded Row Count": count,
            }
            for (env, obj, field, reason), count in audit_counts.items()
        ]

        audit_summary = pd.DataFrame(rows, columns=excluded_audit_columns)
        if not audit_summary.empty:
            audit_summary = (
                audit_summary.sort_values(["Env", "Obj", "Field"], kind="stable")
                .reset_index(drop=True)
            )

        return audit_summary

    def object_name_from_dir(object_dir: Path) -> str:
        complete_path = object_dir / "_complete.json"
        if complete_path.exists():
            try:
                payload = json.loads(complete_path.read_text(encoding="utf-8"))
                obj = str(payload.get("obj", "")).strip()
                if obj:
                    return obj
            except json.JSONDecodeError:
                pass

        if object_dir.name.startswith("Obj="):
            return object_dir.name[len("Obj="):]

        return object_dir.name

    def list_objects(root: Path) -> Dict[str, Path]:
        objects: Dict[str, Path] = {}
        for object_dir in sorted(root.glob("Obj=*")):
            if object_dir.is_dir():
                objects[object_name_from_dir(object_dir)] = object_dir
        return objects

    def list_buckets(object_dir: Path | None) -> set[str]:
        if object_dir is None:
            return set()

        buckets: set[str] = set()
        for bucket_dir in object_dir.glob("JoinBucket=*"):
            if bucket_dir.is_dir():
                buckets.add(bucket_dir.name[len("JoinBucket="):])
        return buckets

    def read_partition_files(object_dir: Path | None, bucket: str) -> pd.DataFrame:
        if object_dir is None:
            return pd.DataFrame(columns=required_columns)

        bucket_dir = object_dir / f"JoinBucket={safe_path_part(bucket)}"
        chunk_metadata_dir = object_dir / "_chunks"
        part_paths = [
            part_path
            for part_path in sorted(bucket_dir.glob("chunk_*.parquet"))
            if (chunk_metadata_dir / f"chunk_{get_chunk_index(part_path):06d}.json").exists()
        ]

        if not part_paths:
            return pd.DataFrame(columns=required_columns)

        frames = [
            pd.read_parquet(part_path, columns=required_columns)
            for part_path in part_paths
        ]

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=required_columns)

    def load_exclusions(csv_path: str | None) -> Tuple[set[str], Dict[Tuple[str, str], str], Dict[str, str]]:
        if not csv_path:
            return set(), {}, {}

        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"Diff exclusions CSV not found: {path}")

        exclusions_df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
        required = {"Obj", "Field"}
        missing = required - set(exclusions_df.columns)
        if missing:
            raise ValueError(
                f"Diff exclusions CSV is missing required column(s): {', '.join(sorted(missing))}"
            )

        object_exclusions: set[str] = set()
        object_reasons: Dict[str, str] = {}
        field_exclusions: Dict[Tuple[str, str], str] = {}

        for _, row in exclusions_df.iterrows():
            obj = str(row.get("Obj", "")).strip()
            field = str(row.get("Field", "")).strip()
            reason = str(row.get("Reason", "")).strip()

            if not obj:
                continue
            if field:
                field_exclusions[(obj, field)] = reason
            else:
                object_exclusions.add(obj)
                object_reasons[obj] = reason

        return object_exclusions, field_exclusions, object_reasons

    def normalize_comparison_excluded_fields(fields: List[str] | None) -> Tuple[str, ...]:
        if not fields:
            return tuple()

        normalized_fields: List[str] = []
        for field in fields:
            field_text = str(field).strip()
            if field_text:
                normalized_fields.append(field_text)

        return tuple(dict.fromkeys(normalized_fields))

    comparison_exclusion_specs = normalize_comparison_excluded_fields(comparison_excluded_fields)

    def matching_comparison_exclusion_spec(obj: str, field: str) -> str | None:
        obj_text = "" if is_missing_value(obj) else str(obj)
        field_text = "" if is_missing_value(field) else str(field)

        for spec in comparison_exclusion_specs:
            if "." in spec:
                obj_spec, field_spec = spec.split(".", 1)
            else:
                obj_spec, field_spec = "*", spec

            if fnmatchcase(obj_text, obj_spec) and fnmatchcase(field_text, field_spec):
                return spec

        return None

    object_exclusions, field_exclusions, object_reasons = load_exclusions(exclusions_csv_path)
    excluded_audit_counts: Dict[Tuple[str, str, str, str], int] = {}
    cleaned_value_rows: List[Dict[str, Any]] = []

    def exclusion_reason(row: pd.Series) -> str:
        obj = row["Obj"]
        field = row["Field"]
        if obj in object_exclusions:
            return object_reasons.get(obj, "")
        if (obj, field) in field_exclusions:
            return field_exclusions.get((obj, field), "")

        spec = matching_comparison_exclusion_spec(obj, field)
        if spec:
            return f"Comparison-only field exclusion from function parameter: {spec}"

        return ""

    def apply_exclusions(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or (not object_exclusions and not field_exclusions):
            return df

        object_mask = df["Obj"].isin(object_exclusions)
        field_mask = pd.Series(
            [(obj, field) in field_exclusions for obj, field in zip(df["Obj"], df["Field"])],
            index=df.index,
        )
        exclusion_mask = object_mask | field_mask

        if exclusion_mask.any():
            excluded = df.loc[exclusion_mask, ["Env", "Obj", "Field"]].copy()
            excluded["Reason"] = df.loc[exclusion_mask].apply(exclusion_reason, axis=1)

            grouped = (
                excluded.groupby(["Env", "Obj", "Field", "Reason"], dropna=False)
                .size()
                .reset_index(name="Excluded Row Count")
            )

            for _, row in grouped.iterrows():
                key = (
                    str(row["Env"]),
                    str(row["Obj"]),
                    str(row["Field"]),
                    str(row["Reason"]),
                )
                excluded_audit_counts[key] = excluded_audit_counts.get(key, 0) + int(row["Excluded Row Count"])

        return df.loc[~exclusion_mask].copy()

    def comparison_exclusion_mask(df: pd.DataFrame) -> pd.Series:
        if df.empty or not comparison_exclusion_specs:
            return pd.Series(False, index=df.index)

        return pd.Series(
            [
                matching_comparison_exclusion_spec(obj, field) is not None
                for obj, field in zip(df["Obj"], df["Field"])
            ],
            index=df.index,
        )

    def audit_comparison_exclusions(df: pd.DataFrame) -> None:
        if df.empty or not comparison_exclusion_specs:
            return

        exclusion_mask = comparison_exclusion_mask(df)
        if not exclusion_mask.any():
            return

        excluded = df.loc[exclusion_mask, ["Env", "Obj", "Field"]].copy()
        excluded["Reason"] = df.loc[exclusion_mask].apply(exclusion_reason, axis=1)

        grouped = (
            excluded.groupby(["Env", "Obj", "Field", "Reason"], dropna=False)
            .size()
            .reset_index(name="Excluded Row Count")
        )

        for _, row in grouped.iterrows():
            key = (
                str(row["Env"]),
                str(row["Obj"]),
                str(row["Field"]),
                str(row["Reason"]),
            )
            excluded_audit_counts[key] = excluded_audit_counts.get(key, 0) + int(row["Excluded Row Count"])

    def remove_comparison_excluded_fields(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or not comparison_exclusion_specs:
            return df

        return df.loc[~comparison_exclusion_mask(df)].copy()

    def apply_value_cleanup(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        value_text = df["Value"].astype(str)
        cleanup_mask = value_text.str.contains(invalid_markup_pattern, na=False, regex=True)

        if not cleanup_mask.any():
            return df

        for idx, row in df.loc[cleanup_mask].iterrows():
            original_value = row["Value"]
            cleaned_value, changed = clean_markup_value(original_value)

            if not changed:
                continue

            df.at[idx, "Value"] = cleaned_value
            cleaned_value_rows.append(
                {
                    "Env": row["Env"],
                    "Obj": row["Obj"],
                    "RecordId": row["RecordId"],
                    "Field": row["Field"],
                    "Reason": "Removed HTML/CSS/Office markup from value",
                    "Original Value Sample": to_csv_value(str(original_value)[:1000]),
                    "Cleaned Value Sample": to_csv_value(str(cleaned_value)[:1000]),
                }
            )

        return df

    def load_partition(
        object_dir: Path | None,
        bucket: str,
        join_id_column: str,
    ) -> pd.DataFrame:
        df = read_partition_files(object_dir, bucket)
        df = df[required_columns].copy()

        for col in ["Env", "Obj", "RecordId", "Field", "External_Id__c"]:
            df[col] = normalize_text(df[col])

        df = apply_exclusions(df)
        df = apply_value_cleanup(df)
        audit_comparison_exclusions(df)
        df["Value"] = df["Value"].where(pd.notna(df["Value"]), None)
        df["JoinRecordId"] = df[join_id_column]

        return df

    def build_record_payload_lookup(
        df: pd.DataFrame,
        join_id_col: str,
        exclude_comparison_fields: bool = True,
    ):
        lookup = {}
        if exclude_comparison_fields:
            df = remove_comparison_excluded_fields(df)
        for (obj, join_id), group in df[df[join_id_col].notna()].groupby(["Obj", join_id_col]):
            payload = {row["Field"]: row["Value"] for _, row in group.iterrows() if row["Field"]}
            lookup[(obj, join_id)] = payload
        return lookup

    def build_load_only_payload(
        full_payload: Dict[str, Any],
        comparison_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not comparison_exclusion_specs:
            return {}

        return {
            field: value
            for field, value in full_payload.items()
            if field not in comparison_payload
        }

    def build_record_metadata_lookup(df: pd.DataFrame, join_id_col: str):
        lookup = {}
        for (obj, join_id), group in df[df[join_id_col].notna()].groupby(["Obj", join_id_col]):
            first = group.iloc[0]
            lookup[(obj, join_id)] = {
                "RecordId": first["RecordId"],
                "External_Id__c": first["External_Id__c"],
            }
        return lookup

    def build_field_diff(source_payload, target_payload):
        field_diff = {}
        source_update = {}
        target_current = {}

        all_fields = set(source_payload) | set(target_payload)

        for f in all_fields:
            s = source_payload.get(f)
            t = target_payload.get(f)

            if normalize_compare_value(s) == normalize_compare_value(t):
                continue

            field_diff[f] = {"from": t, "to": s}
            source_update[f] = s
            target_current[f] = t

        return field_diff, source_update, target_current

    source_env = load_manifest_env(source_root)
    target_env = load_manifest_env(target_root)
    target_recordid_col = f"{target_env.lower()}_recordid"

    record_output_columns = [
        "Obj",
        f"{source_env.lower()}_recordid",
        target_recordid_col,
        "External_Id__c",
        f"{source_env}_value",
        f"{target_env}_value",
        "field_diff",
        "change_type",
    ]
    source_load_value_col = f"{source_env}_load_value"
    target_load_value_col = f"{target_env}_load_value"
    record_checkpoint_columns = record_output_columns + [
        source_load_value_col,
        target_load_value_col,
    ]
    field_level_output_columns = [
        "Obj",
        "Field",
        f"{source_env.lower()}_recordid",
        target_recordid_col,
        "External_Id__c",
        f"{source_env}_value",
        f"{target_env}_value",
        "change_type",
    ]
    cleaned_values_columns = [
        "Env",
        "Obj",
        "RecordId",
        "Field",
        "Reason",
        "Original Value Sample",
        "Cleaned Value Sample",
    ]
    excluded_audit_columns = [
        "Env",
        "Obj",
        "Field",
        "Reason",
        "Excluded Row Count",
    ]

    output_path = Path(output_feather_path)
    checkpoint_root = (
        Path(diff_checkpoint_dir)
        if diff_checkpoint_dir
        else output_path.with_name(f"{output_path.stem}_diff_parts")
    )
    checkpoint_config = {
        "comparison_excluded_fields": list(comparison_exclusion_specs),
        "exclusions_csv_path": str(exclusions_csv_path or ""),
    }
    checkpoint_config_path = checkpoint_root / "_diff_config.json"

    if resume_from_checkpoint:
        checkpoint_root.mkdir(parents=True, exist_ok=True)
        completed_count = sum(1 for _ in checkpoint_root.glob("Obj=*/JoinBucket=*/_complete.json"))
        if completed_count and checkpoint_config_path.exists():
            previous_config = json.loads(checkpoint_config_path.read_text(encoding="utf-8"))
            if previous_config != checkpoint_config:
                raise ValueError(
                    "Diff checkpoint settings do not match the current function call. "
                    "Set resume_from_checkpoint=False to rebuild the diff with the new "
                    "comparison exclusions."
                )
        elif completed_count and comparison_exclusion_specs:
            raise ValueError(
                "Existing diff checkpoints were created before comparison_excluded_fields "
                "was tracked. Set resume_from_checkpoint=False to rebuild the diff with "
                "the new comparison exclusions."
            )

        if completed_count:
            print(f"Resume enabled for diff; found {completed_count} completed bucket checkpoint(s)")
        else:
            print("Resume enabled for diff; no completed bucket checkpoints found")
    else:
        if checkpoint_root.exists():
            shutil.rmtree(checkpoint_root)
        checkpoint_root.mkdir(parents=True, exist_ok=True)

    write_json_atomic(checkpoint_config_path, checkpoint_config)

    def bucket_checkpoint_dir(obj: str, bucket: str) -> Path:
        return (
            checkpoint_root
            / f"Obj={safe_path_part(obj)}"
            / f"JoinBucket={safe_path_part(bucket)}"
        )

    def bucket_complete_path(obj: str, bucket: str) -> Path:
        return bucket_checkpoint_dir(obj, bucket) / "_complete.json"

    def write_bucket_checkpoint(
        obj: str,
        bucket: str,
        record_rows: List[Dict[str, Any]],
        field_rows: List[Dict[str, Any]],
        excluded_counts_before: Dict[Tuple[str, str, str, str], int],
        cleaned_count_before: int,
    ) -> None:
        bucket_dir = bucket_checkpoint_dir(obj, bucket)
        if bucket_dir.exists():
            shutil.rmtree(bucket_dir)
        bucket_dir.mkdir(parents=True, exist_ok=True)

        record_df = coerce_output_columns(
            pd.DataFrame(record_rows, columns=record_checkpoint_columns),
            record_checkpoint_columns,
        )
        field_df = coerce_output_columns(
            pd.DataFrame(field_rows, columns=field_level_output_columns),
            field_level_output_columns,
        )

        write_parquet_atomic(record_df, bucket_dir / "record_diff.parquet")
        write_parquet_atomic(field_df, bucket_dir / "field_diff.parquet")

        excluded_rows: List[Dict[str, Any]] = []
        for key, after_count in excluded_audit_counts.items():
            before_count = excluded_counts_before.get(key, 0)
            delta_count = after_count - before_count
            if delta_count <= 0:
                continue

            env, audit_obj, field, reason = key
            excluded_rows.append(
                {
                    "Env": env,
                    "Obj": audit_obj,
                    "Field": field,
                    "Reason": reason,
                    "Excluded Row Count": delta_count,
                }
            )

        if excluded_rows:
            excluded_df = coerce_output_columns(
                pd.DataFrame(excluded_rows, columns=excluded_audit_columns),
                excluded_audit_columns,
            )
            write_parquet_atomic(excluded_df, bucket_dir / "excluded_audit.parquet")

        cleaned_rows = cleaned_value_rows[cleaned_count_before:]
        if cleaned_rows:
            cleaned_df = coerce_output_columns(
                pd.DataFrame(cleaned_rows, columns=cleaned_values_columns),
                cleaned_values_columns,
            )
            write_parquet_atomic(cleaned_df, bucket_dir / "cleaned_values.parquet")

        write_json_atomic(
            bucket_dir / "_complete.json",
            {
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "obj": obj,
                "bucket": bucket,
                "record_diff_rows": len(record_df),
                "field_diff_rows": len(field_df),
            },
        )

    def add_field_rows(
        field_rows: List[Dict[str, Any]],
        obj: str,
        source_recordid: Any,
        target_recordid: Any,
        external_id: Any,
        source_payload: Dict[str, Any],
        target_payload: Dict[str, Any],
        fields: List[str],
        change_type: str,
        include_null_only_fields: bool = True,
    ) -> None:
        for field in sorted(fields):
            source_value = source_payload.get(field)
            target_value = target_payload.get(field)

            if (
                not include_null_only_fields
                and is_missing_value(source_value)
                and is_missing_value(target_value)
            ):
                continue

            field_rows.append(
                {
                    "Obj": obj,
                    "Field": field,
                    f"{source_env.lower()}_recordid": to_csv_value(source_recordid),
                    target_recordid_col: to_csv_value(target_recordid),
                    "External_Id__c": to_csv_value(external_id),
                    f"{source_env}_value": to_csv_value(source_value),
                    f"{target_env}_value": to_csv_value(target_value),
                    "change_type": change_type,
                }
            )

    source_objects = list_objects(source_root)
    target_objects = list_objects(target_root)

    for obj in sorted(set(source_objects) | set(target_objects)):
        source_object_dir = source_objects.get(obj)
        target_object_dir = target_objects.get(obj)
        buckets = sorted(list_buckets(source_object_dir) | list_buckets(target_object_dir))

        print(f"\nDiffing {obj} ({len(buckets)} bucket(s))")

        for bucket in buckets:
            if resume_from_checkpoint and bucket_complete_path(obj, bucket).exists():
                print(f"Skipping completed diff bucket: {obj} / {bucket}")
                continue

            bucket_dir = bucket_checkpoint_dir(obj, bucket)
            if bucket_dir.exists():
                shutil.rmtree(bucket_dir)

            excluded_counts_before = dict(excluded_audit_counts)
            cleaned_count_before = len(cleaned_value_rows)
            record_rows: List[Dict[str, Any]] = []
            field_rows: List[Dict[str, Any]] = []

            source_df = load_partition(
                object_dir=source_object_dir,
                bucket=bucket,
                join_id_column="External_Id__c",
            )
            target_df = load_partition(
                object_dir=target_object_dir,
                bucket=bucket,
                join_id_column="RecordId",
            )

            source_payloads = build_record_payload_lookup(source_df, "JoinRecordId")
            target_payloads = build_record_payload_lookup(target_df, "JoinRecordId")
            source_full_payloads = build_record_payload_lookup(
                source_df,
                "JoinRecordId",
                exclude_comparison_fields=False,
            )
            target_full_payloads = build_record_payload_lookup(
                target_df,
                "JoinRecordId",
                exclude_comparison_fields=False,
            )

            source_meta = build_record_metadata_lookup(source_df, "JoinRecordId")
            target_meta = build_record_metadata_lookup(target_df, "JoinRecordId")

            all_keys = set(source_meta) | set(target_meta)

            for key_obj, join_id in sorted(all_keys):
                source_exists = (key_obj, join_id) in source_meta
                target_exists = (key_obj, join_id) in target_meta

                source_payload = source_payloads.get((key_obj, join_id), {})
                target_payload = target_payloads.get((key_obj, join_id), {})
                source_full_payload = source_full_payloads.get((key_obj, join_id), {})
                target_full_payload = target_full_payloads.get((key_obj, join_id), {})
                source_load_only_payload = build_load_only_payload(
                    source_full_payload,
                    source_payload,
                )
                target_load_only_payload = build_load_only_payload(
                    target_full_payload,
                    target_payload,
                )

                s_meta = source_meta.get((key_obj, join_id), {})
                t_meta = target_meta.get((key_obj, join_id), {})

                source_recordid = s_meta.get("RecordId", pd.NA)
                target_recordid = t_meta.get("RecordId", pd.NA)
                external_id = s_meta.get("External_Id__c", pd.NA)

                if source_exists and target_exists:
                    field_diff, s_update, t_update = build_field_diff(source_payload, target_payload)

                    if not field_diff:
                        continue

                    record_rows.append(
                        {
                            "Obj": key_obj,
                            f"{source_env.lower()}_recordid": source_recordid,
                            target_recordid_col: target_recordid,
                            "External_Id__c": external_id,
                            f"{source_env}_value": to_json_string(s_update),
                            f"{target_env}_value": to_json_string(t_update),
                            "field_diff": to_json_string(field_diff),
                            "change_type": "data_gap",
                            source_load_value_col: to_json_string(source_load_only_payload),
                            target_load_value_col: to_json_string(target_load_only_payload),
                        }
                    )
                    add_field_rows(
                        field_rows=field_rows,
                        obj=key_obj,
                        source_recordid=source_recordid,
                        target_recordid=target_recordid,
                        external_id=external_id,
                        source_payload=source_payload,
                        target_payload=target_payload,
                        fields=list(field_diff.keys()),
                        change_type="data_gap",
                    )

                elif source_exists and not target_exists:
                    record_rows.append(
                        {
                            "Obj": key_obj,
                            f"{source_env.lower()}_recordid": source_recordid,
                            target_recordid_col: pd.NA,
                            "External_Id__c": external_id,
                            f"{source_env}_value": to_json_string(source_payload),
                            f"{target_env}_value": to_json_string({}),
                            "field_diff": to_json_string(
                                {k: {"from": None, "to": v} for k, v in source_payload.items()}
                            ),
                            "change_type": f"missing_from_{target_env.lower()}",
                            source_load_value_col: to_json_string(source_load_only_payload),
                            target_load_value_col: to_json_string({}),
                        }
                    )
                    add_field_rows(
                        field_rows=field_rows,
                        obj=key_obj,
                        source_recordid=source_recordid,
                        target_recordid=pd.NA,
                        external_id=external_id,
                        source_payload=source_payload,
                        target_payload={},
                        fields=list(source_payload.keys()),
                        change_type=f"missing_from_{target_env.lower()}",
                        include_null_only_fields=False,
                    )

                elif not source_exists and target_exists:
                    record_rows.append(
                        {
                            "Obj": key_obj,
                            f"{source_env.lower()}_recordid": pd.NA,
                            target_recordid_col: target_recordid,
                            "External_Id__c": pd.NA,
                            f"{source_env}_value": to_json_string({}),
                            f"{target_env}_value": to_json_string(target_payload),
                            "field_diff": to_json_string(
                                {k: {"from": v, "to": None} for k, v in target_payload.items()}
                            ),
                            "change_type": f"missing_from_{source_env.lower()}",
                            source_load_value_col: to_json_string({}),
                            target_load_value_col: to_json_string(target_load_only_payload),
                        }
                    )
                    add_field_rows(
                        field_rows=field_rows,
                        obj=key_obj,
                        source_recordid=pd.NA,
                        target_recordid=target_recordid,
                        external_id=pd.NA,
                        source_payload={},
                        target_payload=target_payload,
                        fields=list(target_payload.keys()),
                        change_type=f"missing_from_{source_env.lower()}",
                        include_null_only_fields=False,
                    )

            write_bucket_checkpoint(
                obj=obj,
                bucket=bucket,
                record_rows=record_rows,
                field_rows=field_rows,
                excluded_counts_before=excluded_counts_before,
                cleaned_count_before=cleaned_count_before,
            )
            print(
                f"Diff checkpoint written: {obj} / {bucket} "
                f"({len(record_rows)} record rows, {len(field_rows)} field rows)"
            )

    completed_bucket_dirs = sorted(
        path.parent
        for path in checkpoint_root.glob("Obj=*/JoinBucket=*/_complete.json")
    )
    print(f"\nFinalizing diff outputs from {len(completed_bucket_dirs)} completed bucket checkpoint(s)")

    record_part_paths = [
        bucket_dir / "record_diff.parquet"
        for bucket_dir in completed_bucket_dirs
        if (bucket_dir / "record_diff.parquet").exists()
    ]
    field_part_paths = [
        bucket_dir / "field_diff.parquet"
        for bucket_dir in completed_bucket_dirs
        if (bucket_dir / "field_diff.parquet").exists()
    ]
    excluded_part_paths = [
        bucket_dir / "excluded_audit.parquet"
        for bucket_dir in completed_bucket_dirs
        if (bucket_dir / "excluded_audit.parquet").exists()
    ]
    cleaned_part_paths = [
        bucket_dir / "cleaned_values.parquet"
        for bucket_dir in completed_bucket_dirs
        if (bucket_dir / "cleaned_values.parquet").exists()
    ]

    record_diff_rows = parquet_row_count(record_part_paths)
    field_diff_rows = parquet_row_count(field_part_paths)

    print(f"Writing {output_path} from checkpoint parts")
    stream_parquet_parts_to_feather(
        part_paths=record_part_paths,
        path=output_path,
        columns=record_output_columns,
    )

    if output_csv:
        csv_path = output_path.with_suffix(".csv")
        print(f"Writing {csv_path} from checkpoint parts")
        stream_parquet_parts_to_csv(
            part_paths=record_part_paths,
            path=csv_path,
            columns=record_output_columns,
        )

        field_level_csv_path = output_path.with_name(f"{output_path.stem}_field_level.csv")
        print(f"Writing {field_level_csv_path} from checkpoint parts")
        stream_parquet_parts_to_csv(
            part_paths=field_part_paths,
            path=field_level_csv_path,
            columns=field_level_output_columns,
            atomic=False,
        )

    audit_path = output_path.with_name(f"{output_path.stem}_excluded_rows.csv")
    audit_df = summarize_excluded_audit_parts(excluded_part_paths)
    audit_df.to_csv(audit_path, index=False, encoding="utf-8-sig")

    cleaned_values_path = output_path.with_name(f"{output_path.stem}_cleaned_values.csv")
    stream_parquet_parts_to_csv(
        part_paths=cleaned_part_paths,
        path=cleaned_values_path,
        columns=cleaned_values_columns,
    )

    write_json_atomic(
        checkpoint_root / "_diff_manifest.json",
        {
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "source_extract_dir": str(source_root),
            "target_extract_dir": str(target_root),
            "output_feather_path": str(output_path),
            "comparison_excluded_fields": list(comparison_exclusion_specs),
            "completed_bucket_count": len(completed_bucket_dirs),
            "record_diff_rows": record_diff_rows,
            "field_diff_rows": field_diff_rows,
        },
    )

    if return_dataframe:
        return pd.read_feather(output_path)

    return pd.DataFrame(columns=record_output_columns)

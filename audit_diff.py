from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
import argparse
import csv
import json
import random
import re

import pandas as pd
import pyarrow.parquet as pq


def audit_diff_outputs(
    diff_checkpoint_dir: str = "diff_diff_parts",
    source_extract_dir: str = "mcuat_extract_parquet",
    target_extract_dir: str = "prod_extract_parquet",
    metadata_scope_csv_path: str = "metadata_scope.csv",
    output_prefix: str = "diff_audit",
    sample_per_group: int = 5,
    batch_size: int = 50000,
    scan_record_level: bool = True,
    scan_field_level: bool = True,
    sample_field_values: bool = True,
) -> None:
    """
    Create small audit CSVs from diff checkpoint Parquet files.

    This avoids opening huge files such as diff.csv and diff_field_level.csv.
    """

    checkpoint_root = Path(diff_checkpoint_dir)
    source_root = Path(source_extract_dir)
    target_root = Path(target_extract_dir)
    metadata_path = Path(metadata_scope_csv_path)

    if not checkpoint_root.exists():
        raise FileNotFoundError(f"Diff checkpoint directory not found: {checkpoint_root}")
    if not source_root.exists():
        raise FileNotFoundError(f"Source extract directory not found: {source_root}")
    if not target_root.exists():
        raise FileNotFoundError(f"Target extract directory not found: {target_root}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata scope CSV not found: {metadata_path}")

    source_env = read_manifest_env(source_root)
    target_env = read_manifest_env(target_root)

    record_columns = [
        "Obj",
        f"{source_env.lower()}_recordid",
        f"{target_env.lower()}_recordid",
        "External_Id__c",
        f"{source_env}_value",
        f"{target_env}_value",
        "field_diff",
        "change_type",
    ]
    field_columns = [
        "Obj",
        "Field",
        f"{source_env.lower()}_recordid",
        f"{target_env.lower()}_recordid",
        "External_Id__c",
        f"{source_env}_value",
        f"{target_env}_value",
        "change_type",
    ]

    completed_bucket_dirs = sorted(
        path.parent for path in checkpoint_root.glob("Obj=*/JoinBucket=*/_complete.json")
    )
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

    valid_objects = object_names_from_extract(source_root) | object_names_from_extract(target_root)
    valid_field_pairs = field_pairs_from_metadata(metadata_path)

    record_counts: Counter = Counter()
    invalid_object_counts: Counter = Counter()
    field_counts: Counter = Counter()
    invalid_field_pair_counts: Counter = Counter()
    audited_field_diff_rows = 0

    if scan_record_level:
        print(f"Auditing {len(record_part_paths)} record checkpoint part(s)")
        record_counts, invalid_object_counts, record_samples = audit_record_parts(
            part_paths=record_part_paths,
            columns=record_columns,
            valid_objects=valid_objects,
            sample_per_group=sample_per_group,
            batch_size=batch_size,
        )

        write_counter_csv(
            path=Path(f"{output_prefix}_object_change_counts.csv"),
            header=["Obj", "change_type", "row_count"],
            counter=record_counts,
        )
        write_counter_csv(
            path=Path(f"{output_prefix}_invalid_record_objects.csv"),
            header=["Obj", "row_count"],
            counter=invalid_object_counts,
        )
        write_sample_csv(
            path=Path(f"{output_prefix}_record_samples.csv"),
            columns=record_columns,
            samples=record_samples,
        )

    if scan_field_level:
        print(f"Auditing {len(field_part_paths)} field-level checkpoint part(s)")
        field_counts, invalid_field_pair_counts, audited_field_diff_rows = audit_field_parts(
            part_paths=field_part_paths,
            valid_objects=valid_objects,
            valid_field_pairs=valid_field_pairs,
            batch_size=batch_size,
        )
        write_counter_csv(
            path=Path(f"{output_prefix}_field_change_counts.csv"),
            header=["Obj", "Field", "change_type", "row_count"],
            counter=field_counts,
        )
        write_counter_csv(
            path=Path(f"{output_prefix}_invalid_field_pairs.csv"),
            header=["Obj", "Field", "row_count"],
            counter=invalid_field_pair_counts,
        )

    if sample_field_values:
        if not field_counts:
            counts_path = Path(f"{output_prefix}_field_change_counts.csv")
            if counts_path.exists():
                field_counts = read_field_change_counts(counts_path)

        if field_counts:
            print(f"Sampling field-level value deltas from {len(field_counts)} Obj/Field/change group(s)")
            field_value_samples = sample_field_value_parts(
                part_paths=field_part_paths,
                columns=field_columns,
                field_counts=field_counts,
                sample_per_group=sample_per_group,
                batch_size=batch_size,
            )
            write_field_value_sample_csv(
                path=Path(f"{output_prefix}_field_value_samples.csv"),
                columns=field_columns,
                samples=field_value_samples,
            )

    if not scan_record_level and not scan_field_level:
        return

    write_manifest_summary(
        path=Path(f"{output_prefix}_summary.csv"),
        checkpoint_root=checkpoint_root,
        completed_bucket_count=len(completed_bucket_dirs),
        record_part_count=len(record_part_paths),
        field_part_count=len(field_part_paths),
        record_counts=record_counts,
        invalid_object_counts=invalid_object_counts,
        field_counts=field_counts,
        invalid_field_pair_counts=invalid_field_pair_counts,
        audited_field_diff_rows=audited_field_diff_rows,
        scan_record_level=scan_record_level,
        scan_field_level=scan_field_level,
    )


def read_manifest_env(root: Path) -> str:
    manifest_path = root / "_extract_manifest.json"
    if not manifest_path.exists():
        return root.name

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return str(manifest.get("env") or root.name)


def should_ignore_row(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "t", "yes", "y", "1"}


def object_names_from_extract(root: Path) -> set[str]:
    objects: set[str] = set()

    for object_dir in root.glob("Obj=*"):
        if not object_dir.is_dir():
            continue

        complete_path = object_dir / "_complete.json"
        if complete_path.exists():
            try:
                payload = json.loads(complete_path.read_text(encoding="utf-8"))
                obj = str(payload.get("obj") or "").strip()
                if obj:
                    objects.add(obj)
                    continue
            except json.JSONDecodeError:
                pass

        objects.add(object_dir.name.removeprefix("Obj="))

    return objects


def field_pairs_from_metadata(path: Path) -> set[Tuple[str, str]]:
    pairs: set[Tuple[str, str]] = set()

    with path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        required_columns = {
            "SF 1.0 Object",
            "SF 1.0 Field",
            "SF 2.0 Object",
            "SF 2.0 Field",
        }
        missing_columns = required_columns - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                "metadata_scope.csv is missing required column(s): "
                + ", ".join(sorted(missing_columns))
            )

        for row in reader:
            if should_ignore_row(row.get("Ignore", "")):
                continue

            for object_col, field_col in (
                ("SF 1.0 Object", "SF 1.0 Field"),
                ("SF 2.0 Object", "SF 2.0 Field"),
            ):
                obj = str(row.get(object_col) or "").strip()
                field = str(row.get(field_col) or "").strip()
                if obj and field:
                    pairs.add((obj, field))

    return pairs


def is_valid_salesforce_api_name(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", text))


def iter_batch_dicts(
    part_paths: Iterable[Path],
    columns: List[str],
    batch_size: int,
):
    for part_index, part_path in enumerate(part_paths, start=1):
        if part_index % 100 == 0:
            print(f"  scanned {part_index} part(s)")

        parquet_file = pq.ParquetFile(part_path)
        for batch in parquet_file.iter_batches(columns=columns, batch_size=batch_size):
            yield batch.to_pydict()


def audit_record_parts(
    part_paths: List[Path],
    columns: List[str],
    valid_objects: set[str],
    sample_per_group: int,
    batch_size: int,
) -> Tuple[Counter, Counter, Dict[Tuple[str, str], List[Dict[str, Any]]]]:
    record_counts: Counter = Counter()
    invalid_object_counts: Counter = Counter()
    seen_by_group: Counter = Counter()
    samples: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    rng = random.Random(8451)

    for batch in iter_batch_dicts(part_paths, columns, batch_size):
        row_count = len(batch[columns[0]])

        for i in range(row_count):
            obj = normalize_cell(batch["Obj"][i])
            change_type = normalize_cell(batch["change_type"][i])
            group_key = (obj, change_type)

            record_counts[group_key] += 1

            if obj not in valid_objects or not is_valid_salesforce_api_name(obj):
                invalid_object_counts[(obj,)] += 1

            seen_by_group[group_key] += 1
            row = {col: batch[col][i] for col in columns}
            maybe_add_sample(
                samples=samples[group_key],
                row=row,
                seen_count=seen_by_group[group_key],
                sample_size=sample_per_group,
                rng=rng,
            )

    return record_counts, invalid_object_counts, samples


def audit_field_parts(
    part_paths: List[Path],
    valid_objects: set[str],
    valid_field_pairs: set[Tuple[str, str]],
    batch_size: int,
) -> Tuple[Counter, Counter, int]:
    columns = ["Obj", "Field", "change_type"]
    field_counts: Counter = Counter()
    invalid_field_pair_counts: Counter = Counter()
    valid_pair_index = pd.MultiIndex.from_tuples(
        sorted(valid_field_pairs),
        names=["Obj", "Field"],
    )
    audited_row_count = 0

    for part_index, part_path in enumerate(part_paths, start=1):
        if part_index % 25 == 0:
            print(f"  scanned {part_index} field-level part(s)")

        parquet_file = pq.ParquetFile(part_path)
        for batch in parquet_file.iter_batches(columns=columns, batch_size=batch_size):
            df = batch.to_pandas()
            if df.empty:
                continue

            for col in columns:
                df[col] = df[col].fillna("").astype(str)

            audited_row_count += len(df)

            grouped = df.groupby(columns, dropna=False).size()
            for key, count in grouped.items():
                field_counts[key] += int(count)

            obj_valid_mask = (
                df["Obj"].isin(valid_objects)
                & df["Obj"].str.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", na=False)
            )
            field_api_name = df["Field"].str.rsplit(".", n=1).str[-1]
            field_valid_mask = field_api_name.str.fullmatch(
                r"[A-Za-z][A-Za-z0-9_]*",
                na=False,
            )
            pair_valid_mask = pd.MultiIndex.from_frame(df[["Obj", "Field"]]).isin(
                valid_pair_index
            )
            invalid_mask = ~(obj_valid_mask & field_valid_mask & pair_valid_mask)

            if invalid_mask.any():
                invalid_grouped = df.loc[invalid_mask].groupby(["Obj", "Field"], dropna=False).size()
                for key, count in invalid_grouped.items():
                    invalid_field_pair_counts[key] += int(count)

    return field_counts, invalid_field_pair_counts, audited_row_count


def read_field_change_counts(path: Path) -> Counter:
    counts: Counter = Counter()

    with path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        required_columns = {"Obj", "Field", "change_type", "row_count"}
        missing_columns = required_columns - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                f"{path} is missing required column(s): {', '.join(sorted(missing_columns))}"
            )

        for row in reader:
            key = (
                normalize_cell(row.get("Obj")),
                normalize_cell(row.get("Field")),
                normalize_cell(row.get("change_type")),
            )
            counts[key] = int(row.get("row_count") or 0)

    return counts


def sample_field_value_parts(
    part_paths: List[Path],
    columns: List[str],
    field_counts: Counter,
    sample_per_group: int,
    batch_size: int,
) -> Dict[Tuple[str, str, str], List[Dict[str, Any]]]:
    samples: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    needed_counts = {
        key: min(sample_per_group, int(count))
        for key, count in field_counts.items()
        if int(count) > 0
    }
    group_columns = ["Obj", "Field", "change_type"]

    for part_index, part_path in enumerate(part_paths, start=1):
        if part_index % 25 == 0:
            print(f"  sampled through {part_index} field-level part(s)")

        if all(len(samples[key]) >= needed for key, needed in needed_counts.items()):
            break

        parquet_file = pq.ParquetFile(part_path)
        for batch in parquet_file.iter_batches(columns=columns, batch_size=batch_size):
            df = batch.to_pandas()
            if df.empty:
                continue

            for col in group_columns:
                df[col] = df[col].fillna("").astype(str)

            for key, group_df in df.groupby(group_columns, sort=False, dropna=False):
                needed = needed_counts.get(key, 0)
                current_count = len(samples[key])
                remaining = needed - current_count
                if remaining <= 0:
                    continue

                for row in group_df.head(remaining).to_dict("records"):
                    samples[key].append(
                        {
                            col: truncate_cell(row.get(col))
                            for col in columns
                        }
                    )

    return samples


def maybe_add_sample(
    samples: List[Dict[str, Any]],
    row: Dict[str, Any],
    seen_count: int,
    sample_size: int,
    rng: random.Random,
) -> None:
    if sample_size <= 0:
        return

    trimmed_row = {
        key: truncate_cell(value)
        for key, value in row.items()
    }

    if len(samples) < sample_size:
        samples.append(trimmed_row)
        return

    replacement_index = rng.randrange(seen_count)
    if replacement_index < sample_size:
        samples[replacement_index] = trimmed_row


def normalize_cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def truncate_cell(value: Any, max_length: int = 2000) -> str:
    text = normalize_cell(value)
    text = text.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")
    if len(text) <= max_length:
        return text
    return text[:max_length] + "...[truncated]"


def write_counter_csv(path: Path, header: List[str], counter: Counter) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(header)

        for key, count in sorted(counter.items()):
            writer.writerow(list(key) + [count])


def write_sample_csv(
    path: Path,
    columns: List[str],
    samples: Dict[Tuple[str, str], List[Dict[str, Any]]],
) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["sample_group", "sample_number"] + columns,
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
        )
        writer.writeheader()

        for group_key, rows in sorted(samples.items()):
            sample_group = " | ".join(group_key)
            for sample_number, row in enumerate(rows, start=1):
                writer.writerow(
                    {
                        "sample_group": sample_group,
                        "sample_number": sample_number,
                        **row,
                    }
                )


def write_field_value_sample_csv(
    path: Path,
    columns: List[str],
    samples: Dict[Tuple[str, str, str], List[Dict[str, Any]]],
) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["Obj", "Field", "change_type", "sample_number"] + [
                col for col in columns if col not in {"Obj", "Field", "change_type"}
            ],
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
        )
        writer.writeheader()

        for key, rows in sorted(samples.items()):
            obj, field, change_type = key
            for sample_number, row in enumerate(rows, start=1):
                writer.writerow(
                    {
                        "Obj": obj,
                        "Field": field,
                        "change_type": change_type,
                        "sample_number": sample_number,
                        **{
                            col: row.get(col, "")
                            for col in columns
                            if col not in {"Obj", "Field", "change_type"}
                        },
                    }
                )


def write_manifest_summary(
    path: Path,
    checkpoint_root: Path,
    completed_bucket_count: int,
    record_part_count: int,
    field_part_count: int,
    record_counts: Counter,
    invalid_object_counts: Counter,
    field_counts: Counter,
    invalid_field_pair_counts: Counter,
    audited_field_diff_rows: int,
    scan_record_level: bool,
    scan_field_level: bool,
) -> None:
    manifest_path = checkpoint_root / "_diff_manifest.json"
    manifest = {}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    summary_rows = [
        ("completed_bucket_count", completed_bucket_count),
        ("record_part_count", record_part_count),
        ("field_part_count", field_part_count),
        ("manifest_record_diff_rows", manifest.get("record_diff_rows", "")),
        ("manifest_field_diff_rows", manifest.get("field_diff_rows", "")),
        ("audited_record_diff_rows", sum(record_counts.values())),
        ("invalid_record_object_rows", sum(invalid_object_counts.values())),
        ("record_level_scan_completed", scan_record_level),
        ("audited_field_diff_rows", audited_field_diff_rows),
        ("invalid_field_pair_rows", sum(invalid_field_pair_counts.values())),
        ("unique_field_change_groups", len(field_counts)),
        ("field_level_scan_completed", scan_field_level),
    ]

    with path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["metric", "value"])
        writer.writerows(summary_rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create small audit CSVs from diff checkpoint outputs.")
    parser.add_argument("--diff-checkpoint-dir", default="diff_diff_parts")
    parser.add_argument("--source-extract-dir", default="mcuat_extract_parquet")
    parser.add_argument("--target-extract-dir", default="prod_extract_parquet")
    parser.add_argument("--metadata-scope-csv-path", default="metadata_scope.csv")
    parser.add_argument("--output-prefix", default="diff_audit")
    parser.add_argument("--sample-per-group", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--skip-field-level", action="store_true")
    parser.add_argument("--field-level-only", action="store_true")
    parser.add_argument("--skip-field-value-samples", action="store_true")
    parser.add_argument("--field-value-samples-only", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    audit_diff_outputs(
        diff_checkpoint_dir=args.diff_checkpoint_dir,
        source_extract_dir=args.source_extract_dir,
        target_extract_dir=args.target_extract_dir,
        metadata_scope_csv_path=args.metadata_scope_csv_path,
        output_prefix=args.output_prefix,
        sample_per_group=args.sample_per_group,
        batch_size=args.batch_size,
        scan_record_level=not args.field_level_only and not args.field_value_samples_only,
        scan_field_level=not args.skip_field_level and not args.field_value_samples_only,
        sample_field_values=not args.skip_field_value_samples,
    )

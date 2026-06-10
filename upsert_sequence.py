from __future__ import annotations

from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import csv

import pandas as pd

from auth import get_salesforce_connection


def generate_upsert_sequence_csv(
    target_env: str,
    metadata_scope_csv_path: str,
    output_csv_path: str = "upsert_sequence.csv",
    object_column: str = "SF 2.0 Object",
    field_column: str = "SF 2.0 Field",
    include_only_metadata_fields: bool = True,
    base_order_start: int = 10,
    base_order_step: int = 10,
) -> pd.DataFrame:
    """
    Generate a reviewable object load sequence for future Salesforce upserts.

    This function does not create/update Salesforce records. It reads the target
    objects from metadata_scope.csv, describes those objects in the target org,
    detects lookup/master-detail dependencies between scoped objects, and writes
    a starter sequence CSV that can be manually reviewed before load work begins.
    """

    metadata_path = Path(metadata_scope_csv_path)
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata scope CSV not found: {metadata_path}")

    metadata_df = pd.read_csv(metadata_path, dtype=str, encoding="utf-8-sig").fillna("")
    required_columns = {object_column, field_column}
    missing_columns = required_columns - set(metadata_df.columns)
    if missing_columns:
        raise ValueError(
            f"{metadata_path} is missing required column(s): {', '.join(sorted(missing_columns))}"
        )

    scoped_fields_by_object = build_scoped_fields_by_object(
        metadata_df=metadata_df,
        object_column=object_column,
        field_column=field_column,
    )
    scoped_objects = set(scoped_fields_by_object)

    print(f"Connecting to Salesforce env [{target_env}] for upsert sequence metadata")
    sf = get_salesforce_connection(env=target_env)

    describe_by_object: Dict[str, Optional[Dict[str, Any]]] = {}
    field_defs_by_object: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for obj in sorted(scoped_objects):
        try:
            print(f"Describing {target_env}.{obj}")
            describe_result = getattr(sf, obj).describe()
            describe_by_object[obj] = describe_result
            field_defs_by_object[obj] = {
                field_def.get("name"): field_def
                for field_def in describe_result.get("fields", [])
                if field_def.get("name")
            }
        except Exception as exc:
            print(f"Could not describe {target_env}.{obj}: {exc}")
            describe_by_object[obj] = None
            field_defs_by_object[obj] = {}

    dependency_rows = build_dependency_rows(
        scoped_fields_by_object=scoped_fields_by_object,
        scoped_objects=scoped_objects,
        field_defs_by_object=field_defs_by_object,
        include_only_metadata_fields=include_only_metadata_fields,
    )
    dependency_objects_by_object = {
        obj: {
            row["dependency_object"]
            for row in rows
            if row["dependency_object"] != obj
        }
        for obj, rows in dependency_rows.items()
    }

    ordered_objects, cycle_objects = topological_object_order(
        objects=scoped_objects,
        dependency_objects_by_object=dependency_objects_by_object,
    )

    sequence_rows: List[Dict[str, Any]] = []
    order_by_object = {
        obj: base_order_start + index * base_order_step
        for index, obj in enumerate(ordered_objects)
    }

    for obj in ordered_objects:
        dependencies = sorted(dependency_objects_by_object.get(obj, set()))
        rows_for_object = dependency_rows.get(obj, [])
        self_dependency_fields = sorted(
            {
                row["field"]
                for row in rows_for_object
                if row["dependency_object"] == obj
            }
        )
        external_dependencies = sorted(
            {
                f"{row['field']} -> {row['dependency_object']}"
                for row in rows_for_object
                if row["dependency_object"] not in scoped_objects
            }
        )

        describe_status = "described" if describe_by_object.get(obj) else "describe_failed"
        dependency_status = dependency_review_status(
            obj=obj,
            dependencies=dependencies,
            self_dependency_fields=self_dependency_fields,
            external_dependencies=external_dependencies,
            cycle_objects=cycle_objects,
            describe_status=describe_status,
        )

        sequence_rows.append(
            {
                "Load_Order": order_by_object[obj],
                "Object": obj,
                "Enabled": "TRUE",
                "Manual_Override_Order": "",
                "Dependency_Objects": join_list(dependencies),
                "Dependency_Fields": join_list(
                    sorted(
                        {
                            f"{row['field']} -> {row['dependency_object']}"
                            for row in rows_for_object
                            if row["dependency_object"] in scoped_objects
                            and row["dependency_object"] != obj
                        }
                    )
                ),
                "Self_Dependency_Fields": join_list(self_dependency_fields),
                "External_Dependency_Fields": join_list(external_dependencies),
                "Dependency_Status": dependency_status,
                "Reason": build_reason(
                    dependencies=dependencies,
                    self_dependency_fields=self_dependency_fields,
                    external_dependencies=external_dependencies,
                    describe_status=describe_status,
                ),
                "Notes": "",
            }
        )

    sequence_df = pd.DataFrame(sequence_rows, columns=output_columns())
    sequence_df = sequence_df.sort_values(["Load_Order", "Object"], kind="stable").reset_index(drop=True)
    sequence_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)

    print(f"Upsert sequence written: {output_csv_path} ({len(sequence_df)} object rows)")
    return sequence_df


def should_ignore_row(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "t", "yes", "y", "1"}


def normalize_metadata_cell(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>", "#n/a", "n/a", "na"}:
        return None
    return text


def build_scoped_fields_by_object(
    metadata_df: pd.DataFrame,
    object_column: str,
    field_column: str,
) -> Dict[str, Set[str]]:
    working_df = metadata_df.copy()
    if "Ignore" in working_df.columns:
        working_df = working_df.loc[
            ~working_df["Ignore"].fillna("").map(should_ignore_row)
        ].copy()

    scoped_fields_by_object: Dict[str, Set[str]] = defaultdict(set)

    for _, row in working_df.iterrows():
        if {"SF 1.0 Object", "SF 1.0 Field"}.issubset(working_df.columns):
            source_object = normalize_metadata_cell(row.get("SF 1.0 Object"))
            source_field = normalize_metadata_cell(row.get("SF 1.0 Field"))
            if not source_object or not source_field:
                continue

        obj = normalize_metadata_cell(row.get(object_column))
        field = normalize_metadata_cell(row.get(field_column))

        if not obj or not field:
            continue

        scoped_fields_by_object[obj].add(field)

    return dict(scoped_fields_by_object)


def build_dependency_rows(
    scoped_fields_by_object: Dict[str, Set[str]],
    scoped_objects: Set[str],
    field_defs_by_object: Dict[str, Dict[str, Dict[str, Any]]],
    include_only_metadata_fields: bool,
) -> Dict[str, List[Dict[str, str]]]:
    dependency_rows: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for obj, field_defs in field_defs_by_object.items():
        metadata_fields = scoped_fields_by_object.get(obj, set())
        relationship_field_defs = {
            str(field_def.get("relationshipName")).strip(): field_def
            for field_def in field_defs.values()
            if field_def.get("relationshipName")
        }
        field_names: Iterable[str]

        if include_only_metadata_fields:
            field_names = metadata_fields
        else:
            field_names = field_defs.keys()

        for field in sorted(field_names):
            direct_field_name = field.split(".")[0]
            field_def = field_defs.get(direct_field_name) or relationship_field_defs.get(direct_field_name)
            if not field_def:
                continue

            if field_def.get("type") != "reference":
                continue

            reference_targets = [
                str(reference_to).strip()
                for reference_to in field_def.get("referenceTo", [])
                if str(reference_to).strip()
            ]

            for dependency_object in reference_targets:
                dependency_rows[obj].append(
                    {
                        "object": obj,
                        "field": field_def.get("name") or direct_field_name,
                        "dependency_object": dependency_object,
                        "dependency_scope": (
                            "scoped" if dependency_object in scoped_objects else "external"
                        ),
                    }
                )

    return dict(dependency_rows)


def topological_object_order(
    objects: Set[str],
    dependency_objects_by_object: Dict[str, Set[str]],
) -> Tuple[List[str], Set[str]]:
    scoped_dependencies = {
        obj: set(dependency_objects_by_object.get(obj, set())) & objects
        for obj in objects
    }

    dependents_by_object: Dict[str, Set[str]] = defaultdict(set)
    in_degree = {obj: 0 for obj in objects}

    for obj, dependencies in scoped_dependencies.items():
        in_degree[obj] = len(dependencies)
        for dependency in dependencies:
            dependents_by_object[dependency].add(obj)

    queue = deque(sorted(obj for obj, count in in_degree.items() if count == 0))
    ordered: List[str] = []

    while queue:
        obj = queue.popleft()
        ordered.append(obj)

        for dependent in sorted(dependents_by_object.get(obj, set())):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    cycle_objects = set(objects) - set(ordered)
    ordered.extend(sorted(cycle_objects))

    return ordered, cycle_objects


def dependency_review_status(
    obj: str,
    dependencies: List[str],
    self_dependency_fields: List[str],
    external_dependencies: List[str],
    cycle_objects: Set[str],
    describe_status: str,
) -> str:
    statuses: List[str] = []

    if describe_status != "described":
        statuses.append("DESCRIBE_FAILED")
    if obj in cycle_objects:
        statuses.append("MANUAL_REVIEW_CYCLE")
    if self_dependency_fields:
        statuses.append("MANUAL_REVIEW_SELF_REFERENCE")
    if external_dependencies:
        statuses.append("HAS_EXTERNAL_DEPENDENCIES")
    if dependencies and not statuses:
        statuses.append("ORDERED_AFTER_DEPENDENCIES")
    if not dependencies and not statuses:
        statuses.append("ROOT")

    return "; ".join(statuses)


def build_reason(
    dependencies: List[str],
    self_dependency_fields: List[str],
    external_dependencies: List[str],
    describe_status: str,
) -> str:
    reasons: List[str] = []

    if describe_status != "described":
        reasons.append("Object describe failed; review manually.")
    if dependencies:
        reasons.append(f"Depends on scoped object(s): {join_list(dependencies)}.")
    else:
        reasons.append("No scoped object dependencies detected.")
    if self_dependency_fields:
        reasons.append(f"Self-reference field(s): {join_list(self_dependency_fields)}.")
    if external_dependencies:
        reasons.append(f"External reference field(s): {join_list(external_dependencies)}.")

    return " ".join(reasons)


def join_list(values: Iterable[str]) -> str:
    return "; ".join(str(value) for value in values if str(value).strip())


def output_columns() -> List[str]:
    return [
        "Load_Order",
        "Object",
        "Enabled",
        "Manual_Override_Order",
        "Dependency_Objects",
        "Dependency_Fields",
        "Self_Dependency_Fields",
        "External_Dependency_Fields",
        "Dependency_Status",
        "Reason",
        "Notes",
    ]

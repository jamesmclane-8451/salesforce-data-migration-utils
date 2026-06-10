from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote
import ast
import csv
import json
import re

import pandas as pd
import pyarrow.parquet as pq

from auth import get_salesforce_connection


LOAD_STEP_CONFIGS = {
    1: {
        "name": "DRY RUN SAMPLE",
        "dry_run": True,
        "sample_size": None,
        "sample_size_per_object": 5,
        "dependency_aware_sample": True,
        "field_coverage_sample": False,
        "record_noop_skips": False,
    },
    2: {
        "name": "LIVE FIELD-COVERAGE SAMPLE",
        "dry_run": False,
        "sample_size": None,
        "sample_size_per_object": None,
        "dependency_aware_sample": True,
        "field_coverage_sample": True,
        "record_noop_skips": False,
    },
    3: {
        "name": "FULL LIVE LOAD",
        "dry_run": False,
        "sample_size": None,
        "sample_size_per_object": None,
        "dependency_aware_sample": False,
        "field_coverage_sample": False,
        "record_noop_skips": False,
    },
}

def load_salesforce_diff_to_target(
    target_env: str,
    source_env: str,
    diff_checkpoint_dir: str,
    metadata_scope_csv_path: str,
    upsert_sequence_csv_path: str,
    object_source_policy_csv_path: Optional[str] = "migration_object_source_policy.csv",
    target_extract_dir: Optional[str] = None,
    load_step: int = 1,
    results_csv_path: str = "load_results.csv",
    sample_size: Optional[int] = None,
    sample_size_per_object: Optional[int] = None,
    dry_run: Optional[bool] = None,
    object_filter: Optional[List[str]] = None,
    change_type_filter: Optional[List[str]] = None,
    resolve_relationships: bool = True,
    relationship_resolution_fallback_to_salesforce: bool = True,
    dependency_aware_sample: Optional[bool] = None,
    field_coverage_sample: Optional[bool] = None,
    record_noop_skips: Optional[bool] = None,
    resume_from_results: bool = False,
    use_bulk_api: Optional[bool] = None,
    bulk_batch_size: int = 500,
    bulk_use_serial: bool = True,
    batch_size: int = 50000,
) -> pd.DataFrame:
    """
    Create/update target Salesforce records from diff output using source values.

    This loader is intentionally dry-run first. It reads record-level diff
    checkpoint Parquet files, uses metadata_scope.csv to map PROD/SF 1.0 fields
    to MCUAT/SF 2.0 fields, follows upsert_sequence.csv order, and writes a
    result CSV with one row per attempted operation.
    """

    checkpoint_root = Path(diff_checkpoint_dir)
    metadata_path = Path(metadata_scope_csv_path)
    sequence_path = Path(upsert_sequence_csv_path)
    object_source_policy_path = (
        Path(object_source_policy_csv_path)
        if object_source_policy_csv_path
        else None
    )
    target_extract_root = Path(target_extract_dir) if target_extract_dir else None
    step_config = resolve_load_step_config(
        load_step=load_step,
        sample_size=sample_size,
        sample_size_per_object=sample_size_per_object,
        dry_run=dry_run,
        dependency_aware_sample=dependency_aware_sample,
        field_coverage_sample=field_coverage_sample,
        record_noop_skips=record_noop_skips,
    )
    sample_size = step_config["sample_size"]
    sample_size_per_object = step_config["sample_size_per_object"]
    dry_run = step_config["dry_run"]
    dependency_aware_sample = step_config["dependency_aware_sample"]
    field_coverage_sample = step_config["field_coverage_sample"]
    record_noop_skips = step_config["record_noop_skips"]
    if use_bulk_api is None:
        use_bulk_api = load_step == 3 and not dry_run
    use_bulk_api = bool(use_bulk_api and not dry_run)

    if not checkpoint_root.exists():
        raise FileNotFoundError(f"Diff checkpoint directory not found: {checkpoint_root}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata scope CSV not found: {metadata_path}")
    if not sequence_path.exists():
        raise FileNotFoundError(f"Upsert sequence CSV not found: {sequence_path}")
    if target_extract_root is not None and not target_extract_root.exists():
        raise FileNotFoundError(f"Target extract directory not found: {target_extract_root}")

    change_types = set(change_type_filter or ["data_gap", f"missing_from_{target_env.lower()}"])
    object_filter_set = set(object_filter or [])

    source_record_id_col = f"{source_env.lower()}_recordid"
    target_record_id_col = f"{target_env.lower()}_recordid"
    source_value_col = f"{source_env}_value"
    source_load_value_col = f"{source_env}_load_value"
    diff_columns = [
        "Obj",
        source_record_id_col,
        target_record_id_col,
        "External_Id__c",
        source_value_col,
        "change_type",
    ]

    field_mappings, source_objects_by_target = build_prod_to_target_field_mappings(metadata_path)
    load_objects = read_upsert_sequence(sequence_path, object_filter_set)
    load_objects = apply_object_source_policy(
        load_objects=load_objects,
        source_env=source_env,
        policy_path=object_source_policy_path,
    )

    print(f"Connecting to Salesforce env [{target_env}] for load metadata")
    sf = get_salesforce_connection(env=target_env)
    describe_cache: Dict[str, Dict[str, Any]] = {}
    external_id_lookup_cache: Dict[Tuple[str, str], Optional[str]] = {}
    extract_external_id_lookup_cache: Dict[str, Dict[str, str]] = {}
    current_load_record_ids: Dict[Tuple[str, str], str] = {}
    opportunity_contact_role_cache: Dict[str, Any] = {
        "loaded_opportunity_ids": set(),
        "roles_by_pair": {},
        "roles_by_id": {},
    }
    processed_load_keys: Set[Tuple[str, str]] = set()
    dependency_load_stack: Set[Tuple[str, str]] = set()
    load_object_set = set(load_objects)

    sample_source_ids: Dict[str, Set[str]] = {}
    if field_coverage_sample:
        print("\nPreparing field-coverage sample records")
        sample_source_ids = build_field_coverage_sample_source_ids(
            sf=sf,
            checkpoint_root=checkpoint_root,
            load_objects=load_objects,
            source_objects_by_target=source_objects_by_target,
            field_mappings=field_mappings,
            source_record_id_col=source_record_id_col,
            target_record_id_col=target_record_id_col,
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            change_types=change_types,
            describe_cache=describe_cache,
            batch_size=batch_size,
        )
        if dependency_aware_sample:
            expand_sample_source_ids_with_dependencies(
                sample_source_ids=sample_source_ids,
                sf=sf,
                checkpoint_root=checkpoint_root,
                load_objects=load_objects,
                source_objects_by_target=source_objects_by_target,
                field_mappings=field_mappings,
                source_record_id_col=source_record_id_col,
                target_record_id_col=target_record_id_col,
                source_value_col=source_value_col,
                source_load_value_col=source_load_value_col,
                change_types=change_types,
                target_extract_root=target_extract_root,
                describe_cache=describe_cache,
                extract_lookup_cache=extract_external_id_lookup_cache,
                batch_size=batch_size,
            )
        coverage_summary = ", ".join(
            f"{obj}={len(source_ids)}"
            for obj, source_ids in sorted(sample_source_ids.items())
            if source_ids
        )
        print(f"Field-coverage sample records: {coverage_summary or 'none'}")
    elif dependency_aware_sample and sample_size_per_object is not None:
        print("\nPreparing dependency-aware sample records")
        sample_source_ids = build_dependency_sample_source_ids(
            sf=sf,
            checkpoint_root=checkpoint_root,
            load_objects=load_objects,
            source_objects_by_target=source_objects_by_target,
            field_mappings=field_mappings,
            source_record_id_col=source_record_id_col,
            target_record_id_col=target_record_id_col,
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            change_types=change_types,
            sample_size_per_object=sample_size_per_object,
            target_extract_root=target_extract_root,
            describe_cache=describe_cache,
            extract_lookup_cache=extract_external_id_lookup_cache,
            batch_size=batch_size,
        )
        dependency_summary = ", ".join(
            f"{obj}={len(source_ids)}"
            for obj, source_ids in sorted(sample_source_ids.items())
            if source_ids
        )
        print(f"Dependency sample additions: {dependency_summary or 'none'}")

    result_rows = LoadResultRows(
        results_csv_path=results_csv_path,
        keep_rows=load_step != 3,
        resume_from_existing=resume_from_results,
    )
    if result_rows.processed_load_keys:
        processed_load_keys.update(result_rows.processed_load_keys)
        print(
            f"Resuming from existing load results: "
            f"{len(result_rows.processed_load_keys)} previously attempted record(s) will be skipped"
        )
    bulk_buffer = (
        BulkOperationBuffer(
            sf=sf,
            target_env=target_env,
            source_env=source_env,
            result_rows=result_rows,
            current_load_record_ids=current_load_record_ids,
            bulk_batch_size=bulk_batch_size,
            bulk_use_serial=bulk_use_serial,
        )
        if use_bulk_api
        else None
    )
    total_attempted = 0

    print(
        f"\nStarting load step {load_step}: {step_config['name']} "
        f"into {target_env} "
        f"from {source_env} values"
    )
    if use_bulk_api:
        print(
            f"Bulk API mode enabled: batch_size={bulk_batch_size}, "
            f"use_serial={bulk_use_serial}"
        )
    if sample_size is not None:
        print(f"Sample size limit: {sample_size} total operation(s)")
    if sample_size_per_object is not None and not field_coverage_sample:
        print(f"Sample size per object limit: {sample_size_per_object}")
    if field_coverage_sample:
        print("Step 2 sample mode: cover every non-ignored mapped field that appears in diff data")
    print_load_sequence(load_objects)

    def handle_bulk_success_post_actions(
        operation_row: Dict[str, Any],
        result_row: Dict[str, Any],
    ) -> None:
        if operation_row.get("target_object") == "OpportunityContactRole":
            remember_opportunity_contact_role_cache_from_operation(
                operation_row=operation_row,
                result_row=result_row,
                opportunity_contact_role_cache=opportunity_contact_role_cache,
            )

        for action in operation_row.get("post_success_actions", []):
            if action.get("type") != "sync_opportunity_contact_role":
                continue

            contact_role_result = sync_opportunity_contact_role_from_opportunity_contact_id(
                sf=sf,
                target_env=target_env,
                source_env=source_env,
                source_opportunity_id=action.get("source_opportunity_id"),
                target_opportunity_id=(
                    normalize_blank(result_row.get("Target_RecordId"))
                    or normalize_blank(operation_row.get("target_record_id"))
                ),
                source_contact_id=action.get("source_contact_id"),
                change_type=operation_row.get("change_type") or "",
                dry_run=dry_run,
                target_extract_root=target_extract_root,
                extract_lookup_cache=extract_external_id_lookup_cache,
                salesforce_lookup_cache=external_id_lookup_cache,
                current_load_record_ids=current_load_record_ids,
                describe_cache=describe_cache,
                checkpoint_root=checkpoint_root,
                source_objects_by_target=source_objects_by_target,
                field_mappings=field_mappings,
                load_object_set=load_object_set,
                source_record_id_col=source_record_id_col,
                target_record_id_col=target_record_id_col,
                source_value_col=source_value_col,
                source_load_value_col=source_load_value_col,
                change_types=change_types,
                processed_load_keys=processed_load_keys,
                dependency_load_stack=dependency_load_stack,
                result_rows=result_rows,
                fallback_to_salesforce=relationship_resolution_fallback_to_salesforce,
                record_noop_skips=record_noop_skips,
                batch_size=batch_size,
                bulk_buffer=bulk_buffer,
                opportunity_contact_role_cache=opportunity_contact_role_cache,
            )
            if contact_role_result is not None:
                result_rows.append(contact_role_result)

    if bulk_buffer is not None:
        bulk_buffer.after_success = handle_bulk_success_post_actions

    for object_index, target_object in enumerate(load_objects, start=1):
        if sample_size is not None and total_attempted >= sample_size:
            break

        source_objects = sorted(source_objects_by_target.get(target_object, {target_object}))
        selected_source_ids = sample_source_ids.get(target_object, set())
        object_attempted = 0
        object_skipped_already_attempted = 0
        object_attempted_by_change_type: Counter[str] = Counter()

        print(
            f"\n=== Object {object_index}/{len(load_objects)}: "
            f"{target_env}.{target_object} ==="
        )
        print(f"Source diff object(s): {', '.join(source_objects)}")
        print("Status: preparing target metadata")
        describe = get_object_describe(sf, target_object, describe_cache)
        field_defs = {
            field_def["name"]: field_def
            for field_def in describe.get("fields", [])
            if field_def.get("name")
        }
        relationship_field_defs = {
            str(field_def.get("relationshipName")).strip(): field_def
            for field_def in field_defs.values()
            if field_def.get("relationshipName")
        }
        target_external_id_field = field_defs.get("External_Id__c")

        print("Status: scanning diff records")
        if field_coverage_sample:
            print(f"Field-coverage sample record(s): {len(selected_source_ids)}")
            if not selected_source_ids:
                print(f"Finished {target_object}: no selected sample records")
                continue

        for part_path in iter_record_diff_part_paths(checkpoint_root, source_objects):
            if sample_size is not None and total_attempted >= sample_size:
                break
            selected_pending = has_pending_source_ids(
                target_object=target_object,
                selected_source_ids=selected_source_ids,
                processed_load_keys=processed_load_keys,
            )
            if field_coverage_sample and not selected_pending:
                break
            if (
                not field_coverage_sample
                and sample_size_per_object is not None
                and object_attempted >= sample_size_per_object
                and not selected_pending
            ):
                break

            parquet_file = pq.ParquetFile(part_path)
            available_columns = set(parquet_file.schema_arrow.names)
            missing_diff_columns = set(diff_columns) - available_columns
            if missing_diff_columns:
                raise ValueError(
                    f"{part_path} is missing required load column(s): "
                    f"{', '.join(sorted(missing_diff_columns))}"
                )

            batch_columns = list(diff_columns)
            if source_load_value_col in available_columns:
                batch_columns.append(source_load_value_col)

            for batch in parquet_file.iter_batches(columns=batch_columns, batch_size=batch_size):
                if sample_size is not None and total_attempted >= sample_size:
                    break
                selected_pending = has_pending_source_ids(
                    target_object=target_object,
                    selected_source_ids=selected_source_ids,
                    processed_load_keys=processed_load_keys,
                )
                if field_coverage_sample and not selected_pending:
                    break
                if (
                    not field_coverage_sample
                    and sample_size_per_object is not None
                    and object_attempted >= sample_size_per_object
                    and not selected_pending
                ):
                    break

                batch_rows = batch.to_pydict()
                row_count = len(batch_rows["Obj"])
                if target_object == "OpportunityContactRole":
                    preload_opportunity_contact_role_natural_keys_for_batch(
                        sf=sf,
                        target_object=target_object,
                        batch_rows=batch_rows,
                        row_count=row_count,
                        source_record_id_col=source_record_id_col,
                        target_record_id_col=target_record_id_col,
                        source_value_col=source_value_col,
                        source_load_value_col=source_load_value_col,
                        change_types=change_types,
                        field_mappings=field_mappings,
                        field_defs=field_defs,
                        relationship_field_defs=relationship_field_defs,
                        target_external_id_field=target_external_id_field,
                        processed_load_keys=processed_load_keys,
                        target_extract_root=target_extract_root,
                        extract_lookup_cache=extract_external_id_lookup_cache,
                        salesforce_lookup_cache=external_id_lookup_cache,
                        current_load_record_ids=current_load_record_ids,
                        describe_cache=describe_cache,
                        fallback_to_salesforce=relationship_resolution_fallback_to_salesforce,
                        opportunity_contact_role_cache=opportunity_contact_role_cache,
                        extra_field_names=[
                            field_name
                            for field_name in ("Role", "Opportunity_Contact_Role_Id__c")
                            if field_name in field_defs
                        ],
                    )

                for row_index in range(row_count):
                    if sample_size is not None and total_attempted >= sample_size:
                        break
                    selected_pending = has_pending_source_ids(
                        target_object=target_object,
                        selected_source_ids=selected_source_ids,
                        processed_load_keys=processed_load_keys,
                    )
                    if field_coverage_sample and not selected_pending:
                        break
                    if (
                        not field_coverage_sample
                        and sample_size_per_object is not None
                        and object_attempted >= sample_size_per_object
                        and not selected_pending
                    ):
                        break

                    change_type = normalize_blank(batch_rows["change_type"][row_index])
                    if change_type not in change_types:
                        continue
                    if (
                        not field_coverage_sample
                        and sample_size_per_object is not None
                        and object_attempted >= sample_size_per_object
                    ):
                        break

                    source_object = normalize_blank(batch_rows["Obj"][row_index])
                    source_record_id = normalize_blank(batch_rows[source_record_id_col][row_index])
                    target_record_id = normalize_blank(batch_rows[target_record_id_col][row_index])
                    if not source_record_id:
                        continue

                    load_key = (target_object, source_record_id)
                    if load_key in processed_load_keys:
                        object_skipped_already_attempted += 1
                        if (
                            target_object == "Opportunity"
                            and ("OpportunityContactRole", source_record_id) not in processed_load_keys
                        ):
                            source_payload = parse_json_dict(batch_rows[source_value_col][row_index])
                            source_load_payload = (
                                parse_json_dict(batch_rows[source_load_value_col][row_index])
                                if source_load_value_col in batch_rows
                                else {}
                            )
                            source_payload = merge_load_payload(
                                source_payload=source_payload,
                                source_load_payload=source_load_payload,
                            )
                            resume_operation = resolve_operation(
                                target_record_id=target_record_id,
                                source_record_id=source_record_id,
                                target_external_id_field=target_external_id_field,
                            )
                            resume_operation_hint = operation_hint_for_operation(
                                operation=resume_operation,
                                target_record_id=target_record_id,
                            )
                            resume_payload, resume_skipped_fields = map_source_payload_to_target(
                                source_object=source_object,
                                source_payload=source_payload,
                                target_object=target_object,
                                field_mappings=field_mappings,
                                field_defs=field_defs,
                                relationship_field_defs=relationship_field_defs,
                                operation_hint=resume_operation_hint,
                            )
                            routed_resume_contact_source_id = get_routed_opportunity_contact_source_id(
                                target_object=target_object,
                                payload=resume_payload,
                            )
                            if routed_resume_contact_source_id:
                                print(
                                    "Resume recovery: syncing missing OpportunityContactRole "
                                    f"for {source_record_id}"
                                )
                                contact_role_result = sync_opportunity_contact_role_from_opportunity_contact_id(
                                    sf=sf,
                                    target_env=target_env,
                                    source_env=source_env,
                                    source_opportunity_id=source_record_id,
                                    target_opportunity_id=target_record_id,
                                    source_contact_id=routed_resume_contact_source_id,
                                    change_type=change_type or "",
                                    dry_run=dry_run,
                                    target_extract_root=target_extract_root,
                                    extract_lookup_cache=extract_external_id_lookup_cache,
                                    salesforce_lookup_cache=external_id_lookup_cache,
                                    current_load_record_ids=current_load_record_ids,
                                    describe_cache=describe_cache,
                                    checkpoint_root=checkpoint_root,
                                    source_objects_by_target=source_objects_by_target,
                                    field_mappings=field_mappings,
                                    load_object_set=load_object_set,
                                    source_record_id_col=source_record_id_col,
                                    target_record_id_col=target_record_id_col,
                                    source_value_col=source_value_col,
                                    source_load_value_col=source_load_value_col,
                                    change_types=change_types,
                                    processed_load_keys=processed_load_keys,
                                    dependency_load_stack=dependency_load_stack,
                                    result_rows=result_rows,
                                    fallback_to_salesforce=relationship_resolution_fallback_to_salesforce,
                                    record_noop_skips=record_noop_skips,
                                    batch_size=batch_size,
                                    bulk_buffer=bulk_buffer,
                                    opportunity_contact_role_cache=opportunity_contact_role_cache,
                                )
                                if contact_role_result is not None:
                                    result_rows.append(contact_role_result)
                        continue

                    if field_coverage_sample and source_record_id not in selected_source_ids:
                        continue

                    is_selected_sample_record = source_record_id in selected_source_ids
                    if (
                        not field_coverage_sample
                        and not is_selected_sample_record
                        and sample_size_per_object is not None
                        and object_attempted >= sample_size_per_object
                    ):
                        continue

                    source_payload = parse_json_dict(batch_rows[source_value_col][row_index])
                    source_load_payload = (
                        parse_json_dict(batch_rows[source_load_value_col][row_index])
                        if source_load_value_col in batch_rows
                        else {}
                    )
                    source_payload = merge_load_payload(
                        source_payload=source_payload,
                        source_load_payload=source_load_payload,
                    )
                    operation = resolve_operation(
                        target_record_id=target_record_id,
                        source_record_id=source_record_id,
                        target_external_id_field=target_external_id_field,
                    )
                    if operation == "create" and source_record_id and target_external_id_field:
                        existing_target_record_id = find_target_record_id_by_external_id(
                            sf=sf,
                            target_object=target_object,
                            source_record_id=source_record_id,
                            target_external_id_field=target_external_id_field,
                            lookup_cache=external_id_lookup_cache,
                        )
                        if existing_target_record_id:
                            target_record_id = existing_target_record_id
                            operation = "update"

                    write_operation_hint = operation_hint_for_operation(
                        operation=operation,
                        target_record_id=target_record_id,
                    )

                    mapped_payload, skipped_fields = map_source_payload_to_target(
                        source_object=source_object,
                        source_payload=source_payload,
                        target_object=target_object,
                        field_mappings=field_mappings,
                        field_defs=field_defs,
                        relationship_field_defs=relationship_field_defs,
                        operation_hint=write_operation_hint,
                    )
                    routed_opportunity_contact_source_id = get_routed_opportunity_contact_source_id(
                        target_object=target_object,
                        payload=mapped_payload,
                    )

                    if not mapped_payload:
                        processed_load_keys.add(load_key)
                        if record_noop_skips:
                            result_rows.append(
                                build_result_row(
                                    target_env=target_env,
                                    source_env=source_env,
                                    target_object=target_object,
                                    source_object=source_object,
                                    source_record_id=source_record_id,
                                    target_record_id=target_record_id,
                                    change_type=change_type,
                                    operation="skip",
                                    dry_run=dry_run,
                                    success=False,
                                    payload={},
                                    skipped_fields=skipped_fields,
                                    message="No writable mapped fields",
                                )
                            )
                        continue

                    payload_error = prepare_payload_for_operation(
                        payload=mapped_payload,
                        operation=operation,
                        source_record_id=source_record_id,
                        target_external_id_field=target_external_id_field,
                        operation_hint=write_operation_hint,
                    )
                    if payload_error:
                        result_rows.append(
                            build_result_row(
                                target_env=target_env,
                                source_env=source_env,
                                target_object=target_object,
                                source_object=source_object,
                                source_record_id=source_record_id,
                                target_record_id=target_record_id,
                                change_type=change_type,
                                operation=operation,
                                dry_run=dry_run,
                                success=False,
                                payload=mapped_payload,
                                skipped_fields=skipped_fields,
                                message=payload_error,
                            )
                        )
                        total_attempted += 1
                        object_attempted += 1
                        object_attempted_by_change_type[change_type] += 1
                        processed_load_keys.add(load_key)
                        continue

                    if resolve_relationships:
                        ensure_missing_relationship_dependencies_loaded(
                            payload=mapped_payload,
                            relationship_field_defs=relationship_field_defs,
                            sf=sf,
                            target_env=target_env,
                            source_env=source_env,
                            checkpoint_root=checkpoint_root,
                            target_extract_root=target_extract_root,
                            source_objects_by_target=source_objects_by_target,
                            field_mappings=field_mappings,
                            load_object_set=load_object_set,
                            source_record_id_col=source_record_id_col,
                            target_record_id_col=target_record_id_col,
                            source_value_col=source_value_col,
                            source_load_value_col=source_load_value_col,
                            change_types=change_types,
                            describe_cache=describe_cache,
                            extract_lookup_cache=extract_external_id_lookup_cache,
                            salesforce_lookup_cache=external_id_lookup_cache,
                            current_load_record_ids=current_load_record_ids,
                            processed_load_keys=processed_load_keys,
                            dependency_load_stack=dependency_load_stack,
                            result_rows=result_rows,
                            dry_run=dry_run,
                            fallback_to_salesforce=relationship_resolution_fallback_to_salesforce,
                            record_noop_skips=record_noop_skips,
                            batch_size=batch_size,
                            bulk_buffer=bulk_buffer,
                        )
                        resolve_relationship_payload_to_ids(
                            payload=mapped_payload,
                            relationship_field_defs=relationship_field_defs,
                            sf=sf,
                            describe_cache=describe_cache,
                            target_extract_root=target_extract_root,
                            extract_lookup_cache=extract_external_id_lookup_cache,
                            salesforce_lookup_cache=external_id_lookup_cache,
                            current_load_record_ids=current_load_record_ids,
                            skipped_fields=skipped_fields,
                            fallback_to_salesforce=relationship_resolution_fallback_to_salesforce,
                        )
                        if not mapped_payload:
                            if routed_opportunity_contact_source_id:
                                result_rows.append(
                                    build_result_row(
                                        target_env=target_env,
                                        source_env=source_env,
                                        target_object=target_object,
                                        source_object=source_object,
                                        source_record_id=source_record_id,
                                        target_record_id=target_record_id,
                                        change_type=change_type,
                                        operation="skip",
                                        dry_run=dry_run,
                                        success=True,
                                        payload={},
                                        skipped_fields=skipped_fields,
                                        message="No direct Opportunity fields to write after relationship resolution; ContactId routed to OpportunityContactRole",
                                    )
                                )
                                contact_role_result = sync_opportunity_contact_role_from_opportunity_contact_id(
                                    sf=sf,
                                    target_env=target_env,
                                    source_env=source_env,
                                    source_opportunity_id=source_record_id,
                                    target_opportunity_id=target_record_id,
                                    source_contact_id=routed_opportunity_contact_source_id,
                                    change_type=change_type,
                                    dry_run=dry_run,
                                    target_extract_root=target_extract_root,
                                    extract_lookup_cache=extract_external_id_lookup_cache,
                                    salesforce_lookup_cache=external_id_lookup_cache,
                                    current_load_record_ids=current_load_record_ids,
                                    describe_cache=describe_cache,
                                    checkpoint_root=checkpoint_root,
                                    source_objects_by_target=source_objects_by_target,
                                    field_mappings=field_mappings,
                                    load_object_set=load_object_set,
                                    source_record_id_col=source_record_id_col,
                                    target_record_id_col=target_record_id_col,
                                    source_value_col=source_value_col,
                                    source_load_value_col=source_load_value_col,
                                    change_types=change_types,
                                    processed_load_keys=processed_load_keys,
                                    dependency_load_stack=dependency_load_stack,
                                    result_rows=result_rows,
                                    fallback_to_salesforce=relationship_resolution_fallback_to_salesforce,
                                    record_noop_skips=record_noop_skips,
                                    batch_size=batch_size,
                                    bulk_buffer=bulk_buffer,
                                    opportunity_contact_role_cache=opportunity_contact_role_cache,
                                )
                                if contact_role_result is not None:
                                    result_rows.append(contact_role_result)
                            elif record_noop_skips:
                                result_rows.append(
                                    build_result_row(
                                        target_env=target_env,
                                        source_env=source_env,
                                        target_object=target_object,
                                        source_object=source_object,
                                        source_record_id=source_record_id,
                                        target_record_id=target_record_id,
                                        change_type=change_type,
                                        operation="skip",
                                        dry_run=dry_run,
                                        success=False,
                                        payload={},
                                        skipped_fields=skipped_fields,
                                        message="No writable mapped fields after relationship resolution",
                                    )
                                )
                            processed_load_keys.add(load_key)
                            continue

                    natural_key_target_record_id = find_target_record_id_by_natural_key(
                        sf=sf,
                        target_object=target_object,
                        payload=mapped_payload,
                        opportunity_contact_role_cache=opportunity_contact_role_cache,
                    )
                    if natural_key_target_record_id:
                        target_record_id = natural_key_target_record_id
                        operation = "update"
                        add_external_id_for_natural_key_update(
                            payload=mapped_payload,
                            source_record_id=source_record_id,
                            target_external_id_field=target_external_id_field,
                            skipped_fields=skipped_fields,
                        )
                    apply_object_specific_payload_rules(
                        payload=mapped_payload,
                        target_object=target_object,
                        operation=operation,
                        target_record_id=target_record_id,
                        field_defs=field_defs,
                        skipped_fields=skipped_fields,
                    )
                    apply_source_target_payload_rules(
                        payload=mapped_payload,
                        source_object=source_object,
                        target_object=target_object,
                        operation=operation,
                        target_record_id=target_record_id,
                        field_defs=field_defs,
                        skipped_fields=skipped_fields,
                        sf=sf,
                        describe_cache=describe_cache,
                    )
                    if not mapped_payload:
                        if routed_opportunity_contact_source_id:
                            result_rows.append(
                                build_result_row(
                                    target_env=target_env,
                                    source_env=source_env,
                                    target_object=target_object,
                                    source_object=source_object,
                                    source_record_id=source_record_id,
                                    target_record_id=target_record_id,
                                    change_type=change_type,
                                    operation="skip",
                                    dry_run=dry_run,
                                    success=True,
                                    payload={},
                                    skipped_fields=skipped_fields,
                                    message="No direct Opportunity fields to write; ContactId routed to OpportunityContactRole",
                                )
                            )
                            contact_role_result = sync_opportunity_contact_role_from_opportunity_contact_id(
                                sf=sf,
                                target_env=target_env,
                                source_env=source_env,
                                source_opportunity_id=source_record_id,
                                target_opportunity_id=target_record_id,
                                source_contact_id=routed_opportunity_contact_source_id,
                                change_type=change_type,
                                dry_run=dry_run,
                                target_extract_root=target_extract_root,
                                extract_lookup_cache=extract_external_id_lookup_cache,
                                salesforce_lookup_cache=external_id_lookup_cache,
                                current_load_record_ids=current_load_record_ids,
                                describe_cache=describe_cache,
                                checkpoint_root=checkpoint_root,
                                source_objects_by_target=source_objects_by_target,
                                field_mappings=field_mappings,
                                load_object_set=load_object_set,
                                source_record_id_col=source_record_id_col,
                                target_record_id_col=target_record_id_col,
                                source_value_col=source_value_col,
                                source_load_value_col=source_load_value_col,
                                change_types=change_types,
                                processed_load_keys=processed_load_keys,
                                dependency_load_stack=dependency_load_stack,
                                result_rows=result_rows,
                                fallback_to_salesforce=relationship_resolution_fallback_to_salesforce,
                                record_noop_skips=record_noop_skips,
                                batch_size=batch_size,
                                bulk_buffer=bulk_buffer,
                                opportunity_contact_role_cache=opportunity_contact_role_cache,
                            )
                            if contact_role_result is not None:
                                result_rows.append(contact_role_result)
                        elif record_noop_skips:
                            result_rows.append(
                                build_result_row(
                                    target_env=target_env,
                                    source_env=source_env,
                                    target_object=target_object,
                                    source_object=source_object,
                                    source_record_id=source_record_id,
                                    target_record_id=target_record_id,
                                    change_type=change_type,
                                    operation="skip",
                                    dry_run=dry_run,
                                    success=False,
                                    payload={},
                                    skipped_fields=skipped_fields,
                                    message="No writable mapped fields after object-specific payload rules",
                                )
                            )
                        processed_load_keys.add(load_key)
                        continue

                    if change_type.startswith("missing_from_") and operation == "update":
                        current_values_override = None
                        if target_object == "OpportunityContactRole":
                            current_values_override = get_cached_opportunity_contact_role_by_id(
                                target_record_id=target_record_id,
                                opportunity_contact_role_cache=opportunity_contact_role_cache,
                            )
                        filter_payload_to_actual_deltas(
                            sf=sf,
                            target_object=target_object,
                            target_record_id=target_record_id,
                            payload=mapped_payload,
                            field_defs=field_defs,
                            skipped_fields=skipped_fields,
                            current_values_override=current_values_override,
                        )
                        if not mapped_payload:
                            result_rows.append(
                                build_result_row(
                                    target_env=target_env,
                                    source_env=source_env,
                                    target_object=target_object,
                                    source_object=source_object,
                                    source_record_id=source_record_id,
                                    target_record_id=target_record_id,
                                    change_type=change_type,
                                    operation="skip",
                                    dry_run=dry_run,
                                    success=True,
                                    payload={},
                                    skipped_fields=skipped_fields,
                                    message="No actual deltas after target lookup; no Salesforce write performed",
                                )
                            )
                            processed_load_keys.add(load_key)
                            continue

                    post_success_actions: List[Dict[str, Any]] = []
                    if routed_opportunity_contact_source_id:
                        post_success_actions.append(
                            {
                                "type": "sync_opportunity_contact_role",
                                "source_opportunity_id": source_record_id,
                                "source_contact_id": routed_opportunity_contact_source_id,
                            }
                        )

                    if bulk_buffer is not None:
                        bulk_buffer.add(
                            target_object=target_object,
                            source_object=source_object,
                            source_record_id=source_record_id,
                            target_record_id=target_record_id,
                            change_type=change_type,
                            operation=operation,
                            payload=mapped_payload,
                            skipped_fields=skipped_fields,
                            post_success_actions=post_success_actions,
                        )
                    else:
                        result_row = execute_or_preview_operation(
                            sf=sf,
                            target_env=target_env,
                            source_env=source_env,
                            target_object=target_object,
                            source_object=source_object,
                            source_record_id=source_record_id,
                            target_record_id=target_record_id,
                            change_type=change_type,
                            operation=operation,
                            payload=mapped_payload,
                            skipped_fields=skipped_fields,
                            dry_run=dry_run,
                            single_record_reason="Bulk API disabled for this loader run",
                        )
                        result_rows.append(result_row)
                        remember_loaded_record_id(
                            result_row=result_row,
                            target_object=target_object,
                            source_record_id=source_record_id,
                            current_load_record_ids=current_load_record_ids,
                        )
                        if (
                            routed_opportunity_contact_source_id
                            and str(result_row.get("Success", "")).upper() == "TRUE"
                        ):
                            contact_role_result = sync_opportunity_contact_role_from_opportunity_contact_id(
                                sf=sf,
                                target_env=target_env,
                                source_env=source_env,
                                source_opportunity_id=source_record_id,
                                target_opportunity_id=normalize_blank(result_row.get("Target_RecordId")) or target_record_id,
                                source_contact_id=routed_opportunity_contact_source_id,
                                change_type=change_type,
                                dry_run=dry_run,
                                target_extract_root=target_extract_root,
                                extract_lookup_cache=extract_external_id_lookup_cache,
                                salesforce_lookup_cache=external_id_lookup_cache,
                                current_load_record_ids=current_load_record_ids,
                                describe_cache=describe_cache,
                                checkpoint_root=checkpoint_root,
                                source_objects_by_target=source_objects_by_target,
                                field_mappings=field_mappings,
                                load_object_set=load_object_set,
                                source_record_id_col=source_record_id_col,
                                target_record_id_col=target_record_id_col,
                                source_value_col=source_value_col,
                                source_load_value_col=source_load_value_col,
                                change_types=change_types,
                                processed_load_keys=processed_load_keys,
                                dependency_load_stack=dependency_load_stack,
                                result_rows=result_rows,
                                fallback_to_salesforce=relationship_resolution_fallback_to_salesforce,
                                record_noop_skips=record_noop_skips,
                                batch_size=batch_size,
                                opportunity_contact_role_cache=opportunity_contact_role_cache,
                            )
                            if contact_role_result is not None:
                                result_rows.append(contact_role_result)
                    total_attempted += 1
                    object_attempted += 1
                    object_attempted_by_change_type[change_type] += 1
                    processed_load_keys.add(load_key)

        if bulk_buffer is not None:
            bulk_buffer.flush_target_object(target_object)

        object_change_type_summary = ", ".join(
            f"{change_type}={count}"
            for change_type, count in sorted(object_attempted_by_change_type.items())
        )
        object_summary_parts = [
            f"attempted={object_attempted}",
            f"resume_skipped={object_skipped_already_attempted}",
        ]
        if object_change_type_summary:
            object_summary_parts.append(object_change_type_summary)
        print(f"Finished {target_object}: " + "; ".join(object_summary_parts))

    if bulk_buffer is not None:
        bulk_buffer.flush_all()

    results_df = result_rows.to_dataframe()
    result_rows.write_error_outputs()

    print(
        f"\nLoad results written: {results_csv_path} "
        f"({len(result_rows)} row(s), {result_rows.success_count} success, "
        f"{result_rows.failure_count} failed/skipped)"
    )
    print(f"Failed rows written: {result_rows.failed_rows_csv_path}")
    print(f"Error summary written: {result_rows.error_summary_csv_path}")
    print(f"Error examples written: {result_rows.error_examples_csv_path}")

    return results_df


def resolve_load_step_config(
    load_step: int,
    sample_size: Optional[int],
    sample_size_per_object: Optional[int],
    dry_run: Optional[bool],
    dependency_aware_sample: Optional[bool],
    field_coverage_sample: Optional[bool],
    record_noop_skips: Optional[bool],
) -> Dict[str, Any]:
    if load_step not in LOAD_STEP_CONFIGS:
        raise ValueError("load_step must be 1, 2, or 3")

    config = dict(LOAD_STEP_CONFIGS[load_step])

    if sample_size is not None:
        config["sample_size"] = sample_size
    if sample_size_per_object is not None:
        config["sample_size_per_object"] = sample_size_per_object
    if dry_run is not None:
        config["dry_run"] = bool(dry_run)
    if dependency_aware_sample is not None:
        config["dependency_aware_sample"] = bool(dependency_aware_sample)
    if field_coverage_sample is not None:
        config["field_coverage_sample"] = bool(field_coverage_sample)
    if record_noop_skips is not None:
        config["record_noop_skips"] = bool(record_noop_skips)

    return config


def print_load_sequence(load_objects: List[str]) -> None:
    preview_count = 8
    preview = " -> ".join(load_objects[:preview_count])
    if len(load_objects) > preview_count:
        preview += f" -> ... ({len(load_objects)} total objects)"
    print(f"Load sequence: {preview}")
    print(
        "Resume behavior: records already present in the result CSV are skipped; "
        "objects may still be scanned to find remaining unattempted records."
    )


def build_prod_to_target_field_mappings(
    metadata_path: Path,
) -> Tuple[Dict[Tuple[str, str], List[Dict[str, Any]]], Dict[str, Set[str]]]:
    metadata_df = pd.read_csv(metadata_path, dtype=str, encoding="utf-8-sig").fillna("")
    required_columns = {
        "SF 1.0 Object",
        "SF 1.0 Field",
        "SF 2.0 Object",
        "SF 2.0 Field",
    }
    missing_columns = required_columns - set(metadata_df.columns)
    if missing_columns:
        raise ValueError(
            f"{metadata_path} is missing required column(s): {', '.join(sorted(missing_columns))}"
        )

    mappings: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    source_objects_by_target: Dict[str, Set[str]] = {}

    for row_number, row in metadata_df.iterrows():
        if should_ignore_row(row.get("Ignore", "")):
            continue

        source_object = normalize_metadata_cell(row.get("SF 1.0 Object"))
        source_field = normalize_metadata_cell(row.get("SF 1.0 Field"))
        target_object = normalize_metadata_cell(row.get("SF 2.0 Object"))
        target_field = normalize_metadata_cell(row.get("SF 2.0 Field"))

        if not source_object or not source_field or not target_object or not target_field:
            continue

        transformation_logic = parse_transformation_logic(
            row.get("Transformation Logic (JSON)"),
            row_number=row_number + 2,
            source_object=source_object,
            source_field=source_field,
            target_object=target_object,
            target_field=target_field,
        )

        mappings.setdefault((source_object, source_field), []).append(
            {
                "target_object": target_object,
                "target_field": target_field,
                "transformation_logic": transformation_logic,
            }
        )
        source_objects_by_target.setdefault(target_object, set()).add(source_object)

    return mappings, source_objects_by_target


def read_upsert_sequence(path: Path, object_filter: Set[str]) -> List[str]:
    sequence_df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    required_columns = {"Load_Order", "Object", "Enabled"}
    missing_columns = required_columns - set(sequence_df.columns)
    if missing_columns:
        raise ValueError(
            f"{path} is missing required column(s): {', '.join(sorted(missing_columns))}"
        )

    def effective_order(row: pd.Series) -> float:
        override = normalize_blank(row.get("Manual_Override_Order"))
        if override:
            return float(override)
        return float(normalize_blank(row.get("Load_Order")) or 0)

    sequence_df = sequence_df.loc[
        sequence_df["Enabled"].fillna("").str.strip().str.lower().isin({"true", "t", "yes", "y", "1"})
    ].copy()
    sequence_df["Effective_Order"] = sequence_df.apply(effective_order, axis=1)
    sequence_df = sequence_df.sort_values(["Effective_Order", "Object"], kind="stable")

    objects = [
        normalize_blank(obj)
        for obj in sequence_df["Object"].tolist()
        if normalize_blank(obj)
    ]
    if object_filter:
        objects = [obj for obj in objects if obj in object_filter]

    return objects


def apply_object_source_policy(
    load_objects: List[str],
    source_env: str,
    policy_path: Optional[Path],
) -> List[str]:
    if not policy_path or not policy_path.exists():
        return load_objects

    policy_df = pd.read_csv(policy_path, dtype=str, encoding="utf-8-sig").fillna("")
    required_columns = {"Object", "Required_Source_Env_When_Loaded"}
    missing_columns = required_columns - set(policy_df.columns)
    if missing_columns:
        raise ValueError(
            f"{policy_path} is missing required column(s): "
            f"{', '.join(sorted(missing_columns))}"
        )

    required_source_by_object = {
        normalize_blank(row.get("Object")): normalize_blank(row.get("Required_Source_Env_When_Loaded"))
        for _, row in policy_df.iterrows()
        if normalize_blank(row.get("Object"))
        and normalize_blank(row.get("Required_Source_Env_When_Loaded"))
    }
    if not required_source_by_object:
        return load_objects

    kept_objects: List[str] = []
    skipped_objects: List[Tuple[str, str]] = []
    normalized_source_env = source_env.upper()
    for obj in load_objects:
        required_source_env = required_source_by_object.get(obj)
        if required_source_env and required_source_env.upper() != normalized_source_env:
            skipped_objects.append((obj, required_source_env))
            continue
        kept_objects.append(obj)

    if skipped_objects:
        skipped_summary = ", ".join(
            f"{obj} requires source_env={required_source_env}"
            for obj, required_source_env in skipped_objects
        )
        print(
            "Object source policy: skipping object(s) for this run because "
            f"source_env={source_env}: {skipped_summary}"
        )

    return kept_objects


def build_field_coverage_sample_source_ids(
    sf,
    checkpoint_root: Path,
    load_objects: List[str],
    source_objects_by_target: Dict[str, Set[str]],
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    source_record_id_col: str,
    target_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    change_types: Set[str],
    describe_cache: Dict[str, Dict[str, Any]],
    batch_size: int,
) -> Dict[str, Set[str]]:
    sample_source_ids: Dict[str, Set[str]] = {}

    for target_object in load_objects:
        describe = get_object_describe(sf, target_object, describe_cache)
        field_defs = {
            field_def["name"]: field_def
            for field_def in describe.get("fields", [])
            if field_def.get("name")
        }
        relationship_field_defs = {
            str(field_def.get("relationshipName")).strip(): field_def
            for field_def in field_defs.values()
            if field_def.get("relationshipName")
        }
        target_external_id_field = field_defs.get("External_Id__c")
        covered_fields: Set[str] = set()
        diffed_fields: Set[str] = set()

        for diff_row in iter_load_diff_rows_for_target_object(
            checkpoint_root=checkpoint_root,
            target_object=target_object,
            source_objects_by_target=source_objects_by_target,
            source_record_id_col=source_record_id_col,
            target_record_id_col=target_record_id_col,
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            change_types=change_types,
            batch_size=batch_size,
        ):
            source_record_id = normalize_blank(diff_row.get("source_record_id"))
            if not source_record_id:
                continue

            row_fields = target_fields_for_diff_row(
                target_object=target_object,
                diff_row=diff_row,
                field_mappings=field_mappings,
                field_defs=field_defs,
                relationship_field_defs=relationship_field_defs,
                target_external_id_field=target_external_id_field,
            )
            if not row_fields:
                continue

            diffed_fields.update(row_fields)
            new_fields = row_fields - covered_fields
            if not new_fields:
                continue

            sample_source_ids.setdefault(target_object, set()).add(source_record_id)
            covered_fields.update(row_fields)

        print(
            f"Field coverage {target_object}: "
            f"{len(sample_source_ids.get(target_object, set()))} record(s), "
            f"{len(covered_fields)}/{len(diffed_fields)} field(s)"
        )

    return sample_source_ids


def target_fields_for_diff_row(
    target_object: str,
    diff_row: Dict[str, Any],
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    field_defs: Dict[str, Dict[str, Any]],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    target_external_id_field: Optional[Dict[str, Any]],
) -> Set[str]:
    source_record_id = normalize_blank(diff_row.get("source_record_id"))
    target_record_id = normalize_blank(diff_row.get("target_record_id"))
    source_object = normalize_blank(diff_row.get("source_object"))
    source_payload = merge_load_payload(
        source_payload=diff_row.get("source_payload") or {},
        source_load_payload=diff_row.get("source_load_payload") or {},
    )
    operation = resolve_operation(
        target_record_id=target_record_id,
        source_record_id=source_record_id,
        target_external_id_field=target_external_id_field,
    )
    write_operation_hint = operation_hint_for_operation(
        operation=operation,
        target_record_id=target_record_id,
    )
    mapped_payload, skipped_fields = map_source_payload_to_target(
        source_object=source_object,
        source_payload=source_payload,
        target_object=target_object,
        field_mappings=field_mappings,
        field_defs=field_defs,
        relationship_field_defs=relationship_field_defs,
        operation_hint=write_operation_hint,
    )
    if not mapped_payload:
        return set()

    apply_object_specific_payload_rules(
        payload=mapped_payload,
        target_object=target_object,
        operation=operation,
        target_record_id=target_record_id,
        field_defs=field_defs,
        skipped_fields=skipped_fields,
    )
    return payload_coverage_fields(
        payload=mapped_payload,
        relationship_field_defs=relationship_field_defs,
    )


def payload_coverage_fields(
    payload: Dict[str, Any],
    relationship_field_defs: Dict[str, Dict[str, Any]],
) -> Set[str]:
    fields: Set[str] = set()

    for field_name, value in payload.items():
        if isinstance(value, dict):
            relationship_field_def = relationship_field_defs.get(field_name)
            target_field_name = normalize_blank(
                relationship_field_def.get("name") if relationship_field_def else None
            )
            if target_field_name:
                fields.add(target_field_name)
            else:
                fields.add(field_name)
        else:
            fields.add(field_name)

    return fields


def expand_sample_source_ids_with_dependencies(
    sample_source_ids: Dict[str, Set[str]],
    sf,
    checkpoint_root: Path,
    load_objects: List[str],
    source_objects_by_target: Dict[str, Set[str]],
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    source_record_id_col: str,
    target_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    change_types: Set[str],
    target_extract_root: Optional[Path],
    describe_cache: Dict[str, Dict[str, Any]],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    batch_size: int,
) -> None:
    load_object_set = set(load_objects)
    queued_dependencies: List[Tuple[str, str]] = [
        (target_object, source_id)
        for target_object, source_ids in sample_source_ids.items()
        for source_id in source_ids
    ]
    processed_dependencies: Set[Tuple[str, str]] = set()

    while queued_dependencies:
        target_object, source_id = queued_dependencies.pop(0)
        dependency_key = (target_object, source_id)
        if dependency_key in processed_dependencies:
            continue
        processed_dependencies.add(dependency_key)

        diff_row = find_diff_row_by_source_id(
            checkpoint_root=checkpoint_root,
            target_object=target_object,
            source_objects_by_target=source_objects_by_target,
            source_record_id_col=source_record_id_col,
            target_record_id_col=target_record_id_col,
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            source_record_id=source_id,
            change_types=change_types,
            batch_size=batch_size,
        )
        if diff_row is None:
            continue

        dependencies, is_actionable = collect_dependencies_for_diff_row(
            sf=sf,
            target_object=target_object,
            diff_row=diff_row,
            field_mappings=field_mappings,
            target_extract_root=target_extract_root,
            describe_cache=describe_cache,
            extract_lookup_cache=extract_lookup_cache,
        )
        if not is_actionable:
            continue

        for dependency_object, dependency_source_id in dependencies:
            if dependency_object not in load_object_set:
                continue
            if add_dependency_sample_source_id(
                dependency_source_ids=sample_source_ids,
                dependency_object=dependency_object,
                dependency_source_id=dependency_source_id,
            ):
                queued_dependencies.append((dependency_object, dependency_source_id))


def has_pending_source_ids(
    target_object: str,
    selected_source_ids: Set[str],
    processed_load_keys: Set[Tuple[str, str]],
) -> bool:
    if not selected_source_ids:
        return False
    return any(
        (target_object, source_id) not in processed_load_keys
        for source_id in selected_source_ids
    )


def build_dependency_sample_source_ids(
    sf,
    checkpoint_root: Path,
    load_objects: List[str],
    source_objects_by_target: Dict[str, Set[str]],
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    source_record_id_col: str,
    target_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    change_types: Set[str],
    sample_size_per_object: int,
    target_extract_root: Optional[Path],
    describe_cache: Dict[str, Dict[str, Any]],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    batch_size: int,
) -> Dict[str, Set[str]]:
    load_object_set = set(load_objects)
    dependency_source_ids: Dict[str, Set[str]] = {}
    queued_dependencies: List[Tuple[str, str]] = []
    processed_dependencies: Set[Tuple[str, str]] = set()

    for target_object in load_objects:
        candidate_count = 0
        for diff_row in iter_load_diff_rows_for_target_object(
            checkpoint_root=checkpoint_root,
            target_object=target_object,
            source_objects_by_target=source_objects_by_target,
            source_record_id_col=source_record_id_col,
            target_record_id_col=target_record_id_col,
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            change_types=change_types,
            batch_size=batch_size,
        ):
            dependencies, is_actionable = collect_dependencies_for_diff_row(
                sf=sf,
                target_object=target_object,
                diff_row=diff_row,
                field_mappings=field_mappings,
                target_extract_root=target_extract_root,
                describe_cache=describe_cache,
                extract_lookup_cache=extract_lookup_cache,
            )
            if not is_actionable:
                continue

            for dependency in dependencies:
                dependency_object, dependency_source_id = dependency
                if dependency_object not in load_object_set:
                    continue
                if add_dependency_sample_source_id(
                    dependency_source_ids=dependency_source_ids,
                    dependency_object=dependency_object,
                    dependency_source_id=dependency_source_id,
                ):
                    queued_dependencies.append(dependency)

            candidate_count += 1
            if candidate_count >= sample_size_per_object:
                break

    while queued_dependencies:
        dependency_object, dependency_source_id = queued_dependencies.pop(0)
        dependency_key = (dependency_object, dependency_source_id)
        if dependency_key in processed_dependencies:
            continue
        processed_dependencies.add(dependency_key)

        diff_row = find_diff_row_by_source_id(
            checkpoint_root=checkpoint_root,
            target_object=dependency_object,
            source_objects_by_target=source_objects_by_target,
            source_record_id_col=source_record_id_col,
            target_record_id_col=target_record_id_col,
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            source_record_id=dependency_source_id,
            change_types=change_types,
            batch_size=batch_size,
        )
        if diff_row is None:
            continue

        dependencies, is_actionable = collect_dependencies_for_diff_row(
            sf=sf,
            target_object=dependency_object,
            diff_row=diff_row,
            field_mappings=field_mappings,
            target_extract_root=target_extract_root,
            describe_cache=describe_cache,
            extract_lookup_cache=extract_lookup_cache,
        )
        if not is_actionable:
            continue

        for dependency in dependencies:
            nested_dependency_object, nested_dependency_source_id = dependency
            if nested_dependency_object not in load_object_set:
                continue
            if add_dependency_sample_source_id(
                dependency_source_ids=dependency_source_ids,
                dependency_object=nested_dependency_object,
                dependency_source_id=nested_dependency_source_id,
            ):
                queued_dependencies.append(dependency)

    return dependency_source_ids


def add_dependency_sample_source_id(
    dependency_source_ids: Dict[str, Set[str]],
    dependency_object: str,
    dependency_source_id: str,
) -> bool:
    if not dependency_object or not dependency_source_id:
        return False

    object_source_ids = dependency_source_ids.setdefault(dependency_object, set())
    if dependency_source_id in object_source_ids:
        return False

    object_source_ids.add(dependency_source_id)
    return True


def iter_load_diff_rows_for_target_object(
    checkpoint_root: Path,
    target_object: str,
    source_objects_by_target: Dict[str, Set[str]],
    source_record_id_col: str,
    target_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    change_types: Set[str],
    batch_size: int,
) -> Iterable[Dict[str, Any]]:
    diff_columns = [
        "Obj",
        source_record_id_col,
        target_record_id_col,
        source_value_col,
        "change_type",
    ]
    source_objects = sorted(source_objects_by_target.get(target_object, {target_object}))

    for part_path in iter_record_diff_part_paths(checkpoint_root, source_objects):
        parquet_file = pq.ParquetFile(part_path)
        available_columns = set(parquet_file.schema_arrow.names)
        missing_columns = set(diff_columns) - available_columns
        if missing_columns:
            continue

        batch_columns = list(diff_columns)
        if source_load_value_col in available_columns:
            batch_columns.append(source_load_value_col)

        for batch in parquet_file.iter_batches(columns=batch_columns, batch_size=batch_size):
            batch_rows = batch.to_pydict()
            row_count = len(batch_rows["Obj"])

            for row_index in range(row_count):
                change_type = normalize_blank(batch_rows["change_type"][row_index])
                if change_type not in change_types:
                    continue

                yield {
                    "source_object": normalize_blank(batch_rows["Obj"][row_index]),
                    "source_record_id": normalize_blank(batch_rows[source_record_id_col][row_index]),
                    "target_record_id": normalize_blank(batch_rows[target_record_id_col][row_index]),
                    "source_payload": parse_json_dict(batch_rows[source_value_col][row_index]),
                    "source_load_payload": (
                        parse_json_dict(batch_rows[source_load_value_col][row_index])
                        if source_load_value_col in batch_rows
                        else {}
                    ),
                    "change_type": change_type,
                }


def find_diff_row_by_source_id(
    checkpoint_root: Path,
    target_object: str,
    source_objects_by_target: Dict[str, Set[str]],
    source_record_id_col: str,
    target_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    source_record_id: str,
    change_types: Set[str],
    batch_size: int,
) -> Optional[Dict[str, Any]]:
    for diff_row in iter_load_diff_rows_for_target_object(
        checkpoint_root=checkpoint_root,
        target_object=target_object,
        source_objects_by_target=source_objects_by_target,
        source_record_id_col=source_record_id_col,
        target_record_id_col=target_record_id_col,
        source_value_col=source_value_col,
        source_load_value_col=source_load_value_col,
        change_types=change_types,
        batch_size=batch_size,
    ):
        if normalize_blank(diff_row.get("source_record_id")) == source_record_id:
            return diff_row

    return None


def collect_dependencies_for_diff_row(
    sf,
    target_object: str,
    diff_row: Dict[str, Any],
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    target_extract_root: Optional[Path],
    describe_cache: Dict[str, Dict[str, Any]],
    extract_lookup_cache: Dict[str, Dict[str, str]],
) -> Tuple[List[Tuple[str, str]], bool]:
    describe = get_object_describe(sf, target_object, describe_cache)
    field_defs = {
        field_def["name"]: field_def
        for field_def in describe.get("fields", [])
        if field_def.get("name")
    }
    relationship_field_defs = {
        str(field_def.get("relationshipName")).strip(): field_def
        for field_def in field_defs.values()
        if field_def.get("relationshipName")
    }
    target_external_id_field = field_defs.get("External_Id__c")
    source_record_id = normalize_blank(diff_row.get("source_record_id"))
    target_record_id = normalize_blank(diff_row.get("target_record_id"))
    source_object = normalize_blank(diff_row.get("source_object"))
    source_payload = merge_load_payload(
        source_payload=diff_row.get("source_payload") or {},
        source_load_payload=diff_row.get("source_load_payload") or {},
    )
    operation = resolve_operation(
        target_record_id=target_record_id,
        source_record_id=source_record_id,
        target_external_id_field=target_external_id_field,
    )
    write_operation_hint = operation_hint_for_operation(
        operation=operation,
        target_record_id=target_record_id,
    )
    mapped_payload, _ = map_source_payload_to_target(
        source_object=source_object,
        source_payload=source_payload,
        target_object=target_object,
        field_mappings=field_mappings,
        field_defs=field_defs,
        relationship_field_defs=relationship_field_defs,
        operation_hint=write_operation_hint,
    )
    if not mapped_payload:
        return [], False

    dependencies: List[Tuple[str, str]] = []
    routed_opportunity_contact_source_id = get_routed_opportunity_contact_source_id(
        target_object=target_object,
        payload=mapped_payload,
    )
    if routed_opportunity_contact_source_id:
        dependencies.append(("Contact", routed_opportunity_contact_source_id))

    for relationship_name, relationship_value in mapped_payload.items():
        if not isinstance(relationship_value, dict):
            continue

        field_def = relationship_field_defs.get(relationship_name)
        if not field_def:
            continue

        external_id_value = normalize_blank(relationship_value.get("External_Id__c"))
        if not external_id_value:
            continue

        for reference_object in field_def.get("referenceTo", []):
            dependency_object = normalize_blank(reference_object)
            if not dependency_object:
                continue
            if find_record_id_in_target_extract(
                target_extract_root=target_extract_root,
                object_name=dependency_object,
                external_id_value=external_id_value,
                extract_lookup_cache=extract_lookup_cache,
            ):
                continue
            dependencies.append((dependency_object, external_id_value))

    return dependencies, True


def iter_record_diff_part_paths(checkpoint_root: Path, source_objects: Iterable[str]) -> Iterable[Path]:
    for source_object in source_objects:
        object_dir = checkpoint_root / f"Obj={safe_path_part(source_object)}"
        yield from sorted(object_dir.glob("JoinBucket=*/record_diff.parquet"))


def map_source_payload_to_target(
    source_object: str,
    source_payload: Dict[str, Any],
    target_object: str,
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    field_defs: Dict[str, Dict[str, Any]],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    operation_hint: str,
) -> Tuple[Dict[str, Any], List[str]]:
    payload: Dict[str, Any] = {}
    skipped_fields: List[str] = []

    for source_field, source_value in source_payload.items():
        mappings = [
            mapping
            for mapping in field_mappings.get((source_object, source_field), [])
            if mapping["target_object"] == target_object
        ]
        if not mappings:
            if add_inferred_source_target_payload_value(
                payload=payload,
                source_object=source_object,
                source_field=source_field,
                source_value=source_value,
                target_object=target_object,
                field_defs=field_defs,
                relationship_field_defs=relationship_field_defs,
                operation_hint=operation_hint,
            ):
                continue
            skipped_fields.append(f"{source_field}: no metadata mapping")
            continue

        for mapping in mappings:
            target_field = mapping["target_field"]
            if target_field == "Id":
                skipped_fields.append(f"{source_field}: target Id is not writable")
                continue

            mapped_value = apply_transformation_logic(
                source_value,
                mapping.get("transformation_logic"),
            )
            added = add_target_payload_value(
                payload=payload,
                target_field=target_field,
                value=mapped_value,
                field_defs=field_defs,
                relationship_field_defs=relationship_field_defs,
                operation_hint=operation_hint,
            )
            if not added:
                if add_inferred_source_target_payload_value(
                    payload=payload,
                    source_object=source_object,
                    source_field=source_field,
                    source_value=mapped_value,
                    target_object=target_object,
                    field_defs=field_defs,
                    relationship_field_defs=relationship_field_defs,
                    operation_hint=operation_hint,
                ):
                    skipped_fields.append(
                        f"{source_field} -> {target_field}: not available; used inferred target field"
                    )
                else:
                    skipped_fields.append(f"{source_field} -> {target_field}: not writable or unsupported")

    return payload, skipped_fields


def add_inferred_source_target_payload_value(
    payload: Dict[str, Any],
    source_object: str,
    source_field: str,
    source_value: Any,
    target_object: str,
    field_defs: Dict[str, Dict[str, Any]],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    operation_hint: str,
) -> bool:
    if (
        source_object == "OpportunityLineItemSchedule"
        and target_object == "OpportunityLineItemSchedule__c"
    ):
        return add_inferred_opportunity_line_item_schedule_value(
            payload=payload,
            source_field=source_field,
            source_value=source_value,
            field_defs=field_defs,
            relationship_field_defs=relationship_field_defs,
            operation_hint=operation_hint,
        )
    return False


def add_inferred_opportunity_line_item_schedule_value(
    payload: Dict[str, Any],
    source_field: str,
    source_value: Any,
    field_defs: Dict[str, Dict[str, Any]],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    operation_hint: str,
) -> bool:
    if source_field == "OpportunityLineItemId":
        return add_reference_payload_by_target_object(
            payload=payload,
            reference_object="OpportunityLineItem",
            external_id_value=source_value,
            field_defs=field_defs,
            operation_hint=operation_hint,
        )

    for target_field in inferred_target_field_candidates(source_field):
        if target_field not in field_defs:
            continue
        if add_target_payload_value(
            payload=payload,
            target_field=target_field,
            value=source_value,
            field_defs=field_defs,
            relationship_field_defs=relationship_field_defs,
            operation_hint=operation_hint,
        ):
            return True

    return False


def inferred_target_field_candidates(source_field: str) -> List[str]:
    if source_field == "Id":
        return ["External_Id__c", "Line_Item_Schedule_Id__c"]

    candidates: List[str] = [source_field]
    if not source_field.endswith("__c"):
        candidates.append(f"{source_field}__c")
        underscored = re.sub(r"(?<!^)(?=[A-Z])", "_", source_field)
        candidates.append(f"{underscored}__c")

    return list(dict.fromkeys(candidates))


def add_reference_payload_by_target_object(
    payload: Dict[str, Any],
    reference_object: str,
    external_id_value: Any,
    field_defs: Dict[str, Dict[str, Any]],
    operation_hint: str,
) -> bool:
    if normalize_blank(external_id_value) is None:
        for field_def in field_defs.values():
            if field_def.get("type") != "reference":
                continue
            reference_targets = {
                str(reference_to).strip()
                for reference_to in field_def.get("referenceTo", [])
                if str(reference_to).strip()
            }
            if reference_object not in reference_targets:
                continue
            if not field_def.get("nillable", True) or not is_field_writable(field_def, operation_hint):
                return False
            payload[field_def["name"]] = None
            return True
        return False

    for field_def in field_defs.values():
        if field_def.get("type") != "reference":
            continue
        reference_targets = {
            str(reference_to).strip()
            for reference_to in field_def.get("referenceTo", [])
            if str(reference_to).strip()
        }
        if reference_object not in reference_targets:
            continue
        if not is_field_writable(field_def, operation_hint):
            continue
        relationship_name = normalize_blank(field_def.get("relationshipName"))
        if not relationship_name:
            continue
        payload[relationship_name] = {"External_Id__c": external_id_value}
        return True

    return False


def add_target_payload_value(
    payload: Dict[str, Any],
    target_field: str,
    value: Any,
    field_defs: Dict[str, Dict[str, Any]],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    operation_hint: str,
) -> bool:
    if "." in target_field:
        relationship_name, external_id_field = target_field.split(".", 1)
        if not relationship_name or not external_id_field:
            return False
        field_def = relationship_field_defs.get(relationship_name)
        if not field_def:
            return False
        if not is_field_writable(field_def, operation_hint):
            return False
        if normalize_blank(value) is None:
            if (
                not field_def.get("nillable", True)
                or not is_field_writable(field_def, operation_hint)
            ):
                return False
            payload[field_def["name"]] = None
            return True

        payload.setdefault(relationship_name, {})
        payload[relationship_name][external_id_field] = value
        return True

    field_def = field_defs.get(target_field)
    if not field_def:
        return False

    if target_field == "Id":
        return False

    if not is_field_writable(field_def, operation_hint):
        return False

    if field_def.get("type") == "reference":
        if normalize_blank(value) is None:
            if not field_def.get("nillable", True):
                return False
            payload[target_field] = None
            return True

        reference_targets = {
            str(reference_to).strip()
            for reference_to in field_def.get("referenceTo", [])
            if str(reference_to).strip()
        }
        if reference_targets & {"RecordType"}:
            return False

        relationship_name = normalize_blank(field_def.get("relationshipName"))
        if relationship_name and normalize_blank(value):
            payload[relationship_name] = {"External_Id__c": value}
            return True

    payload[target_field] = normalize_salesforce_value(value, field_def)
    return True


def prepare_payload_for_operation(
    payload: Dict[str, Any],
    operation: str,
    source_record_id: Optional[str],
    target_external_id_field: Optional[Dict[str, Any]],
    operation_hint: str,
) -> Optional[str]:
    """
    Keep External_Id__c aligned with the migration strategy while obeying
    Salesforce API rules:
    - upsert uses External_Id__c in the URL, not in the JSON body
    - create must include External_Id__c when preserving the legacy SF 1.0 Id
    """
    if operation == "upsert":
        payload.pop("External_Id__c", None)
        return None

    if operation != "create" or not source_record_id:
        return None

    if not target_external_id_field:
        return (
            "Cannot create missing record because External_Id__c is not available "
            "on the target object; legacy SF 1.0 Id would not be preserved"
        )

    if "External_Id__c" in payload:
        return None

    if not is_field_writable(target_external_id_field, operation_hint):
        return (
            "Cannot create missing record because External_Id__c is not createable "
            "on the target object; legacy SF 1.0 Id would not be preserved"
        )

    payload["External_Id__c"] = source_record_id
    return None


def ensure_missing_relationship_dependencies_loaded(
    payload: Dict[str, Any],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    sf,
    target_env: str,
    source_env: str,
    checkpoint_root: Path,
    target_extract_root: Optional[Path],
    source_objects_by_target: Dict[str, Set[str]],
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    load_object_set: Set[str],
    source_record_id_col: str,
    target_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    change_types: Set[str],
    describe_cache: Dict[str, Dict[str, Any]],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    salesforce_lookup_cache: Dict[Tuple[str, str], Optional[str]],
    current_load_record_ids: Dict[Tuple[str, str], str],
    processed_load_keys: Set[Tuple[str, str]],
    dependency_load_stack: Set[Tuple[str, str]],
    result_rows: List[Dict[str, Any]],
    dry_run: bool,
    fallback_to_salesforce: bool,
    record_noop_skips: bool,
    batch_size: int,
    bulk_buffer: Optional[BulkOperationBuffer] = None,
) -> None:
    dependencies = relationship_dependencies_needing_load(
        payload=payload,
        relationship_field_defs=relationship_field_defs,
        sf=sf,
        target_extract_root=target_extract_root,
        extract_lookup_cache=extract_lookup_cache,
        salesforce_lookup_cache=salesforce_lookup_cache,
        current_load_record_ids=current_load_record_ids,
        load_object_set=load_object_set,
        describe_cache=describe_cache,
        fallback_to_salesforce=fallback_to_salesforce,
    )

    for dependency_object, dependency_source_id in dependencies:
        load_dependency_record(
            target_object=dependency_object,
            source_record_id=dependency_source_id,
            sf=sf,
            target_env=target_env,
            source_env=source_env,
            checkpoint_root=checkpoint_root,
            target_extract_root=target_extract_root,
            source_objects_by_target=source_objects_by_target,
            field_mappings=field_mappings,
            load_object_set=load_object_set,
            source_record_id_col=source_record_id_col,
            target_record_id_col=target_record_id_col,
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            change_types=change_types,
            describe_cache=describe_cache,
            extract_lookup_cache=extract_lookup_cache,
            salesforce_lookup_cache=salesforce_lookup_cache,
            current_load_record_ids=current_load_record_ids,
            processed_load_keys=processed_load_keys,
            dependency_load_stack=dependency_load_stack,
            result_rows=result_rows,
            dry_run=dry_run,
            fallback_to_salesforce=fallback_to_salesforce,
            record_noop_skips=record_noop_skips,
            batch_size=batch_size,
            bulk_buffer=bulk_buffer,
        )


def relationship_dependencies_needing_load(
    payload: Dict[str, Any],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    sf,
    target_extract_root: Optional[Path],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    salesforce_lookup_cache: Dict[Tuple[str, str], Optional[str]],
    current_load_record_ids: Dict[Tuple[str, str], str],
    load_object_set: Set[str],
    describe_cache: Dict[str, Dict[str, Any]],
    fallback_to_salesforce: bool,
) -> List[Tuple[str, str]]:
    dependencies: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()

    for relationship_name, relationship_value in list(payload.items()):
        if not isinstance(relationship_value, dict):
            continue

        field_def = relationship_field_defs.get(relationship_name)
        if not field_def:
            continue

        external_id_value = normalize_blank(relationship_value.get("External_Id__c"))
        if not external_id_value:
            continue

        for reference_object in field_def.get("referenceTo", []):
            object_name = normalize_blank(reference_object)
            if not object_name or object_name not in load_object_set:
                continue

            if current_load_record_ids.get((object_name, external_id_value)):
                continue
            if find_record_id_in_target_extract(
                target_extract_root=target_extract_root,
                object_name=object_name,
                external_id_value=external_id_value,
                extract_lookup_cache=extract_lookup_cache,
            ):
                continue
            if fallback_to_salesforce and find_salesforce_record_id_by_external_id(
                sf=sf,
                object_name=object_name,
                external_id_value=external_id_value,
                describe_cache=describe_cache,
                lookup_cache=salesforce_lookup_cache,
            ):
                continue

            dependency_key = (object_name, external_id_value)
            if dependency_key not in seen:
                dependencies.append(dependency_key)
                seen.add(dependency_key)

    return dependencies


def load_dependency_record(
    target_object: str,
    source_record_id: str,
    sf,
    target_env: str,
    source_env: str,
    checkpoint_root: Path,
    target_extract_root: Optional[Path],
    source_objects_by_target: Dict[str, Set[str]],
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    load_object_set: Set[str],
    source_record_id_col: str,
    target_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    change_types: Set[str],
    describe_cache: Dict[str, Dict[str, Any]],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    salesforce_lookup_cache: Dict[Tuple[str, str], Optional[str]],
    current_load_record_ids: Dict[Tuple[str, str], str],
    processed_load_keys: Set[Tuple[str, str]],
    dependency_load_stack: Set[Tuple[str, str]],
    result_rows: List[Dict[str, Any]],
    dry_run: bool,
    fallback_to_salesforce: bool,
    record_noop_skips: bool,
    batch_size: int,
    bulk_buffer: Optional[BulkOperationBuffer] = None,
) -> bool:
    dependency_key = (target_object, source_record_id)
    if current_load_record_ids.get(dependency_key):
        return True
    if dependency_key in dependency_load_stack:
        return False
    if dependency_key in processed_load_keys:
        return bool(current_load_record_ids.get(dependency_key))

    existing_target_id = find_record_id_in_target_extract(
        target_extract_root=target_extract_root,
        object_name=target_object,
        external_id_value=source_record_id,
        extract_lookup_cache=extract_lookup_cache,
    )
    if not existing_target_id and fallback_to_salesforce:
        existing_target_id = find_salesforce_record_id_by_external_id(
            sf=sf,
            object_name=target_object,
            external_id_value=source_record_id,
            describe_cache=describe_cache,
            lookup_cache=salesforce_lookup_cache,
        )
    if existing_target_id:
        current_load_record_ids[dependency_key] = existing_target_id
        return True

    diff_row = find_diff_row_by_source_id(
        checkpoint_root=checkpoint_root,
        target_object=target_object,
        source_objects_by_target=source_objects_by_target,
        source_record_id_col=source_record_id_col,
        target_record_id_col=target_record_id_col,
        source_value_col=source_value_col,
        source_load_value_col=source_load_value_col,
        source_record_id=source_record_id,
        change_types=change_types,
        batch_size=batch_size,
    )
    if diff_row is None:
        return False

    dependency_load_stack.add(dependency_key)
    try:
        describe = get_object_describe(sf, target_object, describe_cache)
        field_defs = {
            field_def["name"]: field_def
            for field_def in describe.get("fields", [])
            if field_def.get("name")
        }
        relationship_field_defs = {
            str(field_def.get("relationshipName")).strip(): field_def
            for field_def in field_defs.values()
            if field_def.get("relationshipName")
        }
        target_external_id_field = field_defs.get("External_Id__c")
        target_record_id = normalize_blank(diff_row.get("target_record_id"))
        source_object = normalize_blank(diff_row.get("source_object"))
        source_payload = merge_load_payload(
            source_payload=diff_row.get("source_payload") or {},
            source_load_payload=diff_row.get("source_load_payload") or {},
        )
        operation = resolve_operation(
            target_record_id=target_record_id,
            source_record_id=source_record_id,
            target_external_id_field=target_external_id_field,
        )
        if operation == "create" and source_record_id and target_external_id_field:
            existing_target_record_id = find_target_record_id_by_external_id(
                sf=sf,
                target_object=target_object,
                source_record_id=source_record_id,
                target_external_id_field=target_external_id_field,
                lookup_cache=salesforce_lookup_cache,
            )
            if existing_target_record_id:
                target_record_id = existing_target_record_id
                operation = "update"

        write_operation_hint = operation_hint_for_operation(
            operation=operation,
            target_record_id=target_record_id,
        )
        mapped_payload, skipped_fields = map_source_payload_to_target(
            source_object=source_object,
            source_payload=source_payload,
            target_object=target_object,
            field_mappings=field_mappings,
            field_defs=field_defs,
            relationship_field_defs=relationship_field_defs,
            operation_hint=write_operation_hint,
        )
        routed_opportunity_contact_source_id = get_routed_opportunity_contact_source_id(
            target_object=target_object,
            payload=mapped_payload,
        )
        if not mapped_payload:
            if routed_opportunity_contact_source_id:
                result_rows.append(
                    build_result_row(
                        target_env=target_env,
                        source_env=source_env,
                        target_object=target_object,
                        source_object=source_object,
                        source_record_id=source_record_id,
                        target_record_id=target_record_id,
                        change_type=normalize_blank(diff_row.get("change_type")) or "",
                        operation="skip",
                        dry_run=dry_run,
                        success=False,
                        payload={},
                        skipped_fields=skipped_fields,
                        message="No writable mapped fields",
                    )
                )
            return False

        payload_error = prepare_payload_for_operation(
            payload=mapped_payload,
            operation=operation,
            source_record_id=source_record_id,
            target_external_id_field=target_external_id_field,
            operation_hint=write_operation_hint,
        )
        if payload_error:
            result_row = build_result_row(
                target_env=target_env,
                source_env=source_env,
                target_object=target_object,
                source_object=source_object,
                source_record_id=source_record_id,
                target_record_id=target_record_id,
                change_type=normalize_blank(diff_row.get("change_type")) or "",
                operation=operation,
                dry_run=dry_run,
                success=False,
                payload=mapped_payload,
                skipped_fields=skipped_fields,
                message=payload_error,
            )
            result_rows.append(result_row)
            processed_load_keys.add(dependency_key)
            return False

        ensure_missing_relationship_dependencies_loaded(
            payload=mapped_payload,
            relationship_field_defs=relationship_field_defs,
            sf=sf,
            target_env=target_env,
            source_env=source_env,
            checkpoint_root=checkpoint_root,
            target_extract_root=target_extract_root,
            source_objects_by_target=source_objects_by_target,
            field_mappings=field_mappings,
            load_object_set=load_object_set,
            source_record_id_col=source_record_id_col,
            target_record_id_col=target_record_id_col,
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            change_types=change_types,
            describe_cache=describe_cache,
            extract_lookup_cache=extract_lookup_cache,
            salesforce_lookup_cache=salesforce_lookup_cache,
            current_load_record_ids=current_load_record_ids,
            processed_load_keys=processed_load_keys,
            dependency_load_stack=dependency_load_stack,
            result_rows=result_rows,
            dry_run=dry_run,
            fallback_to_salesforce=fallback_to_salesforce,
            record_noop_skips=record_noop_skips,
            batch_size=batch_size,
            bulk_buffer=bulk_buffer,
        )
        resolve_relationship_payload_to_ids(
            payload=mapped_payload,
            relationship_field_defs=relationship_field_defs,
            sf=sf,
            describe_cache=describe_cache,
            target_extract_root=target_extract_root,
            extract_lookup_cache=extract_lookup_cache,
            salesforce_lookup_cache=salesforce_lookup_cache,
            current_load_record_ids=current_load_record_ids,
            skipped_fields=skipped_fields,
            fallback_to_salesforce=fallback_to_salesforce,
        )
        if not mapped_payload:
            processed_load_keys.add(dependency_key)
            if record_noop_skips:
                result_rows.append(
                    build_result_row(
                        target_env=target_env,
                        source_env=source_env,
                        target_object=target_object,
                        source_object=source_object,
                        source_record_id=source_record_id,
                        target_record_id=target_record_id,
                        change_type=normalize_blank(diff_row.get("change_type")) or "",
                        operation="skip",
                        dry_run=dry_run,
                        success=True,
                        payload={},
                        skipped_fields=skipped_fields,
                        message="No direct Opportunity fields to write after relationship resolution; ContactId routed to OpportunityContactRole",
                    )
                )
                contact_role_result = sync_opportunity_contact_role_from_opportunity_contact_id(
                    sf=sf,
                    target_env=target_env,
                    source_env=source_env,
                    source_opportunity_id=source_record_id,
                    target_opportunity_id=target_record_id,
                    source_contact_id=routed_opportunity_contact_source_id,
                    change_type=normalize_blank(diff_row.get("change_type")) or "",
                    dry_run=dry_run,
                    target_extract_root=target_extract_root,
                    extract_lookup_cache=extract_lookup_cache,
                    salesforce_lookup_cache=salesforce_lookup_cache,
                    current_load_record_ids=current_load_record_ids,
                    describe_cache=describe_cache,
                    checkpoint_root=checkpoint_root,
                    source_objects_by_target=source_objects_by_target,
                    field_mappings=field_mappings,
                    load_object_set=load_object_set,
                    source_record_id_col=source_record_id_col,
                    target_record_id_col=target_record_id_col,
                    source_value_col=source_value_col,
                    source_load_value_col=source_load_value_col,
                    change_types=change_types,
                    processed_load_keys=processed_load_keys,
                    dependency_load_stack=dependency_load_stack,
                    result_rows=result_rows,
                    fallback_to_salesforce=fallback_to_salesforce,
                    record_noop_skips=record_noop_skips,
                    batch_size=batch_size,
                    bulk_buffer=bulk_buffer,
                )
                if contact_role_result is not None:
                    result_rows.append(contact_role_result)
                processed_load_keys.add(dependency_key)
                return (
                    contact_role_result is None
                    or str(contact_role_result.get("Success", "")).upper() == "TRUE"
                )
            if record_noop_skips:
                result_rows.append(
                    build_result_row(
                        target_env=target_env,
                        source_env=source_env,
                        target_object=target_object,
                        source_object=source_object,
                        source_record_id=source_record_id,
                        target_record_id=target_record_id,
                        change_type=normalize_blank(diff_row.get("change_type")) or "",
                        operation="skip",
                        dry_run=dry_run,
                        success=False,
                        payload={},
                        skipped_fields=skipped_fields,
                        message="No writable mapped fields after relationship resolution",
                    )
                )
            processed_load_keys.add(dependency_key)
            return False

        natural_key_target_record_id = find_target_record_id_by_natural_key(
            sf=sf,
            target_object=target_object,
            payload=mapped_payload,
        )
        if natural_key_target_record_id:
            target_record_id = natural_key_target_record_id
            operation = "update"
            add_external_id_for_natural_key_update(
                payload=mapped_payload,
                source_record_id=source_record_id,
                target_external_id_field=target_external_id_field,
                skipped_fields=skipped_fields,
            )
        apply_object_specific_payload_rules(
            payload=mapped_payload,
            target_object=target_object,
            operation=operation,
            target_record_id=target_record_id,
            field_defs=field_defs,
            skipped_fields=skipped_fields,
        )
        apply_source_target_payload_rules(
            payload=mapped_payload,
            source_object=source_object,
            target_object=target_object,
            operation=operation,
            target_record_id=target_record_id,
            field_defs=field_defs,
            skipped_fields=skipped_fields,
            sf=sf,
            describe_cache=describe_cache,
        )
        if not mapped_payload:
            if routed_opportunity_contact_source_id:
                result_rows.append(
                    build_result_row(
                        target_env=target_env,
                        source_env=source_env,
                        target_object=target_object,
                        source_object=source_object,
                        source_record_id=source_record_id,
                        target_record_id=target_record_id,
                        change_type=normalize_blank(diff_row.get("change_type")) or "",
                        operation="skip",
                        dry_run=dry_run,
                        success=True,
                        payload={},
                        skipped_fields=skipped_fields,
                        message="No direct Opportunity fields to write; ContactId routed to OpportunityContactRole",
                    )
                )
                contact_role_result = sync_opportunity_contact_role_from_opportunity_contact_id(
                    sf=sf,
                    target_env=target_env,
                    source_env=source_env,
                    source_opportunity_id=source_record_id,
                    target_opportunity_id=target_record_id,
                    source_contact_id=routed_opportunity_contact_source_id,
                    change_type=normalize_blank(diff_row.get("change_type")) or "",
                    dry_run=dry_run,
                    target_extract_root=target_extract_root,
                    extract_lookup_cache=extract_lookup_cache,
                    salesforce_lookup_cache=salesforce_lookup_cache,
                    current_load_record_ids=current_load_record_ids,
                    describe_cache=describe_cache,
                    checkpoint_root=checkpoint_root,
                    source_objects_by_target=source_objects_by_target,
                    field_mappings=field_mappings,
                    load_object_set=load_object_set,
                    source_record_id_col=source_record_id_col,
                    target_record_id_col=target_record_id_col,
                    source_value_col=source_value_col,
                    source_load_value_col=source_load_value_col,
                    change_types=change_types,
                    processed_load_keys=processed_load_keys,
                    dependency_load_stack=dependency_load_stack,
                    result_rows=result_rows,
                    fallback_to_salesforce=fallback_to_salesforce,
                    record_noop_skips=record_noop_skips,
                    batch_size=batch_size,
                    bulk_buffer=bulk_buffer,
                )
                if contact_role_result is not None:
                    result_rows.append(contact_role_result)
                    contact_role_success = str(contact_role_result.get("Success", "")).upper() == "TRUE"
                else:
                    contact_role_success = True
            elif record_noop_skips:
                result_rows.append(
                    build_result_row(
                        target_env=target_env,
                        source_env=source_env,
                        target_object=target_object,
                        source_object=source_object,
                        source_record_id=source_record_id,
                        target_record_id=target_record_id,
                        change_type=normalize_blank(diff_row.get("change_type")) or "",
                        operation="skip",
                        dry_run=dry_run,
                        success=False,
                        payload={},
                        skipped_fields=skipped_fields,
                        message="No writable mapped fields after object-specific payload rules",
                    )
                )
                contact_role_success = False
            else:
                contact_role_success = False
            processed_load_keys.add(dependency_key)
            return contact_role_success

        if normalize_blank(diff_row.get("change_type", "")).startswith("missing_from_") and operation == "update":
            filter_payload_to_actual_deltas(
                sf=sf,
                target_object=target_object,
                target_record_id=target_record_id,
                payload=mapped_payload,
                field_defs=field_defs,
                skipped_fields=skipped_fields,
            )
            if not mapped_payload:
                result_row = build_result_row(
                    target_env=target_env,
                    source_env=source_env,
                    target_object=target_object,
                    source_object=source_object,
                    source_record_id=source_record_id,
                    target_record_id=target_record_id,
                    change_type=normalize_blank(diff_row.get("change_type")) or "",
                    operation="skip",
                    dry_run=dry_run,
                    success=True,
                    payload={},
                    skipped_fields=skipped_fields,
                    message="No actual deltas after target lookup; no Salesforce write performed",
                )
                result_rows.append(result_row)
                processed_load_keys.add(dependency_key)
                return True

        result_row = execute_or_preview_operation(
            sf=sf,
            target_env=target_env,
            source_env=source_env,
            target_object=target_object,
            source_object=source_object,
            source_record_id=source_record_id,
            target_record_id=target_record_id,
            change_type=normalize_blank(diff_row.get("change_type")) or "",
            operation=operation,
            payload=mapped_payload,
            skipped_fields=skipped_fields,
            dry_run=dry_run,
            single_record_reason="relationship dependency required before child lookup can resolve",
        )
        result_rows.append(result_row)
        remember_loaded_record_id(
            result_row=result_row,
            target_object=target_object,
            source_record_id=source_record_id,
            current_load_record_ids=current_load_record_ids,
        )
        if (
            routed_opportunity_contact_source_id
            and str(result_row.get("Success", "")).upper() == "TRUE"
        ):
            contact_role_result = sync_opportunity_contact_role_from_opportunity_contact_id(
                sf=sf,
                target_env=target_env,
                source_env=source_env,
                source_opportunity_id=source_record_id,
                target_opportunity_id=normalize_blank(result_row.get("Target_RecordId")) or target_record_id,
                source_contact_id=routed_opportunity_contact_source_id,
                change_type=normalize_blank(diff_row.get("change_type")) or "",
                dry_run=dry_run,
                target_extract_root=target_extract_root,
                extract_lookup_cache=extract_lookup_cache,
                salesforce_lookup_cache=salesforce_lookup_cache,
                current_load_record_ids=current_load_record_ids,
                describe_cache=describe_cache,
                checkpoint_root=checkpoint_root,
                source_objects_by_target=source_objects_by_target,
                field_mappings=field_mappings,
                load_object_set=load_object_set,
                source_record_id_col=source_record_id_col,
                target_record_id_col=target_record_id_col,
                source_value_col=source_value_col,
                source_load_value_col=source_load_value_col,
                change_types=change_types,
                processed_load_keys=processed_load_keys,
                dependency_load_stack=dependency_load_stack,
                result_rows=result_rows,
                fallback_to_salesforce=fallback_to_salesforce,
                record_noop_skips=record_noop_skips,
                batch_size=batch_size,
                bulk_buffer=bulk_buffer,
            )
            if contact_role_result is not None:
                result_rows.append(contact_role_result)
        processed_load_keys.add(dependency_key)
        return str(result_row.get("Success", "")).upper() == "TRUE"
    finally:
        dependency_load_stack.discard(dependency_key)


def apply_object_specific_payload_rules(
    payload: Dict[str, Any],
    target_object: str,
    operation: str,
    target_record_id: Optional[str],
    field_defs: Dict[str, Dict[str, Any]],
    skipped_fields: List[str],
) -> None:
    if target_object == "Opportunity" and "ContactId" in payload:
        payload.pop("ContactId", None)
        skipped_fields.append(
            "ContactId: routed to OpportunityContactRole because Opportunity.ContactId is not writable"
        )

    skip_catalog_lookup_update_fields(
        payload=payload,
        target_object=target_object,
        operation=operation,
        skipped_fields=skipped_fields,
    )

    if target_object == "AccountContactRelation" and operation == "update":
        for field_name in ("AccountId", "ContactId"):
            if field_name in payload:
                payload.pop(field_name, None)
                skipped_fields.append(
                    f"{field_name}: skipped for AccountContactRelation update; used only for natural-key matching"
                )

    if target_object == "Quote" and operation == "update" and "OwnerId" in payload:
        payload.pop("OwnerId", None)
        skipped_fields.append("OwnerId: skipped for Quote update because Salesforce rejected owner changes")

    if (
        target_object == "OpportunityContactRole"
        and operation == "update"
    ):
        for field_name in ("OpportunityId", "ContactId"):
            if field_name in payload:
                payload.pop(field_name, None)
                skipped_fields.append(
                    f"{field_name}: skipped for OpportunityContactRole update; used only for natural-key matching"
                )

    if target_object == "Account_Relationship__c" and operation == "update":
        for field_name in ("Main_Account__c", "Secondary_Account__c"):
            if field_name in payload:
                payload.pop(field_name, None)
                skipped_fields.append(
                    f"{field_name}: skipped for Account_Relationship__c update; used only for natural-key matching"
                )

    if target_object == "OpportunityLineItem" and "UnitPrice" in payload and "TotalPrice" in payload:
        payload.pop("TotalPrice", None)
        skipped_fields.append("TotalPrice: skipped because Salesforce allows UnitPrice or TotalPrice, not both")

    if target_object == "Contact" and operation == "update" and "ContactD365Id__c" in payload:
        payload.pop("ContactD365Id__c", None)
        skipped_fields.append("ContactD365Id__c: skipped for Contact update to avoid duplicate external system id failure")

    if (
        target_object == "Subscriptions__c"
        and "SL_Reason_for_Product_Change__c" in field_defs
        and "SL_Reason_for_Product_Change__c" not in payload
        and is_field_writable(field_defs["SL_Reason_for_Product_Change__c"], operation_hint_for_operation(operation, target_record_id))
    ):
        payload["SL_Reason_for_Product_Change__c"] = "Data Migration"
        skipped_fields.append("SL_Reason_for_Product_Change__c: defaulted to Data Migration")


def skip_catalog_lookup_update_fields(
    payload: Dict[str, Any],
    target_object: str,
    operation: str,
    skipped_fields: List[str],
) -> None:
    if operation != "update":
        return

    catalog_lookup_fields_by_object = {
        "Opportunity": ("Pricebook2Id", "Pricebook2"),
        "Quote": ("Pricebook2Id", "Pricebook2"),
        "OpportunityLineItem": ("PricebookEntryId", "PricebookEntry", "Product2Id", "Product2"),
        "QuoteLineItem": ("PricebookEntryId", "PricebookEntry", "Product2Id", "Product2"),
    }
    catalog_lookup_fields = catalog_lookup_fields_by_object.get(target_object)
    if not catalog_lookup_fields:
        return

    for field_name in catalog_lookup_fields:
        if field_name in payload:
            payload.pop(field_name, None)
            skipped_fields.append(
                f"{field_name}: skipped on {target_object} update because "
                "Pricebook2/Product2/PricebookEntry are sourced from MCUAT, not SF 1.0 Prod"
            )


def apply_source_target_payload_rules(
    payload: Dict[str, Any],
    source_object: str,
    target_object: str,
    operation: str,
    target_record_id: Optional[str],
    field_defs: Dict[str, Dict[str, Any]],
    skipped_fields: List[str],
    sf,
    describe_cache: Dict[str, Dict[str, Any]],
) -> None:
    if source_object != "Brand__c" or target_object != "Account":
        return

    operation_hint = operation_hint_for_operation(operation, target_record_id)
    if operation_hint != "create":
        return

    record_type_field = field_defs.get("RecordTypeId")
    if not record_type_field or not is_field_writable(record_type_field, operation_hint):
        skipped_fields.append("RecordTypeId: Brand account record type could not be set because field is not createable")
        return

    record_type_id = get_record_type_id_by_developer_name(
        sf=sf,
        object_name="Account",
        developer_name="Brand",
        describe_cache=describe_cache,
    )
    if not record_type_id:
        skipped_fields.append("RecordTypeId: Brand account record type was not found")
        return

    payload.setdefault("RecordTypeId", record_type_id)
    skipped_fields.append("RecordTypeId: defaulted to Account record type Brand for legacy Brand__c record")


def add_external_id_for_natural_key_update(
    payload: Dict[str, Any],
    source_record_id: Optional[str],
    target_external_id_field: Optional[Dict[str, Any]],
    skipped_fields: List[str],
) -> None:
    clean_source_record_id = normalize_blank(source_record_id)
    if not clean_source_record_id or "External_Id__c" in payload:
        return

    if not target_external_id_field:
        skipped_fields.append("External_Id__c: not available to backfill after natural-key match")
        return

    if not is_field_writable(target_external_id_field, "update"):
        skipped_fields.append("External_Id__c: not updateable after natural-key match")
        return

    payload["External_Id__c"] = clean_source_record_id
    skipped_fields.append("External_Id__c: backfilled after existing target record was found by natural key")


def get_routed_opportunity_contact_source_id(
    target_object: str,
    payload: Dict[str, Any],
) -> Optional[str]:
    if target_object != "Opportunity":
        return None

    direct_contact_id = normalize_blank(payload.get("ContactId"))
    if direct_contact_id:
        return direct_contact_id

    contact_relationship = payload.get("Contact")
    if isinstance(contact_relationship, dict):
        return normalize_blank(contact_relationship.get("External_Id__c"))

    return None


def sync_opportunity_contact_role_from_opportunity_contact_id(
    sf,
    target_env: str,
    source_env: str,
    source_opportunity_id: Optional[str],
    target_opportunity_id: Optional[str],
    source_contact_id: Optional[str],
    change_type: str,
    dry_run: bool,
    target_extract_root: Optional[Path],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    salesforce_lookup_cache: Dict[Tuple[str, str], Optional[str]],
    current_load_record_ids: Dict[Tuple[str, str], str],
    describe_cache: Dict[str, Dict[str, Any]],
    checkpoint_root: Path,
    source_objects_by_target: Dict[str, Set[str]],
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    load_object_set: Set[str],
    source_record_id_col: str,
    target_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    change_types: Set[str],
    processed_load_keys: Set[Tuple[str, str]],
    dependency_load_stack: Set[Tuple[str, str]],
    result_rows: List[Dict[str, Any]],
    fallback_to_salesforce: bool,
    record_noop_skips: bool,
    batch_size: int,
    bulk_buffer: Optional[BulkOperationBuffer] = None,
    opportunity_contact_role_cache: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    skipped_fields: List[str] = [
        "Opportunity.ContactId routed into OpportunityContactRole"
    ]
    clean_source_opportunity_id = normalize_blank(source_opportunity_id)
    clean_source_contact_id = normalize_blank(source_contact_id)

    def mark_processed() -> None:
        if clean_source_opportunity_id:
            processed_load_keys.add(("OpportunityContactRole", clean_source_opportunity_id))

    if not clean_source_opportunity_id or not clean_source_contact_id:
        mark_processed()
        return build_result_row(
            target_env=target_env,
            source_env=source_env,
            target_object="OpportunityContactRole",
            source_object="Opportunity",
            source_record_id=clean_source_opportunity_id,
            target_record_id=None,
            change_type=change_type,
            operation="skip",
            dry_run=dry_run,
            success=False,
            payload={},
            skipped_fields=skipped_fields,
            message="Cannot route Opportunity.ContactId because source OpportunityId or ContactId is blank",
        )

    clean_target_opportunity_id = resolve_target_record_id_from_external_id(
        object_name="Opportunity",
        external_id_value=clean_source_opportunity_id,
        existing_target_record_id=target_opportunity_id,
        sf=sf,
        describe_cache=describe_cache,
        target_extract_root=target_extract_root,
        extract_lookup_cache=extract_lookup_cache,
        salesforce_lookup_cache=salesforce_lookup_cache,
        current_load_record_ids=current_load_record_ids,
        fallback_to_salesforce=fallback_to_salesforce,
    )
    if not clean_target_opportunity_id:
        mark_processed()
        return build_result_row(
            target_env=target_env,
            source_env=source_env,
            target_object="OpportunityContactRole",
            source_object="Opportunity",
            source_record_id=clean_source_opportunity_id,
            target_record_id=None,
            change_type=change_type,
            operation="skip",
            dry_run=dry_run,
            success=False,
            payload={},
            skipped_fields=skipped_fields,
            message="Cannot route Opportunity.ContactId because target Opportunity was not found",
        )

    clean_target_contact_id = resolve_target_record_id_from_external_id(
        object_name="Contact",
        external_id_value=clean_source_contact_id,
        existing_target_record_id=None,
        sf=sf,
        describe_cache=describe_cache,
        target_extract_root=target_extract_root,
        extract_lookup_cache=extract_lookup_cache,
        salesforce_lookup_cache=salesforce_lookup_cache,
        current_load_record_ids=current_load_record_ids,
        fallback_to_salesforce=fallback_to_salesforce,
    )
    if not clean_target_contact_id and "Contact" in load_object_set:
        load_dependency_record(
            sf=sf,
            target_env=target_env,
            source_env=source_env,
            target_object="Contact",
            source_record_id=clean_source_contact_id,
            checkpoint_root=checkpoint_root,
            target_extract_root=target_extract_root,
            source_objects_by_target=source_objects_by_target,
            field_mappings=field_mappings,
            load_object_set=load_object_set,
            source_record_id_col=source_record_id_col,
            target_record_id_col=target_record_id_col,
            source_value_col=source_value_col,
            source_load_value_col=source_load_value_col,
            change_types=change_types,
            describe_cache=describe_cache,
            extract_lookup_cache=extract_lookup_cache,
            salesforce_lookup_cache=salesforce_lookup_cache,
            current_load_record_ids=current_load_record_ids,
            processed_load_keys=processed_load_keys,
            dependency_load_stack=dependency_load_stack,
            result_rows=result_rows,
            dry_run=dry_run,
            fallback_to_salesforce=fallback_to_salesforce,
            record_noop_skips=record_noop_skips,
            batch_size=batch_size,
            bulk_buffer=bulk_buffer,
        )
        clean_target_contact_id = resolve_target_record_id_from_external_id(
            object_name="Contact",
            external_id_value=clean_source_contact_id,
            existing_target_record_id=None,
            sf=sf,
            describe_cache=describe_cache,
            target_extract_root=target_extract_root,
            extract_lookup_cache=extract_lookup_cache,
            salesforce_lookup_cache=salesforce_lookup_cache,
            current_load_record_ids=current_load_record_ids,
            fallback_to_salesforce=fallback_to_salesforce,
        )

    if not clean_target_contact_id:
        mark_processed()
        return build_result_row(
            target_env=target_env,
            source_env=source_env,
            target_object="OpportunityContactRole",
            source_object="Opportunity",
            source_record_id=clean_source_opportunity_id,
            target_record_id=None,
            change_type=change_type,
            operation="skip",
            dry_run=dry_run,
            success=False,
            payload={},
            skipped_fields=skipped_fields,
            message=(
                "Cannot route Opportunity.ContactId because target Contact was not found "
                f"for source ContactId {clean_source_contact_id}"
            ),
        )

    preload_opportunity_contact_role_cache(
        sf=sf,
        opportunity_ids=[clean_target_opportunity_id],
        opportunity_contact_role_cache=opportunity_contact_role_cache,
    )
    existing_contact_role = find_opportunity_contact_role(
        sf=sf,
        opportunity_id=clean_target_opportunity_id,
        contact_id=clean_target_contact_id,
        opportunity_contact_role_cache=opportunity_contact_role_cache,
    )
    if existing_contact_role:
        contact_role_id = normalize_blank(existing_contact_role.get("Id"))
        is_primary = existing_contact_role.get("IsPrimary")
        if is_primary is True:
            mark_processed()
            return build_result_row(
                target_env=target_env,
                source_env=source_env,
                target_object="OpportunityContactRole",
                source_object="Opportunity",
                source_record_id=clean_source_opportunity_id,
                target_record_id=contact_role_id,
                change_type=change_type,
                operation="skip",
                dry_run=dry_run,
                success=True,
                payload={},
                skipped_fields=skipped_fields,
                message="OpportunityContactRole already exists and is primary; no Salesforce write performed",
            )

        payload = {"IsPrimary": True}
        if bulk_buffer is not None and not dry_run:
            bulk_buffer.add(
                target_object="OpportunityContactRole",
                source_object="Opportunity",
                source_record_id=clean_source_opportunity_id,
                target_record_id=contact_role_id,
                change_type=change_type,
                operation="update",
                payload=payload,
                skipped_fields=skipped_fields,
            )
            mark_processed()
            return None

        mark_processed()
        return execute_or_preview_operation(
            sf=sf,
            target_env=target_env,
            source_env=source_env,
            target_object="OpportunityContactRole",
            source_object="Opportunity",
            source_record_id=clean_source_opportunity_id,
            target_record_id=contact_role_id,
            change_type=change_type,
            operation="update",
            payload=payload,
            skipped_fields=skipped_fields,
            dry_run=dry_run,
            single_record_reason="Opportunity.ContactId OCR routing with Bulk API unavailable in caller",
        )

    payload = {
        "OpportunityId": clean_target_opportunity_id,
        "ContactId": clean_target_contact_id,
        "IsPrimary": True,
    }
    if bulk_buffer is not None and not dry_run:
        bulk_buffer.add(
            target_object="OpportunityContactRole",
            source_object="Opportunity",
            source_record_id=clean_source_opportunity_id,
            target_record_id=None,
            change_type=change_type,
            operation="create",
            payload=payload,
            skipped_fields=skipped_fields,
        )
        mark_processed()
        return None

    mark_processed()
    return execute_or_preview_operation(
        sf=sf,
        target_env=target_env,
        source_env=source_env,
        target_object="OpportunityContactRole",
        source_object="Opportunity",
        source_record_id=clean_source_opportunity_id,
        target_record_id=None,
        change_type=change_type,
        operation="create",
        payload=payload,
        skipped_fields=skipped_fields,
        dry_run=dry_run,
        single_record_reason="Opportunity.ContactId OCR routing with Bulk API unavailable in caller",
    )


def preload_opportunity_contact_role_natural_keys_for_batch(
    sf,
    target_object: str,
    batch_rows: Dict[str, List[Any]],
    row_count: int,
    source_record_id_col: str,
    target_record_id_col: str,
    source_value_col: str,
    source_load_value_col: str,
    change_types: Set[str],
    field_mappings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    field_defs: Dict[str, Dict[str, Any]],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    target_external_id_field: Optional[Dict[str, Any]],
    processed_load_keys: Set[Tuple[str, str]],
    target_extract_root: Optional[Path],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    salesforce_lookup_cache: Dict[Tuple[str, str], Optional[str]],
    current_load_record_ids: Dict[Tuple[str, str], str],
    describe_cache: Dict[str, Dict[str, Any]],
    fallback_to_salesforce: bool,
    opportunity_contact_role_cache: Optional[Dict[str, Any]],
    extra_field_names: Optional[Iterable[str]] = None,
) -> None:
    if target_object != "OpportunityContactRole" or opportunity_contact_role_cache is None:
        return

    opportunity_ids: Set[str] = set()
    for row_index in range(row_count):
        change_type = normalize_blank(batch_rows["change_type"][row_index])
        if change_type not in change_types:
            continue

        source_record_id = normalize_blank(batch_rows[source_record_id_col][row_index])
        if not source_record_id or (target_object, source_record_id) in processed_load_keys:
            continue

        source_object = normalize_blank(batch_rows["Obj"][row_index])
        source_payload = parse_json_dict(batch_rows[source_value_col][row_index])
        source_load_payload = (
            parse_json_dict(batch_rows[source_load_value_col][row_index])
            if source_load_value_col in batch_rows
            else {}
        )
        source_payload = merge_load_payload(
            source_payload=source_payload,
            source_load_payload=source_load_payload,
        )
        target_record_id = normalize_blank(batch_rows[target_record_id_col][row_index])
        operation = resolve_operation(
            target_record_id=target_record_id,
            source_record_id=source_record_id,
            target_external_id_field=target_external_id_field,
        )
        operation_hint = operation_hint_for_operation(
            operation=operation,
            target_record_id=target_record_id,
        )
        mapped_payload, _ = map_source_payload_to_target(
            source_object=source_object,
            source_payload=source_payload,
            target_object=target_object,
            field_mappings=field_mappings,
            field_defs=field_defs,
            relationship_field_defs=relationship_field_defs,
            operation_hint=operation_hint,
        )
        if not mapped_payload:
            continue

        source_opportunity_id = get_reference_external_id_from_payload(
            payload=mapped_payload,
            relationship_field_defs=relationship_field_defs,
            field_name="OpportunityId",
            default_relationship_name="Opportunity",
        )
        source_contact_id = get_reference_external_id_from_payload(
            payload=mapped_payload,
            relationship_field_defs=relationship_field_defs,
            field_name="ContactId",
            default_relationship_name="Contact",
        )
        if not source_opportunity_id or not source_contact_id:
            continue

        target_opportunity_id = resolve_target_record_id_from_external_id(
            object_name="Opportunity",
            external_id_value=source_opportunity_id,
            existing_target_record_id=None,
            sf=sf,
            describe_cache=describe_cache,
            target_extract_root=target_extract_root,
            extract_lookup_cache=extract_lookup_cache,
            salesforce_lookup_cache=salesforce_lookup_cache,
            current_load_record_ids=current_load_record_ids,
            fallback_to_salesforce=fallback_to_salesforce,
        )
        target_contact_id = resolve_target_record_id_from_external_id(
            object_name="Contact",
            external_id_value=source_contact_id,
            existing_target_record_id=None,
            sf=sf,
            describe_cache=describe_cache,
            target_extract_root=target_extract_root,
            extract_lookup_cache=extract_lookup_cache,
            salesforce_lookup_cache=salesforce_lookup_cache,
            current_load_record_ids=current_load_record_ids,
            fallback_to_salesforce=fallback_to_salesforce,
        )
        if target_opportunity_id and target_contact_id:
            opportunity_ids.add(target_opportunity_id)

    preload_opportunity_contact_role_cache(
        sf=sf,
        opportunity_ids=opportunity_ids,
        opportunity_contact_role_cache=opportunity_contact_role_cache,
        extra_field_names=extra_field_names,
    )


def get_reference_external_id_from_payload(
    payload: Dict[str, Any],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    field_name: str,
    default_relationship_name: str,
) -> Optional[str]:
    relationship_value = payload.get(default_relationship_name)
    if isinstance(relationship_value, dict):
        external_id_value = normalize_blank(relationship_value.get("External_Id__c"))
        if external_id_value:
            return external_id_value

    for relationship_name, field_def in relationship_field_defs.items():
        if normalize_blank(field_def.get("name")) != field_name:
            continue
        relationship_value = payload.get(relationship_name)
        if isinstance(relationship_value, dict):
            external_id_value = normalize_blank(relationship_value.get("External_Id__c"))
            if external_id_value:
                return external_id_value

    return normalize_blank(payload.get(field_name))


def preload_opportunity_contact_role_cache(
    sf,
    opportunity_ids: Iterable[str],
    opportunity_contact_role_cache: Optional[Dict[str, Any]],
    extra_field_names: Optional[Iterable[str]] = None,
    chunk_size: int = 500,
) -> None:
    if opportunity_contact_role_cache is None:
        return

    loaded_opportunity_ids: Set[str] = opportunity_contact_role_cache.setdefault(
        "loaded_opportunity_ids",
        set(),
    )
    pending_opportunity_ids = sorted(
        {
            normalize_blank(opportunity_id)
            for opportunity_id in opportunity_ids
            if normalize_blank(opportunity_id)
            and normalize_blank(opportunity_id) not in loaded_opportunity_ids
        }
    )
    if not pending_opportunity_ids:
        return

    print(
        "Status: preloading existing OpportunityContactRole natural keys "
        f"for {len(pending_opportunity_ids)} Opportunity record(s)"
    )
    query_fields = ["Id", "OpportunityId", "ContactId", "IsPrimary"]
    for field_name in extra_field_names or []:
        clean_field_name = normalize_blank(field_name)
        if clean_field_name and clean_field_name not in query_fields:
            query_fields.append(clean_field_name)

    for opportunity_id_chunk in chunked(pending_opportunity_ids, chunk_size):
        quoted_opportunity_ids = ", ".join(
            f"'{escape_soql_string(opportunity_id)}'"
            for opportunity_id in opportunity_id_chunk
        )
        soql = (
            f"SELECT {', '.join(query_fields)} "
            "FROM OpportunityContactRole "
            f"WHERE OpportunityId IN ({quoted_opportunity_ids})"
        )
        try:
            records = query_all_salesforce_records(sf, soql)
        except Exception as exc:
            print(
                "Status: OCR natural-key preload failed for "
                f"{len(opportunity_id_chunk)} Opportunity record(s): {exc}"
            )
            continue

        for record in records:
            remember_opportunity_contact_role_cache_entry(
                opportunity_contact_role_cache=opportunity_contact_role_cache,
                opportunity_id=normalize_blank(record.get("OpportunityId")),
                contact_id=normalize_blank(record.get("ContactId")),
                record_id=normalize_blank(record.get("Id")),
                is_primary=record.get("IsPrimary"),
                record_values=record,
            )
        loaded_opportunity_ids.update(opportunity_id_chunk)


def query_all_salesforce_records(sf, soql: str) -> List[Dict[str, Any]]:
    result = sf.query(soql)
    records = list(result.get("records", []) if isinstance(result, dict) else [])
    while isinstance(result, dict) and not result.get("done", True):
        result = sf.query_more(result["nextRecordsUrl"], True)
        records.extend(result.get("records", []))
    return [
        {
            key: value
            for key, value in record.items()
            if key != "attributes"
        }
        for record in records
    ]


def get_cached_opportunity_contact_role(
    opportunity_id: str,
    contact_id: str,
    opportunity_contact_role_cache: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if opportunity_contact_role_cache is None:
        return None

    roles_by_pair: Dict[Tuple[str, str], Dict[str, Any]] = opportunity_contact_role_cache.setdefault(
        "roles_by_pair",
        {},
    )
    return roles_by_pair.get((opportunity_id, contact_id))


def get_cached_opportunity_contact_role_by_id(
    target_record_id: Optional[str],
    opportunity_contact_role_cache: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    clean_target_record_id = normalize_blank(target_record_id)
    if not clean_target_record_id or opportunity_contact_role_cache is None:
        return None

    roles_by_id: Dict[str, Dict[str, Any]] = opportunity_contact_role_cache.setdefault(
        "roles_by_id",
        {},
    )
    return roles_by_id.get(clean_target_record_id)


def is_opportunity_contact_role_opportunity_loaded(
    opportunity_id: str,
    opportunity_contact_role_cache: Optional[Dict[str, Any]],
) -> bool:
    if opportunity_contact_role_cache is None:
        return False

    loaded_opportunity_ids: Set[str] = opportunity_contact_role_cache.setdefault(
        "loaded_opportunity_ids",
        set(),
    )
    return opportunity_id in loaded_opportunity_ids


def remember_opportunity_contact_role_cache_entry(
    opportunity_contact_role_cache: Optional[Dict[str, Any]],
    opportunity_id: Optional[str],
    contact_id: Optional[str],
    record_id: Optional[str],
    is_primary: Any,
    record_values: Optional[Dict[str, Any]] = None,
) -> None:
    if opportunity_contact_role_cache is None:
        return

    clean_opportunity_id = normalize_blank(opportunity_id)
    clean_contact_id = normalize_blank(contact_id)
    clean_record_id = normalize_blank(record_id)
    if not clean_opportunity_id or not clean_contact_id or not clean_record_id:
        return

    roles_by_pair: Dict[Tuple[str, str], Dict[str, Any]] = opportunity_contact_role_cache.setdefault(
        "roles_by_pair",
        {},
    )
    cache_entry = dict(record_values or {})
    cache_entry.update(
        {
            "Id": clean_record_id,
            "OpportunityId": clean_opportunity_id,
            "ContactId": clean_contact_id,
            "IsPrimary": is_primary,
        }
    )
    roles_by_pair[(clean_opportunity_id, clean_contact_id)] = cache_entry

    roles_by_id: Dict[str, Dict[str, Any]] = opportunity_contact_role_cache.setdefault(
        "roles_by_id",
        {},
    )
    roles_by_id[clean_record_id] = cache_entry


def remember_opportunity_contact_role_cache_from_operation(
    operation_row: Dict[str, Any],
    result_row: Dict[str, Any],
    opportunity_contact_role_cache: Optional[Dict[str, Any]],
) -> None:
    if str(result_row.get("Success", "")).upper() != "TRUE":
        return

    payload = operation_row.get("payload") or {}
    remember_opportunity_contact_role_cache_entry(
        opportunity_contact_role_cache=opportunity_contact_role_cache,
        opportunity_id=normalize_blank(payload.get("OpportunityId")),
        contact_id=normalize_blank(payload.get("ContactId")),
        record_id=normalize_blank(result_row.get("Target_RecordId")),
        is_primary=payload.get("IsPrimary"),
    )


def chunked(values: List[str], chunk_size: int) -> Iterable[List[str]]:
    for start_index in range(0, len(values), chunk_size):
        yield values[start_index:start_index + chunk_size]


def resolve_target_record_id_from_external_id(
    object_name: str,
    external_id_value: str,
    existing_target_record_id: Optional[str],
    sf,
    describe_cache: Dict[str, Dict[str, Any]],
    target_extract_root: Optional[Path],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    salesforce_lookup_cache: Dict[Tuple[str, str], Optional[str]],
    current_load_record_ids: Dict[Tuple[str, str], str],
    fallback_to_salesforce: bool,
) -> Optional[str]:
    clean_existing_target_record_id = normalize_blank(existing_target_record_id)
    if clean_existing_target_record_id:
        return clean_existing_target_record_id

    clean_external_id_value = normalize_blank(external_id_value)
    if not clean_external_id_value:
        return None

    loaded_record_id = current_load_record_ids.get((object_name, clean_external_id_value))
    if loaded_record_id:
        return loaded_record_id

    extract_record_id = find_record_id_in_target_extract(
        target_extract_root=target_extract_root,
        object_name=object_name,
        external_id_value=clean_external_id_value,
        extract_lookup_cache=extract_lookup_cache,
    )
    if extract_record_id:
        return extract_record_id

    if fallback_to_salesforce:
        return find_salesforce_record_id_by_external_id(
            sf=sf,
            object_name=object_name,
            external_id_value=clean_external_id_value,
            describe_cache=describe_cache,
            lookup_cache=salesforce_lookup_cache,
        )

    return None


def find_opportunity_contact_role(
    sf,
    opportunity_id: str,
    contact_id: str,
    opportunity_contact_role_cache: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    cached_contact_role = get_cached_opportunity_contact_role(
        opportunity_id=opportunity_id,
        contact_id=contact_id,
        opportunity_contact_role_cache=opportunity_contact_role_cache,
    )
    if cached_contact_role is not None:
        return cached_contact_role

    if is_opportunity_contact_role_opportunity_loaded(
        opportunity_id=opportunity_id,
        opportunity_contact_role_cache=opportunity_contact_role_cache,
    ):
        return None

    soql = (
        "SELECT Id, IsPrimary "
        "FROM OpportunityContactRole "
        f"WHERE OpportunityId = '{escape_soql_string(opportunity_id)}' "
        f"AND ContactId = '{escape_soql_string(contact_id)}' "
        "LIMIT 1"
    )
    try:
        result = sf.query(soql)
    except Exception:
        return None

    records = result.get("records", []) if isinstance(result, dict) else []
    if not records:
        return None

    contact_role = {
        key: value
        for key, value in records[0].items()
        if key != "attributes"
    }
    remember_opportunity_contact_role_cache_entry(
        opportunity_contact_role_cache=opportunity_contact_role_cache,
        opportunity_id=opportunity_id,
        contact_id=contact_id,
        record_id=normalize_blank(contact_role.get("Id")),
        is_primary=contact_role.get("IsPrimary"),
    )
    return contact_role


def filter_payload_to_actual_deltas(
    sf,
    target_object: str,
    target_record_id: Optional[str],
    payload: Dict[str, Any],
    field_defs: Dict[str, Dict[str, Any]],
    skipped_fields: List[str],
    current_values_override: Optional[Dict[str, Any]] = None,
) -> None:
    clean_target_record_id = normalize_blank(target_record_id)
    if not clean_target_record_id or not payload:
        return

    query_fields = [
        field_name
        for field_name in sorted(payload)
        if field_name in field_defs and field_name != "Id"
    ]
    if not query_fields:
        return

    current_values = current_values_override
    if current_values is None:
        current_values = query_current_target_values(
            sf=sf,
            target_object=target_object,
            target_record_id=clean_target_record_id,
            query_fields=query_fields,
        )
    if current_values is None:
        skipped_fields.append(
            "Actual delta check skipped: unable to query current target values"
        )
        return

    for field_name in list(payload.keys()):
        field_def = field_defs.get(field_name, {})
        if field_name not in current_values:
            continue

        if salesforce_values_equivalent(
            payload.get(field_name),
            current_values.get(field_name),
            field_def,
        ):
            payload.pop(field_name, None)
            skipped_fields.append(f"{field_name}: already matches target value")


def query_current_target_values(
    sf,
    target_object: str,
    target_record_id: str,
    query_fields: List[str],
) -> Optional[Dict[str, Any]]:
    if not query_fields:
        return {}

    soql = (
        f"SELECT {', '.join(query_fields)} "
        f"FROM {target_object} "
        f"WHERE Id = '{escape_soql_string(target_record_id)}' "
        "LIMIT 1"
    )
    try:
        result = sf.query(soql)
    except Exception:
        return None

    records = result.get("records", []) if isinstance(result, dict) else []
    if not records:
        return None

    return {
        key: value
        for key, value in records[0].items()
        if key != "attributes"
    }


def salesforce_values_equivalent(
    source_value: Any,
    target_value: Any,
    field_def: Dict[str, Any],
) -> bool:
    field_type = field_def.get("type")
    if is_blank_value(source_value) and is_blank_value(target_value):
        return True

    if field_type == "boolean":
        return parse_bool_value(source_value) == parse_bool_value(target_value)

    if field_type in {"int", "double", "currency", "percent"}:
        return parse_float_value(source_value) == parse_float_value(target_value)

    return str(source_value).strip() == str(target_value).strip()


def is_blank_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (dict, list, tuple, set)):
        return False
    if pd.isna(value):
        return True
    return str(value).strip().lower() in {"", "none", "null", "nan", "<na>"}


def parse_bool_value(value: Any) -> Optional[bool]:
    if is_blank_value(value):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def parse_float_value(value: Any) -> Optional[float]:
    if is_blank_value(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def find_target_record_id_by_natural_key(
    sf,
    target_object: str,
    payload: Dict[str, Any],
    opportunity_contact_role_cache: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    if target_object not in {"AccountContactRelation", "OpportunityContactRole", "Account_Relationship__c"}:
        return None

    if target_object == "AccountContactRelation":
        account_id = normalize_blank(payload.get("AccountId"))
        contact_id = normalize_blank(payload.get("ContactId"))
        if not account_id or not contact_id:
            return None

        soql = (
            "SELECT Id FROM AccountContactRelation "
            f"WHERE AccountId = '{escape_soql_string(account_id)}' "
            f"AND ContactId = '{escape_soql_string(contact_id)}' "
            "LIMIT 1"
        )
    elif target_object == "OpportunityContactRole":
        opportunity_id = normalize_blank(payload.get("OpportunityId"))
        contact_id = normalize_blank(payload.get("ContactId"))
        if not opportunity_id or not contact_id:
            return None

        contact_role = find_opportunity_contact_role(
            sf=sf,
            opportunity_id=opportunity_id,
            contact_id=contact_id,
            opportunity_contact_role_cache=opportunity_contact_role_cache,
        )
        return normalize_blank(contact_role.get("Id")) if contact_role else None
    else:
        main_account_id = normalize_blank(payload.get("Main_Account__c"))
        secondary_account_id = normalize_blank(payload.get("Secondary_Account__c"))
        if not main_account_id or not secondary_account_id:
            return None

        party_type = normalize_blank(payload.get("Secondary_Party_Type__c"))
        where_clause = (
            f"Main_Account__c = '{escape_soql_string(main_account_id)}' "
            f"AND Secondary_Account__c = '{escape_soql_string(secondary_account_id)}'"
        )
        if party_type:
            where_clause += f" AND Secondary_Party_Type__c = '{escape_soql_string(party_type)}'"

        soql = (
            "SELECT Id FROM Account_Relationship__c "
            f"WHERE {where_clause} "
            "LIMIT 1"
        )

    try:
        result = sf.query(soql)
    except Exception:
        return None

    records = result.get("records", []) if isinstance(result, dict) else []
    return normalize_blank(records[0].get("Id")) if records else None


def execute_or_preview_operation(
    sf,
    target_env: str,
    source_env: str,
    target_object: str,
    source_object: str,
    source_record_id: Optional[str],
    target_record_id: Optional[str],
    change_type: str,
    operation: str,
    payload: Dict[str, Any],
    skipped_fields: List[str],
    dry_run: bool,
    single_record_reason: str = "",
) -> Dict[str, Any]:
    if operation == "upsert":
        payload = dict(payload)
        payload.pop("External_Id__c", None)

    payload_keys = sorted(payload)
    print(
        f"{'DRY RUN ' if dry_run else 'SINGLE '}RECORD {operation.upper()} "
        f"{target_env}.{target_object} "
        f"source_id={source_record_id or ''} target_id={target_record_id or ''} "
        f"fields={len(payload_keys)} [{', '.join(payload_keys[:8])}]"
        f"{f' reason={single_record_reason}' if single_record_reason else ''}"
    )

    if dry_run:
        return build_result_row(
            target_env=target_env,
            source_env=source_env,
            target_object=target_object,
            source_object=source_object,
            source_record_id=source_record_id,
            target_record_id=target_record_id,
            change_type=change_type,
            operation=operation,
            dry_run=dry_run,
            success=True,
            payload=payload,
            skipped_fields=skipped_fields,
            message="Dry run only; no Salesforce write performed",
        )

    try:
        object_client = getattr(sf, target_object)
        sf_result: Any

        if operation == "update":
            sf_result = object_client.update(target_record_id, payload)
            new_target_id = target_record_id
        elif operation == "upsert":
            external_id_value = quote(str(source_record_id), safe="")
            sf_result = object_client.upsert(f"External_Id__c/{external_id_value}", payload)
            new_target_id = target_record_id or find_target_record_id_after_upsert(
                sf=sf,
                target_object=target_object,
                source_record_id=source_record_id,
            )
        elif operation == "create":
            sf_result = object_client.create(payload)
            new_target_id = sf_result.get("id") if isinstance(sf_result, dict) else target_record_id
        else:
            raise ValueError(f"Unsupported operation: {operation}")

        print(f"SINGLE RECORD SUCCESS {operation.upper()} {target_env}.{target_object} id={new_target_id or ''}")
        return build_result_row(
            target_env=target_env,
            source_env=source_env,
            target_object=target_object,
            source_object=source_object,
            source_record_id=source_record_id,
            target_record_id=new_target_id,
            change_type=change_type,
            operation=operation,
            dry_run=dry_run,
            success=True,
            payload=payload,
            skipped_fields=skipped_fields,
            message=json.dumps(sf_result, default=str),
        )
    except Exception as exc:
        print(f"SINGLE RECORD FAILED {operation.upper()} {target_env}.{target_object}: {exc}")
        return build_result_row(
            target_env=target_env,
            source_env=source_env,
            target_object=target_object,
            source_object=source_object,
            source_record_id=source_record_id,
            target_record_id=target_record_id,
            change_type=change_type,
            operation=operation,
            dry_run=dry_run,
            success=False,
            payload=payload,
            skipped_fields=skipped_fields,
            message=str(exc),
        )


class BulkOperationBuffer:
    def __init__(
        self,
        sf,
        target_env: str,
        source_env: str,
        result_rows,
        current_load_record_ids: Dict[Tuple[str, str], str],
        bulk_batch_size: int,
        bulk_use_serial: bool,
        after_success: Optional[Callable[[Dict[str, Any], Dict[str, Any]], None]] = None,
    ) -> None:
        self.sf = sf
        self.target_env = target_env
        self.source_env = source_env
        self.result_rows = result_rows
        self.current_load_record_ids = current_load_record_ids
        self.bulk_batch_size = bulk_batch_size
        self.bulk_use_serial = bulk_use_serial
        self.after_success = after_success
        self.buffers: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    def add(
        self,
        target_object: str,
        source_object: str,
        source_record_id: Optional[str],
        target_record_id: Optional[str],
        change_type: str,
        operation: str,
        payload: Dict[str, Any],
        skipped_fields: List[str],
        post_success_actions: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        if operation not in {"create", "update", "upsert"}:
            raise ValueError(f"Unsupported bulk operation: {operation}")

        buffer_key = (target_object, operation)
        operation_rows = self.buffers.setdefault(buffer_key, [])
        operation_rows.append(
            {
                "target_object": target_object,
                "source_object": source_object,
                "source_record_id": source_record_id,
                "target_record_id": target_record_id,
                "change_type": change_type,
                "operation": operation,
                "payload": dict(payload),
                "skipped_fields": list(skipped_fields),
                "post_success_actions": list(post_success_actions or []),
            }
        )
        if len(operation_rows) >= self.bulk_batch_size:
            self.flush_buffer(buffer_key)

    def flush_target_object(self, target_object: str) -> None:
        for buffer_key in list(self.buffers):
            if buffer_key[0] == target_object:
                self.flush_buffer(buffer_key)

    def flush_all(self) -> None:
        for buffer_key in list(self.buffers):
            self.flush_buffer(buffer_key)

    def flush_buffer(self, buffer_key: Tuple[str, str]) -> None:
        operation_rows = self.buffers.pop(buffer_key, [])
        if not operation_rows:
            return

        target_object, operation = buffer_key
        bulk_operation = "insert" if operation == "create" else operation
        print(
            f"Status: flushing Bulk API batch for {self.target_env}.{target_object} "
            f"operation={operation.upper()} "
            f"records={len(operation_rows)} batch_size={self.bulk_batch_size} "
            f"use_serial={self.bulk_use_serial}"
        )

        bulk_payloads = [
            self.build_bulk_payload(operation_row)
            for operation_row in operation_rows
        ]

        try:
            object_client = getattr(self.sf.bulk, target_object)
            if operation == "create":
                bulk_results = list(
                    object_client.insert(
                        bulk_payloads,
                        batch_size=self.bulk_batch_size,
                        use_serial=self.bulk_use_serial,
                    )
                )
            elif operation == "update":
                bulk_results = list(
                    object_client.update(
                        bulk_payloads,
                        batch_size=self.bulk_batch_size,
                        use_serial=self.bulk_use_serial,
                    )
                )
            elif operation == "upsert":
                bulk_results = list(
                    object_client.upsert(
                        bulk_payloads,
                        "External_Id__c",
                        batch_size=self.bulk_batch_size,
                        use_serial=self.bulk_use_serial,
                    )
                )
            else:
                raise ValueError(f"Unsupported bulk operation: {operation}")
        except Exception as exc:
            print(f"Status: Bulk API batch failed for {self.target_env}.{target_object} operation={operation.upper()}: {exc}")
            for operation_row in operation_rows:
                self.append_result_for_bulk_exception(operation_row, exc)
            return

        if len(bulk_results) != len(operation_rows):
            mismatch_error = RuntimeError(
                f"Bulk result count mismatch for {target_object}.{operation}: "
                f"{len(bulk_results)} result(s) for {len(operation_rows)} payload(s)"
            )
            for operation_row in operation_rows:
                self.append_result_for_bulk_exception(operation_row, mismatch_error)
            return

        success_count = 0
        failure_count = 0
        for operation_row, bulk_result in zip(operation_rows, bulk_results):
            result_row = self.build_result_row_from_bulk_result(operation_row, bulk_result)
            if str(result_row.get("Success", "")).upper() == "TRUE":
                success_count += 1
            else:
                failure_count += 1
            self.result_rows.append(result_row)
            remember_loaded_record_id(
                result_row=result_row,
                target_object=target_object,
                source_record_id=operation_row.get("source_record_id"),
                current_load_record_ids=self.current_load_record_ids,
            )
            if (
                str(result_row.get("Success", "")).upper() == "TRUE"
                and self.after_success is not None
            ):
                self.after_success(operation_row, result_row)

        print(
            f"Status: Bulk API result for {self.target_env}.{target_object} "
            f"operation={operation.upper()}: "
            f"{success_count} success, {failure_count} failed"
        )

    @staticmethod
    def build_bulk_payload(operation_row: Dict[str, Any]) -> Dict[str, Any]:
        operation = operation_row["operation"]
        payload = dict(operation_row["payload"])
        if operation == "update":
            target_record_id = normalize_blank(operation_row.get("target_record_id"))
            if target_record_id:
                payload["Id"] = target_record_id
        elif operation == "upsert":
            source_record_id = normalize_blank(operation_row.get("source_record_id"))
            if source_record_id:
                payload["External_Id__c"] = source_record_id
        return payload

    def append_result_for_bulk_exception(
        self,
        operation_row: Dict[str, Any],
        exc: Exception,
    ) -> None:
        result_row = build_result_row(
            target_env=self.target_env,
            source_env=self.source_env,
            target_object=operation_row["target_object"],
            source_object=operation_row["source_object"],
            source_record_id=operation_row.get("source_record_id"),
            target_record_id=operation_row.get("target_record_id"),
            change_type=operation_row["change_type"],
            operation=operation_row["operation"],
            dry_run=False,
            success=False,
            payload=operation_row["payload"],
            skipped_fields=operation_row["skipped_fields"],
            message=f"Bulk API batch failed before row-level results were returned: {exc}",
        )
        self.result_rows.append(result_row)

    def build_result_row_from_bulk_result(
        self,
        operation_row: Dict[str, Any],
        bulk_result: Any,
    ) -> Dict[str, Any]:
        result_dict = bulk_result if isinstance(bulk_result, dict) else {"result": bulk_result}
        success = parse_bulk_success(result_dict)
        target_record_id = (
            normalize_blank(result_dict.get("id"))
            or normalize_blank(result_dict.get("Id"))
            or normalize_blank(operation_row.get("target_record_id"))
        )
        message = json.dumps(result_dict, ensure_ascii=False, default=str)

        return build_result_row(
            target_env=self.target_env,
            source_env=self.source_env,
            target_object=operation_row["target_object"],
            source_object=operation_row["source_object"],
            source_record_id=operation_row.get("source_record_id"),
            target_record_id=target_record_id,
            change_type=operation_row["change_type"],
            operation=operation_row["operation"],
            dry_run=False,
            success=success,
            payload=operation_row["payload"],
            skipped_fields=operation_row["skipped_fields"],
            message=message,
        )


def parse_bulk_success(result_dict: Dict[str, Any]) -> bool:
    success_value = result_dict.get("success")
    if isinstance(success_value, bool):
        return success_value
    if isinstance(success_value, str):
        return success_value.strip().lower() == "true"
    if "errors" in result_dict and result_dict.get("errors"):
        return False
    return bool(success_value)


def get_object_describe(sf, target_object: str, describe_cache: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    if target_object not in describe_cache:
        describe_cache[target_object] = getattr(sf, target_object).describe()
    return describe_cache[target_object]


def get_record_type_id_by_developer_name(
    sf,
    object_name: str,
    developer_name: str,
    describe_cache: Dict[str, Dict[str, Any]],
) -> Optional[str]:
    cache_key = f"__record_type_id__::{object_name}::{developer_name}"
    if cache_key in describe_cache:
        return normalize_blank(describe_cache[cache_key].get("Id"))

    record_type_id: Optional[str] = None
    try:
        describe = get_object_describe(sf, object_name, describe_cache)
        for record_type_info in describe.get("recordTypeInfos", []):
            if (
                normalize_blank(record_type_info.get("developerName")) == developer_name
                or normalize_blank(record_type_info.get("name")) == developer_name
            ):
                record_type_id = normalize_blank(
                    record_type_info.get("recordTypeId")
                    or record_type_info.get("recordTypeId".lower())
                )
                break
    except Exception:
        record_type_id = None

    if not record_type_id:
        soql = (
            "SELECT Id FROM RecordType "
            f"WHERE SobjectType = '{escape_soql_string(object_name)}' "
            f"AND DeveloperName = '{escape_soql_string(developer_name)}' "
            "LIMIT 1"
        )
        try:
            result = sf.query(soql)
            records = result.get("records", []) if isinstance(result, dict) else []
            record_type_id = normalize_blank(records[0].get("Id")) if records else None
        except Exception:
            record_type_id = None

    describe_cache[cache_key] = {"Id": record_type_id or ""}
    return record_type_id


def find_target_record_id_by_external_id(
    sf,
    target_object: str,
    source_record_id: str,
    target_external_id_field: Dict[str, Any],
    lookup_cache: Dict[Tuple[str, str], Optional[str]],
) -> Optional[str]:
    cache_key = (target_object, source_record_id)
    if cache_key in lookup_cache:
        return lookup_cache[cache_key]

    if target_external_id_field.get("filterable") is False:
        lookup_cache[cache_key] = None
        return None

    escaped_source_record_id = escape_soql_string(source_record_id)
    soql = (
        f"SELECT Id FROM {target_object} "
        f"WHERE External_Id__c = '{escaped_source_record_id}' "
        "LIMIT 1"
    )

    try:
        result = sf.query(soql)
    except Exception:
        lookup_cache[cache_key] = None
        return None

    records = result.get("records", []) if isinstance(result, dict) else []
    target_record_id = normalize_blank(records[0].get("Id")) if records else None
    lookup_cache[cache_key] = target_record_id
    return target_record_id


def find_target_record_id_after_upsert(
    sf,
    target_object: str,
    source_record_id: Optional[str],
) -> Optional[str]:
    clean_source_record_id = normalize_blank(source_record_id)
    if not clean_source_record_id:
        return None

    escaped_source_record_id = escape_soql_string(clean_source_record_id)
    soql = (
        f"SELECT Id FROM {target_object} "
        f"WHERE External_Id__c = '{escaped_source_record_id}' "
        "LIMIT 1"
    )
    try:
        result = sf.query(soql)
    except Exception:
        return None

    records = result.get("records", []) if isinstance(result, dict) else []
    return normalize_blank(records[0].get("Id")) if records else None


def resolve_relationship_payload_to_ids(
    payload: Dict[str, Any],
    relationship_field_defs: Dict[str, Dict[str, Any]],
    sf,
    describe_cache: Dict[str, Dict[str, Any]],
    target_extract_root: Optional[Path],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    salesforce_lookup_cache: Dict[Tuple[str, str], Optional[str]],
    current_load_record_ids: Dict[Tuple[str, str], str],
    skipped_fields: List[str],
    fallback_to_salesforce: bool,
) -> None:
    for relationship_name in list(payload.keys()):
        relationship_value = payload.get(relationship_name)
        if not isinstance(relationship_value, dict):
            continue

        field_def = relationship_field_defs.get(relationship_name)
        if not field_def:
            continue

        external_id_value = normalize_blank(relationship_value.get("External_Id__c"))
        field_name = normalize_blank(field_def.get("name"))
        if not field_name:
            payload.pop(relationship_name, None)
            skipped_fields.append(f"{relationship_name}.External_Id__c: relationship field not found")
            continue

        if not external_id_value:
            payload.pop(relationship_name, None)
            if field_def.get("nillable", True):
                payload[field_name] = None
            else:
                skipped_fields.append(f"{relationship_name}.External_Id__c: blank required relationship")
            continue

        target_record_id = resolve_related_record_id(
            external_id_value=external_id_value,
            reference_objects=field_def.get("referenceTo", []),
            sf=sf,
            describe_cache=describe_cache,
            target_extract_root=target_extract_root,
            extract_lookup_cache=extract_lookup_cache,
            salesforce_lookup_cache=salesforce_lookup_cache,
            current_load_record_ids=current_load_record_ids,
            fallback_to_salesforce=fallback_to_salesforce,
        )

        payload.pop(relationship_name, None)
        if target_record_id:
            payload[field_name] = target_record_id
        else:
            reference_list = "; ".join(
                str(reference_object).strip()
                for reference_object in field_def.get("referenceTo", [])
                if str(reference_object).strip()
            )
            skipped_fields.append(
                f"{relationship_name}.External_Id__c={external_id_value}: "
                f"related record not found in {reference_list or 'reference object'}"
            )


def resolve_related_record_id(
    external_id_value: str,
    reference_objects: Iterable[Any],
    sf,
    describe_cache: Dict[str, Dict[str, Any]],
    target_extract_root: Optional[Path],
    extract_lookup_cache: Dict[str, Dict[str, str]],
    salesforce_lookup_cache: Dict[Tuple[str, str], Optional[str]],
    current_load_record_ids: Dict[Tuple[str, str], str],
    fallback_to_salesforce: bool,
) -> Optional[str]:
    for reference_object in reference_objects:
        object_name = normalize_blank(reference_object)
        if not object_name:
            continue

        loaded_record_id = current_load_record_ids.get((object_name, external_id_value))
        if loaded_record_id:
            return loaded_record_id

        extract_record_id = find_record_id_in_target_extract(
            target_extract_root=target_extract_root,
            object_name=object_name,
            external_id_value=external_id_value,
            extract_lookup_cache=extract_lookup_cache,
        )
        if extract_record_id:
            return extract_record_id

        if fallback_to_salesforce:
            salesforce_record_id = find_salesforce_record_id_by_external_id(
                sf=sf,
                object_name=object_name,
                external_id_value=external_id_value,
                describe_cache=describe_cache,
                lookup_cache=salesforce_lookup_cache,
            )
            if salesforce_record_id:
                return salesforce_record_id

    return None


def find_record_id_in_target_extract(
    target_extract_root: Optional[Path],
    object_name: str,
    external_id_value: str,
    extract_lookup_cache: Dict[str, Dict[str, str]],
) -> Optional[str]:
    if target_extract_root is None:
        return None

    if object_name not in extract_lookup_cache:
        extract_lookup_cache[object_name] = build_external_id_lookup_from_extract(
            target_extract_root=target_extract_root,
            object_name=object_name,
        )

    return extract_lookup_cache[object_name].get(external_id_value)


def build_external_id_lookup_from_extract(
    target_extract_root: Path,
    object_name: str,
) -> Dict[str, str]:
    object_dir = target_extract_root / f"Obj={safe_path_part(object_name)}"
    lookup: Dict[str, str] = {}

    if not object_dir.exists():
        return lookup

    for part_path in sorted(object_dir.glob("JoinBucket=*/chunk_*.parquet")):
        parquet_file = pq.ParquetFile(part_path)
        available_columns = set(parquet_file.schema_arrow.names)
        if not {"RecordId", "External_Id__c"}.issubset(available_columns):
            continue

        for batch in parquet_file.iter_batches(
            columns=["RecordId", "External_Id__c"],
            batch_size=250000,
        ):
            batch_rows = batch.to_pydict()
            for record_id, external_id in zip(
                batch_rows["RecordId"],
                batch_rows["External_Id__c"],
            ):
                clean_record_id = normalize_blank(record_id)
                clean_external_id = normalize_blank(external_id)
                if clean_record_id and clean_external_id and clean_external_id not in lookup:
                    lookup[clean_external_id] = clean_record_id

    return lookup


def find_salesforce_record_id_by_external_id(
    sf,
    object_name: str,
    external_id_value: str,
    describe_cache: Dict[str, Dict[str, Any]],
    lookup_cache: Dict[Tuple[str, str], Optional[str]],
) -> Optional[str]:
    cache_key = (object_name, external_id_value)
    if cache_key in lookup_cache:
        return lookup_cache[cache_key]

    try:
        describe = get_object_describe(sf, object_name, describe_cache)
    except Exception:
        lookup_cache[cache_key] = None
        return None

    field_defs = {
        field_def["name"]: field_def
        for field_def in describe.get("fields", [])
        if field_def.get("name")
    }
    external_id_field = field_defs.get("External_Id__c")
    if not external_id_field or external_id_field.get("filterable") is False:
        lookup_cache[cache_key] = None
        return None

    escaped_external_id_value = escape_soql_string(external_id_value)
    soql = (
        f"SELECT Id FROM {object_name} "
        f"WHERE External_Id__c = '{escaped_external_id_value}' "
        "LIMIT 1"
    )

    try:
        result = sf.query(soql)
    except Exception:
        lookup_cache[cache_key] = None
        return None

    records = result.get("records", []) if isinstance(result, dict) else []
    record_id = normalize_blank(records[0].get("Id")) if records else None
    lookup_cache[cache_key] = record_id
    return record_id


def remember_loaded_record_id(
    result_row: Dict[str, Any],
    target_object: str,
    source_record_id: Optional[str],
    current_load_record_ids: Dict[Tuple[str, str], str],
) -> None:
    if str(result_row.get("Success", "")).upper() != "TRUE":
        return

    clean_source_record_id = normalize_blank(source_record_id)
    clean_target_record_id = normalize_blank(result_row.get("Target_RecordId"))
    if clean_source_record_id and clean_target_record_id:
        current_load_record_ids[(target_object, clean_source_record_id)] = clean_target_record_id


def resolve_operation(
    target_record_id: Optional[str],
    source_record_id: Optional[str],
    target_external_id_field: Optional[Dict[str, Any]],
) -> str:
    if target_record_id:
        return "update"
    if source_record_id and is_external_id_upsert_key(target_external_id_field):
        return "upsert"
    return "create"


def operation_hint_for_operation(operation: str, target_record_id: Optional[str]) -> str:
    return "create" if operation in {"create", "upsert"} and not target_record_id else "update"


def is_external_id_upsert_key(field_def: Optional[Dict[str, Any]]) -> bool:
    if not field_def:
        return False
    return bool(field_def.get("externalId"))


def is_field_writable(field_def: Dict[str, Any], operation_hint: str) -> bool:
    if operation_hint == "create":
        return bool(field_def.get("createable"))
    return bool(field_def.get("updateable"))


def normalize_salesforce_value(value: Any, field_def: Dict[str, Any]) -> Any:
    if value is None:
        return None

    field_type = field_def.get("type")
    if isinstance(value, str):
        text = value.strip()
        if text.lower() in {"", "none", "null", "nan", "<na>"}:
            return None
        if field_type == "boolean":
            return text.lower() == "true"
        if field_type in {"int"}:
            return int(float(text))
        if field_type in {"double", "currency", "percent"}:
            return float(text)
        return value

    return value


def parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value

    text = normalize_blank(value)
    if text is None:
        return {}

    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError(f"Expected JSON object, received {type(parsed)}")

    return parsed


def merge_load_payload(
    source_payload: Dict[str, Any],
    source_load_payload: Dict[str, Any],
) -> Dict[str, Any]:
    if not source_load_payload:
        return source_payload
    if not source_payload:
        return source_load_payload

    merged_payload = dict(source_load_payload)
    merged_payload.update(source_payload)
    return merged_payload


def parse_transformation_logic(
    value: Any,
    row_number: int,
    source_object: str,
    source_field: str,
    target_object: str,
    target_field: str,
) -> Optional[Dict[str, Any]]:
    text = normalize_blank(value)
    if text is None:
        return None

    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError(f"Transformation Logic JSON must be an object on metadata row {row_number}")

    parsed["_direction"] = transformation_direction(
        transformation_logic=parsed,
        source_object=source_object,
        source_field=source_field,
        target_object=target_object,
        target_field=target_field,
    )
    return parsed


def transformation_direction(
    transformation_logic: Dict[str, Any],
    source_object: str,
    source_field: str,
    target_object: str,
    target_field: str,
) -> str:
    source = transformation_logic.get("source") or {}
    target = transformation_logic.get("target") or {}
    source_key = (normalize_blank(source.get("object")), normalize_blank(source.get("field")))
    target_key = (normalize_blank(target.get("object")), normalize_blank(target.get("field")))

    if source_key == (source_object, source_field) and target_key == (target_object, target_field):
        return "direct"
    if source_key == (target_object, target_field) and target_key == (source_object, source_field):
        return "inverse"
    return "direct"


def apply_transformation_logic(value: Any, transformation_logic: Optional[Dict[str, Any]]) -> Any:
    if not transformation_logic:
        return value

    value_mapping = transformation_logic.get("mapping")
    if not isinstance(value_mapping, dict):
        return value

    if transformation_logic.get("_direction") == "inverse":
        lookup = {
            canonical_lookup_key(mapped_value): source_value
            for source_value, mapped_value in value_mapping.items()
        }
    else:
        lookup = {
            canonical_lookup_key(source_value): mapped_value
            for source_value, mapped_value in value_mapping.items()
        }

    return lookup.get(canonical_lookup_key(value), value)


def canonical_lookup_key(value: Any) -> Tuple[str, Any]:
    if value is None or pd.isna(value):
        return ("blank", None)
    if isinstance(value, bool):
        return ("bool", value)

    text = str(value).strip()
    lower_text = text.lower()
    if lower_text in {"", "nan", "none", "null", "<na>"}:
        return ("blank", None)
    if lower_text == "true":
        return ("bool", True)
    if lower_text == "false":
        return ("bool", False)
    return ("text", text)


class LoadResultRows:
    def __init__(
        self,
        results_csv_path: str,
        keep_rows: bool,
        resume_from_existing: bool = False,
        example_limit_per_error: int = 5,
    ) -> None:
        self.results_csv_path = Path(results_csv_path)
        self.failed_rows_csv_path = derive_load_output_path(
            self.results_csv_path,
            "failed_rows",
        )
        self.error_summary_csv_path = derive_load_output_path(
            self.results_csv_path,
            "error_summary",
        )
        self.error_examples_csv_path = derive_load_output_path(
            self.results_csv_path,
            "error_examples",
        )
        self.keep_rows = keep_rows
        self.resume_from_existing = resume_from_existing
        self.example_limit_per_error = example_limit_per_error
        self.rows: List[Dict[str, Any]] = []
        self.processed_load_keys: Set[Tuple[str, str]] = set()
        self.total_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.error_summary: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        self.error_examples: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
        self.columns = result_columns()
        self.failed_columns = self.columns + [
            "Error_Category",
            "Error_Code",
            "Error_Fields",
            "Error_Message",
        ]
        self.summary_columns = [
            "Target_Object",
            "Source_Object",
            "Operation",
            "Error_Category",
            "Error_Code",
            "Error_Fields",
            "Error_Message",
            "Count",
            "Sample_Source_RecordId",
            "Sample_Target_RecordId",
            "Sample_Payload_Field_Count",
            "Sample_Payload_JSON",
            "Sample_Message",
        ]
        self.results_csv_path.parent.mkdir(parents=True, exist_ok=True)
        should_resume = (
            self.resume_from_existing
            and self.results_csv_path.exists()
            and self.results_csv_path.stat().st_size > 0
        )
        if not should_resume:
            self._initialize_csv(self.results_csv_path, self.columns)
        self._initialize_csv(self.failed_rows_csv_path, self.failed_columns)
        self._initialize_csv(self.error_summary_csv_path, self.summary_columns)
        self._initialize_csv(self.error_examples_csv_path, self.failed_columns)
        if should_resume:
            self._load_existing_results()

    def append(self, row: Dict[str, Any]) -> None:
        clean_row = {column: row.get(column, "") for column in self.columns}
        self._track_processed_key(clean_row)
        self.total_count += 1

        if str(clean_row.get("Success", "")).upper() == "TRUE":
            self.success_count += 1
        else:
            self.failure_count += 1
            error_details = classify_load_result_error(clean_row)
            failed_row = {
                **clean_row,
                "Error_Category": error_details["category"],
                "Error_Code": error_details["code"],
                "Error_Fields": error_details["fields"],
                "Error_Message": error_details["message"],
            }
            self._append_csv(self.failed_rows_csv_path, self.failed_columns, failed_row)
            self._track_error_summary(failed_row)
            if self.failure_count <= 25 or self.failure_count % 100 == 0:
                self.write_error_outputs()

        self._append_csv(self.results_csv_path, self.columns, clean_row)
        if self.keep_rows:
            self.rows.append(clean_row)

    def _load_existing_results(self) -> None:
        with self.results_csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                return

            for raw_row in reader:
                clean_row = {column: raw_row.get(column, "") for column in self.columns}
                self._track_processed_key(clean_row)
                self.total_count += 1

                if str(clean_row.get("Success", "")).upper() == "TRUE":
                    self.success_count += 1
                else:
                    self.failure_count += 1
                    error_details = classify_load_result_error(clean_row)
                    failed_row = {
                        **clean_row,
                        "Error_Category": error_details["category"],
                        "Error_Code": error_details["code"],
                        "Error_Fields": error_details["fields"],
                        "Error_Message": error_details["message"],
                    }
                    self._append_csv(self.failed_rows_csv_path, self.failed_columns, failed_row)
                    self._track_error_summary(failed_row)

                if self.keep_rows:
                    self.rows.append(clean_row)

        self.write_error_outputs()

    def _track_processed_key(self, row: Dict[str, Any]) -> None:
        target_object = normalize_blank(row.get("Target_Object"))
        source_record_id = normalize_blank(row.get("Source_RecordId"))
        if target_object and source_record_id:
            self.processed_load_keys.add((target_object, source_record_id))

    def __len__(self) -> int:
        return self.total_count

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=self.columns)

    def write_error_outputs(self) -> None:
        summary_rows = sorted(
            self.error_summary.values(),
            key=lambda row: int(row.get("Count", 0)),
            reverse=True,
        )
        self._write_csv(self.error_summary_csv_path, self.summary_columns, summary_rows)

        example_rows: List[Dict[str, Any]] = []
        for key in sorted(self.error_examples):
            example_rows.extend(self.error_examples[key])
        self._write_csv(self.error_examples_csv_path, self.failed_columns, example_rows)

    def _track_error_summary(self, failed_row: Dict[str, Any]) -> None:
        key = (
            normalize_blank(failed_row.get("Target_Object")) or "",
            normalize_blank(failed_row.get("Source_Object")) or "",
            normalize_blank(failed_row.get("Operation")) or "",
            normalize_blank(failed_row.get("Error_Category")) or "",
            normalize_blank(failed_row.get("Error_Code")) or "",
            normalize_blank(failed_row.get("Error_Fields")) or "",
            normalize_blank(failed_row.get("Error_Message")) or "",
        )
        summary_row = self.error_summary.get(key)
        if summary_row is None:
            summary_row = {
                "Target_Object": failed_row.get("Target_Object", ""),
                "Source_Object": failed_row.get("Source_Object", ""),
                "Operation": failed_row.get("Operation", ""),
                "Error_Category": failed_row.get("Error_Category", ""),
                "Error_Code": failed_row.get("Error_Code", ""),
                "Error_Fields": failed_row.get("Error_Fields", ""),
                "Error_Message": failed_row.get("Error_Message", ""),
                "Count": 0,
                "Sample_Source_RecordId": failed_row.get("Source_RecordId", ""),
                "Sample_Target_RecordId": failed_row.get("Target_RecordId", ""),
                "Sample_Payload_Field_Count": failed_row.get("Payload_Field_Count", ""),
                "Sample_Payload_JSON": failed_row.get("Payload_JSON", ""),
                "Sample_Message": failed_row.get("Message", ""),
            }
            self.error_summary[key] = summary_row

        summary_row["Count"] = int(summary_row["Count"]) + 1
        examples = self.error_examples.setdefault(key, [])
        if len(examples) < self.example_limit_per_error:
            examples.append(failed_row)

    @staticmethod
    def _initialize_csv(path: Path, columns: List[str]) -> None:
        with path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, quoting=csv.QUOTE_ALL)
            writer.writeheader()

    @staticmethod
    def _append_csv(path: Path, columns: List[str], row: Dict[str, Any]) -> None:
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, quoting=csv.QUOTE_ALL)
            writer.writerow({column: row.get(column, "") for column in columns})

    @staticmethod
    def _write_csv(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
        with path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in rows:
                writer.writerow({column: row.get(column, "") for column in columns})


def derive_load_output_path(results_csv_path: Path, suffix: str) -> Path:
    stem = results_csv_path.stem
    if stem.endswith("_full"):
        stem = stem[:-5]
    return results_csv_path.with_name(f"{stem}_{suffix}.csv")


def classify_load_result_error(row: Dict[str, Any]) -> Dict[str, str]:
    operation = normalize_blank(row.get("Operation")) or ""
    raw_message = normalize_blank(row.get("Message")) or ""
    parsed_error = parse_salesforce_error_message(raw_message)
    error_code = parsed_error.get("code") or ""
    error_fields = parsed_error.get("fields") or ""
    error_message = parsed_error.get("message") or simplify_error_message(raw_message)
    category = categorize_load_error(
        operation=operation,
        error_code=error_code,
        error_message=error_message,
    )

    return {
        "category": category,
        "code": error_code,
        "fields": error_fields,
        "message": error_message,
    }


def parse_salesforce_error_message(raw_message: str) -> Dict[str, str]:
    if not raw_message:
        return {"code": "", "fields": "", "message": ""}

    response_content = raw_message
    if "Response content:" in raw_message:
        response_content = raw_message.split("Response content:", 1)[1].strip()

    try:
        parsed = json.loads(response_content)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(response_content)
        except (SyntaxError, ValueError):
            return parse_salesforce_error_message_with_regex(raw_message)

    if isinstance(parsed, list) and parsed:
        first_error = parsed[0] if isinstance(parsed[0], dict) else {}
    elif isinstance(parsed, dict):
        first_error = parsed
    else:
        return parse_salesforce_error_message_with_regex(raw_message)

    bulk_errors = first_error.get("errors")
    if isinstance(bulk_errors, list) and bulk_errors:
        first_bulk_error = bulk_errors[0]
        if isinstance(first_bulk_error, dict):
            return {
                "code": normalize_blank(
                    first_bulk_error.get("statusCode")
                    or first_bulk_error.get("errorCode")
                ) or "",
                "fields": normalize_error_fields(first_bulk_error.get("fields") or []),
                "message": normalize_blank(first_bulk_error.get("message")) or "",
            }
        return {
            "code": "",
            "fields": "",
            "message": normalize_blank(first_bulk_error) or "",
        }
    if isinstance(bulk_errors, str) and normalize_blank(bulk_errors):
        return {
            "code": "",
            "fields": "",
            "message": normalize_blank(bulk_errors) or "",
        }

    duplicate_result = first_error.get("duplicateResult")
    if isinstance(duplicate_result, dict):
        error_code = normalize_blank(first_error.get("errorCode")) or "DUPLICATES_DETECTED"
        error_message = normalize_blank(
            duplicate_result.get("errorMessage")
            or first_error.get("message")
        )
        fields = first_error.get("fields") or []
        return {
            "code": error_code or "",
            "fields": normalize_error_fields(fields),
            "message": error_message or "",
        }

    return {
        "code": normalize_blank(first_error.get("errorCode")) or "",
        "fields": normalize_error_fields(first_error.get("fields") or []),
        "message": normalize_blank(first_error.get("message")) or "",
    }


def parse_salesforce_error_message_with_regex(raw_message: str) -> Dict[str, str]:
    error_code_match = re.search(r"['\"]errorCode['\"]:\s*['\"]([^'\"]+)['\"]", raw_message)
    message_match = re.search(r"['\"]message['\"]:\s*['\"]([^'\"]+)['\"]", raw_message)
    fields_match = re.search(r"['\"]fields['\"]:\s*(\[[^\]]*\])", raw_message)
    return {
        "code": error_code_match.group(1) if error_code_match else "",
        "fields": normalize_error_fields(fields_match.group(1) if fields_match else ""),
        "message": message_match.group(1) if message_match else simplify_error_message(raw_message),
    }


def normalize_error_fields(fields: Any) -> str:
    if isinstance(fields, str):
        clean_fields = fields.strip()
        if clean_fields.startswith("["):
            try:
                parsed = ast.literal_eval(clean_fields)
                return normalize_error_fields(parsed)
            except (SyntaxError, ValueError):
                return clean_fields
        return clean_fields
    if isinstance(fields, (list, tuple, set)):
        return "; ".join(
            str(field).strip()
            for field in fields
            if str(field).strip()
        )
    return ""


def simplify_error_message(message: str) -> str:
    clean_message = re.sub(r"https://\S+", "URL", message or "")
    clean_message = re.sub(r"\s+", " ", clean_message).strip()
    return clean_message


def categorize_load_error(
    operation: str,
    error_code: str,
    error_message: str,
) -> str:
    code = (error_code or "").upper()
    message = (error_message or "").lower()

    if operation == "skip":
        if "related record not found" in message or "target" in message and "not found" in message:
            return "Missing Related Record"
        if "no writable mapped fields" in message:
            return "No Writable Mapped Fields"
        return "Loader Skip"

    if code in {"DUPLICATES_DETECTED", "DUPLICATE_VALUE"} or "duplicate" in message:
        return "Duplicate Rule / Duplicate Value"
    if code == "FIELD_CUSTOM_VALIDATION_EXCEPTION" or "validation" in message:
        return "Validation Rule"
    if code == "REQUIRED_FIELD_MISSING" or "required fields are missing" in message:
        return "Missing Required Field"
    if code == "INVALID_FIELD_FOR_INSERT_UPDATE" or "unable to create/update fields" in message:
        return "Read-Only Field / Field Access"
    if code == "FIELD_INTEGRITY_EXCEPTION" or "field integrity exception" in message:
        return "Field Integrity / Reference"
    if code == "INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST" or "restricted picklist" in message:
        return "Restricted Picklist"
    if code == "STRING_TOO_LONG" or "data value too large" in message:
        return "Field Length"
    if code == "INACTIVE_OWNER_OR_USER" or "inactive user" in message or "inactive owner" in message:
        return "Inactive Owner/User"
    if code == "CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY" or "trigger" in message or "flow" in message:
        return "Automation / Trigger / Flow"
    if code:
        return code
    return "Unclassified"


def build_result_row(
    target_env: str,
    source_env: str,
    target_object: str,
    source_object: str,
    source_record_id: Optional[str],
    target_record_id: Optional[str],
    change_type: str,
    operation: str,
    dry_run: bool,
    success: bool,
    payload: Dict[str, Any],
    skipped_fields: List[str],
    message: str,
) -> Dict[str, Any]:
    return {
        "Target_Env": target_env,
        "Source_Env": source_env,
        "Target_Object": target_object,
        "Source_Object": source_object,
        "Source_RecordId": source_record_id or "",
        "Target_RecordId": target_record_id or "",
        "Change_Type": change_type,
        "Operation": operation,
        "Dry_Run": str(bool(dry_run)).upper(),
        "Success": str(bool(success)).upper(),
        "Payload_Field_Count": len(payload),
        "Payload_JSON": json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
        "Skipped_Fields": "; ".join(skipped_fields),
        "Message": message,
    }


def result_columns() -> List[str]:
    return [
        "Target_Env",
        "Source_Env",
        "Target_Object",
        "Source_Object",
        "Source_RecordId",
        "Target_RecordId",
        "Change_Type",
        "Operation",
        "Dry_Run",
        "Success",
        "Payload_Field_Count",
        "Payload_JSON",
        "Skipped_Fields",
        "Message",
    ]


def should_ignore_row(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "t", "yes", "y", "1"}


def normalize_blank(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return None
    return text


def normalize_metadata_cell(value: Any) -> Optional[str]:
    text = normalize_blank(value)
    if text is None:
        return None
    if text.strip().lower() in {"#n/a", "n/a", "na"}:
        return None
    return text


def safe_path_part(value: str) -> str:
    safe_value = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value).strip())
    return safe_value.strip("._") or "blank"


def escape_soql_string(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")

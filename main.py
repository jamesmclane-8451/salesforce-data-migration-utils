import csv

"""
Next Steps
- How to handle where Accounts don't exist in Prod because they were formerly Brands?
-
- Build transformation mapping
-

"""

from extract import extract_salesforce_records_to_parquet_for_two_envs
from diff import diff_salesforce_parquet_extracts
from audit_diff import audit_diff_outputs
from metadata_diff import compare_salesforce_object_metadata
from upsert_sequence import generate_upsert_sequence_csv
from transform import transform_salesforce_diff_for_sf1_load
from pre_run_cleanup import replace_inactive_owner_ids_in_diff_checkpoints
from load import (
    load_prepared_salesforce_work,
    load_salesforce_diff_to_target,
    prepare_salesforce_load_work,
)
from update_validation_rules import update_validation_rules_with_bypass_permission

"""
# Update Validation Rules, adding Bypass Validation toggle (Customer Permission + Permission Set)
df_deploy = update_validation_rules_with_bypass_permission(
    env_name="MCUAT8451",
    dry_run=False,
    include_inactive=True,
    validation_rule_ids=[],
)

quit()
"""

""" Extract 
extract_salesforce_records_to_parquet_for_two_envs(
    env_1="MCUAT8451",
    env_1_output_dir="mcuat_extract_parquet",
    env_2="PROD",
    env_2_output_dir="prod_extract_parquet",
    csv_path="metadata_scope.csv",
    limit_per_object=100000000,
    resume_from_checkpoint=False,
    checkpoint_batch_size=2000,
)

quit()
"""

""" Diff 
diff_salesforce_parquet_extracts(
    source_extract_dir="mcuat_extract_parquet",
    target_extract_dir="prod_extract_parquet",
    output_feather_path="diff.feather",
    output_csv=False,
    exclusions_csv_path="diff_exclusions.csv",
    resume_from_checkpoint=True,
    return_dataframe=False,
    comparison_excluded_fields=[
        "Id",
        "*Id",
    ],
)

quit()
"""

""" Audit 
audit_diff_outputs(
    diff_checkpoint_dir="diff_diff_parts",
    source_extract_dir="mcuat_extract_parquet",
    target_extract_dir="prod_extract_parquet",
    metadata_scope_csv_path="metadata_scope.csv",
    output_prefix="diff_audit",
    sample_per_group=3,
    batch_size=250000,
)
"""

""" Upsert Sequence 
upsert_sequence_df = generate_upsert_sequence_csv(
    target_env="MCUAT8451",
    metadata_scope_csv_path="metadata_scope.csv",
    output_csv_path="upsert_sequence.csv",
    object_column="SF 2.0 Object",
    field_column="SF 2.0 Field",
    include_only_metadata_fields=True,
)
"""

""" Pre-run Cleanup 
# Update inactive owners
# other pre-run tasks
replace_inactive_owner_ids_in_diff_checkpoints(
    target_env="MCUAT8451",
    source_env="PROD",
    diff_checkpoint_dir="diff_diff_parts",
    metadata_scope_csv_path="metadata_scope.csv",
    output_diff_checkpoint_dir="diff_diff_parts_prerun_cleaned",
    fallback_owner_id="005ao000007Br4zAAC",
    replacement_audit_csv_path="pre_run_inactive_owner_replacements.csv",
)

quit()
"""

""" Load: Prepare local work, then execute only pending prepared work """
SKIP_OBJECTS_FOR_THIS_RUN = {
    # "OpportunityContactRole",
}

load_object_filter = None
if SKIP_OBJECTS_FOR_THIS_RUN:
    with open("upsert_sequence.csv", newline="", encoding="utf-8-sig") as handle:
        load_object_filter = [
            row["Object"].strip()
            for row in csv.DictReader(handle)
            if row.get("Enabled", "").strip().upper() == "TRUE"
            and row.get("Object", "").strip() not in SKIP_OBJECTS_FOR_THIS_RUN
        ]
    print(
        "TEMP RUN: skipping "
        f"{', '.join(sorted(SKIP_OBJECTS_FOR_THIS_RUN))}. "
        f"Running {len(load_object_filter)} enabled object(s)."
    )

prepare_salesforce_load_work(
    target_env="MCUAT8451",
    source_env="PROD",
    diff_checkpoint_dir="diff_diff_parts_prerun_cleaned",
    metadata_scope_csv_path="metadata_scope.csv",
    upsert_sequence_csv_path="upsert_sequence.csv",
    object_source_policy_csv_path="migration_object_source_policy.csv",
    results_csv_path="load_results_step3_full.csv",
    object_filter=load_object_filter,
    resume_from_results=True,
    rebuild_prepared_work=False,
    batch_size=50000,
)

load_results_df = load_prepared_salesforce_work(
    target_env="MCUAT8451",
    source_env="PROD",
    metadata_scope_csv_path="metadata_scope.csv",
    upsert_sequence_csv_path="upsert_sequence.csv",
    object_source_policy_csv_path="migration_object_source_policy.csv",
    target_extract_dir="mcuat_extract_parquet",
    results_csv_path="load_results_step3_full.csv",
    resume_from_results=True,
    use_bulk_api=True,
    bulk_batch_size=500,
    bulk_use_serial=False,
    object_filter=load_object_filter,
)

quit()

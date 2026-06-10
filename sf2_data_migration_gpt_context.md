# Context for Salesforce 2.0 Data Migration Workflow

Use this document as context for discussing the Salesforce 2.0 data migration workflow outside Codex. It is self-contained and intentionally excludes credentials, token files, and large output files.

## Project Goal

84.51 is migrating Salesforce data from SF 1.0 Production into SF 2.0 environments.

The workflow is designed to:

1. Define migration scope through a mapping file.
2. Extract source and target Salesforce data.
3. Diff the environments to identify gaps.
4. Audit those gaps in human-reviewable files.
5. Generate a dependency-based load sequence.
6. Load corrected source data into SF 2.0 in controlled stages.
7. Capture load results for review and signoff.

The current working directory is:

```text
/Users/j825724/PycharmProjects/EventBridge
```

## Key Files

### `metadata_scope.csv`

This is the central migration scope and field mapping file.

Current header:

```text
Temp UID,SF 1.0 Object,SF 1.0 Field,SF 2.0 Object,SF 2.0 Field,Ignore,temp,Filter Logic (SOQL),Transformation Logic (JSON)
```

Purpose:

- Defines which SF 1.0 objects/fields map to which SF 2.0 objects/fields.
- Controls which objects and fields are extracted, diffed, transformed, and loaded.
- Rows with `Ignore = TRUE` are skipped.
- `Filter Logic (SOQL)` is intentionally ignored by the current code.
- `Transformation Logic (JSON)` can be used for field-level value mapping logic.

### `extract.py`

Primary function:

```python
extract_salesforce_records_to_parquet_for_two_envs(...)
```

Purpose:

- Connects to two Salesforce environments.
- Extracts records based on `metadata_scope.csv`.
- Outputs partitioned Parquet files instead of one large Feather/CSV.
- Supports resume/checkpoint behavior.
- Captures bad/missing field errors instead of failing the full extract.
- Skips rows marked `Ignore = TRUE`.

Important current behavior:

- The prior `LastModifiedDate` filter was removed. Extract now pulls all in-scope records, subject to `limit_per_object`.
- For MCUAT/SF 2.0 records, `External_Id__c` is used to match back to SF 1.0 Production IDs.
- Field extraction errors are written to `field_extract_errors.csv`.

Typical output directories:

```text
mcuat_extract_parquet/
prod_extract_parquet/
```

### `diff.py`

Primary function:

```python
diff_salesforce_parquet_extracts(...)
```

Purpose:

- Compares partitioned Parquet extracts.
- Matches MCUAT records to PROD records using:
  - MCUAT/SF 2.0 `External_Id__c`
  - PROD/SF 1.0 `RecordId`
- Writes resumable checkpoint parts under `diff_diff_parts/`.
- Can produce `diff.feather`, `diff.csv`, and `diff_field_level.csv`, but CSV output can be disabled to avoid giant files.

Important current behavior:

- `comparison_excluded_fields=["Id", "*Id"]` is used so Salesforce record ID fields and lookup ID fields are not counted as false-positive data gaps.
- Public diff outputs exclude those fields from reporting.
- Diff checkpoint Parquet files preserve those excluded ID fields in hidden load-only payload columns, such as `PROD_load_value`, so `load.py` can still build correct relationship payloads.
- This means `AccountId`, `OpportunityId`, `Product2Id`, etc. do not appear as parity issues, but they can still be used to relate child records during load.

Current intended diff call:

```python
diff_salesforce_parquet_extracts(
    source_extract_dir="mcuat_extract_parquet",
    target_extract_dir="prod_extract_parquet",
    output_feather_path="diff.feather",
    output_csv=False,
    exclusions_csv_path="diff_exclusions.csv",
    resume_from_checkpoint=False,
    return_dataframe=False,
    comparison_excluded_fields=[
        "Id",
        "*Id",
    ],
)
```

Important caution:

- If `diff.py` is interrupted while running with `resume_from_checkpoint=False`, `diff_diff_parts/` may be partially rebuilt.
- Do not run `load.py` until the diff completes successfully.
- If resuming a partially completed diff after interruption, use `resume_from_checkpoint=True` with the same config.

### `diff_exclusions.csv`

Purpose:

- Excludes known noisy objects/fields from diff processing.
- Current examples include operational logs and email body fields that can contain Salesforce error text, row-lock messages, HTML, or Office markup.

This is separate from `comparison_excluded_fields`.

- `diff_exclusions.csv` removes object/field data from the diff entirely.
- `comparison_excluded_fields` removes fields from parity reporting but still preserves them internally where needed for load relationships.

### `audit_diff.py`

Primary function:

```python
audit_diff_outputs(...)
```

Purpose:

- Creates smaller audit CSVs from the diff checkpoint parts.
- Intended for manual spot checks and stakeholder review.

Typical outputs:

```text
diff_audit_summary.csv
diff_audit_object_change_counts.csv
diff_audit_record_samples.csv
diff_audit_invalid_record_objects.csv
diff_audit_field_change_counts.csv
diff_audit_invalid_field_pairs.csv
diff_audit_field_value_samples.csv
```

Most useful audit file for actual value deltas:

```text
diff_audit_field_value_samples.csv
```

It shows examples by object, field, change type, MCUAT value, and PROD value.

### `upsert_sequence.py`

Primary function:

```python
generate_upsert_sequence_csv(...)
```

Purpose:

- Uses Salesforce describe metadata and `metadata_scope.csv` to infer object dependencies.
- Generates `upsert_sequence.csv`.
- The loader uses this file to process parent/dependency objects before child objects.

Important output columns:

```text
Load_Order
Object
Enabled
Manual_Override_Order
Dependency_Objects
Dependency_Fields
Self_Dependency_Fields
External_Dependency_Fields
Dependency_Status
Reason
Notes
```

Important caveat:

- Some objects have circular or ambiguous dependencies.
- Those rows may be flagged with statuses like `MANUAL_REVIEW_CYCLE`, `MANUAL_REVIEW_SELF_REFERENCE`, or `HAS_EXTERNAL_DEPENDENCIES`.
- `Manual_Override_Order` can be used to force a specific load sequence.

### `load.py`

Primary function:

```python
load_salesforce_diff_to_target(...)
```

Purpose:

- Reads diff checkpoint parts.
- Uses `metadata_scope.csv` to map SF 1.0 fields to SF 2.0 fields.
- Uses `upsert_sequence.csv` for load order.
- Creates/updates SF 2.0 records based on the correct SF 1.0 Production values.
- Writes `load_results.csv`.

Current load stages:

```python
load_step=1  # Dry run sample
load_step=2  # Small live sample
load_step=3  # Full live load
```

Current step behavior:

| Step | Name | Writes to Salesforce? | Scope |
|---|---|---:|---|
| 1 | Dry run sample | No | Up to 5 operations per enabled object |
| 2 | Live sample | Yes | Up to 1 operation per enabled object |
| 3 | Full live load | Yes | All eligible diff records |

Important behavior:

- `load.py` reads `PROD_value` for actual diff fields.
- It also reads `PROD_load_value` when present, which contains relationship fields excluded from public diff reporting.
- This lets the loader build relationship payloads like:

```json
{
  "Account": {
    "External_Id__c": "001PROD..."
  }
}
```

Result file:

```text
load_results.csv
```

Useful columns:

```text
Target_Object
Source_RecordId
Target_RecordId
Change_Type
Operation
Dry_Run
Success
Payload_Field_Count
Payload_JSON
Skipped_Fields
Message
```

`Payload_JSON` is the most important review column. It shows the exact payload that would be or was sent to Salesforce.

## Current `main.py` Pattern

`main.py` is being used as a manual orchestrator. Sections are commented/uncommented as needed, with `quit()` calls to stop after a phase.

Current active block is the diff run:

```python
diff_salesforce_parquet_extracts(
    source_extract_dir="mcuat_extract_parquet",
    target_extract_dir="prod_extract_parquet",
    output_feather_path="diff.feather",
    output_csv=False,
    exclusions_csv_path="diff_exclusions.csv",
    resume_from_checkpoint=False,
    return_dataframe=False,
    comparison_excluded_fields=[
        "Id",
        "*Id",
    ],
)

quit()
```

After diff completes, the next practical phase is usually:

```python
load_salesforce_diff_to_target(
    target_env="MCUAT8451",
    source_env="PROD",
    diff_checkpoint_dir="diff_diff_parts",
    metadata_scope_csv_path="metadata_scope.csv",
    upsert_sequence_csv_path="upsert_sequence.csv",
    results_csv_path="load_results.csv",
    load_step=1,
)
```

## Recommended End-to-End Run Order

### 1. Confirm mapping and exclusions

Review:

```text
metadata_scope.csv
diff_exclusions.csv
upsert_sequence.csv
```

Confirm:

- Required objects and fields are in scope.
- `Ignore = TRUE` rows are intentional.
- Operational/noisy fields are excluded only where appropriate.
- Object sequencing is acceptable.

### 2. Extract data

Run extract for MCUAT and PROD.

Expected outputs:

```text
mcuat_extract_parquet/
prod_extract_parquet/
field_extract_errors.csv
```

### 3. Run diff

Run diff with:

```python
output_csv=False
resume_from_checkpoint=False
comparison_excluded_fields=["Id", "*Id"]
```

Expected outputs:

```text
diff_diff_parts/
diff.feather
diff_excluded_rows.csv
diff_cleaned_values.csv
```

The loader reads `diff_diff_parts/`.

### 4. Optional audit outputs

Run `audit_diff_outputs(...)` if human-readable audit samples are needed.

Expected outputs:

```text
diff_audit_summary.csv
diff_audit_object_change_counts.csv
diff_audit_field_change_counts.csv
diff_audit_field_value_samples.csv
```

### 5. Generate or review object load sequence

Run or review:

```python
generate_upsert_sequence_csv(...)
```

Expected output:

```text
upsert_sequence.csv
```

Review any rows flagged for manual dependency review.

### 6. Load step 1: dry run sample

Run:

```python
load_step=1
```

Expected output:

```text
load_results.csv
```

Review:

- Payload fields.
- Skipped fields.
- Any unsupported mappings.
- Whether relationship payloads look correct.

### 7. Load step 2: small live sample

Run:

```python
load_step=2
```

This writes a small number of records to Salesforce.

Review:

- Success/failure counts.
- Salesforce validation errors.
- Trigger/flow behavior.
- Whether parent-child relationships are created correctly.

### 8. Load step 3: full load

Run:

```python
load_step=3
```

Only run this after dry-run and live-sample results are reviewed and approved.

## Validation Rules, Triggers, and Flows Strategy

Do not automatically deactivate all validation rules, triggers, and flows.

Current recommendation:

1. Keep automation active for dry run and the first small live sample.
2. Capture failures in `load_results.csv`.
3. Classify failures as:
   - Data quality issue.
   - Mapping issue.
   - Sequence/dependency issue.
   - Expected migration-only blocker.
   - Automation side effect requiring targeted bypass.
4. Apply targeted bypasses only where justified.

Reason:

- Automation errors are useful signal.
- Blanket disabling can load bad data and hide issues until UAT.

## Key Design Decisions Already Made

### Partitioned Parquet instead of final combined Feather as primary extract output

Reason:

- Large data volume caused memory pressure and SIGKILL failures.
- Partitioned Parquet supports checkpointing and partial processing.

### Diff checkpoints are the primary input to load

Reason:

- Avoids loading massive `diff.csv` or `diff.feather` into memory.
- Supports resumable processing.

### Salesforce record IDs are excluded from parity reporting

Reason:

- IDs differ between Salesforce orgs by design.
- Counting `Account.Id`, `Opportunity.Id`, `Opportunity.AccountId`, etc. as gaps creates false positives.

### Relationship IDs are still preserved internally for load

Reason:

- Child records need parent references.
- Example: `Opportunity.AccountId` should not be reported as a data mismatch, but the PROD Account ID is still needed to set `Opportunity.Account.External_Id__c` in SF 2.0.

### Three load steps

Reason:

- Reduces risk.
- Supports manual review.
- Captures real Salesforce errors before full load.

## Known Caveats / Open Items

1. `upsert_sequence.csv` should be reviewed before live loading.
2. The step 2 live sample runs across enabled objects in sequence, but it is not currently guaranteed to be one fully connected account/opportunity/product cohort.
3. Validation rules and flows may block sample writes. That is expected and should be reviewed before bypassing anything.
4. If diff is interrupted with `resume_from_checkpoint=False`, rerun or resume the diff before loading.
5. Old diff outputs may not contain hidden load relationship payloads. For the current load design, rerun `diff.py` to completion with the latest code before testing all-object load.

## Relevant Timeline From Deployment Plan

One source row shows `6/3/2926`; this is assumed to mean `6/3/2026`.

| Date / Range | Activity | Owner / DRI | Status / Notes |
|---|---|---|---|
| 5/14/2026 - 5/26/2026 | DM parity check between SF 1.0 Production and SF 2.0 UAT | James | Complete. Findings shared; gaps identified and reviewed. |
| 6/3/2026 - 6/8/2026 | Full data migration to MCUAT using 84.51 migration method | James / 84.51 | In progress. Confirms production migration readiness. |
| 6/8/2026 | Data Migration Signoff for MCUAT | James | Signoff means confidence that all SF 1.0 Production data was successfully migrated to SF 2.0 MCUAT. |
| 6/8/2026 | Integration Signoff in MCUAT | James | Confirms SF 2.0 MCUAT integrations are operational, tested, and signed off by end users. |
| 6/8/2026 - 6/10/2026 | Full end-to-end business testing in UAT | Liz / Andrew | Business validation window. |
| 6/8/2026 - 6/12/2026 | UAT issue resolution and final platform readiness signoff | Liz / James | Target is 0 Sev 1 issues; only bug fixes after signoff. |
| 6/15/2026 | Final signoff of mapping file | James / Silverline | Confirms object and field mapping alignment. |
| 6/15/2026 | Silverline handoff of migration sequence and additional data files | Jennifer / James | Includes migration exceptions and pricing-related files where applicable. |
| 6/15/2026 - 6/19/2026 | Delete any existing data in SF 2.0 Production, if needed | James / 84.51 | Production preparation. |
| 6/15/2026 - 6/19/2026 | Initial data migration from SF 1.0 Production to SF 2.0 Production | James / 84.51 | Initial production load. |
| 6/15/2026 - 6/19/2026 | Parity check between SF 1.0 and SF 2.0 Production | James / 84.51 | Target noted as greater than 95% parity; includes `External_Id__c` and related triggers. |
| 6/15/2026 - 6/19/2026 | Signoff of initial Production data migration | James / 84.51 | Signoff that data is present and materially correct. |
| 6/15/2026 - 6/26/2026 | Integrations migrated to SF 2.0 and read/write tested | Mohana / James | Integration validation window. |
| 6/29/2026 - 7/6/2026 | End-user smoke testing for functionality, data migration, and integrations | Liz / 84.51 | Business-facing smoke test window. |
| 7/6/2026 | End-user signoff for SF 2.0 testing | 84.51 | No additional Production deployments after this date without deliberate agreement. |
| 7/20/2026 - 7/24/2026 | Final delta migration from SF 1.0 Production to SF 2.0 Production | James / 84.51 | Final load during cutover window. |
| 7/20/2026 - 7/24/2026 | Final parity check between SF 1.0 and SF 2.0 Production | James / 84.51 | Final validation before go-live. |
| 7/20/2026 - 7/24/2026 | Signoff on final data migration | James / 84.51 | Final migration approval. |
| After hours 7/23/2026 | Production go-live deployment begins | 84.51 / Silverline | Deployment begins after 5:00 PM. |
| 7/24/2026 | SF 2.0 Production live for users | 84.51 / Silverline | Team available over the weekend for support if needed. |

## What To Ask Web ChatGPT To Help With

When uploading this file to web ChatGPT, useful asks include:

- Review this migration workflow for gaps or risks.
- Help create stakeholder-friendly explanations of the load stages.
- Help create signoff criteria or an approval checklist.
- Help interpret `load_results.csv` after a dry run or sample live load.
- Help categorize Salesforce validation errors from `load_results.csv`.
- Help identify where a migration bypass may or may not be justified.

If asking for code-specific guidance, also upload the relevant source file, such as `load.py`, `diff.py`, or `metadata_scope.csv`. This context file explains the process, but it does not include full source code.

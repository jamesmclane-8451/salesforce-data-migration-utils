# Salesforce 2.0 Data Migration Strategy

## TL;DR

84.51 will execute and validate the Salesforce data migration from SF 1.0 Production to SF 2.0 using a controlled, auditable process. The migration will be tested first in MCUAT, then executed in SF 2.0 Production in phases.

The strategy is:

1. Use the approved mapping file to define which objects and fields are in scope.
2. Extract SF 1.0 and SF 2.0 data into partitioned files.
3. Run a parity/diff process to identify missing records, missing values, incorrect values, formatting issues, and mismapped records.
4. Load records in dependency order so parent records are available before child records.
5. Run three load stages: dry run, small live sample, full load.
6. Keep validation rules, triggers, and flows active for early testing so errors are visible and can be reviewed.
7. Use parity checks and business testing to support migration signoff.

Business signoff should mean: we are confident that all in-scope SF 1.0 Production data has been migrated into SF 2.0, object relationships are intact, critical field values are correct, integrations can read/write successfully, and any remaining gaps are understood, accepted, or assigned for remediation.

## Scope

The migration scope is controlled by `metadata_scope.csv`.

This file defines:

- Which SF 1.0 objects and fields are in scope.
- Which SF 2.0 objects and fields they map to.
- Which rows should be ignored.
- Any field-level transformation logic.

Rows marked `Ignore = TRUE` are excluded from extract, diff, and load processing.

Salesforce record ID fields are not treated as business-value mismatches because record IDs are expected to differ between SF 1.0 and SF 2.0. Those ID fields are excluded from parity reporting, but they are still preserved internally where needed to maintain parent-child relationships during load.

## Migration Approach

### 1. Extract

Data is extracted from both environments:

- Source: SF 1.0 Production
- Target validation environment: SF 2.0 MCUAT
- Final target: SF 2.0 Production

Extracts are stored as partitioned Parquet files so the process can handle large data volumes without requiring one massive file in memory.

### 2. Diff / Parity Check

The diff process compares SF 1.0 source data to SF 2.0 target data using the expected external ID relationship.

The diff identifies:

- Records present in SF 1.0 but missing from SF 2.0.
- Records present in SF 2.0 but not found in SF 1.0.
- Field values that differ on matched records.
- Blank values where SF 1.0 has data.
- Formatting or encoding differences.
- Mismapped records.

Record ID fields and lookup ID fields are excluded from parity reporting because they are expected to be different across Salesforce orgs. The loader still preserves those references internally so child records can be related to parent records through `External_Id__c`.

### 3. Sequence

Records are loaded by object dependency order, not randomly.

The sequence is generated from Salesforce metadata and reviewed through `upsert_sequence.csv`. Parent objects are loaded before child objects wherever possible. Examples:

- Accounts before Contacts, Opportunities, and Account relationships.
- Products and Pricebooks before Pricebook Entries and Opportunity Line Items.
- Opportunities before Opportunity Line Items, Schedules, Quotes, and related contact roles.

Objects with circular or ambiguous dependencies are flagged for review in the sequence file. Manual override order can be used where the automated sequence cannot determine a safe order.

### 4. Load

The load process uses the diff output and mapping file to create or update SF 2.0 records from the correct SF 1.0 Production values.

The load has three stages:

| Stage | Name | Salesforce Writes? | Purpose |
|---|---:|---:|---|
| 1 | Dry run sample | No | Review sample payloads across all enabled objects before making changes. |
| 2 | Small live sample | Yes | Validate real upsert behavior, sequencing, validation rules, triggers, flows, and integration side effects using a controlled sample. |
| 3 | Full load | Yes | Execute the complete in-scope migration after sample results are reviewed and approved. |

Each load run writes `load_results.csv`, which shows:

- Target object.
- Source record ID.
- Target record ID, if available.
- Operation type: update, upsert, create, or skip.
- Payload fields that would be or were sent to Salesforce.
- Success/failure status.
- Salesforce error messages, if any.

For dry runs, `load_results.csv` is the primary review artifact.

## Validation Rules, Triggers, and Flows

The initial strategy is not to deactivate validation rules, triggers, or flows by default.

Reason: those errors are useful signal. They show whether the migration is trying to load invalid values, missing relationships, bad picklist values, locked-status records, or data that conflicts with SF 2.0 business logic.

The process is:

1. Keep automation active during dry run and the first small live sample.
2. Capture all errors in `load_results.csv`.
3. Review each recurring error and classify it as:
   - Data quality issue to fix.
   - Mapping issue to correct.
   - Sequence/dependency issue.
   - Expected migration-only blocker.
   - Automation side effect that requires a temporary bypass.
4. Apply targeted bypasses only where justified.

Any bypass should be controlled, documented, and limited to the migration window.

## Acceptance Criteria

Data migration is ready for signoff when:

- All in-scope objects and fields have been extracted and diffed.
- Material missing-record and field-value gaps are resolved or accepted.
- Parent-child relationships are validated through external IDs.
- Dry-run payloads have been reviewed.
- Small live sample results are reviewed and do not show unresolved systemic issues.
- Full load completes with successful result reporting.
- Parity checks meet the agreed threshold.
- Integrations can read and write against SF 2.0 successfully.
- Business users can complete smoke testing and critical end-to-end test cases.
- Any remaining exceptions are documented with owners and resolution plans.

## Relevant Timeline

The timeline below is pulled from the go-live deployment plan. One row shows `6/3/2926`; this is assumed to be a typo for `6/3/2026`.

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

## Key Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Missing or mismapped records are discovered late. | Run MCUAT migration, diff/parity checks, dry-run load review, and small live samples before full production load. |
| Child records load before parent records. | Use `upsert_sequence.csv` and external IDs to load parent objects first and preserve relationships. |
| Validation rules, triggers, or flows block records. | Capture errors during sample load, classify the cause, and apply targeted bypasses only where justified. |
| Large data volumes make manual review impractical. | Use partitioned Parquet outputs, summarized audit files, sample payload review, and targeted spot checks. |
| Final delta load misses late-changing data. | Run final delta migration and parity check during the 7/20-7/24 cutover window before final signoff. |
| Production data is loaded before the business can validate. | Use MCUAT rehearsal, Production initial load, parity checks, and end-user smoke testing before go-live. |

## Signoff Decision

Stakeholder approval of this strategy means approval of the following:

- 84.51 will own execution of the migration process.
- The migration will be validated first in MCUAT before Production.
- Production migration will use staged execution: dry run, sample live load, full load.
- Object sequencing will be dependency-driven and reviewed before live loads.
- Automation will remain active initially so errors can be evaluated instead of hidden.
- Final signoff will be based on parity results, load results, integration validation, and business smoke testing.

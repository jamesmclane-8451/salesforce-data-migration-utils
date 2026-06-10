# EventBridge Salesforce Data Migration Utilities

Utilities for extracting Salesforce records, diffing SF 1.0 vs SF 2.0 data, auditing deltas, preparing load sequencing, and loading approved deltas into a target Salesforce environment.

## What is tracked

This repo tracks the reusable migration code and configuration:

- Python migration utilities
- metadata mapping files
- load sequencing/config CSVs
- lightweight documentation

Generated extracts, diffs, load results, checkpoint folders, OAuth token files, and local environment files are intentionally ignored by Git.

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy `.env.example` to `.env` and populate Salesforce connected app credentials and environment hosts.

## Main workflow

The current workflow is orchestrated from `main.py` by uncommenting/running the relevant section:

1. Extract Salesforce data to partitioned Parquet.
2. Diff source and target extracts.
3. Audit diff outputs.
4. Generate or update load sequence.
5. Run pre-load cleanup, such as inactive owner replacement.
6. Load deltas to the target environment with resumable result tracking.

Large runtime outputs should remain local and should not be committed.

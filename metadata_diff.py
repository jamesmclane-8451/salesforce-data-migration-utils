

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from auth import get_salesforce_connection


def compare_salesforce_object_metadata(
    source_env: str,
    target_env: str,
    source_objects: List[str],
    output_csv_path: str,
    object_name_mappings: Optional[Dict[str, str]] = None,
    env_name_mappings: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    =====================================================================================
    TL;DR
    =====================================================================================
    This function compares Salesforce object field metadata between two environments.

    It uses the same Salesforce login pattern as the other working files in this
    project, resolves env aliases like "MCUAT" to the real configured env label
    such as "MCUAT8451", retrieves object describe metadata for the requested objects,
    compares fields by API name, supports object-name mappings where object names
    differ between envs, and writes a CSV containing only unmatched fields.

    =====================================================================================
    HOW IT WORKS
    =====================================================================================
    1. Resolve the input env names to the real auth-configured env labels
    2. Connect to Salesforce using:
           get_salesforce_connection(env=...)
    3. For each requested source object:
        - Determine the matching target object name
        - Pull describe metadata for both objects
        - Compare field API names between the two objects
    4. Output rows only where:
        - the field exists in source but not target
        - the field exists in target but not source
        - the object does not exist in one side
    5. Write the diff output to CSV and return it as a DataFrame

    =====================================================================================
    WHY THIS EXISTS
    =====================================================================================
    This identifies field-level metadata gaps between SF 2.0 and SF 1.0 so you can
    define the translation / mapping layer required for the Event Bridge.

    =====================================================================================
    PARAMETERS
    =====================================================================================
    source_env : str
        Source Salesforce environment name or alias.
        Example:
            "MCUAT"
            "MCUAT8451"
            "PROD"

    target_env : str
        Target Salesforce environment name or alias.
        Example:
            "PROD"
            "MCUAT"
            "MCUAT8451"

    source_objects : List[str]
        List of source object API names to compare.
        These are treated as the canonical input objects.

    output_csv_path : str
        Full file path where the CSV diff output should be written.

    object_name_mappings : Optional[Dict[str, str]]
        Optional dict mapping source object name -> target object name
        for cases where the object API names differ between orgs.

        Example:
            {
                "Account_Relationship__c": "CPG__c"
            }

    env_name_mappings : Optional[Dict[str, str]]
        Optional dict mapping user-friendly env aliases to the real env labels
        expected by auth.py.

        Example:
            {
                "MCUAT": "MCUAT8451",
                "PROD": "PROD"
            }

        If not provided, a default alias mapping is used.

    =====================================================================================
    RETURNS
    =====================================================================================
    pd.DataFrame
        DataFrame containing only unmatched field metadata rows.

    =====================================================================================
    OUTPUT COLUMNS
    =====================================================================================
    source_env
    target_env
    source_object
    target_object
    source_field_api_name
    target_field_api_name
    comparison_result

    comparison_result values:
        - missing_in_target
        - missing_in_source
        - source_object_not_found
        - target_object_not_found

    =====================================================================================
    IMPORTANT NOTES
    =====================================================================================
    - This is a metadata comparison, not a data comparison.
    - It compares field API names only, for example Some_Field__c.
    - It does not compare field type, length, formula logic, requiredness, picklist
      values, or other metadata attributes.
    - It outputs only non-matches, because those are the rows needed to inform
      mapping work for the Event Bridge.
    =====================================================================================
    """

    object_name_mappings = object_name_mappings or {}

    default_env_name_mappings: Dict[str, str] = {
        "MCUAT": "MCUAT8451",
        "PROD": "PROD",
    }

    if env_name_mappings:
        default_env_name_mappings.update(
            {k.strip().upper(): v.strip() for k, v in env_name_mappings.items()}
        )

    def resolve_env_name(env_name: str) -> str:
        """
        Resolve a user-facing env alias to the real env label expected by auth.py.

        Examples:
            MCUAT -> MCUAT8451
            PROD -> PROD
        """
        cleaned = env_name.strip()
        upper_cleaned = cleaned.upper()

        return default_env_name_mappings.get(upper_cleaned, cleaned)

    def connect_to_salesforce(env_name: str) -> Any:
        """
        Connect to Salesforce using the same auth pattern as the working files
        in this project.

        Important:
        auth.py expects the real configured env label, not necessarily the short alias.
        """
        resolved_env_name = resolve_env_name(env_name)
        print(f"Connecting using env label [{resolved_env_name}]")
        return get_salesforce_connection(env=resolved_env_name)

    def get_field_api_names(sf: Any, object_api_name: str) -> Optional[Set[str]]:
        """
        Describe one Salesforce object and return the set of field API names.
        Returns None if the object does not exist or describe fails.
        """
        try:
            describe_result = getattr(sf, object_api_name).describe()
            return {
                field["name"].strip()
                for field in describe_result.get("fields", [])
                if field.get("name")
            }
        except Exception as exc:
            print(f"Could not describe object {object_api_name}: {exc}")
            return None

    source_sf = connect_to_salesforce(source_env)
    target_sf = connect_to_salesforce(target_env)

    diff_rows: List[Dict[str, Any]] = []

    for source_object in source_objects:
        target_object = object_name_mappings.get(source_object, source_object)

        print(
            f"Comparing source env [{source_env}] object [{source_object}] "
            f"to target env [{target_env}] object [{target_object}]"
        )

        source_fields = get_field_api_names(source_sf, source_object)
        target_fields = get_field_api_names(target_sf, target_object)

        if source_fields is None:
            diff_rows.append(
                {
                    "source_env": source_env,
                    "target_env": target_env,
                    "source_object": source_object,
                    "target_object": target_object,
                    "source_field_api_name": pd.NA,
                    "target_field_api_name": pd.NA,
                    "comparison_result": "source_object_not_found",
                }
            )
            continue

        if target_fields is None:
            diff_rows.append(
                {
                    "source_env": source_env,
                    "target_env": target_env,
                    "source_object": source_object,
                    "target_object": target_object,
                    "source_field_api_name": pd.NA,
                    "target_field_api_name": pd.NA,
                    "comparison_result": "target_object_not_found",
                }
            )
            continue

        source_only_fields = sorted(source_fields - target_fields)
        target_only_fields = sorted(target_fields - source_fields)

        for source_field in source_only_fields:
            diff_rows.append(
                {
                    "source_env": source_env,
                    "target_env": target_env,
                    "source_object": source_object,
                    "target_object": target_object,
                    "source_field_api_name": source_field,
                    "target_field_api_name": pd.NA,
                    "comparison_result": "missing_in_target",
                }
            )

        for target_field in target_only_fields:
            diff_rows.append(
                {
                    "source_env": source_env,
                    "target_env": target_env,
                    "source_object": source_object,
                    "target_object": target_object,
                    "source_field_api_name": pd.NA,
                    "target_field_api_name": target_field,
                    "comparison_result": "missing_in_source",
                }
            )

    diff_df = pd.DataFrame(
        diff_rows,
        columns=[
            "source_env",
            "target_env",
            "source_object",
            "target_object",
            "source_field_api_name",
            "target_field_api_name",
            "comparison_result",
        ],
    )

    if not diff_df.empty:
        diff_df = diff_df.sort_values(
            by=[
                "source_object",
                "target_object",
                "comparison_result",
                "source_field_api_name",
                "target_field_api_name",
            ],
            kind="stable",
        ).reset_index(drop=True)

    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    diff_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"Metadata diff CSV written: {output_path}")
    print(f"Diff row count: {len(diff_df)}")

    return diff_df
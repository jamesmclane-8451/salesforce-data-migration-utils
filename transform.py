
"""
Source of Field Mappings
- https://8451-my.sharepoint.com/:x:/p/j825724/EaT16MZB6jlJvyIjmKcT9SUBQtkOh0MFDWFPykAOxXo0kw?email=Eli.Hougland.Contractor%408451.com&wdOrigin=TEAMS-MAGLEV.p2p_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1775851353264&web=1


"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import pandas as pd


def transform_salesforce_diff_for_sf1_load(
    diff_feather_path: str,
    metadata_scope_csv_path: str,
    output_feather_path: str,
    source_env: str,
    target_env: str,
    output_csv: bool = True,
) -> pd.DataFrame:
    """
    Transform SF 2.0 diff output into SF 1.0 load-ready payloads.

    Purpose
    -------
    This function is intended to run after `diff_salesforce_parquet_extracts`.

    It reads:
    1. The diff output Feather file
    2. The metadata scope CSV

    It produces:
    - one row per SF 1.0 record operation
    - mapped SF 1.0 object name
    - mapped SF 1.0 field payload
    - operation type, either update or create
    - target SF 1.0 record Id when available
    - payload JSON ready to pass into Salesforce create/update logic

    Important behavior
    ------------------
    - Uses `SF 2.0 Object` + `SF 2.0 Field` to find the corresponding
      `SF 1.0 Object` + `SF 1.0 Field`.
    - Ignores `Filter Logic (SOQL)`.
    - Applies `Transformation Logic (JSON)` mappings when present.
    - Does not include target `Id` inside the payload body.
    - Uses `External_Id__c` to decide whether the SF 2.0 record likely maps
      to an existing SF 1.0 record.
    - If `External_Id__c` equals the SF 2.0 record Id, the record is treated
      as a create.
    - If `External_Id__c` differs from the SF 2.0 record Id, the record is
      treated as an update.

    Parameters
    ----------
    diff_feather_path : str
        Path to the Feather file created by `diff_salesforce_parquet_extracts`.

    metadata_scope_csv_path : str
        Path to the metadata scope CSV. Required columns are:
        - SF 1.0 Object
        - SF 1.0 Field
        - SF 2.0 Object
        - SF 2.0 Field

    output_feather_path : str
        Path where the transformed SF 1.0 load-ready Feather file will be written.

    source_env : str
        Source environment name used in the diff output, for example "MCUAT8451".

    target_env : str
        Target environment name used in the diff output, for example "PROD".

    output_csv : bool
        When True, also writes a CSV next to the Feather output.

    Returns
    -------
    pd.DataFrame
        Load-ready DataFrame with one row per SF 1.0 create/update operation.
    """

    required_metadata_columns: List[str] = [
        "SF 1.0 Object",
        "SF 1.0 Field",
        "SF 2.0 Object",
        "SF 2.0 Field",
    ]

    def normalize_blank(value: Any) -> Optional[str]:
        """
        Convert blank-like values into None, otherwise return a stripped string.

        Parameters
        ----------
        value : Any
            Raw value.

        Returns
        -------
        Optional[str]
            Clean string or None.
        """
        if value is None or pd.isna(value):
            return None

        text = str(value).strip()

        if text.lower() in {"", "nan", "none", "null", "<na>"}:
            return None

        return text

    def parse_json_dict(value: Any) -> Dict[str, Any]:
        """
        Parse a JSON string into a dictionary.

        Parameters
        ----------
        value : Any
            JSON string, dictionary, or blank value.

        Returns
        -------
        Dict[str, Any]
            Parsed dictionary. Returns empty dict when value is blank.
        """
        if isinstance(value, dict):
            return value

        text = normalize_blank(value)

        if text is None:
            return {}

        parsed = json.loads(text)

        if not isinstance(parsed, dict):
            raise ValueError(f"Expected JSON object, received: {type(parsed)}")

        return parsed

    def to_json_string(value: Any) -> str:
        """
        Serialize a value into stable JSON.

        Parameters
        ----------
        value : Any
            Value to serialize.

        Returns
        -------
        str
            JSON string.
        """
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

    def find_first_matching_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        """
        Return the first candidate column that exists in a DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to inspect.

        candidates : List[str]
            Candidate column names.

        Returns
        -------
        Optional[str]
            First matching column name, or None.
        """
        for candidate in candidates:
            if candidate in df.columns:
                return candidate

        return None

    def canonical_lookup_key(value: Any) -> tuple[str, Any]:
        """
        Convert a mapping input into a stable lookup key.

        This lets JSON booleans in the metadata match string values such as
        "True" / "False" that came through the flattened extract.
        """
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

    def parse_transformation_logic(value: Any, row_number: int) -> Optional[Dict[str, Any]]:
        """
        Parse optional Transformation Logic JSON from metadata_scope.csv.
        """
        text = normalize_blank(value)

        if text is None:
            return None

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid Transformation Logic (JSON) on metadata row {row_number}: {exc}"
            ) from exc

        if not isinstance(parsed, dict):
            raise ValueError(
                "Transformation Logic (JSON) must be a JSON object "
                f"on metadata row {row_number}"
            )

        return parsed

    def transformation_direction(
        transformation_logic: Dict[str, Any],
        sf1_object: str,
        sf1_field: str,
        sf2_object: str,
        sf2_field: str,
    ) -> str:
        """
        Determine whether the JSON mapping is direct or needs to be inverted.

        The transform function moves SF 2.0 -> SF 1.0. If the JSON says
        source=SF 1.0 and target=SF 2.0, the value map is inverted.
        """
        source = transformation_logic.get("source") or {}
        target = transformation_logic.get("target") or {}

        source_key = (
            normalize_blank(source.get("object")),
            normalize_blank(source.get("field")),
        )
        target_key = (
            normalize_blank(target.get("object")),
            normalize_blank(target.get("field")),
        )
        sf1_key = (sf1_object, sf1_field)
        sf2_key = (sf2_object, sf2_field)

        if source_key == sf2_key and target_key == sf1_key:
            return "direct"

        if source_key == sf1_key and target_key == sf2_key:
            return "inverse"

        return "direct"

    def apply_transformation_logic(value: Any, transformation_logic: Optional[Dict[str, Any]]) -> Any:
        """
        Apply a metadata value mapping to a single SF 2.0 value.
        """
        if not transformation_logic:
            return value

        value_mapping = transformation_logic.get("mapping")

        if not isinstance(value_mapping, dict):
            return value

        direction = transformation_logic.get("_direction", "direct")

        if direction == "inverse":
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

    def build_metadata_mapping(metadata_df: pd.DataFrame) -> Dict[tuple[str, str], List[Dict[str, Any]]]:
        """
        Build a mapping from SF 2.0 object/field to SF 1.0 object/field.

        Parameters
        ----------
        metadata_df : pd.DataFrame
            Metadata scope DataFrame.

        Returns
        -------
        Dict[tuple[str, str], List[Dict[str, Any]]]
            Mapping shaped as:
            (sf2_object, sf2_field) -> target mapping details
        """
        missing_columns = [
            col for col in required_metadata_columns if col not in metadata_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Metadata scope file is missing required columns: {missing_columns}"
            )

        working_df = metadata_df.copy()

        for col in required_metadata_columns:
            working_df[col] = working_df[col].map(normalize_blank)

        mapping: Dict[tuple[str, str], List[Dict[str, Any]]] = {}

        for row_number, row in working_df.iterrows():
            sf1_object = row["SF 1.0 Object"]
            sf1_field = row["SF 1.0 Field"]
            sf2_object = row["SF 2.0 Object"]
            sf2_field = row["SF 2.0 Field"]

            if not sf1_object or not sf1_field or not sf2_object or not sf2_field:
                continue

            raw_transformation_logic = row.get("Transformation Logic (JSON)")
            parsed_transformation_logic = parse_transformation_logic(
                raw_transformation_logic,
                row_number=row_number + 2,
            )

            if parsed_transformation_logic:
                parsed_transformation_logic["_direction"] = transformation_direction(
                    transformation_logic=parsed_transformation_logic,
                    sf1_object=sf1_object,
                    sf1_field=sf1_field,
                    sf2_object=sf2_object,
                    sf2_field=sf2_field,
                )

            mapping.setdefault((sf2_object, sf2_field), []).append(
                {
                    "sf1_object": sf1_object,
                    "sf1_field": sf1_field,
                    "transformation_logic": parsed_transformation_logic,
                }
            )

        return mapping

    def infer_operation(
        source_record_id: Optional[str],
        target_record_id: Optional[str],
        external_id: Optional[str],
    ) -> str:
        """
        Determine whether the row should be loaded as an update or create.

        Parameters
        ----------
        source_record_id : Optional[str]
            SF 2.0 record Id.

        target_record_id : Optional[str]
            SF 1.0 record Id from the diff output, when present.

        external_id : Optional[str]
            SF 2.0 External_Id__c value.

        Returns
        -------
        str
            "update" or "create".
        """
        if target_record_id:
            return "update"

        if external_id and source_record_id and external_id != source_record_id:
            return "update"

        return "create"

    def resolve_target_record_id(
        source_record_id: Optional[str],
        target_record_id: Optional[str],
        external_id: Optional[str],
    ) -> Optional[str]:
        """
        Resolve the SF 1.0 record Id to use for an update.

        Parameters
        ----------
        source_record_id : Optional[str]
            SF 2.0 record Id.

        target_record_id : Optional[str]
            SF 1.0 record Id from the diff output, when present.

        external_id : Optional[str]
            SF 2.0 External_Id__c value.

        Returns
        -------
        Optional[str]
            SF 1.0 record Id, or None for creates.
        """
        if target_record_id:
            return target_record_id

        if external_id and source_record_id and external_id != source_record_id:
            return external_id

        return None

    metadata_df = pd.read_csv(metadata_scope_csv_path, dtype=str, encoding="utf-8-sig").fillna("")
    field_mapping = build_metadata_mapping(metadata_df)

    diff_df = pd.read_feather(diff_feather_path)

    source_value_col = find_first_matching_column(
        diff_df,
        [
            f"{source_env}_value",
            f"{source_env.upper()}_value",
            f"{source_env.lower()}_value",
        ],
    )

    source_record_id_col = find_first_matching_column(
        diff_df,
        [
            f"{source_env.lower()}_recordid",
            f"{source_env.upper()}_recordid",
            f"{source_env}_recordid",
        ],
    )

    target_record_id_col = find_first_matching_column(
        diff_df,
        [
            f"{target_env.lower()}_recordid",
            f"{target_env.upper()}_recordid",
            f"{target_env}_recordid",
        ],
    )

    if source_value_col is None:
        raise ValueError(f"Could not find source value column for source_env={source_env}")

    if source_record_id_col is None:
        raise ValueError(f"Could not find source record id column for source_env={source_env}")

    if target_record_id_col is None:
        raise ValueError(f"Could not find target record id column for target_env={target_env}")

    output_rows: List[Dict[str, Any]] = []

    for _, row in diff_df.iterrows():
        sf2_object = normalize_blank(row.get("Obj"))
        source_record_id = normalize_blank(row.get(source_record_id_col))
        target_record_id_from_diff = normalize_blank(row.get(target_record_id_col))
        external_id = normalize_blank(row.get("External_Id__c"))
        change_type = normalize_blank(row.get("change_type"))

        if not sf2_object:
            continue

        source_payload = parse_json_dict(row.get(source_value_col))

        mapped_payloads_by_target_object: Dict[str, Dict[str, Any]] = {}
        unmapped_fields: List[str] = []

        for sf2_field, value in source_payload.items():
            mapping_key = (sf2_object, sf2_field)
            target_mappings = field_mapping.get(mapping_key, [])

            if not target_mappings:
                unmapped_fields.append(sf2_field)
                continue

            for target_mapping in target_mappings:
                sf1_object = target_mapping["sf1_object"]
                sf1_field = target_mapping["sf1_field"]

                if sf1_field == "Id":
                    continue

                mapped_value = apply_transformation_logic(
                    value,
                    target_mapping.get("transformation_logic"),
                )
                mapped_payloads_by_target_object.setdefault(sf1_object, {})
                mapped_payloads_by_target_object[sf1_object][sf1_field] = mapped_value

        if not mapped_payloads_by_target_object:
            continue

        for target_object, mapped_payload in mapped_payloads_by_target_object.items():
            if not mapped_payload:
                continue

            operation = infer_operation(
                source_record_id=source_record_id,
                target_record_id=target_record_id_from_diff,
                external_id=external_id,
            )

            resolved_target_record_id = resolve_target_record_id(
                source_record_id=source_record_id,
                target_record_id=target_record_id_from_diff,
                external_id=external_id,
            )

            output_rows.append(
                {
                    "operation": operation,
                    "sf1_object": target_object,
                    "sf1_recordid": resolved_target_record_id,
                    "sf2_object": sf2_object,
                    "sf2_recordid": source_record_id,
                    "external_id__c": external_id,
                    "change_type": change_type,
                    "payload": to_json_string(mapped_payload),
                    "unmapped_sf2_fields": to_json_string(unmapped_fields),
                }
            )

    output_df = pd.DataFrame(
        output_rows,
        columns=[
            "operation",
            "sf1_object",
            "sf1_recordid",
            "sf2_object",
            "sf2_recordid",
            "external_id__c",
            "change_type",
            "payload",
            "unmapped_sf2_fields",
        ],
    )

    output_df.to_feather(output_feather_path)

    if output_csv:
        output_csv_path = str(Path(output_feather_path).with_suffix(".csv"))
        output_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

    return output_df

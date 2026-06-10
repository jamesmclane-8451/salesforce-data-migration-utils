

from __future__ import annotations

import html
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests

from auth import get_salesforce_connection


BYPASS_PERMISSION_REFERENCE = "$Permission.Bypass_Validation_Rules"


def _tooling_request(
    sf,
    endpoint: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Runs a Salesforce Tooling API request.

    Parameters
    ----------
    sf
        Authenticated simple_salesforce connection.

    endpoint : str
        Tooling API endpoint.

    method : str
        HTTP method.

    params : Optional[Dict[str, Any]]
        Query parameters.

    Returns
    -------
    Dict[str, Any]
        Parsed Salesforce response.
    """
    kwargs: Dict[str, Any] = {}

    if params is not None:
        kwargs["params"] = params

    return sf.toolingexecute(endpoint, method=method, **kwargs)


def _tooling_query(sf, query: str) -> List[Dict[str, Any]]:
    """
    Runs a Tooling API SOQL query and pages through all results.

    Parameters
    ----------
    sf
        Authenticated simple_salesforce connection.

    query : str
        Tooling API SOQL query.

    Returns
    -------
    List[Dict[str, Any]]
        All returned records.
    """
    result = _tooling_request(
        sf=sf,
        endpoint="query",
        method="GET",
        params={"q": " ".join(query.split())},
    )

    records = result.get("records", [])

    while not result.get("done", True):
        next_url = result.get("nextRecordsUrl")
        next_endpoint = str(next_url).split("/tooling/")[-1]

        result = _tooling_request(
            sf=sf,
            endpoint=next_endpoint,
            method="GET",
        )

        records.extend(result.get("records", []))

    return records


def _describe_tooling_object(sf, object_api_name: str) -> Set[str]:
    """
    Describes a Tooling API object and returns available field names.

    Parameters
    ----------
    sf
        Authenticated simple_salesforce connection.

    object_api_name : str
        Tooling API object API name.

    Returns
    -------
    Set[str]
        Available field names.
    """
    describe_result = _tooling_request(
        sf=sf,
        endpoint=f"sobjects/{object_api_name}/describe",
        method="GET",
    )

    return {
        field["name"]
        for field in describe_result.get("fields", [])
        if field.get("name")
    }


def _build_select_clause(available_fields: Set[str], desired_fields: List[str]) -> str:
    """
    Builds a Tooling API SELECT clause using only available fields.

    Parameters
    ----------
    available_fields : Set[str]
        Fields available from describe.

    desired_fields : List[str]
        Desired fields.

    Returns
    -------
    str
        Comma-separated SELECT clause.
    """
    selected_fields = [field for field in desired_fields if field in available_fields]

    if "Id" not in selected_fields:
        selected_fields.insert(0, "Id")

    return ", ".join(dict.fromkeys(selected_fields))


def _format_soql_id_list(record_ids: List[str]) -> str:
    """
    Formats Salesforce Ids for a SOQL IN clause.

    Parameters
    ----------
    record_ids : List[str]
        Salesforce record Ids.

    Returns
    -------
    str
        Quoted comma-separated Id list.
    """
    clean_ids = [
        str(record_id).strip().replace("'", "\\'")
        for record_id in record_ids
        if str(record_id).strip()
    ]

    return ", ".join(f"'{record_id}'" for record_id in clean_ids)


def _get_validation_rule_inventory(
    sf,
    include_inactive: bool,
    available_fields: Set[str],
    validation_rule_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Gets ValidationRule inventory.

    Metadata and FullName are intentionally excluded because Tooling API can fail
    when those fields are queried across multiple ValidationRule records.

    Parameters
    ----------
    sf
        Authenticated simple_salesforce connection.

    include_inactive : bool
        Whether inactive validation rules should be included.

    available_fields : Set[str]
        Available ValidationRule fields.

    validation_rule_ids : Optional[List[str]]
        Optional ValidationRule Ids to limit processing.

    Returns
    -------
    List[Dict[str, Any]]
        ValidationRule inventory rows.
    """
    select_clause = _build_select_clause(
        available_fields=available_fields,
        desired_fields=[
            "Id",
            "ValidationName",
            "Active",
            "EntityDefinitionId",
            "NamespacePrefix",
            "ManageableState",
        ],
    )

    where_clauses: List[str] = []

    if validation_rule_ids:
        id_list = _format_soql_id_list(validation_rule_ids)
        where_clauses.append(f"Id IN ({id_list})")

    if not include_inactive and "Active" in available_fields:
        where_clauses.append("Active = true")

    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT {select_clause}
        FROM ValidationRule
        {where_clause}
    """

    return _tooling_query(sf, query)


def _get_validation_rule_metadata(
    sf,
    validation_rule_id: str,
    available_fields: Set[str],
) -> Dict[str, Any]:
    """
    Gets one ValidationRule with Metadata and FullName.

    Parameters
    ----------
    sf
        Authenticated simple_salesforce connection.

    validation_rule_id : str
        ValidationRule Tooling API Id.

    available_fields : Set[str]
        Available ValidationRule fields.

    Returns
    -------
    Dict[str, Any]
        Full ValidationRule row.
    """
    select_clause = _build_select_clause(
        available_fields=available_fields,
        desired_fields=[
            "Id",
            "ValidationName",
            "FullName",
            "Active",
            "EntityDefinitionId",
            "NamespacePrefix",
            "ManageableState",
            "Metadata",
        ],
    )

    query = f"""
        SELECT {select_clause}
        FROM ValidationRule
        WHERE Id = '{validation_rule_id}'
    """

    records = _tooling_query(sf, query)

    if not records:
        raise ValueError(f"No ValidationRule found for Id {validation_rule_id}")

    return records[0]


def _formula_already_has_bypass(formula: Optional[str]) -> bool:
    """
    Checks whether a formula already contains the bypass permission reference.

    Parameters
    ----------
    formula : Optional[str]
        Validation rule formula.

    Returns
    -------
    bool
        True if bypass permission already exists.
    """
    if not formula:
        return False

    return BYPASS_PERMISSION_REFERENCE.lower() in formula.lower()


def _build_updated_formula(original_formula: str) -> str:
    """
    Wraps the original formula with the bypass permission.

    Parameters
    ----------
    original_formula : str
        Existing validation rule formula.

    Returns
    -------
    str
        Updated formula.
    """
    return f"""AND(
    NOT({BYPASS_PERMISSION_REFERENCE}),
    (
{original_formula}
    )
)"""


def _get_metadata_api_version(sf) -> str:
    """
    Gets the Salesforce API version from the simple_salesforce connection.

    Parameters
    ----------
    sf
        Authenticated simple_salesforce connection.

    Returns
    -------
    str
        API version.
    """
    return str(getattr(sf, "sf_version", "59.0"))


def _get_metadata_endpoint(sf) -> str:
    """
    Builds the Metadata API SOAP endpoint.

    Parameters
    ----------
    sf
        Authenticated simple_salesforce connection.

    Returns
    -------
    str
        Metadata API SOAP endpoint.
    """
    api_version = _get_metadata_api_version(sf)
    instance_url = str(getattr(sf, "base_url")).split("/services/data/")[0]

    return f"{instance_url}/services/Soap/m/{api_version}"


def _escape_xml(value: Any) -> str:
    """
    Escapes a value for XML text content.

    Parameters
    ----------
    value : Any
        Raw value.

    Returns
    -------
    str
        XML-escaped text.
    """
    return html.escape(str(value), quote=False)


def _metadata_xml_tag(tag_name: str, value: Optional[Any]) -> str:
    """
    Builds one Metadata API XML tag.

    Parameters
    ----------
    tag_name : str
        XML tag name.

    value : Optional[Any]
        XML value.

    Returns
    -------
    str
        XML tag or empty string when value is None.
    """
    if value is None:
        return ""

    return f"<met:{tag_name}>{_escape_xml(value)}</met:{tag_name}>"


def _build_validation_rule_update_metadata_xml(
    full_name: str,
    metadata: Dict[str, Any],
    active: Optional[bool],
    updated_formula: str,
) -> str:
    """
    Builds the ValidationRule metadata XML used by Metadata API updateMetadata.

    Parameters
    ----------
    full_name : str
        ValidationRule full name, usually ObjectApiName.RuleApiName.

    metadata : Dict[str, Any]
        Existing metadata returned from Tooling API.

    active : Optional[bool]
        Active flag from Tooling API.

    updated_formula : str
        Updated validation rule formula.

    Returns
    -------
    str
        ValidationRule XML fragment.
    """
    active_value = metadata.get("active", active)

    if active_value is None:
        active_value = True

    active_text = "true" if bool(active_value) else "false"

    error_message = metadata.get("errorMessage")
    description = metadata.get("description")
    error_display_field = metadata.get("errorDisplayField")

    if not error_message:
        raise ValueError(f"ValidationRule {full_name} is missing errorMessage")

    validation_rule_xml = f"""
        <met:metadata xsi:type="met:ValidationRule">
            <met:fullName>{_escape_xml(full_name)}</met:fullName>
            <met:active>{active_text}</met:active>
            {_metadata_xml_tag("description", description)}
            <met:errorConditionFormula>{_escape_xml(updated_formula)}</met:errorConditionFormula>
            {_metadata_xml_tag("errorDisplayField", error_display_field)}
            <met:errorMessage>{_escape_xml(error_message)}</met:errorMessage>
        </met:metadata>
    """

    return validation_rule_xml


def _build_update_metadata_soap_envelope(
    session_id: str,
    metadata_xml_fragments: List[str],
) -> str:
    """
    Builds the SOAP envelope for Metadata API updateMetadata.

    Parameters
    ----------
    session_id : str
        Salesforce session Id.

    metadata_xml_fragments : List[str]
        Metadata XML fragments.

    Returns
    -------
    str
        Complete SOAP envelope.
    """
    metadata_xml = "\n".join(metadata_xml_fragments)

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:met="http://soap.sforce.com/2006/04/metadata"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <soapenv:Header>
        <met:SessionHeader>
            <met:sessionId>{_escape_xml(session_id)}</met:sessionId>
        </met:SessionHeader>
    </soapenv:Header>
    <soapenv:Body>
        <met:updateMetadata>
            {metadata_xml}
        </met:updateMetadata>
    </soapenv:Body>
</soapenv:Envelope>"""


def _parse_metadata_api_save_results(response_text: str) -> Tuple[bool, str]:
    """
    Parses Metadata API updateMetadata SOAP response.

    Parameters
    ----------
    response_text : str
        Raw SOAP response.

    Returns
    -------
    Tuple[bool, str]
        success flag and message.
    """
    namespaces = {
        "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
        "met": "http://soap.sforce.com/2006/04/metadata",
    }

    root = ET.fromstring(response_text)

    fault = root.find(".//soapenv:Fault", namespaces)

    if fault is not None:
        fault_string = fault.findtext("faultstring") or ET.tostring(fault, encoding="unicode")
        return False, fault_string

    result_nodes = root.findall(".//met:result", namespaces)

    if not result_nodes:
        return False, response_text

    messages: List[str] = []
    all_success = True

    for result_node in result_nodes:
        success_text = result_node.findtext("met:success", default="false", namespaces=namespaces)
        full_name = result_node.findtext("met:fullName", default="", namespaces=namespaces)

        success = success_text.lower() == "true"
        all_success = all_success and success

        errors = result_node.findall("met:errors", namespaces)

        if errors:
            for error_node in errors:
                message = error_node.findtext("met:message", default="", namespaces=namespaces)
                status_code = error_node.findtext("met:statusCode", default="", namespaces=namespaces)
                messages.append(f"{full_name}: {status_code}: {message}")
        else:
            messages.append(f"{full_name}: success={success_text}")

    return all_success, " | ".join(messages)


def _metadata_api_update_validation_rules(
    sf,
    metadata_xml_fragments: List[str],
    batch_size: int = 10,
) -> Tuple[bool, str]:
    """
    Updates validation rules through Salesforce Metadata API updateMetadata.

    Parameters
    ----------
    sf
        Authenticated simple_salesforce connection.

    metadata_xml_fragments : List[str]
        ValidationRule metadata XML fragments.

    batch_size : int
        Number of validation rules per updateMetadata request.

    Returns
    -------
    Tuple[bool, str]
        Overall success flag and combined message.
    """
    endpoint = _get_metadata_endpoint(sf)
    session_id = sf.session_id

    headers = {
        "Content-Type": "text/xml; charset=UTF-8",
        "SOAPAction": '""',
    }

    all_success = True
    all_messages: List[str] = []

    for start_index in range(0, len(metadata_xml_fragments), batch_size):
        batch = metadata_xml_fragments[start_index : start_index + batch_size]

        envelope = _build_update_metadata_soap_envelope(
            session_id=session_id,
            metadata_xml_fragments=batch,
        )

        response = requests.post(
            endpoint,
            data=envelope.encode("utf-8"),
            headers=headers,
            timeout=120,
        )

        if response.status_code >= 400:
            all_success = False
            all_messages.append(f"HTTP {response.status_code}: {response.text}")
            continue

        batch_success, batch_message = _parse_metadata_api_save_results(response.text)
        all_success = all_success and batch_success
        all_messages.append(batch_message)

        time.sleep(0.2)

    return all_success, " || ".join(all_messages)


def update_validation_rules_with_bypass_permission(
    env_name: str = "MCUAT8451",
    dry_run: bool = True,
    include_inactive: bool = True,
    output_csv_path: Optional[str] = None,
    validation_rule_ids: Optional[List[str]] = None,
    metadata_update_batch_size: int = 10,
) -> pd.DataFrame:
    """
    Updates Salesforce Validation Rules to include:

        NOT($Permission.Bypass_Validation_Rules)

    This version uses:
    - Tooling API only for discovery and backup.
    - Metadata API updateMetadata for the actual update.
    - No Salesforce CLI.
    - No Tooling API PATCH.

    Parameters
    ----------
    env_name : str
        Salesforce environment name expected by auth.get_salesforce_connection.

    dry_run : bool
        True exports before/after CSV only.
        False updates Salesforce through Metadata API.

    include_inactive : bool
        True includes inactive validation rules.
        False includes only active validation rules.

    output_csv_path : Optional[str]
        CSV output path.
        If omitted, a timestamped backup file is created.

    validation_rule_ids : Optional[List[str]]
        Specific ValidationRule Ids to process.
        If None or empty, all matching validation rules are processed.

    metadata_update_batch_size : int
        Number of ValidationRule metadata updates per Metadata API request.

    Returns
    -------
    pd.DataFrame
        DataFrame containing before/after formulas, status, and errors.
    """
    sf = get_salesforce_connection(env=env_name)

    validation_rule_ids = validation_rule_ids or []

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if output_csv_path is None:
        scope_label = "selected_rules" if validation_rule_ids else "all_rules"
        output_csv_path = (
            f"validation_rule_bypass_backup_{env_name}_{scope_label}_{timestamp}.csv"
        )

    print(f"Connected to Salesforce environment: {env_name}")
    print(f"Dry run mode: {dry_run}")
    print(f"Include inactive validation rules: {include_inactive}")
    print(f"ValidationRule Id filter count: {len(validation_rule_ids)}")
    print(f"Output CSV: {output_csv_path}")

    results: List[Dict[str, Any]] = []
    metadata_xml_fragments: List[str] = []
    row_indexes_to_mark_updated: List[int] = []

    try:
        available_fields = _describe_tooling_object(
            sf=sf,
            object_api_name="ValidationRule",
        )

        validation_rules = _get_validation_rule_inventory(
            sf=sf,
            include_inactive=include_inactive,
            available_fields=available_fields,
            validation_rule_ids=validation_rule_ids,
        )

    except Exception as exc:
        error_df = pd.DataFrame(
            [
                {
                    "object_api_name": None,
                    "validation_rule_id": None,
                    "full_name": None,
                    "validation_name": None,
                    "active": None,
                    "status": "ERROR_INVENTORY_QUERY_FAILED",
                    "validation_logic_before": None,
                    "validation_logic_after": None,
                    "error": str(exc),
                    "metadata_api_result": None,
                }
            ]
        )

        error_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
        return error_df

    print(f"Validation rules found: {len(validation_rules)}")

    for index, rule in enumerate(validation_rules, start=1):
        rule_id = rule.get("Id")
        validation_name = rule.get("ValidationName")
        active = rule.get("Active")

        print(f"[{index}/{len(validation_rules)}] Processing {validation_name or rule_id}")

        result_row = {
            "object_api_name": None,
            "validation_rule_id": rule_id,
            "full_name": None,
            "validation_name": validation_name,
            "active": active,
            "status": None,
            "validation_logic_before": None,
            "validation_logic_after": None,
            "error": None,
            "metadata_api_result": None,
        }

        try:
            full_rule = _get_validation_rule_metadata(
                sf=sf,
                validation_rule_id=rule_id,
                available_fields=available_fields,
            )

            metadata = full_rule.get("Metadata") or {}
            full_name = full_rule.get("FullName")
            original_formula = metadata.get("errorConditionFormula")

            result_row["full_name"] = full_name
            result_row["validation_name"] = full_rule.get("ValidationName") or validation_name
            result_row["active"] = full_rule.get("Active", active)
            result_row["validation_logic_before"] = original_formula

            if full_name and "." in str(full_name):
                result_row["object_api_name"] = str(full_name).split(".")[0]

            if not full_name:
                result_row["status"] = "ERROR_MISSING_FULL_NAME"
                result_row["error"] = "ValidationRule FullName is required for Metadata API update"
                results.append(result_row)
                continue

            if not original_formula:
                result_row["status"] = "SKIPPED_NO_FORMULA"
                result_row["validation_logic_after"] = original_formula
                results.append(result_row)
                continue

            if _formula_already_has_bypass(original_formula):
                result_row["status"] = "SKIPPED_ALREADY_HAS_BYPASS"
                result_row["validation_logic_after"] = original_formula
                results.append(result_row)
                continue

            updated_formula = _build_updated_formula(original_formula)

            result_row["validation_logic_after"] = updated_formula

            metadata_fragment = _build_validation_rule_update_metadata_xml(
                full_name=str(full_name),
                metadata=metadata,
                active=result_row["active"],
                updated_formula=updated_formula,
            )

            metadata_xml_fragments.append(metadata_fragment)
            row_indexes_to_mark_updated.append(len(results))

            result_row["status"] = "DRY_RUN_READY_TO_UPDATE" if dry_run else "READY_TO_UPDATE"
            results.append(result_row)

        except Exception as exc:
            result_row["status"] = "ERROR"
            result_row["error"] = str(exc)
            results.append(result_row)

    if not dry_run and metadata_xml_fragments:
        print(f"Updating {len(metadata_xml_fragments)} validation rule(s) through Metadata API")

        update_success, update_message = _metadata_api_update_validation_rules(
            sf=sf,
            metadata_xml_fragments=metadata_xml_fragments,
            batch_size=metadata_update_batch_size,
        )

        for row_index in row_indexes_to_mark_updated:
            results[row_index]["status"] = "UPDATED" if update_success else "ERROR_METADATA_API_UPDATE"
            results[row_index]["metadata_api_result"] = update_message

        print(f"Metadata API update success: {update_success}")
        print(update_message)

    elif dry_run:
        print("Dry run is True, no Salesforce update executed.")

    else:
        print("No validation rules needed update.")

    df = pd.DataFrame(results)
    df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

    print("Done.")
    print(f"CSV exported to: {output_csv_path}")

    if not df.empty:
        print(df["status"].value_counts(dropna=False))

    return df


if __name__ == "__main__":
    df_test = update_validation_rules_with_bypass_permission(
        env_name="MCUAT8451",
        dry_run=True,
        include_inactive=True,
        validation_rule_ids=["03dct000008rp1PAAQ"],
    )
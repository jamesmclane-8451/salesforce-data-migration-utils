from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import csv
import json
import re


INPUT_PATH = Path("load_results_step3_failed_rows.csv")
OUTPUT_PATH = Path("load_results_step3_grouped_failure_types.csv")

SF_ID_RE = re.compile(
    r"\b(?:001|003|005|006|00Q|00v|701|01s|01t|01u|0MI|0MK|00K|"
    r"a[0-9A-Za-z]{2})[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?\b"
)
URL_ID_RE = re.compile(r"/(?:sobjects|r)/([A-Za-z0-9_]+)/(?:[A-Za-z0-9]{15,18})")
CAMPAIGN_MEMBER_RE = re.compile(
    r"member id '[^']*' or the campaign id '[^']*'",
    flags=re.IGNORECASE,
)


def blank(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_text(value: object) -> str:
    text = blank(value)
    if not text:
        return ""

    text = URL_ID_RE.sub(r"/\1/<SF_ID>", text)
    text = SF_ID_RE.sub("<SF_ID>", text)
    text = CAMPAIGN_MEMBER_RE.sub(
        "member id '<MEMBER_ID>' or the campaign id '<CAMPAIGN_ID>'",
        text,
    )
    return re.sub(r"\s+", " ", text).strip()


def payload_fields(payload_json: object) -> str:
    text = blank(payload_json)
    if not text:
        return ""

    try:
        payload = json.loads(text)
    except Exception:
        return ""

    if not isinstance(payload, dict):
        return ""

    return "; ".join(sorted(payload.keys()))


def context_for(row: Dict[str, str]) -> str:
    target = blank(row.get("Target_Object"))
    code = blank(row.get("Error_Code"))
    category = blank(row.get("Error_Category"))
    fields = blank(row.get("Error_Fields"))
    text = " ".join(
        [
            blank(row.get("Normalized_Error_Message")),
            blank(row.get("Sample_Salesforce_Message")),
        ]
    ).lower()
    sample_payload_fields = blank(row.get("Sample_Payload_Fields")).lower()

    if "cannot change pricebook on opportunity with line items" in text:
        return (
            "Salesforce does not allow Pricebook2Id changes once an Opportunity "
            "has line items. Likely cleanup options: do not load Pricebook2Id "
            "for existing Opportunities with line items, or handle pricebook "
            "correction before line items exist/recreate dependent line items."
        )

    if code == "UNABLE_TO_LOCK_ROW" or "unable to obtain exclusive access" in text:
        return (
            "Record-lock contention. Usually transient from automation, rollups, "
            "sharing recalculation, or another load touching related records. "
            "Rerun/resume later; if persistent, isolate parent records and reduce "
            "concurrent updates."
        )

    if (
        "remote end closed connection" in text
        or "connection aborted" in text
        or "bulk api batch failed" in text
    ):
        return (
            "Bulk API transport/batch failure before row-level results were "
            "returned. Treat as retryable; resume/retry these rows after the "
            "current run or in a cleanup pass."
        )

    if target == "CampaignMember" and "member id" in text and "campaign id" in text:
        return (
            "CampaignMember requires CampaignId plus ContactId or LeadId. The "
            "payload has a null member lookup, which usually means the related "
            "Contact/Lead was missing, ignored, failed earlier, or could not be "
            "resolved by External_Id__c."
        )

    if "inactive owner" in text or "inactive_owner_or_user" in text:
        return (
            "Owner/User reference resolves to an inactive user. Pre-run owner "
            "cleanup should replace inactive owners with the migration fallback "
            "user; investigate objects/fields still bypassing that cleanup."
        )

    if (
        "duplicate" in text
        or code in {"DUPLICATES_DETECTED", "DUPLICATE_VALUE", "DUPLICATE_COMM_NICKNAME"}
    ):
        return (
            "Salesforce duplicate/unique rule is blocking the write. This can "
            "happen even on updates if the target record already matches another "
            "record under duplicate rules. Review duplicate rule behavior and "
            "whether the matched target record is correct."
        )

    if (
        code == "REQUIRED_FIELD_MISSING"
        or "required field" in category.lower()
        or "required fields are missing" in text
    ):
        return (
            "Required field missing for create/upsert. Usually a lookup failed "
            "to resolve or a required target field is not mapped/populated. "
            "Review sample payload and missing fields."
        )

    if code == "FIELD_CUSTOM_VALIDATION_EXCEPTION" or "validation" in category.lower():
        return (
            "Target validation rule blocked the write. Confirm bypass custom "
            "permission is active for the integration user/session, or decide "
            "whether source data needs cleanup/default values before load."
        )

    if (
        code == "INVALID_FIELD_FOR_INSERT_UPDATE"
        or "not writable" in text
        or "unable to create/update fields" in text
    ):
        return (
            "Payload includes a field Salesforce will not allow for this "
            "operation/profile/object state. Remove or reroute the field mapping, "
            "or update field-level security only if the field should truly be "
            "writeable."
        )

    if code == "FIELD_INTEGRITY_EXCEPTION" or "field integrity" in category.lower():
        if fields:
            return (
                f"Reference/field integrity failure on {fields}. Most often this "
                "means a lookup points to an invalid/missing target record, a "
                "restricted picklist value is invalid, or Salesforce blocks the "
                "field in the current record state."
            )
        return (
            "Reference/field integrity failure. Most often a lookup points to an "
            "invalid/missing target record, a restricted picklist value is "
            "invalid, or Salesforce blocks the field in the current record state."
        )

    if "lookup filter" in text:
        return (
            "Lookup value resolved to a Salesforce Id, but the target lookup "
            "filter rejects that record. Business/data owner needs to confirm "
            "the correct related record or relax/adjust lookup criteria for "
            "migration."
        )

    if "insufficient access" in text or "insufficient_access" in text:
        return (
            "Permission/access issue for the integration user or referenced "
            "record. Review sharing/FLS/object permissions and whether record "
            "owner or parent access is blocking the load."
        )

    if "external_id__c" in sample_payload_fields and "invalid" in text:
        return (
            "External_Id__c or an external-id relationship value appears invalid "
            "for the target operation. Check legacy ID preservation and "
            "relationship resolution for this object."
        )

    return (
        "Grouped by target/source/operation/error code/fields/message after "
        "normalizing record IDs. Review sample payload and Salesforce message "
        "to decide whether this is data cleanup, mapping change, "
        "validation/automation, or retryable platform behavior."
    )


def build_grouped_failure_csv(
    input_path: Path = INPUT_PATH,
    output_path: Path = OUTPUT_PATH,
) -> None:
    groups: Dict[tuple[str, ...], Dict[str, str | int]] = {}
    row_count = 0

    with input_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_count += 1
            normalized_error = normalize_text(row.get("Error_Message")) or normalize_text(
                row.get("Message")
            )
            normalized_raw_message = normalize_text(row.get("Message"))
            sample_payload_fields = payload_fields(row.get("Payload_JSON"))
            key = (
                blank(row.get("Target_Object")),
                blank(row.get("Source_Object")),
                blank(row.get("Operation")),
                blank(row.get("Change_Type")),
                blank(row.get("Error_Category")),
                blank(row.get("Error_Code")),
                blank(row.get("Error_Fields")),
                normalized_error,
            )

            if key not in groups:
                groups[key] = {
                    "Target_Object": key[0],
                    "Source_Object": key[1],
                    "Operation": key[2],
                    "Change_Type": key[3],
                    "Error_Category": key[4],
                    "Error_Code": key[5],
                    "Error_Fields": key[6],
                    "Normalized_Error_Message": key[7],
                    "Normalized_Raw_Message": normalized_raw_message,
                    "Occurrence_Count": 0,
                    "Sample_Source_RecordId": blank(row.get("Source_RecordId")),
                    "Sample_Target_RecordId": blank(row.get("Target_RecordId")),
                    "Sample_Payload_Field_Count": blank(row.get("Payload_Field_Count")),
                    "Sample_Payload_Fields": sample_payload_fields,
                    "Sample_Payload_JSON": blank(row.get("Payload_JSON")),
                    "Sample_Salesforce_Message": blank(row.get("Message")),
                    "Investigation_Context": "",
                }

            groups[key]["Occurrence_Count"] = int(groups[key]["Occurrence_Count"]) + 1

    rows: List[Dict[str, str | int]] = list(groups.values())
    for row in rows:
        row["Investigation_Context"] = context_for(row)  # type: ignore[arg-type]

    rows.sort(
        key=lambda row: (
            -int(row["Occurrence_Count"]),
            str(row["Target_Object"]),
            str(row["Error_Code"]),
            str(row["Normalized_Error_Message"]),
        )
    )

    columns = [
        "Occurrence_Count",
        "Target_Object",
        "Source_Object",
        "Operation",
        "Change_Type",
        "Error_Category",
        "Error_Code",
        "Error_Fields",
        "Normalized_Error_Message",
        "Investigation_Context",
        "Sample_Source_RecordId",
        "Sample_Target_RecordId",
        "Sample_Payload_Field_Count",
        "Sample_Payload_Fields",
        "Sample_Payload_JSON",
        "Sample_Salesforce_Message",
        "Normalized_Raw_Message",
    ]

    with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Input failed rows: {row_count:,}")
    print(f"Grouped failure types: {len(rows):,}")
    print(f"Output written: {output_path}")
    print("Top 10:")
    for row in rows[:10]:
        print(
            f"{row['Occurrence_Count']} | {row['Target_Object']} | "
            f"{row['Error_Code']} | {str(row['Normalized_Error_Message'])[:160]}"
        )


if __name__ == "__main__":
    build_grouped_failure_csv()

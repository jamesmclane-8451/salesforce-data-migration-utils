"""
UCM-52679 AC6 - Seasonal Surcharge Overrides for Contracted Accounts (Quote-UI path)

Exports the complete, current OmniStudio configuration for the AC6 implementation
from a given org into a single JSON artifact, keyed by business identifiers
(Name / UniqueName / parent-chain / field paths) rather than Salesforce record
Ids, so the artifact can be diffed and applied across orgs with different Ids.

This is the deployable-artifact mechanism for this change because:
  - OmniProcess/OmniProcessElement/OmniDataTransform/OmniDataTransformItem are
    standard *data* objects in this org's OmniStudio install (not Metadata API
    types), so `sf project retrieve/deploy` does not apply to them.
  - The Salesforce CLI is not installed in this environment.
  - This repo's established pattern for Salesforce promotion work is a
    reproducible Python script using auth.py (see the many `_deploy_*.py`
    scripts in this repo), not sfdx source format, for exactly this kind of
    data-object-based configuration.

Usage:
    python3 export_omnistudio_config.py <env> <output_json_path>

Example:
    python3 export_omnistudio_config.py MCDEV8451 export_mcdev8451.json
"""
import sys
import json

sys.path.insert(0, '.')
from auth import get_salesforce_connection

PRICING_IP_UNIQUE_NAME_PREFIX = "Pricing_Procedure_"
CREATE_IP_UNIQUE_NAME_PREFIX = "create_qouteandorder_"

DATA_TRANSFORM_NAMES = [
    "DMTAccounttransforminIP",
    "DMTfinalproductlisttransformforOLI",
    "DMCreateOpportunitySplits",
]


def export_ip(sf, type_, subtype):
    res = sf.query(
        f"SELECT Id, UniqueName, Name, Type, SubType, VersionNumber, IsActive, "
        f"OmniProcessType FROM OmniProcess "
        f"WHERE Type='{type_}' AND SubType='{subtype}' AND IsActive=true"
    )
    if not res['records']:
        raise RuntimeError(f"No active OmniProcess found for Type={type_} SubType={subtype}")
    ip = res['records'][0]
    ip_id = ip['Id']

    elems_res = sf.query(
        f"SELECT Id, Name, Type, ParentElementId, SequenceNumber, Level, "
        f"PropertySetConfig FROM OmniProcessElement "
        f"WHERE OmniProcessId='{ip_id}' ORDER BY SequenceNumber"
    )
    id_to_name = {e['Id']: e['Name'] for e in elems_res['records']}

    elements = []
    for e in elems_res['records']:
        elements.append({
            "name": e['Name'],
            "type": e['Type'],
            "parentElementName": id_to_name.get(e['ParentElementId']),
            "sequenceNumber": e['SequenceNumber'],
            "level": e['Level'],
            "propertySetConfig": json.loads(e['PropertySetConfig']) if e['PropertySetConfig'] else {},
        })

    return {
        "uniqueName": ip['UniqueName'],
        "name": ip['Name'],
        "type": ip['Type'],
        "subType": ip['SubType'],
        "versionNumber": ip['VersionNumber'],
        "omniProcessType": ip['OmniProcessType'],
        "elements": elements,
    }


def export_data_transform(sf, name):
    res = sf.query(
        f"SELECT Id, Name, Type, IsActive, VersionNumber FROM OmniDataTransform "
        f"WHERE Name='{name}' ORDER BY VersionNumber DESC LIMIT 1"
    )
    if not res['records']:
        raise RuntimeError(f"No OmniDataTransform found for Name={name}")
    dt = res['records'][0]

    items_res = sf.query(
        f"SELECT Id, InputFieldName, OutputFieldName, IsDisabled, FormulaExpression, "
        f"FormulaResultPath, FormulaSequence, FilterDataType, FilterGroup, "
        f"FilterOperator, FilterValue, DefaultValue, IsUpsertKey, IsRequiredForUpsert, "
        f"InputObjectName, OutputObjectName, GlobalKey "
        f"FROM OmniDataTransformItem WHERE OmniDataTransformationId='{dt['Id']}'"
    )
    items = []
    for it in items_res['records']:
        items.append({k: v for k, v in it.items() if k != 'attributes'})

    return {
        "name": dt['Name'],
        "type": dt['Type'],
        "isActive": dt['IsActive'],
        "versionNumber": dt['VersionNumber'],
        "items": items,
    }


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    env, out_path = sys.argv[1], sys.argv[2]

    sf = get_salesforce_connection(env=env)

    artifact = {
        "jiraIssue": "UCM-52679",
        "description": "AC6 - Seasonal Surcharge Overrides for Contracted Accounts (Quote-UI OmniScript path)",
        "sourceEnv": env,
        "integrationProcedures": {
            "Pricing_Procedure.MultipleAccounts": export_ip(sf, "Pricing", "Procedure"),
            "create_qouteandorder": export_ip(sf, "create", "qouteandorder"),
        },
        "dataTransforms": {
            name: export_data_transform(sf, name) for name in DATA_TRANSFORM_NAMES
        },
    }

    with open(out_path, "w") as f:
        json.dump(artifact, f, indent=2, sort_keys=True)

    print(f"Exported OmniStudio config from {env} to {out_path}")


if __name__ == "__main__":
    main()

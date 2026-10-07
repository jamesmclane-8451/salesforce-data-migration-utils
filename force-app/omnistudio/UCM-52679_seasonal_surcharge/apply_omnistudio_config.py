"""
UCM-52679 AC6 - Seasonal Surcharge Overrides for Contracted Accounts (Quote-UI path)

Applies a captured OmniStudio configuration artifact (produced by
export_omnistudio_config.py) to a target org. Locates existing
OmniProcess/OmniProcessElement/OmniDataTransform/OmniDataTransformItem
records by business identifiers (UniqueName prefix / Type+SubType / element
name / parent-chain / field paths), not by source-org record Id, so the
same artifact applies cleanly to any target org regardless of that org's
own Ids.

Does NOT create new Integration Procedures, DataRaptor Transforms, or
elements from scratch -- the target org's active OmniProcess/OmniDataTransform
records for each name in the artifact must already exist (e.g. provisioned by
whatever process originally built the base OmniScript/IP skeleton in that
org). This script updates existing elements/items to match the captured
configuration; it does not scaffold new ones.

Usage:
    python3 apply_omnistudio_config.py <target_env> <captured_json_path> [--dry-run]
"""
import sys
import json
import time

sys.path.insert(0, '.')
from auth import get_salesforce_connection


def find_active_ip(sf, type_, subtype):
    res = sf.query(
        f"SELECT Id, UniqueName, VersionNumber FROM OmniProcess "
        f"WHERE Type='{type_}' AND SubType='{subtype}' AND IsActive=true"
    )
    if not res['records']:
        raise RuntimeError(f"No active OmniProcess found in target org for Type={type_} SubType={subtype}")
    return res['records'][0]


def apply_ip(sf, ip_key, ip_artifact, dry_run):
    type_, subtype = ip_artifact['type'], ip_artifact['subType']
    ip = find_active_ip(sf, type_, subtype)
    ip_id = ip['Id']
    print(f"\n[{ip_key}] target active OmniProcess: {ip_id} (v{ip['VersionNumber']})")

    elems_res = sf.query(
        f"SELECT Id, Name, PropertySetConfig FROM OmniProcessElement WHERE OmniProcessId='{ip_id}'"
    )
    name_to_id = {e['Name']: e['Id'] for e in elems_res['records']}
    name_to_cfg = {
        e['Name']: json.loads(e['PropertySetConfig']) if e['PropertySetConfig'] else {}
        for e in elems_res['records']
    }

    updates = []
    for el in ip_artifact['elements']:
        name = el['name']
        if name not in name_to_id:
            print(f"  !! WARNING: element '{name}' not found in target org -- skipping "
                  f"(this script does not create new elements)")
            continue
        current = name_to_cfg[name]
        desired = el['propertySetConfig']
        if current == desired:
            print(f"  = {name}: already matches captured config")
            continue
        print(f"  * {name}: differs -- {'(dry-run, not applying)' if dry_run else 'updating'}")
        updates.append((name_to_id[name], name, desired))

    if dry_run or not updates:
        return

    try:
        sf.OmniProcess.update(ip_id, {'IsActive': False})
        time.sleep(2)
        for elem_id, name, desired in updates:
            sf.OmniProcessElement.update(elem_id, {'PropertySetConfig': json.dumps(desired)})
            print(f"    updated {name} ({elem_id})")
        time.sleep(2)
    finally:
        sf.OmniProcess.update(ip_id, {'IsActive': True})
        print(f"  reactivated {ip_id}")


def apply_data_transform(sf, name, dt_artifact, dry_run):
    res = sf.query(
        f"SELECT Id FROM OmniDataTransform WHERE Name='{name}' ORDER BY VersionNumber DESC LIMIT 1"
    )
    if not res['records']:
        print(f"  !! WARNING: OmniDataTransform '{name}' not found in target org -- skipping")
        return
    dt_id = res['records'][0]['Id']
    print(f"\n[{name}] target OmniDataTransform: {dt_id}")

    items_res = sf.query(
        f"SELECT Id, InputFieldName, OutputFieldName, IsDisabled, FormulaExpression "
        f"FROM OmniDataTransformItem WHERE OmniDataTransformationId='{dt_id}'"
    )
    # Key by (InputFieldName, OutputFieldName) since that pair is stable and
    # unique enough across this org's mapping items for this artifact's scope.
    key_to_item = {(it['InputFieldName'], it['OutputFieldName']): it for it in items_res['records']}

    updates = []
    for art_item in dt_artifact['items']:
        key = (art_item.get('InputFieldName'), art_item.get('OutputFieldName'))
        target_item = key_to_item.get(key)
        if not target_item:
            print(f"  !! WARNING: item {key} not found in target org -- skipping "
                  f"(this script does not create new mapping items)")
            continue
        changed_fields = {}
        for field in ('IsDisabled', 'FormulaExpression'):
            if art_item.get(field) != target_item.get(field):
                changed_fields[field] = art_item.get(field)
        if not changed_fields:
            continue
        print(f"  * {key}: differs ({changed_fields}) -- "
              f"{'(dry-run, not applying)' if dry_run else 'updating'}")
        updates.append((target_item['Id'], key, changed_fields))

    if dry_run:
        return
    for item_id, key, changed_fields in updates:
        sf.OmniDataTransformItem.update(item_id, changed_fields)
        print(f"    updated {key} ({item_id})")


def main():
    args = sys.argv[1:]
    dry_run = '--dry-run' in args
    args = [a for a in args if a != '--dry-run']
    if len(args) != 2:
        print(__doc__)
        sys.exit(1)
    env, json_path = args

    with open(json_path) as f:
        artifact = json.load(f)

    sf = get_salesforce_connection(env=env)

    print(f"Applying captured config ({artifact['jiraIssue']}) to {env}"
          f"{' [DRY RUN]' if dry_run else ''}")

    for ip_key, ip_artifact in artifact['integrationProcedures'].items():
        apply_ip(sf, ip_key, ip_artifact, dry_run)

    for name, dt_artifact in artifact['dataTransforms'].items():
        apply_data_transform(sf, name, dt_artifact, dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()

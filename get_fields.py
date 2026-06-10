

# field_selection_single_env.py
from typing import List, Dict, Any, Optional
import pandas as pd
from oauth_login import get_salesforce_connection


def build_field_list_single_env(
    object_name: str,
    env: str,
    included_outfile: Optional[str] = None,
    object_map_feather_outfile: Optional[str] = None,
) -> List[str]:
    """
    Pull all field definitions for a given Salesforce object from a single environment.
    Automatically names output files as "get_fields_<env>_<object>.csv" and
    "get_fields_<env>_<object>.feather".

    ----------------------------------------------------------------------
    🔧 WHAT THIS FUNCTION DOES
    ----------------------------------------------------------------------
    1️⃣ Connects to one Salesforce environment (the `env` parameter).
    2️⃣ Describes the specified object to get all field metadata.
    3️⃣ Filters out non-queryable or system fields (optional future logic point).
    4️⃣ Exports the full field list with metadata to both CSV and Feather.
    5️⃣ Returns a list of field API names for convenience.

    ----------------------------------------------------------------------
    📦 OUTPUT NAMING
    ----------------------------------------------------------------------
    Files are automatically named:
        "get_fields_<env>_<object>.csv"
        "get_fields_<env>_<object>.feather"
    Example:
        "get_fields_mcuat8451_account.csv"
        "get_fields_mcuat8451_account.feather"

    ----------------------------------------------------------------------
    🧠 PARAMETERS
    ----------------------------------------------------------------------
    object_name: str
        Salesforce object API name (e.g., "Account", "Contact", "Opportunity").

    env: str
        The Salesforce environment name or alias (e.g., "MCUAT8451", "DEV8451").

    included_outfile: Optional[str]
        Optional override for CSV output filename.
        If not provided, defaults to "get_fields_<env>_<object>.csv".

    object_map_feather_outfile: Optional[str]
        Optional override for Feather output filename.
        If not provided, defaults to "get_fields_<env>_<object>.feather".

    ----------------------------------------------------------------------
    🧾 RETURNS
    ----------------------------------------------------------------------
    List[str]: ordered list of all queryable field API names.
    """

    # 1️⃣ Connect to Salesforce environment
    sf = get_salesforce_connection(env)

    # 2️⃣ Describe the object
    desc = getattr(sf, object_name).describe()
    fields = desc["fields"]

    # 3️⃣ Get list of field names (only queryable)
    included_fields = [f["name"] for f in fields if f.get("queryable", True)]

    # 4️⃣ Build filenames dynamically with function name included
    env_clean = env.lower().replace(" ", "_")
    obj_clean = object_name.lower().replace(" ", "_")

    included_csv = included_outfile or f"get_fields_{env_clean}_{obj_clean}.csv"
    included_feather = object_map_feather_outfile or f"get_fields_{env_clean}_{obj_clean}.feather"

    # 5️⃣ Create DataFrame for output
    df = pd.DataFrame(
        [
            {
                "Environment": env,
                "Object": object_name,
                "Field": f["name"],
                "Type": f.get("type", ""),
                "Label": f.get("label", ""),
                "Queryable": f.get("queryable", True),
                "Creatable": f.get("createable", False),
                "Updateable": f.get("updateable", False),
            }
            for f in fields
        ]
    )

    # 6️⃣ Write both CSV and Feather with consistent naming
    df.to_csv(included_csv, index=False)
    try:
        df.to_feather(included_feather)
    except Exception as e:
        print(f"⚠️ Warning: could not write feather file '{included_feather}': {e}")

    print(f"✅ Retrieved {len(included_fields)} fields from {env}.{object_name}")
    print(f"💾 Saved files: {included_csv}, {included_feather}")

    return included_fields

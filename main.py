

from analyze import (calculate_field_population, check_for_records, calculate_field_uniqueness, count_records,
                     get_recently_modified_objects, get_object_mtd_stats)
from data_skew import detect_data_skew
from duplicates import get_dupes
from get_metadata import get_all_objects, get_custom_objects, get_object_fields, get_all_used_objects
from datetime import date, datetime
from temp_functions import (get_all_contentdocuments, map_opportunities_to_values_by_account,
                            extract_pdf_from_contentdocument, share_and_extract_pdf_strong)
from coupon_reassignment import assign_coupon_owners

from temp_functions import share_and_extract_pdf_strong, compare_sf_brands_to_csv_one_call, get_sample_accounts
from oauth_login2 import get_salesforce_connection
from migrate_records import migrate_salesforce_data
from get_data_bulk import export_salesforce_to_csv
from list_migration_fields_temp import list_account_fields_being_queried
from get_fields_old import build_source_select_fields


"""
Salesforce Envs
- Prod
- MCUAT8451
- 
"""



account_fields = [
    "Id",
    "ATV_Target_Current_Fiscal_Year__c",
    "ATV_Target_Previous_Fiscal_Year__c",
    "AccountBillingAddressD365Id__c",
    "AccountSource",
    "Account_Activity_Comments__c",
    "AnnualRevenue",
    "Annual_Budget__c",
    "Auth_Sys_Account_Name__c",
    "Autorenewal__c",
    "BillingCity",
    "BillingCountry",
    "BillingGeocodeAccuracy",
    "BillingLatitude",
    "BillingLongitude",
    "BillingPostalCode",
    "BillingState",
    "BillingStreet",
    "Broker_CPG__c",
    "Broker_Name__c",
    "Business_Unit__c",
    "CAAM_Group_Final__c",
    "CIL_Target_Current_Fiscal_Year__c",
    "CIL_Target_Previous_Fiscal_Year__c",
    "Client_ID_Dynamics__c",
    "Client_Segment__c",
    "Comms_Account_Activity_Comments__c",
    "Comms_Account_Number__c",
    "Comms_Client_Pricing_Type__c",
    "Count_of_Third_Party_CPG_Relationships__c",
    "Data_Warehouse_Id__c",
    "Description",
    "Digital_Extras_Opt_In__c",
    "Dynamics_ID__c",
    "Industry",
    "Jigsaw",
    "K_Number__c",
    "Level_of_Shop_Subscription__c",
    "Manufacturer_Code__c",
    "Manufacturer_Name__c",
    "Market6_Customer_ID__c",
    "Master_Flag__c",
    "Max_Oppty_Probability__c",
    "Media_Target_Current_Fiscal_Year__c",
    "Media_Target_Previous_Fiscal_Year__c",
    "Name",
    "New_Business_Target_Account__c",
    "New_Stratum_Client__c",
    "Non_Billable_Client__c",
    "Non_Endemic__c",
    "NumberOfEmployees",
    "PO_Required_Agency__c",
    "PO_Required_BCC__c",
    "PO_Required_Insights__c",
    "PO_Required_KPM__c",
    "Phone",
    "Pricing_Tier__c",
    "Sales__c",
    "Service_Level__c",
    "ShippingCity",
    "ShippingCountry",
    "ShippingGeocodeAccuracy",
    "ShippingLatitude",
    "ShippingLongitude",
    "ShippingPostalCode",
    "ShippingState",
    "ShippingStreet",
    "Shop_Client__c",
    "SicDesc",
    "Status__c",
    "Type",
    "Type_of_Client__c",
    "Upfront_Target_Current_Fiscal_Year__c",
    "Upfront_Target_Previous_Fiscal_Year__c",
    "Website",
    "master_id__c",
    "percent_change__c",
    "sales_previous__c",
    "Comms_Operating_Parent_MFG_SF_Acc__c",
    "CreatedById",
    "LastModifiedById",
    "MIA_Cost_Center__c",
    "MIA_Mission__c",
    "MasterRecordId",
    "OwnerId",
    "ParentId",
    "RecordTypeId"
]


# analyze_field_delta_population()
# quit()

""" Get Fields 
from get_fields import build_field_list_single_env

# Pull field metadata for Account in MCUAT8451 sandbox
fields = build_field_list_single_env(
    object_name="Contact",
    env="Prod"main.py
)

print(fields[:10])  # show first 10

quit()
"""

""" Get Records 
from get_records import get_records

# Case 1, fetch specific objects, mapping files auto-named and read from current folder
get_records(object_list=["Contact"], env="Prod")

quit()
"""

""" Compare Records """
from compare_records import compare_records_envs

# Compare Contact records between PROD and MCUAT using Email as the matching key
delta = compare_records_envs(
    object_name="Contact",
    source_env="PROD",
    target_env="MCUAT8451",
    source_uid="Email",
    target_uid="Email",
    export_csv=True
)

print(f"✅ {len(delta)} differences found")
print(delta.head())

quit()


""" Migrate Data """
# example_with_recordtypes.py
from migrate_records import migrate_salesforce_data

summary = migrate_salesforce_data(
    object_name="Account",
    from_env="Prod",
    to_env="MCUAT8451",
    source_feather_path="account.feather",
    limit=None,
    suppress_chatter_check=True,
    record_type_mapping_csv="recordtype_map.csv",
    batch_size=500
)

print(summary)

quit()
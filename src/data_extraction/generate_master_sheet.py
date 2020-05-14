import pandas as pd
import logging 
import re
import sys
from azure.storage.blob import BlobClient 

logger = logging


def generate_master_sheet(
    master_sheet_path:str,
    update_sheet_path:str,
    org_sheet_path:str,
    master_sheet_client: BlobClient,
    update_sheet_client: BlobClient,
    org_sheet_client: BlobClient
):
    raw_data_set = pd.read_excel(update_sheet_path)

    master_sheet = pd.read_csv(
        master_sheet_path,
        encoding="utf-8",
        low_memory=False
    )

    raw_org_sheet = pd.read_csv(
        org_sheet_path,
        encoding="utf-8",
        low_memory=False
    )

    logger.debug(
        "Number of rows in update sheet: " + str(raw_data_set.shape[0])
    )

    # extract columns needed 
    update_raw_data = raw_data_set[[
        "Branch", "Region", "Province", "City",
        "Phone Number","Office", "Address", "Postal Code", 
        "Desk Location", "Floor", "Division", 
        "Division Number", "EmpName", "Title",
        "Work Email - AD", "Critical Program / Service", 
        "Sub-Program / Service", "Current Device Type", 
        "Current Asset Number", "Has Appgate", "Has VPN",
        "CriticalEmployee", "Changed"
    ]]


    email_expression = re.compile(
        "[A-Z0-9'._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$",
        re.IGNORECASE
    )

    master_sheet_columns = list(master_sheet.columns)
    if("Branch" not in master_sheet_columns):
        master_sheet["Branch"] = ""
    
    if("Changed" not in master_sheet_columns):
        master_sheet["Changed"] = False

    matching_count = 0
    non_matching_count = 0
    for index, row in update_raw_data.iterrows():
        email = row["Work Email - AD"]

        if (email == email and email_expression.match(email)):
            matching_employee_df = master_sheet[master_sheet["E-mail Address"] == email]

            name = row["EmpName"]

            critical = row["CriticalEmployee"]
            try:
                num_critical = int(critical)
                if num_critical == 0:
                    critical = False 
                else:
                    critical = True
            except:
                critical = False
            
            changed = row["Changed"]
            try:
                num_changed = int(changed)
                if num_changed == 0:
                    changed = False 
                else:
                    changed = True 
            except:
                changed = False 
             

            phone_number = row["Phone Number"]
            if(phone_number != phone_number): phone_number = ""

            title = row["Title"]
            if(title != title): title = ""

            region = row["Region"]
            if(region != region ): region = ""

            province = row["Province"]
            if(province != province): province = ""

            city = row["City"]
            if(city != city ): city = ""

            address = row["Address"]
            if(address != address): address = ""

            postal_code = row["Postal Code"]
            if(postal_code != postal_code): postal_code = ""

            desk_location = row["Desk Location"]
            if(desk_location != desk_location): desk_location = ""

            floor = row["Floor"]
            if(floor != floor): floor = "" 

            office = row["Office"]
            if (office != office): office = ""

            division = row["Division Number"]
            if (division != division): division = ""

            division_name = row["Division"]
            if (division_name != division_name): division_name = ""

            # check if the org exists in the org_sheet
            matching_org_df = raw_org_sheet[raw_org_sheet["Division"] == division ]
            if matching_org_df.shape[0] > 0:
                ind = matching_org_df.index[0]
                raw_org_sheet.at[ind, "Division Name"] = division_name
            else:
                raw_org_sheet.loc[
                    raw_org_sheet.shape[0] + 1
                ] = [
                    division,
                    division_name
                ]
    

            skill = row["Critical Program / Service"]
            if (skill != skill): skill = ""
            else:
                try:
                    int(skill)
                    skill = ""
                except: 
                    pass
                
            sub_skill = row["Sub-Program / Service"]
            if (sub_skill != sub_skill): sub_skill = ""
            else:
                try:
                    int(sub_skill)
                    sub_skill = ""
                except: 
                    pass
            
            device = row["Current Device Type"]
            if (device != device): device = ""

            asset_number = row["Current Asset Number"]
            if (asset_number != asset_number): asset_number = ""


            has_appgate = row["Has Appgate"]
            if (has_appgate != has_appgate ): has_appgate = "FALSE"
            elif (has_appgate.lower().startswith("y")): has_appgate = "TRUE"
            else: has_appgate = "FALSE"

            has_vpn = row["Has VPN"]
            if (has_vpn != has_vpn ): has_vpn = "FALSE"
            elif (has_vpn.lower().startswith("y")): has_vpn = "TRUE"
            else: has_vpn = "FALSE"

            if(matching_employee_df.shape[0] > 0 ):
                matching_count += 1 
                row_index = matching_employee_df.index[0]

                master_sheet.at[row_index,"Region"] = region
                master_sheet.at[row_index,"Skills"] = skill
                master_sheet.at[row_index,"Sub Skills"] = sub_skill
                master_sheet.at[row_index,"VPN"] = has_vpn
                master_sheet.at[row_index,"AppGate"] = has_appgate
                master_sheet.at[row_index, "Phone Number"] = phone_number

                if division != "" and division_name != "":
                    master_sheet.at[row_index,"Division"] = division
                    master_sheet.at[row_index,"Division Name"] = division_name
                
                if device != "" and asset_number != "":
                    master_sheet.at[row_index,"Device Type"] = device
                    master_sheet.at[row_index,"Asset Number"] = asset_number
                
                if office != master_sheet.loc[row_index, "Office"]:
                    master_sheet.at[row_index, "Office"] = office
                    master_sheet.at[row_index, "Postal Code"] = postal_code
                    master_sheet.at[row_index, "Address"] = address
                    master_sheet.at[row_index, "Floor"] = floor
                    master_sheet.at[row_index, "Province"] = province
                    master_sheet.at[row_index, "City"] = city
                    master_sheet.at[row_index, "Desk Location"] = desk_location
                
                master_sheet.at[row_index, "Critical"] = critical
                master_sheet.at[row_index, "Changed"] = changed 
                
            else:
                non_matching_count += 1
                index = master_sheet.shape[0] + 1
                master_sheet.at[index, "Name"] = name
                master_sheet.at[index, "Phone Number"] = phone_number
                master_sheet.at[index, "Title"] = title
                master_sheet.at[index, "Region"] = region
                master_sheet.at[index, "Skills"] = skill
                master_sheet.at[index, "Sub Skills"] = sub_skill
                master_sheet.at[index, "VPN"] = has_vpn
                master_sheet.at[index, "AppGate"] = has_appgate
                master_sheet.at[index, "E-mail Address"] = email
                master_sheet.at[index, "Department"] = "ESDC-ESDC"
                master_sheet.at[index, "Office"] = office
                master_sheet.at[index, "Postal Code"] = postal_code
                master_sheet.at[index, "Address"] = address
                master_sheet.at[index, "Floor"] = floor
                master_sheet.at[index, "Province" ] = province
                master_sheet.at[index, "City"] = city
                master_sheet.at[index, "Desk Location"] = desk_location
                master_sheet.at[index, "Division"] = division
                master_sheet.at[index, "Division Name"] = division_name
                master_sheet.at[index, "Device Type"] = device
                master_sheet.at[index, "Asset Number"] = asset_number
                master_sheet.at[index, "Critical"] = critical
                master_sheet.at[index, "Changed"] = changed
        
        sys.stdout.write("\rEmployees Matched: %i New Employees %i" % (matching_count, non_matching_count))
        sys.stdout.flush()
    
    logging.debug(
        "Employees Matched: " + str(matching_count) + "\n" +
        "New Employees: " + str(non_matching_count)
    )

    master_sheet.to_csv(
        master_sheet_path,
        encoding="utf-8",
        index=False
    )

    raw_org_sheet.to_csv(
        org_sheet_path,
        encoding="utf-8",
        index=False
    )

    logging.debug("Master Sheet and Org Sheet generated")

        
                
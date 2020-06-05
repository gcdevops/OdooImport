import pandas as pd
import logging 
import re
import sys
import os
from azure.storage.blob import BlobClient 

logger = logging


def process_update_sheet(
    save_path: str, 
    update_sheet_path:str
):
    raw_data_set = pd.read_excel(update_sheet_path)

    logger.debug(
        "Number of rows in update sheet: " + str(raw_data_set.shape[0])
    )

    # extract columns needed 
    update_raw_data = raw_data_set[[
        "Branch", "Region", "Province", "City",
        "Phone Number", "Office", "Address", "Postal Code", 
        "Desk Location", "Floor", "Division", 
        "Division Number", "EmpName", "Title",
        "Work Email - AD", "Critical Program / Service", 
        "Sub-Program / Service", "Current Device Type", 
        "Current Asset Number", "Has Appgate", "Has VPN",
        "CriticalEmployee"
    ]]

    update_processed_data = pd.DataFrame(
        columns = [
            "Branch", "Region", "Province", "City",
            "Phone Number", "Office", "Address", "Postal Code", 
            "Desk Location", "Floor", "Division Name", 
            "Division", "Name", "Title",
            "E-mail Address", "Skills", 
            "Sub Skills", "Device Type", 
            "Asset Number", "Appgate", "VPN",
            "Critical"
        ]
    )
   
    email_expression = re.compile(
        "[A-Z0-9'._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$",
        re.IGNORECASE
    )

    count = 0 
    for index, row in update_raw_data.iterrows():
        email = row["Work Email - AD"]

        if (email == email and email_expression.match(email)):

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

            branch = row["Branch"]
            if (branch != branch): branch = ""

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

            update_processed_data.at[count, "Branch"] = branch
            update_processed_data.at[count, "Region"] = region
            update_processed_data.at[count, "Province" ] = province
            update_processed_data.at[count, "City"] = city
            update_processed_data.at[count, "Phone Number"] = phone_number
            update_processed_data.at[count, "Office"] = office
            update_processed_data.at[count, "Address"] = address
            update_processed_data.at[count, "Postal Code"] = postal_code
            update_processed_data.at[count, "Desk Location"] = desk_location
            update_processed_data.at[count, "Floor"] = floor
            update_processed_data.at[count, "Division Name"] = division_name
            update_processed_data.at[count, "Division"] = division 
            update_processed_data.at[count, "Name"] = name
            update_processed_data.at[count, "Title"] = title
            update_processed_data.at[count, "E-mail Address"] = email
            update_processed_data.at[count, "Skills"] = skill
            update_processed_data.at[count, "Sub Skills"] = sub_skill
            update_processed_data.at[count, "Device Type"] = device
            update_processed_data.at[count, "Asset Number"] = asset_number
            update_processed_data.at[count, "AppGate"] = has_appgate
            update_processed_data.at[count, "VPN"] = has_vpn
            update_processed_data.at[count, "Critical"] = critical

            count += 1
        
    
    update_processed_data.to_csv(
        os.path.join(save_path, "employee-updated-data.csv"),
        encoding="utf-8",
        index=False
    )

    logging.debug("Update Sheet Processed")

        
                
import pandas as pd
import numpy as np 
import os 
import sys
import logging

logger = logging


def generate_employees_csv(
    master_sheet_path: str,
    save_path: str
):
    logger.debug("generating employees csv")

    raw_data = pd.read_csv(
        master_sheet_path,
        encoding="utf-8",
        low_memory=False 
    )

    org_structure = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-org-csv.csv"
        ),
        encoding="utf-8"
    )

    jobs = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-jobs-csv.csv"
        ),
        encoding="utf-8"
    )

    buildings = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-buildings-csv.csv"
        ),
        encoding="utf-8"
    )

    regions = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-regions-csv.csv"
        ),
        encoding="utf-8"
    )

    brm_branches = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-brm-branches-csv.csv"
        ),
        encoding="utf-8"
    )

    skills = pd.read_csv(
        os.path.join(
            save_path,
            "skills.csv"
        ),
        encoding="utf-8"
    )


    raw_data_employee_type_cleaned  = raw_data.drop(
        ["Current Device Type", "Current Asset Number"], axis = 1
    )

    def fix_employee_type(data):
        if data != data:
            return data 
        elif data.startswith("TER"):
            return "term"
        elif data == "IND":
            return "indeterminate"
        elif data.startswith("CAS"):
            return "casual"
        elif data.startswith("STD"):
            return "student"
        elif data.startswith("CON"):
            return "contractor"
        return np.NaN

    raw_data_employee_type_cleaned["Employee Type"]= raw_data_employee_type_cleaned[
        "Employee Type"
    ].apply(fix_employee_type)

    raw_data_headset_cleaned = raw_data_employee_type_cleaned
    raw_data_headset_cleaned["Headset"] = [
        False for i in range(0,raw_data_headset_cleaned.shape[0])
    ]

    for index, row in raw_data_employee_type_cleaned.iterrows():
        headset_2_ear = row["Jabra Evolve 40 MS Stereo (2-ear)"]
        headset_1_ear = row["Jabra Evolve 40 Mono (1-ear)"]

        if headset_2_ear != headset_2_ear:
            headset_2_ear = False
        elif headset_1_ear != headset_1_ear:
            headset_1_ear = False 
        
        if headset_1_ear or headset_2_ear:
            raw_data_headset_cleaned.at[index, "Headset"] = True 
        else:
            raw_data_headset_cleaned.at[index, "Headset"] = False 

    raw_data_headset_cleaned = raw_data_headset_cleaned.drop(
        [
            "Jabra Evolve 40 MS Stereo (2-ear)",
            "Jabra Evolve 40 Mono (1-ear)"
        ], axis = 1
    )

    raw_data_floor_cleaned = raw_data_headset_cleaned
    raw_data_floor_cleaned["Floor"] = raw_data_headset_cleaned["Floor"].apply(
        lambda x: x if x != "NONE" else np.NaN
    )
    raw_data_floor_cleaned["Floor"]


    raw_data_cleaned = raw_data_floor_cleaned

    def generateId(data):
        return "-".join(data.split("."))
    
    employee_csv = pd.DataFrame(
        columns = [
            "ID",
            "Employee Name",
            "Work Email",
            "Work Phone",
            "Office floor",
            "Office cubicle",
            "Employment status",
            "Device type",
            "Asset number",
            "Headset availability",
            "Second monitor availability",
            "Mobile hotspot availability",
            "Remote access to network",
            "Remote connection tool",
            "Department/External ID",
            "Branch/External ID",
            "Job Position/External ID",
            "Work Address/External ID",
            "Region/External ID",
            "Skills/Skill Type/External ID",
            "Skills/Skill/External ID",
            "Skills/Skill Level/External ID",
            "Work criticality",
            "Changed"
        ]
    )

    count = 0 
    for index, row in raw_data_cleaned.iterrows():
        name = row["Name"]
        email = row["E-mail Address"]
        branch = row["Branch"]
        phone = row["Phone Number"]
        office = row["Office"]
        floor = row["Floor"]
        cubicle = row["Desk Location"]
        employement_status = row["Employee Type"]
        device_type = row["Device Type"]
        asset_number = row["Asset Number"]
        headset_availability = row["Headset"]
        second_monitor = row["Second Monitor"]
        hotspot = row["Cellular modem (MiFi)"]
        vpn = row["VPN"]
        app_gate = row["AppGate"]
        department_name = row["Division Name"]
        job_title = row["Title"]
        region = row["Region"]
        skill = row["Skills"]
        subskill = row["Sub Skills"]
        critical = row["Critical"]
        changed = row["Changed"]

        # generate id
        row_id = generateId(email)

        if name != name:
            email_array = email.split("@")
            if len(email_array) >= 1:
                name_portion = email_array[0]
                name_array = name_portion.split(".")
                name_array_processed = []
                for i in name_array:
                    portion = i[0].upper()
                    if len(i) > 1:
                        portion = portion + i[1:]
                    
                    name_array_processed.append(portion)
                
                if len(name_array_processed) >=1:
                    name_array_processed.reverse() 
                    name = ", ".join(name_array_processed)
                else:
                    name = ""
            else:
                name = ""


        if phone != phone:
            phone = ""
        if floor != floor:
            floor = ""
        
        if cubicle != cubicle:
            cubicle = ""

        # filter NaN for employment status 
        if(employement_status != employement_status):
            employment_status = ""
        
        # clean device type 
        if device_type != device_type :
            device_type = ""
        elif device_type.strip() == "Desktop":
            device_type = "desktop"
        elif device_type.strip() == "Laptop":
            device_type = "laptop"
        elif device_type.strip() == "Tablet":
            device_type = "tablet"
        else:
            device_type = ""
        
        if asset_number != asset_number:
            asset_number = ""
        
        if(second_monitor != second_monitor):
            second_monitor = False 
        
        if(hotspot != hotspot):
            hotspot = False
        
        if( vpn != vpn ):
            vpn = False 
        
        if( app_gate != app_gate ):
            app_gate = False 
        
        remote_connection_tool = ""
        remote_access_to_network = False 
        if (app_gate and vpn):
            remote_connection_tool = "both"
            remote_access_to_network = True 
        elif app_gate:
            remote_connection_tool = "appgate"
            remote_access_to_network = True 
        elif vpn:
            remote_connection_tool = "vpn"
            remote_access_to_network = True 
        
        department_id = ""

        if department_name != department_name:
            department_id = org_structure.iloc[0]["ID"]
        else:
            department_df = org_structure[org_structure["Department Name"] == department_name]
            try:
                department_id = department_df.iloc[0]["ID"]
            except Exception as e:
                continue
        
        branch_id = ""
        if branch == branch and branch != "":
            branch_df = brm_branches[brm_branches["Name"] == branch]
            try:
                branch_id = branch_df.iloc[0]["ID"]
            except:
                pass
        
        job_id = ""

        if job_title == job_title:
            job_df = jobs[jobs["Job Position"] == job_title]
            job_id = job_df.iloc[0]["ID"]

        
        address_id = ""
        if office == office:
            building_df = buildings[buildings["Name"] == office]
            if building_df.shape[0] > 0:
                address_id = building_df.iloc[0]["ID"]

        region_id = ""
        if (region == region):
            region_df = regions[regions["Name"] == region]
            if region_df.shape[0] > 0:
                region_id = region_df.iloc[0]["ID"]
        
        skill_id = ""
        if (skill == skill):
            skill_df = skills[skills["Parent Skill"] == skill]
            if skill_df.shape[0] > 0:
                skill_id = skill_df.iloc[0]["Parent ID"]
        
        subskill_id = ""
        if (subskill == subskill):
            subskill_df = skills[skills["Parent Skill"] == skill]
            subskill_df_name = subskill_df[subskill_df["Name"] == subskill ]
            if subskill_df_name.shape[0] > 0:
                subskill_id = subskill_df_name.iloc[0]["ID"]
        elif skill == skill:
            subskill_df = skills[skills["Parent Skill"] == skill]
            if subskill_df.shape[0] > 0:
                subskill_df_other = subskill_df[subskill_df["Name"] == "Other"]
                subskill_id = subskill_df_other.iloc[0]["ID"]
            
            
            
        
        skill_level_id = ""
        if (skill_id != ""):
            subskill_df = skills[skills["Parent Skill"] == skill]
            if subskill_df.shape[0] > 0:
                skill_level_id = subskill_df.iloc[0]["Level ID"]
        
        row_data = [
            row_id,
            name,
            email,
            phone,
            floor,
            cubicle,
            employement_status,
            device_type,
            asset_number,
            headset_availability,
            second_monitor,
            hotspot,
            remote_access_to_network,
            remote_connection_tool,
            department_id,
            branch_id,
            job_id,
            address_id,
            region_id,
            skill_id,
            subskill_id,
            skill_level_id,
            critical,
            changed
        ]

        employee_csv.loc[count] = row_data

        count += 1

        sys.stdout.write("\rRows processed: %i" % count)
        sys.stdout.flush()
    
    employee_csv.to_csv(
        os.path.join(
            save_path,
            "odoo-employees-csv.csv"
        ),
        encoding="utf-8",
        index=False
    )

    logger.debug(
        "generated employees csv"
    )
    
    






        
















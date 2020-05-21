from datetime import datetime
from azure.storage.blob import BlobServiceClient
from import_data.importers.utils.rpc_connect import connect_to_rpc
import pandas as pd
import os
import logging
import pytz

logger = logging 

def calculate_diffs(
    blob_service_client: BlobServiceClient,
    diff_container: str,
    save_path: str,
    update_sheet_filename: str,
    username: str, 
    password: str,
    db: str,
    url: str,
):
    logging.debug("Starting diff calculation")
    todays_date = datetime.now().strftime(
        '%Y-%m-%d'
    )
    file_name = f"odoo-ad-diff-{todays_date}.csv"  
    file_path =  os.path.join(
        save_path,
        file_name
    )
    try:
        update_sheet_date = pd.read_excel(
            os.path.join(
                save_path,
                update_sheet_filename
            )
        )

        models, uid = connect_to_rpc(
            username,
            password,
            db,
            url
        )

        employees_in_odoo = models.execute_kw(
           db, uid, password,
           "hr.employee",
           "search_read",
           [[['active', '=', True]]],
           {
               'fields':['work_email', 'x_employee_work_criticality']
           } 
        )

        odoo_email_map = {}
        ad_email_map = {}
        update_sheet_date.rename(columns = {
            "Work Email - AD": "Work Email"
        }, inplace = True)

        columns = list(update_sheet_date.columns)
        columns.append(
            "Odoo Status"
        )
        columns.append(
            "Duplicates Found"
        )
        diff_csv = pd.DataFrame(
            columns = update_sheet_date.columns
        )

        for row in employees_in_odoo:
            email = row["work_email"]
            if odoo_email_map.get(email) is not None:
                odoo_email_map[email]["count"] = odoo_email_map[email]["count"] + 1
                if row["x_employee_work_criticality"] is True:
                    odoo_email_map[email]["critical"] = True
            else:
                odoo_email_map[email] = {}
                odoo_email_map[email]["count"] = 1
                odoo_email_map[email]["critical"] = row["x_employee_work_criticality"]

        
        for index, row in update_sheet_date.iterrows():
            email = row["Work Email"]
            ad_email_map[email] = row
        
        odoo_email_set = set(odoo_email_map.keys())
        ad_email_set = set(ad_email_map.keys())

        emails_in_odoo_but_not_ad = odoo_email_set - ad_email_set
        emails_in_ad_but_not_odoo = ad_email_set - odoo_email_set

        count = 0 
        for key in emails_in_ad_but_not_odoo:
            for name, value in ad_email_map[key].iteritems():
                diff_csv.at[count, name] = value
            diff_csv.at[count, "Odoo Status"] = "AD ONLY"
            diff_csv.at[count, "Duplicates Found"] = False
            count += 1
        

        for key in emails_in_odoo_but_not_ad:
            diff_csv.at[count, "Work Email"] = key
            diff_csv.at[count, "CriticalEmployee"] = odoo_email_map[key]["critical"]
            diff_csv.at[count, "Odoo Status"] = "ODOO ONLY"

            if odoo_email_map[key]["count"] > 1:
                diff_csv.at[count, "Duplicates Found"] = True
            else:
                diff_csv.at[count, "Duplicates Found"] = False
            
            count += 1
    
        diff_csv.to_csv(
            file_path,
            index=False,
            encoding="utf-8"
        )

        diff_sheet_client = blob_service_client.get_blob_client(
            container = diff_container,
            blob = file_name
        )

        with open(file_path, "rb") as f: 
            diff_sheet_client.upload_blob(
                f, overwrite = True
            )
        
        logger.debug("Diff calculation complete")
    except Exception as e:
        logger.critical(
            "Diff calculation failed", exc_info=True
        )
        raise e
    




import pandas as pd 
import os 
import sys
import logging 
import xmlrpc.client
from .utils.crud import create_record, update_record

logger = logging 


def import_departments(
    save_path: str,
    models: xmlrpc.client.ServerProxy,
    db,
    uid,
    password,
    db_cache
):
    logger.debug("Importing Departments into Odoo")
    data = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-org-csv.csv"
        ),
        encoding="utf-8"
    )

    columns = list(data.columns)

    count = 0
    for index, row in data.iterrows():
        row_id = row["ID"]
        name = row["Department Name"]
        parent_id = row["Parent Department/External ID"]

        dept = models.execute_kw(
            db, uid, password,
            "ir.model.data",
            "search_read",
            [[['name', '=', row_id]]],
            {
                'fields': ['res_id']
            } 
        )

        parent_dept = models.execute_kw(
            db, uid, password,
            "ir.model.data",
            "search_read",
            [[["name", "=", parent_id]]],
            {'fields': ['res_id']}
        )

        dept_def = {
            'name': name
        }

        if(parent_dept):
            dept_def['parent_id'] = parent_dept[0]['res_id']
        
        # fetch translation 
        translation = None 
        if ("Translation" in columns and row["Translation"] == row["Translation"] and row["Translation"] != ""):
            translation = row["Translation"]

        if not dept:
            dept_id = create_record(
                models, db, uid, password, 'hr.department',
                row_id, dept_def, 'hr.department,name', name,
                translation
            )

        else:
            dept_id = dept[0]["res_id"]
            update_record(
                models, db, uid, password, 'hr.department',
                dept_id, dept_def, 'hr.department,name', name , 
                translation
            )

        db_cache[row_id] = dept_id
        sys.stdout.write("\rRows processed: %i" % count)
        sys.stdout.flush()
        count +=1
    
    print("\n")
    logger.debug("Departments Imported")
        




import pandas as pd 
import os 
import sys
import logging 
import xmlrpc.client
from .utils.translate import create_or_update_translation 

logger = logging 


def import_departments(
    save_path: str,
    models: xmlrpc.client.ServerProxy,
    db,
    uid,
    password
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

        if not dept:
            # check whether external id currently exists
            dept_id = models.execute(
                db,uid,password,
                'hr.department',
                'create',
                dept_def
            )

            # create the external ID
            models.execute(
                db, uid, password,
                'ir.model.data', 'create',
                {
                    'name': row_id,
                    'module': '__import__',
                    'model': 'hr.department',
                    'res_id': dept_id
                }
            )

            create_or_update_translation(
                models, db, uid, password,
                'hr.department,name', dept_id,
                name, name, 'en_CA'
            )
        # if the department exists
        else:
            dept_id = dept[0]['res_id']
            models.execute_kw(
                db, uid, password,
                'hr.department',
                'write',
                [dept_id, dept_def]
            )

            create_or_update_translation(
                models, db, uid, password,
                'hr.department,name', dept_id,
                name, name, 'en_CA'
            )
        
        if ("Translation" in columns):
            translation = row["Translation"]

            if(translation == translation and translation != ""):
                create_or_update_translation(
                    models, db, uid, password,
                    'hr.department,name', dept_id,
                    name, translation, 'fr_CA'
                )
        
        sys.stdout.write("\rRows processed: %i" % count)
        sys.stdout.flush()
        count +=1
    
    logger.debug("Departments Imported")
        




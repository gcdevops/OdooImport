import pandas as pd 
import os 
import logging
import xmlrpc.client
from .utils.crud import create_record, update_record
from .utils.rpc_connect import connect_to_rpc

logger = logging 

def import_brm_branches(
    save_path: str,
    username,
    password,
    db,
    url,
    db_cache
):
    try:
        models, uid = connect_to_rpc(
            username,
            password,
            db,
            url
        )
        logger.debug("Import BRM Branches into Odoo")
        data = pd.read_csv(
            os.path.join(
                save_path,
                "odoo-brm-branches-csv.csv"
            ),
            encoding="utf-8"
        )
        columns = list(data.columns)
        count = 0 
        for index, row in data.iterrows():
            row_id = row["ID"]
            branch_name = row["Name"]

            branch = models.execute_kw(
                db, uid, password,
                "ir.model.data",
                "search_read",
                [[['name', '=', row_id]]],
                {
                    'fields': ["res_id"]
                }
            )

            translation = None
            if ("Translation" in columns and row["Translation"] == row["Translation"] and row["Translation"] != ""):
                translation = row["Translation"]
            
            if not branch:
                branch_id = create_record(
                    models, db, uid, password, 'hr.branch',
                    row_id, {'name': branch_name}, 'hr.branch,name',
                    branch_name, translation
                )
            else:
                branch_id = branch[0]["res_id"]
                update_record(
                    models, db, uid, password, 'hr.branch',
                    branch_id, {'name': branch_name}, 'hr.branch,name',
                    branch_name, translation
                )
            
            db_cache[row_id] = branch_id
            count += 1
        
        print("\n")
        logger.debug("BRM Branches Imported")
    except Exception as e:
        logger.critical("BRM Branches import failed", exc_info=True)
        raise e
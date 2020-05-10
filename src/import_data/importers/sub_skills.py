import pandas as pd 
import os 
import sys 
import logging
import xmlrpc.client
from .utils.crud import create_record, update_record
from .utils.rpc_connect import connect_to_rpc


logger = logging

def import_sub_skills(
    save_path,
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

        logger.debug(
            "Import Sub Skills into Odoo"
        )

        data = pd.read_csv(
            os.path.join(
                save_path,
                "odoo-sub-skills-csv.csv"
            ),
            encoding = "utf-8"
        )
        
        columns = list(data.columns)
        count = 0
        for index, row in data.iterrows():
            row_id = row["ID"]
            name = row["Name"]

            sub_skill = models.execute_kw(
                db, uid, password,
                'ir.model.data',
                'search_read',
                [[['name', '=', row_id]]], {
                    'fields': ['res_id']
                }
            )

            # fetch translation 
            translation = None 
            if ("Translation" in columns and row["Translation"] == row["Translation"] and row["Translation"] != ""):
                translation = row["Translation"]

            if not sub_skill:
                sub_skill_id = create_record(
                    models, db, uid, password,
                    'hr.skill', row_id, {'name': name}, 'hr.skill,name',
                    name, translation
                )
            else:
                sub_skill_id = sub_skill[0]["res_id"]
                update_record(
                    models, db, uid, password,
                    'hr.skill', sub_skill_id,
                    {'name': name}, 'hr.skill,name',
                    name, translation
                )
            
            db_cache[row_id] = sub_skill_id
            
            count +=1
        
        print("\n")
        logger.debug("Sub skills imported")
    
    except Exception as e:
        logger.critical(
            "Sub skills import failed"
        )
        raise e
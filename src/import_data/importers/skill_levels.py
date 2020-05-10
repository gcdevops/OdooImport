import pandas as pd 
import os 
import sys
import logging
import xmlrpc.client
from .utils.crud import create_record, update_record
from .utils.rpc_connect import connect_to_rpc

logger = logging 

def import_skill_levels(
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
        logger.debug("Import Skill Levels into Odoo")

        data = pd.read_csv(
            os.path.join(
                save_path,
                "odoo-skill-levels-csv.csv"
            ),
            encoding = 'utf-8'
        )

        count = 0
        for index, row in data.iterrows():
            row_id = row["ID"]
            name = row["Name"]

            level = models.execute_kw(
                db, uid, password,
                'ir.model.data',
                'search_read',
                [[['name', '=', row_id]]], {
                    'fields': ['res_id']
                }
            )

            if not level:
                level_id = create_record(
                    models, db, uid, password, 'hr.skill.level',
                    row_id, {'name': name}
                )
            else:
                level_id = level[0]['res_id']
                update_record(
                    models, db, uid, password, 'hr.skill.level',
                    level_id, {'name': name}
                )
            
            db_cache[row_id] = level_id
            count +=1

        print("\n")
        logger.debug("Skill Levels Imported")
    except Exception as e:
        logger.critical(
            "Skill level import failed"
        )
        raise e

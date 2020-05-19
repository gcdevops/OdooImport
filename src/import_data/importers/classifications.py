import pandas as pd 
import os 
import sys
import logging
from .utils.crud import create_record, update_record
from .utils.rpc_connect import connect_to_rpc

logger = logging 

def import_classifications(
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
        csv_path = os.path.join(
            save_path,
            "odoo-classifications-csv.csv"
        )
        
        if os.path.exists(csv_path):
            logger.debug("Importing Classifications")
            data = pd.read_csv(
                csv_path,
                encoding="utf-8"
            )

            count = 0 
            for index, row in data.iterrows():
                row_id = row["ID"]
                name = row["Classification Name"]
                level = row["Level"]

                classification = models.execute_kw(
                    db, uid, password,
                    'ir.model.data',
                    'search_read',
                    [[['name', '=', row_id]]],
                    {
                        'fields': ['res_id']
                    }
                )

                if not classification:
                    classification_id = create_record(
                        models, db, uid, password,
                        'hr.classification', row_id,
                        {
                            'name': name,
                            'level': level
                        }
                    )
                else:
                    classification_id = classification[0]['res_id']
                    update_record(
                        models, db, uid, password, 'hr.classification',
                        classification_id, {
                            'name': name,
                            'level': level
                        }
                    )
                
                db_cache[row_id] = classification_id
                count += 1
            
            print("\n")
            logger.debug(
                "Classifications Imported"
            )
    except Exception as e:
        logger.critical(
            "Classifications import failed", exec_info=True
        )
        raise e





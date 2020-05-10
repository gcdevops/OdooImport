import pandas as pd 
import os 
import sys 
import logging 
import xmlrpc.client
from .utils.crud import create_record, update_record

logger = logging 

def import_regions(
    save_path: str,
    models: xmlrpc.client.ServerProxy,
    db,
    uid,
    password,
    db_cache
):
    logger.debug("Import Regions into Odoo")
    data = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-regions-csv.csv"
        ),
        encoding="utf-8"
    )

    columns = list(data.columns)
    count = 0
    for index, row in data.iterrows():
        row_id = row["ID"]
        region_position = row["Name"]

        region = models.execute_kw(
            db, uid, password,
            "ir.model.data",
            "search_read",
            [[['name', '=', row_id]]],
            {
                'fields': ["res_id"]
            }
        )

        # fetch translation 
        translation = None 
        if ("Translation" in columns and row["Translation"] == row["Translation"] and row["Translation"] != ""):
            translation = row["Translation"]
        
        # create region 
        if not region:
            region_id = create_record(
                models, db, uid, password, 'hr.region',
                row_id, { 'name': region_position}, 'hr.region,name',
                region_position, translation
            )
        #update region
        else:
            region_id = region[0]["res_id"]
            update_record(
                models, db, uid, password, 'hr.region',
                region_id, {'name': region_position}, 'hr.region,name',
                region_position, translation
            )
        
        db_cache[row_id] = region_id
        sys.stdout.write("\rRows processed: %i" % count)
        sys.stdout.flush()
        count +=1

    print("\n")
    logger.debug("Regions Imported")







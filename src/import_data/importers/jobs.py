import pandas as pd 
import os 
import sys 
import logging 
import xmlrpc.client
from .utils.crud import create_record, update_record
from .utils.rpc_connect import connect_to_rpc

logger = logging 

def import_jobs(
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
        logger.debug("Import Jobs into Odoo")
        data = pd.read_csv(
            os.path.join(
                save_path,
                "odoo-jobs-csv.csv"
            ),
            encoding="utf-8"
        )

        columns = list(data.columns)
        count = 0
        for index, row in data.iterrows():
            row_id = row["ID"]
            job_position = row["Job Position"]

            job = models.execute_kw(
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
            
            # create job 
            if not job:
                job_id = create_record(
                    models, db, uid, password, 'hr.job',
                    row_id, { 'name': job_position}, 'hr.job,name',
                    job_position, translation
                )
            #update job
            else:
                job_id = job[0]["res_id"]
                update_record(
                    models, db, uid, password, 'hr.job',
                    job_id, {'name': job_position}, 'hr.job,name',
                    job_position, translation
                )
            
            db_cache[row_id] = job_id
            count +=1
        
        print("\n")
        logger.debug("Jobs Imported")
    except Exception as e:
        logger.critical(
            "Jobs import failed"
        )
        raise e







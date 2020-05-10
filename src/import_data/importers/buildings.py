import pandas as pd 
import os 
import sys
import logging
import xmlrpc.client
from .utils.crud import create_record, update_record

logger = logging

def import_buildings(
    save_path: str,
    models: xmlrpc.client.ServerProxy,
    db,
    uid,
    password,
    db_cache
):
    logger.debug("Import Buildings into Odoo")
    data = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-buildings-csv.csv"
        ),
        encoding="utf-8"
    )

    columns = list(data.columns)
    count = 0

    country_id = models.execute_kw(
        db, uid, password,
        'ir.model.data', 
        'search_read',
        [
            [
                '&',('model', '=', 'res.country'),
                ('name', '=', 'ca')]
        ],
        {'fields': ['res_id']}
    )

    country_id = country_id[0]["res_id"]

    for index, row in data.iterrows():
        row_id = row["ID"]
        name = row["Name"]
        company = row["Company Type"]
        address_type = row["Address Type"]
        street = row["Street"]
        city = row["City"]
        zip_code = row["Zip"]
        state_external_id = row["State/External ID"].replace("base.", "")

        building = models.execute_kw(
            db, uid, password,
            'ir.model.data', 'search_read',
            [[['name', '=', row_id]]],
            {
                'fields': ['res_id']
            }
        )

        state_id = models.execute_kw(
            db, uid, password,
            'ir.model.data', 'search_read',
            [
                [
                    '&',('model', '=', 'res.country.state'),
                    ('name', '=', state_external_id)
                ]
            ],
                {'fields': ['res_id']}
        )

        state_id = state_id[0]["res_id"]

        building_def = {
            'name': name,
            'is_company': "true",
            'street': street,
            'city': city,
            'zip': zip_code,
            'country_id': country_id,
            'state_id': state_id
        }

        if not building:
            building_id = create_record(
                models, db, uid, password,
                'res.partner', row_id,
                building_def
            )
        else:
            building_id = building[0]["res_id"]
            update_record(
                models, db, uid, password,
                'res.partner', building_id, building_def
            )
        
        db_cache[row_id] = building_id

        sys.stdout.write("\rRows processed: %i" % count)
        sys.stdout.flush()
        count +=1
    
    print("\n")
    logger.debug("Buildings Imported")

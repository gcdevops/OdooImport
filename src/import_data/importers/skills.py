import pandas as pd
import os
import sys
import logging
import xmlrpc.client
from .utils.crud import create_record, update_record

logger = logging

def import_skills(
    save_path: str,
    models: xmlrpc.client.ServerProxy,
    db,
    uid,
    password,
    db_cache
):
    logger.debug("Import Skills into Odoo")
    data = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-skills-csv.csv"
        ),
        encoding="utf-8"
    )

    columns = list(data.columns)
    count = 0
    for index, row in data.iterrows():
        sub_skill_external_id = row["skill_ids/id"]

        sub_skill_id = db_cache.get(sub_skill_external_id)
        if sub_skill_id is None:
            sub_skill = models.execute_kw(
                db, uid, password,
                'ir.model.data',
                'search_read',
                [[['name', '=', sub_skill_external_id]]], {
                    'fields': ['res_id']
                }
            )

            sub_skill_id = sub_skill[0]["res_id"]

        if row["ID"] == row["ID"]:
            row_id = row["ID"]
            name = row["name"]
            skill_level_external_id = row["skill_level_ids/name"]

            skill = models.execute_kw(
                db, uid, password,
                'ir.model.data',
                'search_read',
                [[['name', '=', row_id]]], {
                    'fields': ['res_id']
                }
            )

            skill_level_id = db_cache.get(skill_level_external_id)
            if skill_level_id is None:
                skill_level = models.execute_kw(
                    db, uid, password,
                    'ir.model.data',
                    'search_read',
                    [[['name', '=', skill_level_external_id]]], {
                        'fields': ['res_id']
                    }
                )

                skill_level_id = skill_level[0]['res_id']

            translation = None
            if ("Translation" in columns and row["Translation"] == row["Translation"] and row["Translation"] != ""):
                translation = row["Translation"]

            if not skill:
                skill_id = create_record(
                    models, db, uid, password,
                    'hr.skill.type', row_id, {'name': name},
                    'hr.skill.type,name', name, translation
                )

                update_record(
                    models,db, uid, password,
                    "hr.skill", sub_skill_id,
                    {
                        'skill_type_id': skill_id
                    }
                )

                update_record(
                    models, db, uid, password,
                    'hr.skill.level',skill_level_id,
                    {
                        'skill_type_id': skill_id
                    }
                )

            else:
                skill_id = skill[0]['res_id']

                update_record(
                    models, db, uid, password,
                    "hr.skill", sub_skill_id, {
                        'skill_type_id': skill_id
                    }
                )

                update_record(
                    models, db, uid, password,
                    'hr.skill.level', skill_level_id,
                    {
                        'skill_type_id': skill_id
                    }
                )

            db_cache[row_id] = skill_id
        else:
            update_record(
                models, db, uid, password,
                'hr.skill', sub_skill_id,
                {
                    'skill_type_id': skill_id
                }
            )

        sys.stdout.write("\rRows processed: %i" % count)
        sys.stdout.flush()
        count +=1

    print("\n")
    logger.debug("Skill Levels Imported", exc_info=True)

import pandas as pd 
import os 
import sys
import logging 
import xmlrpc.client
from .utils.crud import create_record, update_record

logger = logging 

def import_employees(
    save_path: str,
    models: xmlrpc.client.ServerProxy,
    db,
    uid,
    password
):
    logger.debug("Importing Employees")

    data = pd.read_csv(
        os.path.join(
            save_path,
            "odoo-employees-csv.csv"
        ),
        encoding = "utf-8"
    )

    count = 0

    for index, row in data.iterrows():
        row_id = row["ID"]
        department_external_id = row["Department/External ID"]
        job_external_id = row["Job Position/External ID"]
        building_external_id = row["Work Address/External ID"]
        region_external_id = row["Region/External ID"]
        skills_external_id = row["Skills/Skill Type/External ID"]
        sub_skills_external_id = row["Skills/Skill/External ID"]
        skill_level_external_id = row["Skills/Skill Level/External ID"]
        
        

        employee_def = {
            'name': row["Employee Name"],
            'work_email': row["Work Email"],
            'work_phone': row["Work Phone"] if row["Work Phone"] == row["Work Phone"] else "",
            'x_employee_office_floor': row["Office floor"] if row["Office floor"] == row["Office floor"] else "",
            'x_employee_office_cubicle': row["Office cubicle"] if row["Office cubicle"] == row["Office cubicle"] else "",
            'x_employee_status': row["Employment status"] if row["Employment status"] == row["Employment status"] else "",
            'x_employee_device_type': row["Device type"] if row["Device type"] == row["Device type"] else "",
            'x_employee_asset_number': row["Asset number"] if row["Asset number"] == row["Asset number"] else "",
            'x_employee_headset': row["Headset availability"],
            'x_employee_second_monitor': row["Second monitor availability"],
            'x_employee_mobile_hotspot': row["Mobile hotspot availability"],
            'x_employee_remote_access_network': row["Remote access to network"],
            'x_employee_remote_access_tool': row["Remote connection tool"] if row["Remote connection tool"] == row["Remote connection tool"] else "",
            'x_employee_work_criticality': row["Work criticality"]
        }


        department= models.execute_kw(
            db, uid, password,
            "ir.model.data",
            "search_read",
            [[['name', '=', department_external_id]]],
            {
                'fields': ['res_id']
            }
        )
        if department:
            employee_def["department_id"] =  department[0]['res_id']
        

        job = models.execute_kw(
            db, uid, password,
            "ir.model.data",
            "search_read",
            [[['name', '=', job_external_id]]],
            {
                'fields': ['res_id']
            }
        )
        if job:
            employee_def['job_id'] = job[0]['res_id']

        building = models.execute_kw(
            db, uid, password,
            "ir.model.data",
            "search_read",
            [[['name', '=', building_external_id]]],
            {
                'fields': ['res_id']
            }
        )
        if building:
            employee_def['address_id'] = building[0]['res_id']

        region= models.execute_kw(
            db, uid, password,
            'ir.model.data',
            'search_read',
            [[['name', '=', region_external_id]]],
            {
                'fields': ['res_id']
            }
        )
        if region:
            employee_def["region_id"] = region[0]['res_id']
        
        employee_skill_def = {}

        if skill_level_external_id == skill_level_external_id:
            skill = models.execute_kw(
                db, uid, password,
                'ir.model.data',
                'search_read',
                [[['name', '=', skills_external_id]]],
                {
                    'fields': ['res_id']
                }
            )

            if skill:
                employee_skill_def['skill_type_id'] = skill[0]['res_id']
            
            sub_skill = models.execute_kw(
                db, uid, password,
                'ir.model.data',
                'search_read',
                [[['name', '=', sub_skills_external_id]]],
                {
                    'fields': ['res_id']
                }
            )
            if sub_skill:
                employee_skill_def['skill_id'] = sub_skill[0]['res_id']
            

            skill_level= models.execute_kw(
                db, uid, password,
                'ir.model.data',
                'search_read',
                [[['name', '=', skill_level_external_id]]],
                {
                    'fields': ['res_id']
                }

            )
            if skill_level:
                employee_skill_def['skill_level_id'] = skill_level[0][
                    'res_id'
                ]
        
        employee = models.execute_kw(
            db, uid, password,
            'ir.model.data',
            'search_read',
            [[['name', '=', row_id]]],
            {
                'fields': ['res_id']
            }
        )

        if not employee:
            employee_id = create_record(
                models, db, uid, password,
                'hr.employee', row_id, employee_def
            )

            if  bool(employee_skill_def):
                employee_skill_def['employee_id'] = employee_id

                create_record(
                    models,db, uid, password, 
                    "hr.employee.skill", row_id + "-skill-map",
                    employee_skill_def
                )


        else:
            employee_id = employee[0]['res_id']
            update_record(
                models, db, uid, password,
                'hr.employee', employee_id, employee_def
            )

            if bool(employee_skill_def):
                employee_skill_def['employee_id'] = employee_id

                skill_map = models.execute_kw(
                    db, uid, password,
                    'hr.employee.skill',
                    'search_read',
                    [
                        [
                            '&', '&', '&', 
                            ('employee_id', '=', employee_id),
                            ('skill_id', '=', employee_skill_def['skill_id']),
                            ('skill_level_id', '=', employee_skill_def['skill_level_id']), 
                            ('skill_type_id', '=', employee_skill_def['skill_type_id'])
                        ]
                    ],
                    {'fields': ['id']}
                )

                if not skill_map:
                    create_record(
                        models, db, uid, password,
                        'hr.employee.skill', row_id + "-skill-map",
                        employee_skill_def 
                    )
                
                else:
                    skill_map_id = skill_map[0]['id']
                    update_record(
                        models, db, uid, password,
                        'hr.employee.skill', skill_map_id,
                        employee_skill_def
                    )
        
        sys.stdout.write("\rRows processed: %i" % count)
        sys.stdout.flush()
        count +=1
    
    print("\n")
    logger.debug("Skill Levels Imported")



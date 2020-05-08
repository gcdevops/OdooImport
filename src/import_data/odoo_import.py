import xmlrpc.client 
import os
import logging
from .importers import ( 
    import_departments,
    import_jobs,
    import_buildings,
    import_regions,
    import_skill_levels,
    import_sub_skills
)

logger = logging 

def connect_to_rpc(
    username: str,
    password: str,
    db: str,  
    url: str
):

    try:
        common = xmlrpc.client.ServerProxy(
            url + "/xmlrpc/2/common"
        )
        common.version()
        uid = common.authenticate(
            db, username, password, {}
        )
        models = xmlrpc.client.ServerProxy(
            url + "/xmlrpc/2/object"
        )

        return models, uid
    except Exception as e:
        logger.critical(
            "Failed to authenticate against Odoo endpoint"
        )
        raise e


def import_data_to_odoo(
    username: str,
    password: str,
    db: str,
    url: str,
    save_path: str
):
    models, uid = connect_to_rpc(
        username,
        password,
        db,
        url
    )

    
    try:
        import_departments(
            save_path,
            models,
            db,
            uid,
            password
        )
    except Exception as e:
        logger.critical("Failed to import departments")
        raise e


    try:
        import_jobs(
            save_path,
            models,
            db,
            uid,
            password
        )
    except Exception as e:
        logger.critical("Failed to import jobs")
        raise e
    

    
    try:
        import_buildings(
            save_path,
            models,
            db,
            uid,
            password
        )
    except Exception as e:
        logger.critical("Failed to import buildings")
        raise e
    

    
    try:
        import_regions(
            save_path,
            models,
            db,
            uid,
            password
        )
    except Exception as e:
        logger.critical("Failed to import regions")
        raise e
    
    
    
    try:
        import_skill_levels(
            save_path,
            models,
            db,
            uid,
            password
        )
    except Exception as e:
        logger.critical("Failed to import skill levels")
        raise e
    

    try:
        import_sub_skills(
            save_path,
            models,
            db,
            uid,
            password
        )
    except Exception as e:
        logger.critical("Failed to import sub skill")
        raise e
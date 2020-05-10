import os
import logging
from . importers.utils.rpc_connect import connect_to_rpc
from .importers.utils.propegating_thread import PropagatingThread
from .importers import ( 
    import_departments,
    import_jobs,
    import_buildings,
    import_regions,
    import_skill_levels,
    import_sub_skills,
    import_skills,
    import_employees
)

logger = logging 



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

    db_id_cache = {}
    threads = []

    department_thread = PropagatingThread(
        target=import_departments,
        args=(
            save_path,
            username,
            password,
            db,
            url,
            db_id_cache
        )
    )
    department_thread.start()
    threads.append(department_thread)

    job_thread = PropagatingThread(
        target=import_jobs,
        args=(
            save_path,
            username,
            password,
            db,
            url,
            db_id_cache
        )
    )
    job_thread.start()
    threads.append(job_thread)


    building_thread = PropagatingThread(
        target=import_buildings,
        args=(
            save_path,
            username,
            password,
            db,
            url,
            db_id_cache
        )
    )
    building_thread.start()
    threads.append(building_thread)

    regions_thread = PropagatingThread(
        target=import_regions,
        args=(
            save_path,
            username,
            password,
            db,
            url,
            db_id_cache
        )
    )
    regions_thread.start()
    threads.append(regions_thread)


    skill_levels_thread = PropagatingThread(
        target=import_skill_levels,
        args=(
            save_path,
            username,
            password,
            db,
            url,
            db_id_cache
        )
    )
    skill_levels_thread.start()
    threads.append(skill_levels_thread)    
    
    sub_skill_thread = PropagatingThread(
        target=import_sub_skills,
        args=(
            save_path,
            username,
            password,
            db,
            url,
            db_id_cache
        )
    )
    sub_skill_thread.start()
    threads.append(sub_skill_thread)

    for i in threads:
        i.join()


    try:
        import_skills(
            save_path,
            models,
            db,
            uid,
            password,
            db_id_cache
        )
    except Exception as e:
        logger.critical("Failed to import skills")
        raise e
    

    import_employees(
        save_path,
        username,
        password,
        db,
        url,
        db_id_cache
    )
    
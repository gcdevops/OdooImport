import logging
from utils.connect_to_rpc import connect_to_rpc

logger = logging 

def odoo_only_employees(
    employee_set,
    username,
    password,
    database,
    url
):
    logger.debug("Setting Employee in AD to false for Odoo Only Employees")
    try:
        models, uid = connect_to_rpc(
            username,
            password,
            database,
            url
        )

        for email in employee_set:

            employee_rows = models.execute_kw(
                database, uid, password,
                'hr.employee',
                'search_read',
                [[['work_email', '=', email]]],
                {
                    'fields': ['id']
                }
            )
            if employee_rows:
                for row in employee_rows:
                    models.execute_kw(
                        database, uid, password,
                        "hr.employee",
                        "write",
                        [   row['id'],
                            {
                                'x_employee_in_ad': False
                            }
                        ]
                    )
    except Exception as e:
        logger.critical(
            "Failed to set Employee in AD to false for Odoo Only Employees",
            exc_info=True
        )
        raise e
        
        


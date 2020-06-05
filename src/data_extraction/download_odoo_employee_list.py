from utils.connect_to_rpc import connect_to_rpc
import logging

logger = logging

def download_odoo_employee_list(
    username: str,
    password: str,
    db: str,
    url: str
): 
    logger.debug("Downloading employee list from Odoo")
    models, uid = connect_to_rpc(
        username,
        password,
        db,
        url
    )

    try:
        employee_data = models.execute_kw(
            db, uid,  password,
            "hr.employee",
            "search_read",
            [[["active", "=", True]]],
            {
                'fields': ['work_email']
            }
        )

        odoo_emails = [
            row["work_email"] for row in employee_data 
        ]

        return odoo_emails
    except Exception as e:
        logger.critical(
            "Could not download employee list",
            exc_info=True
        )
        raise e
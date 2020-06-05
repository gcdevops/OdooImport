import logging
from utils.connect_to_rpc import connect_to_rpc


logger = logging 

def intersection_employees(
    employee_list,
    ad_employee_data,
    username,
    password,
    database,
    url
):
    logger.debug("Updating AD Employees in Odoo")
    try: 
        emails_handled = {}
        models, uid = connect_to_rpc(
            username,
            password,
            database,
            url
        )
        for email in employee_list:
            if emails_handled.get(email) is None:
                matched_employee_row = ad_employee_data[
                    ad_employee_data["E-mail Address"] == email
                ]
                odoo_employee_records = models.execute_kw(
                    database, uid, password,
                    "hr.employee",
                    'search_read',
                    [[['work_email', '=', email]]],
                    {
                        'fields': ['id']
                    }
                )

                if odoo_employee_records:
                    asset_number = matched_employee_row.iloc[0]['Asset Number']
                    device_type = matched_employee_row.iloc[0]['Device Type']
                    app_gate = matched_employee_row.iloc[0]['AppGate']
                    vpn = matched_employee_row.iloc[0]['VPN']

                    update_dict = {}

                    if asset_number == asset_number and asset_number != "":
                        update_dict['x_employee_asset_number'] = asset_number
                    
                    if device_type == device_type and device_type.strip() == "Desktop":
                        update_dict['x_employee_device_type'] = "desktop"
                    elif device_type == device_type and device_type.strip() == "Laptop":
                        update_dict['x_employee_device_type'] = "laptop"
                    elif device_type == device_type and device_type.strip() == "Tablet":
                        update_dict['x_employee_device_type'] = "tablet"
                    
                    if app_gate != app_gate:
                        app_gate = False
                    
                    if vpn != vpn:
                        vpn = False
                    
                    if app_gate and vpn:
                        update_dict['x_employee_remote_access_tool'] = "both"
                        update_dict['x_employee_remote_access_network'] = True
                    elif vpn:
                        update_dict['x_employee_remote_access_tool'] = "vpn"
                        update_dict['x_employee_remote_access_network'] = True
                    elif app_gate:
                        update_dict['x_employee_remote_access_tool'] = "appgate"
                        update_dict['x_employee_remote_access_network'] = True
                    else:
                        update_dict['x_employee_remote_access_tool'] = ""
                        update_dict['x_employee_remote_access_network'] = False
                    
                    update_dict['x_employee_in_ad'] = True

                    for record in odoo_employee_records:
                        row_id = record['id']
                        models.execute_kw(
                            database, uid, password,
                            "hr.employee",
                            "write",
                            [
                                row_id,
                                update_dict
                            ]
                        )

                    emails_handled[email] = 1
                    
    except Exception as e:
        logger.critical(
            "Updating AD Employees in Odoo",
            exc_info=True
        )
        raise e            

                    




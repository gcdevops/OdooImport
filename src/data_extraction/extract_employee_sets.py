import pandas
import os 



def extract_employee_sets(
    ad_employee_data,
    odoo_employee_list
):
    ad_email_addresses = list(ad_employee_data["E-mail Address"].unique())

    odoo_email_set = set(odoo_employee_list)
    ad_email_set = set(ad_email_addresses)
    odoo_only = odoo_email_set - ad_email_set
    ad_only = ad_email_set - odoo_email_set
    intersection = odoo_email_set.intersection(ad_email_set)

    return [
        list(odoo_only),
        list(intersection),
        list(ad_only)
    ]


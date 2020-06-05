import os
import logging
import logging.config
import pandas
from utils.logging import SlackFormatter, SlackHandler
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from data_extraction.download_and_process_update_sheet import download_and_process_update_sheet
from data_extraction.download_odoo_employee_list import download_odoo_employee_list
from data_extraction.extract_employee_sets import extract_employee_sets
from import_data.odoo_only_employees import odoo_only_employees
from import_data.intersection_employees import intersection_employees
from import_data.ad_only_employees import ad_only_employees

LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "default": {
            "class": "logging.Formatter",
            "format": "LEVEL: %(levelname)s TIME: %(asctime)s FILENAMEL %(filename)s MODULE: %(module)s MESSAGES: %(message)s \n"
        },
        "slackFormatter": {
            "class": "utils.logging.SlackFormatter"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "NOTSET",
            "formatter": "default"
        },
        "slack": {
            "class": "utils.logging.SlackHandler",
            "level": "ERROR",
            "formatter": "slackFormatter"
        }
    },
    "loggers": {
        "": {
            "handlers": [
                "console", "slack"
            ],
            "level": "NOTSET"
        }
    }
}



logging.config.dictConfig(LOGGING_CONFIG)

def main():
    connectionString = os.environ.get(
        "AZURE_CONNECTION_STRING"
    )

    odooUrl = os.environ.get(
        "ODOO_URL"
    )
    if not odooUrl:
        raise ValueError("ODOO_URL required")

    odooUser = os.environ.get(
        "ODOO_USER"
    )
    if not odooUser:
        raise ValueError("ODOO_USER required")

    odooPassword = os.environ.get(
        "ODOO_PASSWORD"
    )
    if not odooPassword:
        raise ValueError("ODOO_PASSWORD required")

    odooDatabase = os.environ.get(
        "ODOO_DATABASE"
    )
    if not odooDatabase:
        raise ValueError("ODOO_DATABASE")

    updatedSheetContainer = os.environ.get(
        "UPDATE_SHEET_CONTAINER"
    )
    updatedSheetFileName = os.environ.get(
        "UPDATE_SHEET_FILE_NAME"
    )
    if not connectionString:
        raise ValueError(
            "Environment variable AZURE_CONNECTION_STRING required"
        )

    if(updatedSheetContainer is None or updatedSheetFileName is None):
        raise ValueError(
            "Environment variables " +
            "UPDATE_SHEET_CONTAINER and " +
            "UPDATE_SHEET_FILE_NAME " +
            "need to be specified"
        )


    full_path = os.path.abspath("./data")
    if not os.path.isdir(full_path):
        os.mkdir(full_path)

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connectionString)
    except Exception as e:
        logging.critical(
            "Could not connect to Azure Storage"
        )
        raise e

    logging.critical("Import is starting")
    download_and_process_update_sheet(
        blob_service_client,
        full_path,
        updatedSheetContainer,
        updatedSheetFileName
    )

    odoo_employee_list = download_odoo_employee_list(
        odooUser,
        odooPassword,
        odooDatabase,
        odooUrl
    )

    ad_employee_data = pandas.read_csv(
        os.path.join(
            full_path,
            "employee-updated-data.csv"
        ),
        encoding="utf-8"
    )

    employee_sets = extract_employee_sets(
        ad_employee_data,
        odoo_employee_list
    )

    logging.debug(
        "Odoo Only: " + str(len(employee_sets[0])) + 
        " Odoo and AD: " + str(len(employee_sets[1])) + 
        " AD Only: " + str(len(employee_sets[2]))
    )
    
    
    odoo_only_employees(
        employee_sets[0],
        odooUser,
        odooPassword,
        odooDatabase,
        odooUrl
    )
    

    
    intersection_employees(
        employee_sets[1],
        ad_employee_data,
        odooUser,
        odooPassword,
        odooDatabase,
        odooUrl
    )

    ad_only_employees(
        employee_sets[2],
        ad_employee_data,
        odooUser,
        odooPassword,
        odooDatabase,
        odooUrl
    )

    logging.critical(
        "Import is complete"
    )








if(__name__ == "__main__"):
    main()

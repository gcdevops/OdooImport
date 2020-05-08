import os
import logging
import logging.config
from utils.logging import SlackFormatter, SlackHandler
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from data_extraction.update_master_sheet import update_master_sheet
from data_extraction.generate_org_csv import generate_org_csv
from data_extraction.generate_regions_csv import generate_regions_csv
from data_extraction.generate_jobs_csv import generate_jobs_csv
from data_extraction.generate_buildings_csv import generate_buildings_csv
from data_extraction.generating_skills_csv import generate_skills_csv
from data_extraction.generate_employees_csv import generate_employees_csv
from import_data.odoo_import import import_data_to_odoo

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

ORGS_TO_IGNORE = [
    "100000.103642.100832"
]

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


    masterSheetContainer = os.environ.get(
        "EMPLOYEE_MASTER_SHEET_CONTAINER"
    )
    masterSheetFileName = os.environ.get(
        "EMPLOYEE_MASTER_SHEET_FILE_NAME"
    )
    
    updatedSheetContainer = os.environ.get(
        "UPDATE_SHEET_CONTAINER"
    )
    updatedSheetFileName = os.environ.get(
        "UPDATE_SHEET_FILE_NAME"
    )

    orgSheetContainer = os.environ.get(
        "ORG_SHEET_CONTAINER"
    )
    orgSheetFileName = os.environ.get(
        "ORG_SHEET_FILE_NAME"
    )
    
    if not connectionString:
        raise ValueError(
            "Environment variable AZURE_CONNECTION_STRING required"
        )

    if masterSheetContainer is None or masterSheetFileName is None :
        raise ValueError(
            "Environment variables " + 
            "EMPLOYEE_MASTER_SHEET_CONTAINER and " +
            "EMPLOYEE_MASTER_SHEET_FILE_NAME " +
            "need to be specified"
        )

    if(updatedSheetContainer is None or updatedSheetFileName is None):
        raise ValueError(
            "Environment variables " + 
            "UPDATE_SHEET_CONTAINER and " +
            "UPDATE_SHEET_FILE_NAME " +
            "need to be specified"
        )
    
    if(orgSheetContainer is None or orgSheetFileName is None):
        raise ValueError(
            "Environment variables " + 
            "ORG_SHEET_CONTAINER and " +
            "ORG_SHEET_FILE_NAME " +
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

    """
    update_master_sheet(
        blob_service_client,
        full_path,
        masterSheetContainer,
        masterSheetFileName,
        orgSheetContainer,
        orgSheetFileName,
        updatedSheetContainer,
        updatedSheetFileName
    )

    
    generate_org_csv(
        os.path.join(
            full_path,
            orgSheetFileName
        ),
        full_path,
        ORGS_TO_IGNORE
    )

    
    generate_regions_csv(
        os.path.join(
            full_path,
            masterSheetFileName
        ),
        full_path
    )
    

    
    generate_jobs_csv(
        os.path.join(
            full_path,
            masterSheetFileName
        ),
        full_path
    )
    

    generate_buildings_csv(
        os.path.join(
            full_path,
            masterSheetFileName
        ),
        full_path
    )


    
    generate_skills_csv(
        os.path.join(
            full_path,
            masterSheetFileName
        ),
        full_path
    )

    generate_employees_csv(
        os.path.join(
            full_path,
            masterSheetFileName
        ),
        full_path
    )
    """

    import_data_to_odoo(
        odooUser,
        odooPassword,
        odooDatabase,
        odooUrl,
        full_path
    )
    







if(__name__ == "__main__"):
    main()
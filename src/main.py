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
from data_extraction.generate_brm_branches_csv import generate_brm_branches_csv
from data_extraction.generate_classifications_csv import generate_classifications_csv
from diff_calculator import calculate_diffs
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
   # IITB
   "100000.103642.100832",
   # Atlantic Region Reg Opertins & Compliance DGO
   "100000-100762-100713-100905-100908",
   # Quebec region Reg Opertins & Compliance DGO
   "100000-100762-100713-100905-100115",
   # Ontario region Reg Opertins & Compliance DGO
   "100000-100762-100713-100905-100308",
   # Ontario region COO Service Canada
   "100000-101078-101273",
   # Atlantic region COO Service Canada
   "100000-101078-103631",
   # Quebec region COO Service Canada
   "100000-101078-10009",
   # TISMB
   "100000-101078-104621",
   # HRSB
   "100000-103642-100841",
   # ESDC Chief Financial Officer
   "100000.103642.101064",
   # Corporate Secretariat
   "100000.101729.100669",
   # Income Security and Social Development
   "100000.103642.100651",
   # Internal Audit Services
   "100000.103642.100816",
   # Learning
   "100000.103642.100538"
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

    diffContainer = os.environ.get(
        "DIFF_CONTAINER"
    )

    batchSize = os.environ.get(
        "IMPORTER_BATCH_SIZE"
    )

    deltasOnly = os.environ.get(
        "IMPORTER_DELTAS_ONLY"
    )

    if not batchSize:
        batchSize = 1000
    else:
        batchSize = int(batchSize)
        if batchSize < 1:
            raise ValueError("IMPORTER_BATCH_SIZE must be greater than one")

    if not deltasOnly:
        deltasOnly = False
    elif deltasOnly == "True":
        deltasOnly = True
    else:
        deltasOnly = False

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

    if(diffContainer is None):
        raise ValueError(
            "Environment variable " +
            "DIFF_CONTAINER needs to be specifiedß"
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

    logging.critical("Import is starting")

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

    generate_brm_branches_csv(
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

    generate_classifications_csv(
        full_path
    )

    generate_employees_csv(
        os.path.join(
            full_path,
            masterSheetFileName
        ),
        full_path
    )

    import_data_to_odoo(
        odooUser,
        odooPassword,
        odooDatabase,
        odooUrl,
        full_path,
        batchSize,
        deltasOnly
    )


    try:
        master_sheet_client = blob_service_client.get_blob_client(
            container = masterSheetContainer,
            blob = masterSheetFileName
        )

        org_sheet_client = blob_service_client.get_blob_client(
            container= orgSheetContainer,
            blob = orgSheetFileName
        )
    except Exception as e:
        logging.critical(
            "Could not successfully connect to Blobs to upload data"
        )
        raise e

    with open(os.path.join(full_path, "employee_master_sheet.csv"), "rb") as f:
        master_sheet_client.upload_blob(
            f, overwrite = True
        )

    with open(os.path.join(full_path, "org-structure.csv"), "rb") as f:
        org_sheet_client.upload_blob(
            f, overwrite = True
        )

    calculate_diffs(
        blob_service_client,
        diffContainer,
        full_path,
        updatedSheetFileName,
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

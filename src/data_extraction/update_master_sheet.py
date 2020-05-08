import logging
import os
from azure.storage.blob import BlobServiceClient
from .generate_master_sheet import generate_master_sheet

logger = logging

def update_master_sheet(
    client:BlobServiceClient,
    save_path: str,
    master_sheet_container_name: str,
    master_sheet_file_name: str,
    org_sheet_container_name: str,
    org_sheet_file_name: str,
    update_sheet_container_name: str,
    update_sheet_file_name: str
):
    
    try:
        logger.debug("Connecting to Master Sheet Blob")
        master_sheet_client = client.get_blob_client(
            container = master_sheet_container_name,
            blob = master_sheet_file_name
        )

        logger.debug("Connecting to Org Sheet Blob")
        org_sheet_client = client.get_blob_client(
            container = org_sheet_container_name,
            blob = org_sheet_file_name
        )

        logger.debug("Connecting to Update Sheet Blob")
        update_sheet_client = client.get_blob_client(
            container = update_sheet_container_name,
            blob= update_sheet_file_name
        )
    except Exception as e:
        logger.critical(
            "Could not successfully connect to Blobs"
        )
        raise e


    # downloading blobs locally
    logger.debug("Downloading Master Sheet Blob")
    master_sheet_path =  os.path.join(save_path, "employee_master_sheet.csv")
    try:
        with open(master_sheet_path, "wb") as f:
            f.write(
                master_sheet_client.download_blob().readall()
            )
    except Exception as e:
        logger.critical(
            "Could not download Employee Master Sheet blob"
        )
        raise e


    logger.debug("Downloading Org Sheet Blob")
    org_sheet_path = os.path.join(save_path, "org-structure.csv")
    try:
        with open(org_sheet_path, "wb") as f:
            f.write(
                org_sheet_client.download_blob().readall()
            )
    except Exception as e:
        logger.critical(
            "Could not download Org Sheet blob"
        )
        raise e

    
    logger.debug("Downloading Update Sheet Blob")
    update_sheet_path = os.path.join(
        save_path, "employee-updated-data.xlsx"
    )
    try:
        with open(update_sheet_path, "wb") as f:
            f.write(
                update_sheet_client.download_blob().readall()
            )
    except Exception as e:
        logger.critical(
            "Could not download Update Sheet blob"
        )
        raise e

    generate_master_sheet(
        master_sheet_path,
        update_sheet_path,
        org_sheet_path,
        master_sheet_client,
        update_sheet_path,
        org_sheet_client
    )





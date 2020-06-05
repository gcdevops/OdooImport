import logging
import os
from azure.storage.blob import BlobServiceClient
from .process_update_sheet import process_update_sheet

logger = logging

def download_and_process_update_sheet(
    client:BlobServiceClient,
    save_path: str,
    update_sheet_container_name: str,
    update_sheet_file_name: str
):
    
    try:
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

    process_update_sheet(
        save_path,
        update_sheet_path
    )





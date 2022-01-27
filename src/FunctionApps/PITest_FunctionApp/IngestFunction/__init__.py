import logging

import azure.functions as func
from shared.sftp_client import PHDISFTPClient
from shared.storage_client import PHDIStorageClient
from shared.settings import SFTPSettings, StorageClientSettings


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    sftp_settings = SFTPSettings()
    storage_client_settings = StorageClientSettings()
    logging.info(f"Settings: {sftp_settings}, {storage_client_settings}")

    vdh_sftp_client = PHDISFTPClient(sftp_settings)
    azure_storage_client = PHDIStorageClient(storage_client_settings)

    logging.info("Full file listing:")
    file_tree = vdh_sftp_client.get_tree("/")
    logging.info(f"\n\nFile Tree: {file_tree}")

    logging.info("\n\nCopying single file from SFTP to blob storage:")
    vdh_sftp_client.transmit_single_file_from_sftp_to_blob(
        "/eICR/TEST_FILE.TXT", "/bronze/eICR", azure_storage_client
    )

    return func.HttpResponse(f"This HTTP triggered function executed successfully.")

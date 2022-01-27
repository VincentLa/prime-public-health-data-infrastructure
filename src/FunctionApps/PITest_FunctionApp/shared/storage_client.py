import logging
import uuid
from io import BytesIO
from typing import List, Optional
import sys
import traceback

from azure.storage.blob import BlobServiceClient

from .settings import StorageClientSettings


class PHDIStorageClient:
    def __init__(self, settings: StorageClientSettings):
        self._settings = settings
        self._blob_service_client = self._setup_blob_client()

    def _setup_blob_client(self) -> BlobServiceClient:
        try:
            logging.debug(f"Setting up blob client")
            if self._settings.connection_string is None:
                raise TypeError
            blob_service_client = BlobServiceClient.from_connection_string(
                self._settings.connection_string
            )
            return blob_service_client
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(f"{lines}")
            return None

    def create_container(self, name=None) -> bool:
        try:
            if not name:
                name = str(uuid.uuid4())
            # Create the container
            logging.debug(f"Creating container {name}")
            self._blob_service_client.create_container(name)
            return True
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(f"{lines}")
            return False

    def list_blobs_in_container(self, container_name: str) -> List[str]:
        try:
            container_client = self._blob_service_client.get_container_client(
                container_name
            )
            # List the blobs in the container
            return container_client.list_blobs()
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(f"{lines}")
            return []

    def upload_data_to_blob(
        self, data: BytesIO, container_name: str, blob_path: str
    ) -> bool:
        try:
            # Upload a blob to the container
            logging.debug(
                f"Uploading passed-in file object to container {container_name} at {blob_path}"
            )
            blob_client = self._blob_service_client.get_blob_client(
                container=container_name, blob=blob_path
            )
            blob_client.upload_blob(data)
            return True
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(f"{lines}")
            return False

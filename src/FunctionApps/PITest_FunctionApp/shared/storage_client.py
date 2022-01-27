import logging
import uuid
from io import BytesIO

from azure.storage.blob import BlobServiceClient

from .settings import StorageClientSettings


class PHDIStorageClient:
    def __init__(self, settings: StorageClientSettings):
        self._settings = settings
        self._blob_service_client = self._setup_blob_client()

    def _setup_blob_client(self) -> BlobServiceClient:
        logging.debug(f"Setting up blob client")
        if self._settings.connection_string is None:
            raise TypeError
        blob_service_client = BlobServiceClient.from_connection_string(
            self._settings.connection_string
        )
        return blob_service_client

    def create_container(self, name=None) -> None:
        if not name:
            name = str(uuid.uuid4())
        # Create the container
        logging.debug(f"Creating container {name}")
        self._blob_service_client.create_container(name)

    def list_blobs_in_container(self, container_name: str) -> None:
        container_client = self._blob_service_client.get_container_client(
            container_name
        )
        # List the blobs in the container
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            logging.debug("\t" + blob.name)

    def upload_data_to_blob(
        self, data: BytesIO, container_name: str, blob_path: str
    ) -> None:
        # Upload a blob to the container
        logging.debug(
            f"Uploading passed-in file object to container {container_name} at {blob_path}"
        )
        blob_client = self._blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )
        blob_client.upload_blob(data)

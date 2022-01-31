import os

import pysftp
import pytest
from shared.storage_client import PHDIStorageClient
from shared.settings import StorageClientSettings

# Fixtures run before each test and can be passed as arguments to individual tests to enable accessing the variables they define. More info: https://docs.pytest.org/en/latest/fixture.html#fixtures-scope-sharing-and-autouse-autouse-fixtures
@pytest.fixture(scope="session", autouse=True)
def initialize_env_vars():
    # get storage account settings
    storage_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.environ.get("STORAGE_CONTAINER")


    return storage_connection_string, container_name

@pytest.fixture(scope="session")
def storage_client(initialize_env_vars):
    storage_connection_string, _ = initialize_env_vars
    settings = StorageClientSettings()
    settings.connection_string = storage_connection_string
    storage_client = PHDIStorageClient(settings)
    yield storage_client


def test_create_container(storage_client):
    result = storage_client.create_container()
    assert result == True

def test_list_blobs(storage_client):
    storage_connection_string, container_name = initialize_env_vars 
    result = storage_client.list_blobs_in_container(container_name)
    assert result == []


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
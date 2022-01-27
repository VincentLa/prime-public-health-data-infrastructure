import os

import pysftp
import pytest
from shared.storage_client import PHDIStorageClient
from shared.settings import StorageClientSettings

# Fixtures run before each test and can be passed as arguments to individual tests to enable accessing the variables they define. More info: https://docs.pytest.org/en/latest/fixture.html#fixtures-scope-sharing-and-autouse-autouse-fixtures
@pytest.fixture
def initialize_env_vars():
    # get storage account settings
    storage_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.environ.get("STORAGE_CONTAINER")
    return storage_connection_string, container_name


@pytest.fixture(scope="session")
def storage_client(sftpserver):
    settings = StorageClientSettings()
    settings.connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    storage_client = PHDIStorageClient(settings)
    yield storage_client


def test_create_container(storage_client):
    container_name = "test-container1"
    result = storage_client.create_container(container_name)
    assert result == True

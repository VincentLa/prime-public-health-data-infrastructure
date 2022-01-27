import os

import pytest

import IngestFunction as ingest_function
import pysftp

# Fixtures run before each test and can be passed as arguments to individual tests to enable accessing the variables they define. More info: https://docs.pytest.org/en/latest/fixture.html#fixtures-scope-sharing-and-autouse-autouse-fixtures
@pytest.fixture
def initialize_env_vars():
    # get storage account settings
    storage_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.environ.get("STORAGE_CONTAINER")
    return storage_connection_string, container_name

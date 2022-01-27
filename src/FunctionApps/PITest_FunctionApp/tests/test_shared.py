import os

import pytest

from shared.sftp_client import SFTPClient
from shared.storage_client import StorageClient
from shared.settings import SFTPSettings, StorageClientSettings


@pytest.fixture(scope="session")
def sftp_client():
    settings = SFTPSettings()
    stfp_client = SFTPClient(settings)
    yield stfp_client
    stfp_client.teardown_connection()


# This makes use of two fixtures:
# sftpserver, provided by pytest-sftpserver : mimics an SFTP server using a python object, and
def test_get_tree(sftpserver, sftp_client):
    expected_tree_output = {
        "files": [
            "/VXU/file1.hl7",
            "/VXU/file2.hl7",
            "/ELR/file3.hl7",
            "/ELR/file4.hl7",
            "/eICR/file5.hl7",
            "/eICR/file6.hl7",
        ],
        "dirs": ["eICR", "ELR", "VXU"],
        "other": [],
    }
    # fmt: off
    mock_dir_structure = {
        "VXU": {
            "file1.hl7": b"test file contents",
            "file2.hl7": b"test file contents"
        },
        "ELR": {
            "file3.hl7": b"test file contents",
            "file4.hl7": b"test file contents"
        },
        "eICR": {
            "file5.hl7": b"test file contents",
            "file6.hl7": b"test file contents"
        }
    }
    # fmt: on
    with sftpserver.serve_content(mock_dir_structure):
        file_tree = sftp_client.get_tree(sftpserver.connection) == "File content"
        assert file_tree == expected_tree_output

import os

import pytest

from shared.sftp_client import PHDISFTPClient
from shared.storage_client import PHDIStorageClient
from shared.settings import SFTPSettings, StorageClientSettings


@pytest.fixture(scope="session")
def sftp_client(sftpserver):
    settings = SFTPSettings()
    settings.hostname = sftpserver.host
    settings.port = sftpserver.port
    settings.username = "testuser"
    settings.password = "testpassword"
    stfp_client = PHDISFTPClient(settings)
    yield stfp_client
    stfp_client.teardown_connection()


# @pytest.yield_fixture(scope="session")
# def sftp_client(sftpserver):
#     transport = Transport((sftpserver.host, sftpserver.port))
#     transport.connect(username="a", password="b")
#     sftpclient = SFTPClient.from_transport(transport)
#     yield sftpclient
#     sftpclient.close()
#     transport.close()


# This makes use of two fixtures:
# sftpserver, provided by pytest-sftpserver : mimics an SFTP server using a python object, and
def test_get_tree(sftpserver, sftp_client):
    expected_tree_output = {
        "files": set(
            [
                "/VXU/file1.hl7",
                "/VXU/file2.hl7",
                "/ELR/file3.hl7",
                "/ELR/file4.hl7",
                "/eICR/file5.hl7",
                "/eICR/file6.hl7",
            ]
        ),
        "dirs": set(["/eICR", "/ELR", "/VXU"]),
        "other": set([]),
    }
    # fmt: off
    mock_dir_structure = {
        "VXU": {
            "file1.hl7": "testfile1",
            "file2.hl7": "testfile2",
        },
        "ELR": {
            "file3.hl7": "testfile3",
            "file4.hl7": "testfile4",
        },
        "eICR": {
            "file5.hl7": "testfile5",
            "file6.hl7": "testfile6",
        }
    }
    # fmt: on
    with sftpserver.serve_content(mock_dir_structure):
        file_tree = sftp_client.get_tree()
        assert file_tree == expected_tree_output

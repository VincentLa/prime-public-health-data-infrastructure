import logging
import sys
from io import BytesIO
from pathlib import Path

import pysftp

from .settings import SFTPSettings
from .storage_client import PHDIStorageClient


class PHDISFTPClient:
    """
    A client of an SFTP
    """

    def __init__(self, settings: SFTPSettings):
        self._settings = settings
        self._sftp = self._setup_connection()

    def _setup_connection(self) -> pysftp.Connection:
        """
        Initialize the SFTP connection
        """
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        sftp = pysftp.Connection(
            self._settings.hostname,
            username=self._settings.username,
            password=self._settings.password,
            port=self._settings.port,
            cnopts=cnopts,
        )
        return sftp

    def get_tree(self, path: str = "/") -> dict:
        """
        Get a representation of the file tree from the SFTP server
        """
        file_names = []
        dir_names = []
        other_names = []

        def store_files(fname: str):
            file_names.append(fname)

        def store_dirs(dirname: str):
            dir_names.append(dirname)

        def store_other(name: str):
            other_names.append(name)

        self._sftp.walktree(path, store_files, store_dirs, store_other, recurse=True)
        return {
            "files": set(file_names),
            "dirs": set(dir_names),
            "other": set(other_names),
        }

    def get_file_as_bytes(self, path: str):
        """
        Get a file from the SFTP server in the form of a BytesIO object
        """
        logging.debug("Getting file info as blob")
        file_object = BytesIO()
        self._sftp.getfo(path, file_object)
        file_object.seek(0)
        return file_object

    def transmit_single_file_from_sftp_to_blob(
        self,
        sftp_file_path: str,
        blob_file_base_path: str,
        storage_client: PHDIStorageClient,
    ):
        """
        Transmit a single file from the SFTP server to the blob storage
        """
        file_obj = self.get_file_as_bytes(sftp_file_path)
        sftp_file_name = Path(sftp_file_path).name
        blob_file_path = Path(blob_file_base_path) / sftp_file_name

        logging.debug(
            f"Uploading {sftp_file_name} at path {sftp_file_path} to Azure Storage"
        )
        storage_client.upload_data_to_blob(file_obj, sftp_file_name, blob_file_path)

    def transmit_files_recursive_to_blob(
        self, storage_client: PHDIStorageClient, blob_base_path: str
    ) -> None:
        """
        Transmit all files in the SFTP server to the specified Azure Storage account and blob container with base path `base path`
        """

        def handle_file(file_path: str):
            logging.debug(f"Processing file {file_path}")
            self.transmit_single_file_from_sftp_to_blob(
                file_path, blob_base_path, storage_client
            )

        def handle_directory(dir_path: str):
            logging.debug(f"Processing directory {dir_path} (not copying)")

        def handle_other(name: str):
            logging.debug(f"Processing other {name} (not copying)")

        self._sftp.walktree(
            "/", handle_file, handle_directory, handle_other, recurse=True
        )

    def teardown_connection(self):
        """
        Close the SFTP connection
        """
        self._sftp.close()

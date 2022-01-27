import os
from dataclasses import dataclass


@dataclass
class StorageClientSettings:
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")


@dataclass
class SFTPSettings:
    hostname = os.getenv("VDHSFTPHostname")
    username = os.getenv("VDHSFTPUsername")
    password = os.getenv("VDHSFTPPassword")

import os

class Settings:
    hostname = os.environ.get("VDHSFTPHostname")
    username = os.environ.get("VDHSFTPUsername")
    password = os.environ.get("VDHSFTPPassword")
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

def __repr__(self):
    return repr(f"hostname: {self.hostname}, username: {self.username}, password: {self.password}, connection_string: {self.connection_string}")


settings = Settings()

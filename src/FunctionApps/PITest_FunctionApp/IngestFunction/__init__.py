import logging
import pysftp
import sys
import azure.functions as func
from .settings import settings
from azure.storage.blob import BlobServiceClient, ContainerClient
import uuid
from io import BytesIO
from pathlib import Path
import fnmatch

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def print_tree(sftp: pysftp.Connection, path: str):
    file_names = []
    dir_names = []
    other_names = []

    def store_files(fname: str):
        file_names.append(fname)

    def store_dirs(dirname: str):
        dir_names.append(dirname)

    def store_other(name: str):
        other_names.append(name)

    sftp.walktree(path, store_files, store_dirs, store_other, recurse=True)
    logging.info(
        f"File names: {file_names}\nDirectory names: {dir_names}\nOther names: {other_names}"
    )

def create_container_if_not_exists(container_name:str):
    if not settings.connection_string:
        exit(f"Connection string not set")

    # Create the container
    logging.info(f"Creating container {container_name}")

    container = ContainerClient.from_connection_string(settings.connection_string, container_name)
    if container.exists():
        logging.info("\nListing existing blobs...")

        # List the blobs in the container
        blob_list = container.list_blobs()
        for blob in blob_list:
            logging.info("\t" + blob.name)
    else:
        logging.info(f"{container_name} does not exist. Creating...")
        container.create_container()


def upload_blob_to_container(original_file_path: str, container_name: str, destination_prefix:str, data: BytesIO):
    if not settings.connection_string:
        exit(f"Connection string not set")
    blob_service_client = BlobServiceClient.from_connection_string(
        settings.connection_string
    )

    destination_path = f"{destination_prefix}{original_file_path}"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=destination_path)
    
    logging.info(
        "\nUploading passed-in blob to Azure Storage as blob:\n\t" + original_file_path
    )
    blob_client.upload_blob(data)

def test_copy_file(sftp: pysftp.Connection, path:str):
    target_file_name = Path(path).name
    file_object = BytesIO()
    sftp.getfo(path, file_object)
    file_object.seek(0)
    logging.info(
        f"Uploading {target_file_name} at path {path} Azure Storage"
    )
    upload_blob_to_container(target_file_name, "test_container", "prefix", file_object)
    logging.info("Upload succeeded")

def get_file_as_bytes(sftp: pysftp.Connection, path: str):
    """
    Get a file from the SFTP server in the form of a BytesIO object
    """
    logging.info("Getting file info as blob")
    file_object = BytesIO()
    sftp.getfo(path, file_object)
    file_object.seek(0)
    return file_object

def copy_files_recursively(sftp: pysftp.Connection, base_path: str):
    """
    Transmit all files in the SFTP server to the specified Azure Storage account and blob container with base path `base path`
    """

    container_name = "3d6cd2fa-61dc-4657-8938-6bedd4f13d53"
    destination_prefix = "/220128/"
    
    logging.info(f"Copying files from SFTP server to Azure Storage account and blob container {container_name}")

    def handle_file(file_path: str):
        logging.info(f"Processing file {file_path}")
        file_bytes = get_file_as_bytes(sftp, file_path)
        upload_blob_to_container(file_path, container_name, destination_prefix, file_bytes)
        
    def handle_directory(dir_path: str):
        logging.info(f"Processing directory {dir_path} (not copying)")

    def handle_other(name: str):
        logging.info(f"Processing other {name} (not copying)")
    
    sftp.walktree(
        base_path, handle_file, handle_directory, handle_other, recurse=True
    )
    logging.info("Complete.")

def create_test_dir(sftp: pysftp.Connection):
    logging.info("Creating and copying files to temporary dir...")
    test_dir_path = "/test_dir"
    if not sftp.exists(test_dir_path):
        logging.info(f"{test_dir_path} does not exist. Creating...")
        sftp.mkdir(test_dir_path)
    else:
        logging.info(f"{test_dir_path} exists.")

    eICR_files = sftp.listdir("/eICR")
    for file_name in eICR_files:
        
        if fnmatch.fnmatch(file_name, "zip_1_2_840_114350_1_13_198_2_7_8_688883_16098*.xml"):
            logging.info(f"Found match: {file_name}")
            file_path = f"/eICR/{file_name}"
            file_bytes = get_file_as_bytes(sftp, file_path)
            logging.info(f"Uploading file...")
            sftp.putfo(file_bytes, f"{test_dir_path}/{file_name}")
    test_dir_files = sftp.listdir(test_dir_path)
    logging.info(f"Test_dir files ({len(test_dir_files)}): {test_dir_files}")
    logging.info("Completed.")

def handle_file(sftp: pysftp.Connection, file_path: str) -> bool:
    logging.info(f"Processing file {file_path}")
    container_name = "3d6cd2fa-61dc-4657-8938-6bedd4f13d53"
    destination_prefix = "220128"
    file_path = f"/eICR/{file_path}"
    file_bytes = get_file_as_bytes(sftp, file_path)
    logging.info(f"Uploading file {file_path}...")
    upload_blob_to_container(file_path, container_name, destination_prefix, file_bytes)
    logging.info(f"Upload complete for file {file_path}.")
    return (file_path, True)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    logging.info(f"Settings: {settings}")

    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        sftp = pysftp.Connection(
            settings.hostname,
            username=settings.username,
            password=settings.password,
            cnopts=cnopts,
        )
        # logging.info("Top level directory listing:")
        # top_level = sftp.listdir("/")
        # logging.info(f"{top_level}")

        base_dir = "/eICR"
        base_dir_files = sftp.listdir(base_dir)
        logging.info(f"File count= {len(base_dir_files)}")
        files_to_copy = base_dir_files

        # Single File
        # file_name = base_dir_files[0]
        # handle_file(sftp, file_name) 
        # files_to_copy = base_dir_files[:10]

        # files_to_copy = ['zip_1_2_840_114350_1_13_198_2_7_8_688883_160962026_20211223222531.xml', 'zip_1_2_840_114350_1_13_198_2_7_8_688883_160962206_20211223222659.xml', 'zip_1_2_840_114350_1_13_198_2_7_8_688883_160974837_20211224024414.xml', 'zip_1_2_840_114350_1_13_198_2_7_8_688883_160974869_20211224024410.xml', 'zip_1_2_840_114350_1_13_198_2_7_8_688883_160974870_20211224024410.xml', 'zip_1_2_840_114350_1_13_198_2_7_8_688883_160974871_20211224024411.xml', 'zip_1_2_840_114350_1_13_198_2_7_8_688883_160974872_20211224024412.xml', 'zip_1_2_840_114350_1_13_198_2_7_8_688883_160974876_20211224024516.xml', 'zip_1_2_840_114350_1_13_198_2_7_8_688883_160975428_20211224025211.xml', 'zip_1_2_840_114350_1_13_198_2_7_8_688883_160976351_20211224030310.xml']


        # logger.info(f"Files to copy: {files_to_copy}")

        logger.info("Copying files via multiprocessing pool...")
        # with ThreadPool(processes=int(10)) as pool:
        #     result = pool.map(partial(handle_file, sftp), files_to_copy)
        # logging.info(f"Multiprocessing finished. Result: {list(result)}")

        # with ProcessPoolExecutor() as executor:
        #     future = executor.submit(handle_file, sftp, files_to_copy)
        #     print(future.result())

        with ThreadPoolExecutor() as executor:
            result_futures = map(lambda x: executor.submit(partial(handle_file, sftp), x), files_to_copy)
            for future in as_completed(result_futures):
                try:
                    (file_path, success) = future.result()
                    logger.info(f"File {file_path} processed. Success: {success}")
                except Exception as e:
                    logger.info(f"Encountered exception: {e}, {type(e)}")

        # with ThreadPoolExecutor() as executor:
        #     results = list(executor.map(partial(handle_file, sftp), 1235))
            
        # with ThreadPool(processes=int(10)) as pool:
        #     result = pool.map(partial(handle_file, sftp), files_to_copy)
        # pool.close()
        # pool.join()

        logger.info(f"Multiprocessing finished.")
        return func.HttpResponse(f"This HTTP triggered function executed successfully.")
    except:
        e = sys.exc_info()
        logging.error(f"Exception: {e}, Traceback: {e[2]}")
        return func.HttpResponse(f"Error in response: {e}", status_code=500)

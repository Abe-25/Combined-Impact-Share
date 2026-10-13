import yaml
import paramiko
import io, sys
from zipfile import ZipFile
import json
import argparse
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.absolute()))

import logging
import logging.config
logger = logging.getLogger(__name__)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.basicConfig(format='%(levelname)s:%(name)s:%(message)s', level=logging.INFO)

# from datetime import datetime, timedelta
from stat import S_ISDIR
# import email_sendgrid as sg
import sendgrid as sg
from smart_open import open
from azure.storage.blob import BlobServiceClient

from utils.ay_akv import AyAzureKeyVault
from utils.ay_email import AYEmail



parser = argparse.ArgumentParser()
parser.add_argument('--env', dest='env', type=str, help='Project Environment')
parser.add_argument('--run_date', dest='run_date', type=str, help='Run Date')
args = parser.parse_args()

if args.env.lower() not in ['dev', 'prd']:
    raise Exception(f"{args.env.lower()} is not a valid environment. Valid options are only dev, uat, and prod.")

## Read YAML file
with open(f'{Path(__file__).parent.absolute()}/config.yml', 'r') as stream:
    etl_configuration = yaml.safe_load(stream)[args.env.lower()]

## Setting up Key Vault Access 
key_vault_name = etl_configuration['key_vault']
azureKV = AyAzureKeyVault()
vault_uri = f'https://{key_vault_name}.vault.azure.net/'


## Setting up Azure Blob Storage Access and Configuration
az_blob_secret_name = etl_configuration['blob_storage']['secret_name']
container_name = etl_configuration['blob_storage']['container_name']
folder_prefix = etl_configuration['blob_storage']['folder_prefix']

connection_string = azureKV.get_secret(vault_uri, az_blob_secret_name)
blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)


## Setting up SFTP Access and configuration
sftp_secret_name = etl_configuration['sftp']['secret_name']
sftp_folder_path = etl_configuration['sftp']['folder_path'] # this must be a directory
sftp_file_names = etl_configuration['sftp']['file_names']

sftp_dict = json.loads(azureKV.get_secret(vault_uri, sftp_secret_name))
host = sftp_dict['host']
username = sftp_dict['username']
password = sftp_dict['password']

ssh_client = paramiko.SSHClient()
ssh_client.load_system_host_keys()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=host, username=username, allow_agent=False, password=password)

## Setting up Email Access and configuration
email_secret_name = etl_configuration['email']['secret_name']
email_to_address = etl_configuration['email']['to_address']
email_from_address = etl_configuration['email']['from_address']

# email_client = AYEmail(key_vault_name=key_vault_name, secret_name=email_secret_name)


# create a function that takes the sftp connection and a path and a blob container client
# create a directory in blob storage for the sftp_folder_path
# for each of the files in the download the file to blob storage
# if the current path is a directory then call the recursive function

# the endpoint for the sftp start is the directory plus a varable for the date in YYYY-MM-DD format.

def copy_sftp_folder(sftp, folder_path, folder_name = None, blob_filepaths = []):
    """
    Loads the latest files from an SFTP folder to an Azure Blob Storage folder
    """    
    if folder_name != None:
        folder_name = folder_name + '/' + folder_path.split('/')[-1]
    else:
        folder_name = folder_prefix

    for sftp_file_name in sftp.listdir(folder_path):
        if sftp_file_name in sftp_file_names:
            full_path_file_name = folder_path+"/"+sftp_file_name
            logging.info(f'Retrieving STFP file {full_path_file_name}.')
            mode = sftp.stat(full_path_file_name).st_mode
        
            if S_ISDIR(mode): # the path is to a directory
                # Call the function to get the files in the sub directory
                logging.warn(f'File {full_path_file_name} is a directory')
                copy_sftp_folder(sftp, full_path_file_name, folder_name, blob_filepaths)
            else:
                # need to get the file name using split(?) function on the path
                # blob_file_name = full_path_file_name.split("/")[-1]
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{folder_name}/{args.run_date}/{sftp_file_name}")
                logging.info(f"Uploading STFP file {full_path_file_name} to Azure Blob Storage {container_name}/{folder_name}/{args.run_date}/{sftp_file_name}")
        
                # logging.disable = True

                with io.BytesIO() as flo:            
                    sftp.getfo(full_path_file_name, flo, callback=None)
                    flo.seek(0)
                    blob_client.upload_blob(flo, overwrite=True, encoding='utf-16', timeout = 100000)
                
                # logging.disable = False
                
                blob_filepaths.append(f"{folder_name}/{args.run_date}/{sftp_file_name}")
    
    return blob_filepaths

def unzip(blob_filepath):
    tgt_blob = blob_service_client.get_blob_client(container=container_name, blob=blob_filepath)
    zip = tgt_blob.download_blob()

    tounzip = zip.readall()

    zf = ZipFile( io.BytesIO(tounzip), 'r' )
    logging.info(f'List of files contained in {blob_filepath}: {zf.namelist()}')

    for filename in zf.namelist():
        # if filename.endswith('.txt'):
        # logging.disable = True
        upl_blob = blob_service_client.get_blob_client(container=container_name,  blob=f"{blob_filepath}/{filename}") #f"{'/'.join(blob_filepath.split('/')[:-1])}/{filename}")
        with open(zf.open(filename), "rb") as data:
            upl_blob.upload_blob(data, overwrite=True, encoding='utf-16')
        # logging.disable = False
        logging.info(f'Extracted {filename} from {blob_filepath}')
    # remove_blob = blob_service_client.get_blob_client('costar', 'inbox/AvisonYoung.zip')
    # remove_blob.delete_blob()
    tgt_blob.delete_blob()

# def send_mail(subject,html_content):
#     """
#     Sends an email using sendgrid
#     """
#     # subject=subject
#     # to_address="dataarchitectureteam@avisonyoung.com"
#     # from_address="astronomer@avisonyoung.com"
#     # print(sg.send_html_mail(to_address, from_address, subject, html_content))


if __name__ == '__main__':
    logging.info(f'Running COSTAR INGESTION in {args.env.upper()} for date {args.run_date}')
    with ssh_client.open_sftp() as sftp:
        try:
            mode = sftp.stat(sftp_folder_path).st_mode
            if S_ISDIR(mode): # the path is to a directory
                sftp.chdir(sftp_folder_path)
                blob_filepaths = copy_sftp_folder(sftp, sftp_folder_path)
                for blob_filepath in blob_filepaths:
                    unzip(blob_filepath)
                    # html_content=f"Directory {sftp_folder_path} Uploaded to blob"
                    # subject="Costar file pull COMPLETED"
                    # sg.send_mail(subject,html_content)
            else:
                print("Starting path is not a directory")
                # html_content="Starting path is not a directory"
                # subject="Costar Ingestion ERROR"
                # sg.send_mail(subject,html_content)
        except FileNotFoundError:
                print("Starting path does not Exist")
                # html_content="Starting path does not Exist"
                # subject="Costar Ingestion IO-ERROR"
                # sg.send_mail(subject,html_content)

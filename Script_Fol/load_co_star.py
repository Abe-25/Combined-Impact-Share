import yaml
import sys
import argparse
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.absolute()))

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s:%(name)s:%(message)s', level=logging.INFO)

from azure.storage.blob import BlobServiceClient

from utils.snowflake.ay_loader import AYSnowflakeLoader
from utils.ay_akv import AyAzureKeyVault



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


# az_blob_secret_name = etl_configuration['blob_storage']['secret_name']
# container_name = etl_configuration['blob_storage']['container_name']
# folder_prefix = etl_configuration['blob_storage']['folder_prefix']

# connection_string = azureKV.get_secret(vault_uri, az_blob_secret_name)
# blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)


if __name__ == '__main__':
    logging.info(f'Running COSTAR SNOWFLAKE LOADING in {args.env.upper()} for date {args.run_date}')
    for snowflake_configuration in etl_configuration['snowflake']:
        
        snowflake_account = snowflake_configuration['account']
        snowflake_user = snowflake_configuration['user']
        snowflake_role = snowflake_configuration['role']
        snowflake_warehouse = snowflake_configuration['warehouse']
        snowflake_database = snowflake_configuration['database']
        snowflake_schema = snowflake_configuration['schema']

        snowflake_private_key = azureKV.get_secret(vault_uri, snowflake_configuration['private_key']['secret_name']).encode("utf-8")

        with AYSnowflakeLoader(
            snowflake_account = snowflake_account,
            snowflake_user = snowflake_user,
            snowflake_role = snowflake_role,
            snowflake_private_key = snowflake_private_key,
            snowflake_warehouse = snowflake_warehouse,
            snowflake_database = snowflake_database,
            snowflake_schema = snowflake_schema
        ) as snowflake_conn:
            for snowflake_table in snowflake_configuration['tables']:

                ## Truncate Table First
                truncate_result = snowflake_conn.truncate(
                    snowflake_tablename = snowflake_table['name']
                )
                print(truncate_result)
                
                # Run Copy Into Statement
                load_result = snowflake_conn.load_csv(
                    snowflake_tablename = snowflake_table['name'],
                    snowflake_stage = snowflake_configuration['stage'],
                    snowflake_copy_pattern = (snowflake_table['blob_path']).format(DATE=args.run_date),
                    snowflake_copy_file_format = snowflake_table['file_format'],
                    snowflake_copy_on_error = snowflake_table['on_error']
                )
                print(f"Load {snowflake_table} Complete: {load_result}")
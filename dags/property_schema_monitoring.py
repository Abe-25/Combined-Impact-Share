import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

METADATA_DIR = os.getenv('METADATA_DIR', '/data/metadata')
METADATA_FILE = os.path.join(METADATA_DIR, 'property_metadata.json')

def schema_validation():
    target_file = '/data/new_data.csv'
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        old_schema = metadata.get('columns') or list(metadata.get('properties', {}).keys()) or list(metadata.keys())
        target_file = metadata.get('incoming_dataset', target_file)
    else:
        old_schema = ['Property_ID', 'City', 'Property_Type', 'Asset_Class', 'Area_SqFt', 'Price_INR', 'Status', 'Bedrooms', 'Bathrooms']

    new_df = pd.read_csv(target_file)
    new_columns = set(new_df.columns) - set(old_schema)
    dropped_columns = set(old_schema) - set(new_df.columns)

    print(f'New Columns Detected: {new_columns}')
    if dropped_columns:
        print(f'Dropped Columns Detected: {dropped_columns}')

with DAG(
    dag_id='property_schema_monitoring',
    start_date=datetime(2025,1,1),
    schedule='@daily',
    catchup=False
) as dag:
    validate_schema=PythonOperator(task_id='schema_validation', python_callable=schema_validation)

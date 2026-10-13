import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

METADATA_DIR = os.getenv('METADATA_DIR', '/data/metadata')
METADATA_FILE = os.path.join(METADATA_DIR, 'property_metadata.json')

def load_property_data():
    file_path = '/data/property_data.csv'
    expected_columns = None
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        file_path = metadata.get('source_dataset', file_path)
        expected_columns = metadata.get('columns')

    df = pd.read_csv(file_path)
    if expected_columns:
        df = df[[col for col in expected_columns if col in df.columns]]
    print(f"Loaded {len(df)} records from {file_path}")

with DAG(
    dag_id='property_initial_load',
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
) as dag:
    load_task = PythonOperator(task_id='load_property_data', python_callable=load_property_data)

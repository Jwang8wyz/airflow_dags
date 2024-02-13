import os
from datetime import datetime, timedelta
import logging

# Create a directory for logs based on the current date
current_date = datetime.now().strftime("%Y-%m-%d")
log_directory = f'/elt/logs/jwang_app_log/{current_date}' 
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging to create a dynamic file name including the current date
log_file_name = f'{log_directory}/LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY_{current_date}.log'
logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file_name),
                              logging.StreamHandler()])

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import time
import pandas as pd
import pendulum
import sys

local_tz = pendulum.timezone("America/New_York") #EST set for airflow

def wait_for_file_update(file_path, timeout=1200, poke_interval=30, **kwargs):
    start_time = time.time()
    while True:
        try:
            mod_time = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
            current_date = datetime.now().date()
            if mod_time == current_date:
                logging.info(f"File {file_path} has been updated today.")  # This won't be logged due to logging level
                return True
        except Exception as e:
            logging.error(f"Error checking file modification date: {e}")
       
        time.sleep(poke_interval)
        if (time.time() - start_time) > timeout:
            raise ValueError(f"Timeout: File {file_path} was not updated within the specified timeout period.")

# Define the DAG
default_args = {
    'owner': 'jwang8',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'JWANG_LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY',
    default_args=default_args,
    description='DAG for Warehouse Inventory Updates',
    schedule_interval='0 9 * * *',
    catchup=False,
)

file_path = '/mnt/Reports/FreedomSFTP/GW_Inv/csv_inventory_availability.csv'

wait_for_file = PythonOperator(
    task_id='wait_for_file_update',
    python_callable=wait_for_file_update,
    op_kwargs={'file_path': file_path},
    dag=dag,
)

bash_command = "/home/jwang/jwang8_test/venv/SF_connection/bin/python3 /elt/py_pipeline/LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY.py"

snowflake_ops_bash = BashOperator(
    task_id='snowflake_operations',
    bash_command=bash_command,
    dag=dag,
)

wait_for_file >> snowflake_ops_bash
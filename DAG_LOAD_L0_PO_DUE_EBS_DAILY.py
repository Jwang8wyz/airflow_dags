
import os
from datetime import datetime, timedelta, date
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import time
import pandas as pd
import pendulum
import sys

                              
# custom package is in '/elt/PyPkg/'
sys.path.append('/elt/PyPkg/')
# Import the custom utility functions

from logger_setup import LoggerSetup
from cp_file_operater import direct_copy_file

# logger setup
Logger_LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY = LoggerSetup(app_name='LND_L0_PO_DUE_EBS_DAILY',log_level=logging.ERROR)
Logger_LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY.configure_logging()

# EST set for airflow
local_tz = pendulum.timezone("America/Toronto")
start_date = pendulum.datetime(2024, 3, 28, tz=local_tz)

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
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'JWANG8_LOAD_LND_INT_L0_PO_DUE_EBS_DAILY',
    default_args=default_args,
    description='DAG for PO_DUE_EBS_DAILY Updates',
    schedule_interval='30 8 * * *',
    #timezone=local_tz,
    catchup=False,
)

# datetime object containing current date and time
now = datetime.now()
Date = (date.today() - timedelta(days=0)).strftime('%Y%m%d')

# Move File from Share drive to Local
file_path = '/mnt1/JaysonReports/PO_Due/PO_QTY_DUE_Report_CSV_' + Date + '.csv'

wait_for_file = PythonOperator(
    task_id='wait_for_file_update',
    python_callable=wait_for_file_update,
    op_kwargs={'file_path': file_path},
    dag=dag,
)

#bash_command = "/elt/venv/SnowflakeETL/bin/python3 /elt/py_pipeline/LOAD_L0_WAREHOUSE_INVENTORY_EBS_DAILY.py"
bash_command = "source /elt/venv/SnowflakeETL/bin/activate && python3 /elt/py_pipeline/LOAD_L0_PO_DUE_EBS_DAILY.py"

snowflake_ops_bash = BashOperator(
    task_id='snowflake_operations',
    bash_command=bash_command,
    dag=dag,
)


wait_for_file >> snowflake_ops_bash 
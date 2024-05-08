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
Logger_LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY = LoggerSetup(app_name='LND_L0_CP_STATUS_APT',log_level=logging.ERROR)
Logger_LND_L0_WAREHOUSE_INVENTORY_EBS_DAILY.configure_logging()

# EST set for airflow
local_tz = pendulum.timezone("America/Toronto")
start_date = pendulum.datetime(2024, 5, 7, tz=local_tz)

#####################################################################################################################
#####################################################################################################################
#####################################################################################################################

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
    'JWANG8_LOAD_LND_INT_L0_CP_STATUS_APT',
    default_args=default_args,
    description='DAG for L0_CP_STATUS_APT Updates',
    schedule_interval='05 8 * * *',
    #timezone=local_tz,
    catchup=False,
)

# datetime object containing current date and time
now = datetime.now()
Date = (date.today() - timedelta(days=0)).strftime('%Y%m%d')


bash_command_download_txt_files = "source /elt/venv/SnowflakeETL/bin/activate && python3 /elt/py_pipeline/DownLoad_CPStatusAPT_FromCPSFTP.py"

download_txt_files = BashOperator(
    task_id='download_txt_files',
    bash_command=bash_command_download_txt_files,
    dag=dag,
)


#bash_command = "/elt/venv/SnowflakeETL/bin/python3 /elt/py_pipeline/LOAD_L0_WAREHOUSE_INVENTORY_EBS_DAILY.py"
bash_command = "source /elt/venv/SnowflakeETL/bin/activate && python3 /elt/py_pipeline/LOAD_L0_CP_STATUS_APT.py"

snowflake_ops_bash = BashOperator(
    task_id='snowflake_operations',
    bash_command=bash_command,
    dag=dag,
)


download_txt_files >> snowflake_ops_bash 
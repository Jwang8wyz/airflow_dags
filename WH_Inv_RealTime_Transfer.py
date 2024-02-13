from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'jwang8',
    'start_date': datetime(2023, 7, 26),  # Update the date to your desired start date
}

dag = DAG('WH_Inv_RealTime_Transfer',
          default_args=default_args,
          description='WH_Inv_RealTime_Transfer',
          schedule_interval='*/30 * * * *',
	  catchup=False,)

t1 = BashOperator(
    task_id='WH_Inv_RealTime_Transfer',
    bash_command='sudo python3 /mnt/Reports/VM/VM_PyFile/Move_CSV_WH_INV_FiveMin.py',  # Update this to the path of your script
    dag=dag,
)

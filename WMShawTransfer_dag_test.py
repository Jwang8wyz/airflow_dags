from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import pendulum

local_tz = pendulum.timezone("America/Toronto")

default_args = {
    'owner': 'jwang8',
    'start_date': datetime(2023, 7, 19, tzinfo=local_tz ),  # Update the date to your desired start date
}

dag = DAG('my_dag_test_2',
          default_args=default_args,
          description='Transfer_OSLShawFile',
          schedule_interval='@hourly') # '40 20 * * *',)

t1 = BashOperator(
    task_id='run_Test_Transfer_OSLShawFile',
    bash_command='sudo python3 /mnt/VM_Test/VM_Test_Py_File/Test_Transfer_OSLShawFile.py',  # Update this to the path of your script
    dag=dag,
)

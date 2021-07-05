from airflow import DAG
from airflow.operators.dummy import DummyOperator # A operator that do nothing
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain, cross_downstream

from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

default_args = {
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=5),
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'email': 'yagoaraujogarcia@gmail.com'
}

def _dowloading_data(ti, **kwargs):
    with open('/tmp/my_file.txt', 'w') as my_file:
        my_file.write('my_date')
    my_file.close()
    ti.xcom_push(key='my_key', value=42)

def _checking_data(**kwargs):
    ti = kwargs['ti']
    my_xcom = ti.xcom_pull(key='my_key', task_ids=['dowloading_data'])
    print(f'check data {my_xcom}')

def _failure(context):
    print("On callback failure")
    print(context)

with DAG(dag_id='sample_dag', default_args=default_args, schedule_interval="@daily", 
    start_date=days_ago(3), catchup=True) as dags:
    
    dowloading_data = PythonOperator(
        task_id='dowloading_data',
        python_callable=_dowloading_data
    )

    checking_data = PythonOperator(
        task_id='checking_data',
        python_callable=_checking_data
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt'
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 0',
        on_failure_callback=_failure
    )

    chain(dowloading_data, checking_data, waiting_for_data, processing_data)
    
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import time
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 23),
    'email': ['rajnikant.rakesh@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


with DAG('DAG_GCP_INSURANCE_MART_LOAD', schedule_interval=None, catchup=False, default_args=default_args) as dag:
     
     t1 = BashOperator(
     task_id='load_insurance_mart',
     bash_command='echo "Hello Insurance Mart.."'
     )
     
  

t1

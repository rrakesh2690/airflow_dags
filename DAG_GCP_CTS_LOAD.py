from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 10),
    'email': ['rajnikant.rakesh@norcal.aaa.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

schedule_interval = "30 08 * * *"

with DAG('DAG_GCP_CTS_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_GCP_CTS_BIGQUERY',
        bash_command='python /home/airflow/gcs/data/GCPDWH/cts/load_cts_to_bigquery_landing_dataflow.py --config config.properties --productconfig cts.properties --env prod  --connectionprefix d3 --incrementaldate 4 --deposition replace')
    

t1 
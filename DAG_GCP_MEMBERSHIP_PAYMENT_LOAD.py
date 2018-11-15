from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
                              
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12),
    'email': ['aarzoo.malik@mavenwave.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

with DAG('DAG_GCP_MEMBERSHIP_PAYMENT_LOAD', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:

      t1 = BashOperator(
        task_id='T1_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/process_membership_payment.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev"')
      
t1 
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
                              
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 20),
    'email': ['aarzoo.malik@mavenwave.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

with DAG('DAG_GCP_ERS_LOAD', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:

      t1 = BashOperator(
        task_id='T1_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/gig/process_ers.py --config "config.properties" --productconfig "ers.properties" --env "dev"')
      
      print "point 1"
t1
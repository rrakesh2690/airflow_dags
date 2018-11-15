from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
                              
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 17),
    'email': ['atul.guleria@mavenwave.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

schedule_interval='30 13 * * *'

with DAG('DAG_GCP_PCOMP_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
      t1 = BashOperator(
        task_id='T1_GCP_MOVE',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config "config.properties" --productconfig "pcomp.properties" --env "prod"')
      
      print "point 1"
      t2 = BashOperator(
        task_id='T2_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/pcomp/process_pcomp.py --config "config.properties" --productconfig "pcomp.properties" --env "prod"' )

t1 >> t2

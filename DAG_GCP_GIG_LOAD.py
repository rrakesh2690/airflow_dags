from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
                              
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 30),
    'email': ['aarzoo.malik@mavenwave.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

with DAG('DAG_GCP_GIG_LOAD', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:

#     t1 = BashOperator(
#       task_id='T1_COPY_TO_GCS',
#       bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config "config.properties" --productconfig "gig.properties" --env "dev"')

	   # print "point 1"

    t2 = BashOperator(
        task_id='T2_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/gig/process_gig.py --config "config.properties" --productconfig "gig.properties" --env "dev"')
      
#     print "point 2"
      
#     t3 = BashOperator(
#       task_id='T3_GCP_MOVE',
#       bash_command='gsutil mv gs://dw-dev-gig/current/* gs://dw-dev-gig/archive/'
#      )
      # print "point 3"
	   # t1
# t1 >> t2 >> t3
    t2
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 30),
    'email': ['rajnikant.rakesh@mavenwave.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('DAG_GCP_TST_LOAD', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:
     t1 = BashOperator(
     task_id='T1_COPY_TO_GCS',
     bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config "config.properties" --productconfig "travelware.properties" --env "prod"'
     )

     t2 = BashOperator(
        task_id='T2_GCS_TO_BQ',
        bash_command='python /home/airflow/gcs/data/GCPDWH/travelware/tst_load.py --config "config.properties" --productconfig "travelware.properties" --env "prod" --input gs://dw-prod-tst/current/AAANCNU_Data_datetime.datetime.now().strftime("%Y%m%d")* --separator "|" --stripheader "1" --stripdelim "0" --addaudit "1" --output "TRAVEL_TST_LDG" --writeDeposition "WRITE_APPEND"')

     t3 = BashOperator(
        task_id='T3_GCP_MOVE',
        bash_command='gsutil mv gs://dw-prod-tst/current/AAANCNU_Data_datetime.datetime.now().strftime("%Y%m%d")* gs://dw-prod-tst/archive/'
     )

     t1 >> t2 >> t3
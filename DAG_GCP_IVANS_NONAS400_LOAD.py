from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 24),
    'email': ['rajnikant.rakesh@mavenwave.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

batchdate = time.strftime("%Y%m%d")
filename = "gs://dw-dev-insurance/ivans/current/IE_NCNU_" + batchdate + ".DAT"
schedule_interval = "30 19 * * *"

with DAG('DAG_GCP_IVANS_IE_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
#     t1 = BashOperator(
#        task_id='T1_COPY_TO_GCS',
#        bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config "config.properties" --productconfig "ivans.properties" --env "dev"'
#     )

    t2 = BashOperator(
        task_id='T2_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/ivans/load_ie_segments_to_bq_dataflow.py --config "config.properties" --productconfig "ivans.properties" --env "dev" --separator "|" --stripheader "0" --stripdelim "0" --addaudit "1" --writeDeposition "WRITE_APPEND" --system "IE" --input "gs://dw-dev-insurance/ivans/current/IE_NCNU_20180925.DAT"')

#     t3 = BashOperator(
#         task_id='T3_GCP_MOVE',
#         bash_command='gsutil mv gs://dw-dev-insurance/ivans/current/* gs://dw-dev-insurance/ivans/archive/'
#     )

#     t1 >> t2 >> t3
    t2 
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 11),
    'email': ['rajnikant.rakesh@norcal.aaa.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

batchdate =  (datetime.today() - timedelta(days=1)).strftime("%Y%m%d") + "."+datetime.today().strftime("%m.%d.%Y")
print batchdate
filename = "gs://dw-prod-tst/current/AAANCNU_Data_" + batchdate + ".psv"
print filename

schedule_interval = "30 12 * * *"


with DAG('DAG_GCP_TRAVEL_TST_LOAD', schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
     t1 = BashOperator(
     task_id='T1_COPY_TO_GCS',
     bash_command='python /home/airflow/gcs/data/GCPDWH/util/transfer_mountpoint_to_gcs.py --config "config.properties" --productconfig "travelware.properties" --env "prod"'
     )

     t2 = BashOperator(
        task_id='T2_TST_BIGQUERY_LANDING',
        bash_command='python /home/airflow/gcs/data/GCPDWH/travelware/load_travel_tst_to_bigquery_landing.py --config config.properties --productconfig travelware.properties --env prod --input '+ filename + ' --separator "|" --stripheader 1 --stripdelim 0 --addaudit 1 --output TRAVEL_TST_LDG --writeDeposition WRITE_APPEND')

     t1 >> t2 
   
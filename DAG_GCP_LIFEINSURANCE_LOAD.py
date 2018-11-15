from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 13),
    'email': ['atul.guleria@mavenwave.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
 
with DAG('DAG_GCP_LIFEINSURANCE_LOAD', schedule_interval= '@daily', catchup=False, default_args=default_args) as dag:
 
    # task populates LIFE_INS_CSAPRDLY_DAILY_STG .
    t1 = BashOperator(
        task_id='T1_GCP_Load',
        bash_command='python /home/airflow/gcs/data/cloud_sdk/GCPDWH/util/loadcsvtobq.py --config "config.properties" --productconfig "lifeinsurance.properties" --env "dev"  --targettable "LIFE_INS_CSAPRDLY_DAILY_STG" --filename "CSAPRDLY.CSV.031618041023.PGP.dec"')
 
	
	# task  populates LIFE_INS_CSAPRDLY_DAILY .
    t2 = BashOperator(
        task_id='T2_GCP_Load',
        bash_command='python /home/airflow/gcs/data/cloud_sdk/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "lifeinsurance.properties" --env "dev"  --sqlfile  "lifeinsurance\loadcsaadailytable.sql"')
 
    
    # Define the DAG structure.
    t1 >> t2
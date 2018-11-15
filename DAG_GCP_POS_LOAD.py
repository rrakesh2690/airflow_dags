from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 20),
    'email': ['rajnikant.rakesh@mavenwave.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('DAG_GCP_POS_LOAD', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:

    t1 = BashOperator(
        task_id='T1_GCP_DB_TO_WORK_POS_CUSTOMER_PAYMENTS_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/connectsuite/load_from_mssql_to_bigquery.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --targettable "WORK_POS_CUSTOMER_PAYMENTS" --sqlquery "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/pos/work_pos_customer_payments_load.sql" --connectionprefix "cs"')
    
    t2 = BashOperator(
        task_id='T2_GCP_WORK_SAM_TO_LDG_LOAD',
        bash_command='python /home/airflow/gcs/data/cloud_sdk/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev"  --sqlfile "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/pos/pos_customer_payments_load.sql"')

    t3 = BashOperator(
        task_id='T3_GCP_DB_TO_WORK_POS_CUSTOMER_RECIEPT_LINE_ITEM_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/connectsuite/load_from_mssql_to_bigquery.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --targettable "WORK_POS_CUSTOMER_RECIEPT_LINE_ITEMS" --sqlquery "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/pos/work_pos_customer_reciept_line_item_load.sql" --connectionprefix "cs"')
    
    t4 = BashOperator(
        task_id='T4_GCP_WORK_SAM_CUSTOMER_ADDRESS_TO_lDG_LOAD',
        bash_command='python /home/airflow/gcs/data/cloud_sdk/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev"  --sqlfile "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/pos/pos_customer_reciept_line_item_load.sql"')

    t1 >> t2 
    t3 >> t4
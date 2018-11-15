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

with DAG('DAG_GCP_MEMBERSHIP_LOAD', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:
     t1 = BashOperator(
     task_id='T1_DB_TO_WORK_CS_MEMBER_DIM_LOAD',
     bash_command='python /home/airflow/gcs/data/GCPDWH/connectsuite/load_from_mssql_to_bigquery.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --targettable "WORK_CS_MEMBER_DIM" --sqlquery "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/work_cs_member_dim.sql" --connectionprefix "cs"'
     )

     t2 = BashOperator(
        task_id='T2_WORK_CS_MEMBER_DIM_TO_MAIN_TABLE_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --sqlfile "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/connectsuite_member_dim.sql"')

     t3 = BashOperator(
        task_id='T3_DB_TO_WORK_CS_MEMBERSHIP_FACT_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/connectsuite/load_from_mssql_to_bigquery.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --targettable "WORK_CS_MEMBERSHIP_FACT" --sqlquery "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/work_cs_membership_fact.sql" --connectionprefix "cs"'
     )
     
     t4 = BashOperator(
        task_id='T4_WORK_CS_MEMBERSHIP_FACT_TO_MAIN_TABLE_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --sqlfile "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/connectsuite_membership_fact.sql"')

     t5 = BashOperator(
        task_id='T5_DB_TO_WORK_CS_MEMBER_TRANSACTIONS_FACT_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/connectsuite/load_from_mssql_to_bigquery.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --targettable "WORK_CS_MEMBER_TRANSACTIONS_FACT" --sqlquery "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/work_cs_member_transactions_fact.sql" --connectionprefix "cs"'
     )
     
     t6 = BashOperator(
        task_id='T6_WORK_CS_MEMBER_TRANSACTIONS_FACT_TO_MAIN_TABLE_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --sqlfile "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/connectsuite_member_transactions_fact.sql"')
     
     t7 = BashOperator(
        task_id='T7_DB_TO_WORK_CS_MEMBERSHIP_CUSTOMER_DIM_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/connectsuite/load_from_mssql_to_bigquery.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --targettable "WORK_CS_MEMBERSHIP_CUSTOMER_DIM" --sqlquery "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/work_cs_membership_customer_dim.sql" --connectionprefix "cs"'
     )
     
     t8 = BashOperator(
        task_id='T8_WORK_CS_MEMBERSHIP_CUSTOMER_DIM_TO_MAIN_TABLE_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --sqlfile "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/membership_customer_dim.sql"')
     
     t9 = BashOperator(
        task_id='T9_LOAD_CUSTOMER_CONTACT_PREFERENCES',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "connectsuite.properties" --env "dev" --sqlfile "/home/airflow/gcs/data/GCPDWH/connectsuite/sql/customer_contact_preferences.sql"')
     
     t1 >> t2 
     t3 >> t4
     t5 >> t6
     t7 >> t8 >> t9
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

with DAG('DAG_GCP_MSI_LOAD', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:

      t1 = BashOperator(
        task_id='T1_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/msi/process_msi.py --config "config.properties" --productconfig "msi.properties" --env "dev"')
      
      
      t2 = BashOperator(
        task_id='T2_GCP_MOVE',
        bash_command='gsutil mv gs://dw-dev-insurance/msi/commlog/current/* gs://dw-dev-insurance/msi/commlog/archive/'
       )

      t3 = BashOperator(
        task_id='T3_GCP_MOVE',
        bash_command='gsutil mv gs://dw-dev-insurance/msi/erssurvey/current/* gs://dw-dev-insurance/msi/erssurvey/archive/'
       )

      t4 = BashOperator(
        task_id='T4_GCP_MOVE',
        bash_command='gsutil mv gs://dw-dev-insurance/msi/mactriptik/current/* gs://dw-dev-insurance/msi/mactriptik/archive/'
       )

      t5 = BashOperator(
        task_id='T5_GCP_MOVE',
        bash_command='gsutil mv gs://dw-dev-insurance/msi/msiers/current/* gs://dw-dev-insurance/msi/msiers/archive/'
       )

      t6 = BashOperator(
        task_id='T6_GCP_MOVE',
        bash_command='gsutil mv gs://dw-dev-insurance/msi/napa/current/* gs://dw-dev-insurance/msi/napa/archive/'
       )

      t7 = BashOperator(
        task_id='T7_GCP_MOVE',
        bash_command='gsutil mv gs://dw-dev-insurance/msi/skipfile/current/* gs://dw-dev-insurance/msi/skipfile/archive/'
       )

      t8 = BashOperator(
        task_id='T8_GCP_MOVE',
        bash_command='gsutil mv gs://dw-dev-insurance/msi/vendnovationkiosk/current/* gs://dw-dev-insurance/msi/vendnovationkiosk/archive/'
       )	   

t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t1)
t5.set_upstream(t1)
t6.set_upstream(t1)
t7.set_upstream(t1)
t8.set_upstream(t1)
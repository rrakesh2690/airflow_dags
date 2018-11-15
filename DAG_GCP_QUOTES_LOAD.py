from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 8),
    'email': ['rajnikant.rakesh@mavenwave.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

 
with DAG('DAG_GCP_QUOTES_LOAD', schedule_interval='@daily', catchup=False, default_args=default_args) as dag:
 
    # task populates Pas QuotesStg .
    T1= BashOperator(
        task_id='T1_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/loadcsvtobq.py --config "config.properties" --productconfig "insurance_qa.properties" --env "dev"  --targettable "PAS_QUOTES_STG" --filename "quote_summary/Quote Summary Report (21).csv"')
 
	# task  populates QuotesDimStg .
    T2 = BashOperator(
        task_id='T2_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "insurance_qa.properties" --env "dev"  --sqlfile "/home/airflow/gcs/data/cloud_sdk/GCPDWH/insuranceqa_automation/sql/LoadQuotesStg.sql"')
 
 
	# task populates QuotesDim .
    T3 = BashOperator(
        task_id='T3_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "insurance_qa.properties" --env "dev"  --sqlfile "/home/airflow/gcs/data/cloud_sdk/GCPDWH/insuranceqa_automation/sql/InsertQuotesDim.sql"')
 
 
	# task runs insert update QuotesDim  .
    T4= BashOperator(
        task_id='T4_GCP_LOAD',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --confaig "config.properties" --productconfig "insurance_qa.properties" --env "dev"  --sqlfile "/home/airflow/gcs/data/cloud_sdk/GCPDWH/insuranceqa_automation/sql/InsertUpdatedInQuotes.sql"')
 
 
	# task runs update QuotesDim .
    T5 = BashOperator(
        task_id='T5_GCP_UPDATE',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "insurance_qa.properties" --env "dev"  --sqlfile "/home/airflow/gcs/data/cloud_sdk/GCPDWH/insuranceqa_automation/sql/updateDateInQuotes.sql"')
  
 
	# task truncates PasQuotesStg.
    T6= BashOperator(
        task_id='T6_GCP_TRUNCATE',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "insurance_qa.properties" --env "dev"  --sqlfile "/home/airflow/gcs/data/cloud_sdk/GCPDWH/insuranceqa_automation/sql/truncate_pasquotes.sql"')
 
	# task truncates QuotesdimStg.
    T7= BashOperator(
        task_id='T7_GCP_TRUNCATE',
        bash_command='python /home/airflow/gcs/data/GCPDWH/util/runSqlFiles.py --config "config.properties" --productconfig "insurance_qa.properties" --env "dev"  --sqlfile "/home/airflow/gcs/data/cloud_sdk/GCPDWH/insuranceqa_automation/sql/truncate_quotesstgdim.sql"')
	
    
    # Define the DAG structure.
    T1 >> T2 >> T3 >> T4 >> T5 >> T6 >> T7 
	
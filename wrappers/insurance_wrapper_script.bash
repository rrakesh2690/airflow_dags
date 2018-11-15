#!/bin/sh -x

########################################################################################################
#    Bash script to load insurance tables .following are the tables affected in order                 #
#1.WORK_INSURANCE_DIM                                                                                  #
#2.INSURANCE_DIM                                                                                       #
#3.INSURANCE_CUSTOMER_DIM                                                                              #
#4.INSURANCE_TRANSACTION_FACT                                                                          #
#Steps:																							       #		
#1.This module reads SQL files to run queries on BigQuery	                                           #	
#Note : User needs to input environment param as dev, test or prod while running script                #
########################################################################################################

ENV=$1
JOBNAME='INSURANCE_TABLE_LOAD'

SQL_FILE_PATH='/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/insurance/sql'
PYTHON_SCRIPT_PATH='/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/util/run_bigquery_scripts.py'

current_date=$(date +"%Y%m%d")
logfile='/app/Informatica/INFA/server/infa_shared/Scripts/GCP/logs/'${JOBNAME}_${current_date}.log

if [ $ENV = "dev" ]; then
  gcloud auth activate-service-account svcsdkeclipse@aaadata-181822.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/AAAData-69668e42a7cf.json --project=aaadata-181822
elif [ $ENV = "test" ]; then
  gcloud auth activate-service-account svcdatamigrationdwtest@dw-test-196023.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/IntegTools-3e64a42e9ec1.json --project=dw-test-196023
elif [ $ENV = "prod" ]; then
  gcloud auth activate-service-account dw-prod-cloud-sdk@aaa-mwg-dwprod.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/dw-prod-1d813c739a76.json --project=aaa-mwg-dwprod
fi

# Start to write to the logfile and start the job

echo $current_date ": Process Started"                                     > $logfile
echo $current_date ": JOBNAME="$JOBNAME                                   >> $logfile

#Runs to load Work_insurance_dim table 
python $PYTHON_SCRIPT_PATH --config "config.properties" --productconfig "insurance.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/insurance_work_dim.sql" >> $logfile

RETCODE=$?
if [ $RETCODE == 0 ]
then
	echo "WORK_INSURANCE_DIM table loaded successfully"                                     >> $logfile
#Runs to update MD5 Value for  Work_insurance_dim table 
	python $PYTHON_SCRIPT_PATH --config "config.properties" --productconfig "insurance.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/insurance_work_dim_MD5_update.sql" >> $logfile
	RETCODE=$?
	if [ $RETCODE == 0 ]	
	then
		echo "WORK_INSURANCE_DIM MD5 Value updated successfully"                                     >> $logfile

#Runs to load insurance_dim table 

		python $PYTHON_SCRIPT_PATH --config "config.properties" --productconfig "insurance.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/insurance_dim_without_AGYPOLE_insert.sql" >> $logfile
		RETCODE=$?
		if [ $RETCODE == 0 ]	
		then
			echo "INSURANCE_DIM table loaded successfully"                                     >> $logfile
			
		
# Runs to inset update in insurance_dim table 
			python $PYTHON_SCRIPT_PATH --config "config.properties" --productconfig "insurance.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/Insurance_dim_update_insert.sql" >> $logfile

			RETCODE=$?
			if [ $RETCODE == 0 ]
			then
				echo "INSURANCE_DIM insert/update successfully"                                     >> $logfile
#  Runs to perform  update in insurance_dim table
				python $PYTHON_SCRIPT_PATH --config "config.properties" --productconfig "insurance.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/insurance_dim_update.sql" >> $logfile

				RETCODE=$?
				if [ $RETCODE == 0 ]
				then
					echo "INSURANCE_DIM table updated successfully"                                     >> $logfile
#Runs to load insurance_customer_dim table 
					python $PYTHON_SCRIPT_PATH --config "config.properties" --productconfig "insurance.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/insurance_customer_dim.sql" >> $logfile

					RETCODE=$?
					if [ $RETCODE == 0 ]
					then
						echo "INSURANCE_CUSTOMER_DIM table loaded successfully"                                     >> $logfile
# Runs to load insurance_transaction_fact table 
						python $PYTHON_SCRIPT_PATH --config "config.properties" --productconfig "insurance.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/insurance_trans_fact.sql" >> $logfile

						RETCODE=$?
						if [ $RETCODE == 0 ]
						then
						   echo "INSURANCE_TRANSACTION_FACT table loaded successfully"                                     >> $logfile
						else
						   echo "ERROR: INSURANCE_TRANS_FACT table loading failed"                                    >> $logfile
						   exit 0
						fi

					   
					else
					   echo "ERROR: INSURANCE_CUSTOMER_DIM table loading failed"                                    >> $logfile
					   exit 0
					fi

					echo '///////////////////////////////////////'                             >> $logfile
								   
				else
				   echo "ERROR: INSURANCE_DIM table update failed"                                    >> $logfile
				   exit 0
				fi

				echo '///////////////////////////////////////'                             >> $logfile
			   
			else
			   echo "ERROR: INSURANCE_DIM table update/insert failed"                                    >> $logfile
			   exit 0
			fi

			echo '///////////////////////////////////////'                             >> $logfile	
		   
		else
			echo "ERROR: INSURANCE_DIM table loading failed"                                    >> $logfile
			exit 0
		fi

		echo '///////////////////////////////////////'                             >> $logfile
		
	else
		echo "ERROR: WORK_INSURANCE_DIM table MD5 update failed"                                    >> $logfile
		exit 0
	fi

	echo '///////////////////////////////////////'                             >> $logfile
		

else
   echo "ERROR: WORK_INSURANCE_DIM table loading failed"                                    >> $logfile
   exit 0
fi

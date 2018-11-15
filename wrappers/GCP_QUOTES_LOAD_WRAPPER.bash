#!/bin/sh

########################################################################################################
#    Bash script to load quotes tables .following are the tables affected in order                     #
#1.QUOTES_DIM                                                                                          #
#2.SAM_QUOTES_STG                                                                                      #
#3.PAS_QUOTES_STG                                                                                      #
#4.QUOTES_DIM_STG                                                                                          #
#Steps:																							       #		
#1.Check for current date file for quotes in mount point.                                              #
#2.Copy file to GCS current bucket.                     	                                           #
#3.Run loadservertobigquery script with parameters                                                     #
#4.After successful run of script move the file from current folder to archive                         #
#5.Next modules reads SQL files to run queries on BigQuery	                                           #	
#Note : User needs to input environment param as dev, test or prod while running script                #
########################################################################################################

ENV=$1
JOBNAME='QUOTES_SQL_TABLE'

SQL_FILE_PATH='/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/insuranceqa_automation/sql'
PYTHON_SCRIPT_PATH='/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/util'
# FILE_PATH='/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/insuranceqa_automation/data'
MOUNT_POINT='/app/Informatica/INFA/server/infa_shared/SrcFiles/GCP/INSURANCE/pas'

current_date=$(date +"%Y_%b_%d")
logfile='/app/Informatica/INFA/server/infa_shared/Scripts/GCP/logs/'${JOBNAME}_${current_date}.log


# Check all the files of MOUNT_POINT directory

for raw_filename in $MOUNT_POINT/*.csv; do

#Check date is equivalent to current date

	if [[ $raw_filename = *$(date +"%Y_%b_%d")* ]]; then

		if [ $ENV = "dev" ]; then
		  gcloud auth activate-service-account svcsdkeclipse@aaadata-181822.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/AAAData-69668e42a7cf.json --project=aaadata-181822
		  BUCKET_NAME=gs://dw-dev-insurance/pas/current
		  ARCHIVE_BUCKET=gs://dw-dev-insurance/pas/archive/
		  
		elif [ $ENV = "test" ]; then
		  gcloud auth activate-service-account svcdatamigrationdwtest@dw-test-196023.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/IntegTools-3e64a42e9ec1.json --project=dw-test-196023
		  BUCKET_NAME=gs://dw-dev-insurance/pas/current
		  ARCHIVE_BUCKET=gs://dw-dev-insurance/pas/archive/
		  
		elif [ $ENV = "prod" ]; then
		  gcloud auth activate-service-account dw-prod-cloud-sdk@aaa-mwg-dwprod.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/dw-prod-1d813c739a76.json --project=aaa-mwg-dwprod
		  BUCKET_NAME=gs://dw-dev-insurance/pas/current
		  ARCHIVE_BUCKET=gs://dw-dev-insurance/pas/archive/
		fi
		
		FILE_FLAG='1'
# Start to write to the logfile and start the job

		echo $current_date ": Process Started"                                     > $logfile
		echo $current_date ": GCS current bucket ="$BUCKET_NAME                   >> $logfile
		echo $current_date ": GCS archive bucket ="$ARCHIVE_BUCKET                >> $logfile
		echo $current_date ": .csv file ="$raw_filename                           >> $logfile
		echo $current_date ": JOBNAME="$JOBNAME                                   >> $logfile

# Copy file from mount point to GCS bucket current folder

		gsutil cp $raw_filename $BUCKET_NAME                                  >> $logfile

		RETCODE=$?
		if [ $RETCODE == 0 ];
		then
		    echo "File has been copied from MOUNT_POINT to GCS's Current Folder"   >> $logfile

# Load sam data from db into Bigquery
		
			#python $PYTHON_SCRIPT_PATH/loadsqlservertobigquery.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV} --targettable "WORK_SAM_QUOTES" --connectionprefix "cs" >> $logfile
			python /app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/insuranceqa_automation/loadsqlservertobigquery.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV} --targettable "WORK_SAM_QUOTES" --connectionprefix "cs" >> $logfile
			
			RETCODE=$?
			if [ $RETCODE == 0 ];
			then
			    echo "Data from db has been loaded successfully into bigquery"   >> $logfile
			
# Insert data into sam_quotes_stg table

				python $PYTHON_SCRIPT_PATH/run_bigquery_scripts.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/sam_quotes_stg_insert.sql" >> $logfile
				
				RETCODE=$?
				if [ $RETCODE == 0 ];
				then
				    echo "Data has been inserted successfully into sam_quotes_stg table"   >> $logfile
				
# Update data into SAM_QUOTES_STG table
					
					python $PYTHON_SCRIPT_PATH/run_bigquery_scripts.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/sam_quotes_stg_update.sql" >> $logfile
					
					RETCODE=$?
					if [ $RETCODE == 0 ]
					then
					    echo "Data has been updated successfully into sam_quotes_stg table"   >> $logfile
					
# Load pas data from csv file to Bigquery using bqsdk
						filename=$(echo $raw_filename | rev | cut -d"/" -f1 | rev)  >> $logfile
						
						python $PYTHON_SCRIPT_PATH/load_csv_to_bigquery_bqsdk.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV} --targettable "PAS_QUOTES_STG" --filename "pas/current/${filename}" --delimiter "," --deposition 'WRITE_TRUNCATE' --skiprows "0" >> $logfile
						if [ $RETCODE == 0 ]
						then
						    echo "File loaded successfully"   >> $logfile
						
# Move all files from current to archive folder
							gsutil mv $BUCKET_NAME/* $ARCHIVE_BUCKET                                   >> $logfile

							RETCODE=$?
							if [ $RETCODE == 0 ]
							then
								echo "File has been moved successfully from current folder of GCS Bucket into Archive Bucket"                                     >> $logfile
# Run to load data into LoadQuotesStg table
								python $PYTHON_SCRIPT_PATH/run_bigquery_scripts.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV} --sqlfile "${SQL_FILE_PATH}/LoadQuotesStg.sql" >> $logfile
								RETCODE=$?
								if [ $RETCODE == 0 ]
								then
									echo "Data loaded successfully in LoadQuotesStg table"                                     >> $logfile	
# Run to insert data into QUOTES_DIM table
									python $PYTHON_SCRIPT_PATH/run_bigquery_scripts.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV}  --sqlfile "${SQL_FILE_PATH}/InsertQuotesDim.sql" >> $logfile
									
									RETCODE=$?
									if [ $RETCODE == 0 ]
									then
										echo "Data insertion is successful in InsertQuotesDim table"                                     >> $logfile
# Run to insert/update in QUOTES_DIM table
										python $PYTHON_SCRIPT_PATH/run_bigquery_scripts.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV}  --sqlfile "${SQL_FILE_PATH}/InsertUpdatedInQuotes.sql" >> $logfile

										RETCODE=$?
										if [ $RETCODE == 0 ]
										then
											echo "Insert/Update query executed successfully"                                     >> $logfile
# Run to update date in QUOTES_DIM table
											python $PYTHON_SCRIPT_PATH/run_bigquery_scripts.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV}  --sqlfile "${SQL_FILE_PATH}/updateDateInQuotes.sql" >> $logfile

											RETCODE=$?
											if [ $RETCODE == 0 ]
											then
												echo "Date has been updated successfully in QUOTES_DIM table"                                     >> $logfile
# Run to truncate data of PAS_QUOTES_STG table
												python $PYTHON_SCRIPT_PATH/run_bigquery_scripts.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV}  --sqlfile "${SQL_FILE_PATH}/truncate_pasquotes.sql" >> $logfile

												RETCODE=$?
												if [ $RETCODE == 0 ]
												then
												   echo "Data of table PAS_QUOTES_STG has been truncated successfully"                                     >> $logfile

# Run to truncate data of QUOTES_STG_DIM table
												   python $PYTHON_SCRIPT_PATH/run_bigquery_scripts.py --config "config.properties" --productconfig "insurance_qa.properties" --env ${ENV}  --sqlfile "${SQL_FILE_PATH}/truncate_quotesstgdim.sql" >> $logfile

												   RETCODE=$?
												   if [ $RETCODE == 0 ]
												   then
													   echo "Data of QUOTES_STG_DIM table has been truncated successfully"                                     >> $logfile
												   else
													   echo "ERROR: Truncate command of QUOTES_STG_DIM table has failed"                                    >> $logfile
													   exit 0
												   fi

												   echo '///////////////////////////////////////'                             >> $logfile	
												else
												   echo "ERROR: Issue in truncating the data of PAS_QUOTES_STG table"                                    >> $logfile
												   exit 0
												fi

												echo '///////////////////////////////////////'                             >> $logfile
												
											else
											   echo "ERROR: Issue in updating date in QUOTES_DIM table"                                    >> $logfile
											   exit 0
											fi

											echo '///////////////////////////////////////'                             >> $logfile
												   
										else
										   echo "ERROR: Insert/Update query failed in QUOTES_DIM table"                                    >> $logfile
										   exit 0
										fi

										echo '///////////////////////////////////////'                             >> $logfile
									   
									else
									   echo "ERROR: Insertion of data failed in QUOTES_DIM table"                                    >> $logfile
									   exit 0
									fi

									echo '///////////////////////////////////////'                             >> $logfile	
						   
								else
									echo "ERROR: Issue in loading the data in LoadQuotesStg table"                                    >> $logfile
									exit 0
								fi

								echo '///////////////////////////////////////'                             >> $logfile

							else
							   echo "ERROR: Issue in moving the file from current folder to archive folder"         >> $logfile
							   exit 0
							fi
							
							echo '///////////////////////////////////////'                             >> $logfile
						
						else
						   echo "ERROR: File loading failed" >> $logfile
						   exit 0
						fi

						echo '///////////////////////////////////////'                             >> $logfile
						
					else
					   echo "ERROR: Issue in updating the data into sam_quotes_stg table" >> $logfile
					   exit 0
					fi

					echo '///////////////////////////////////////'                             >> $logfile
					
				
				else
				   echo "ERROR: Issue in inserting the data into sam_quotes_stg table" >> $logfile
				   exit 0
				fi

				echo '///////////////////////////////////////'                             >> $logfile
			
			else
			   echo "ERROR: Issue in loading the data from db into bigquery" >> $logfile
			   exit 0
			fi

			echo '///////////////////////////////////////'                             >> $logfile
		
		else
		   echo "ERROR: Issue in copying file from MOUNT_POINT to GCS's Current Folder" >> $logfile
		   exit 0
		fi

	echo '///////////////////////////////////////'                             >> $logfile
	
	else
		echo "ERROR: Couldn't find the file having current date"                                    >> $logfile
	fi

	echo '///////////////////////////////////////'                             >> $logfile
	
done

if [ $FILE_FLAG == 1 ]
then
	echo "Process ran successfully" >> $logfile
else	
	echo "ERROR: Couldn't find the file having current date in filename"       >> $logfile
fi


#!/bin/sh

########################################################################################################
#    Bash script to load segments from AS400.dat file on mount point to Bigquery IVANS tables             #
#Steps:																							       #	
#1.Check for current date file for AS400 in mount point. 											       #	
#2.Copy file to GCS Bucket current folder															   #	
#3.Run Dataflow Pipeline script with parameters 													   #	
#4.After successful run of pipeline move all the files from current folder to archive 				   #	
#Note : User needs to input environment param as dev, test or prod while running script                #
########################################################################################################

ENV=$1
JOBNAME='IVANS_AS400'
scriptdir='/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/ivans'

current_date=$(date +"%Y%m%d")

logfile='/app/Informatica/INFA/server/infa_shared/Scripts/GCP/logs/'${JOBNAME}_${current_date}.log


MOUNT_POINT ='/app/GC_SDK/sys_ig/policy_as400'

# Check all the files of MOUNT_POINT directory

for raw_filename in $MOUNT_POINT/*.DAT; do

#Check date is equivalent to current date

	if [[ $raw_filename = *$(date +"%Y%m%d")* ]]; then

		if [ $ENV = "dev" ]; then
		  gcloud auth activate-service-account svcsdkeclipse@aaadata-181822.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/AAAData-69668e42a7cf.json --project=aaadata-181822
		  BUCKET_NAME=gs://dw-dev-insurance/ivans/current/
		  ARCHIVE_BUCKET=gs://dw-dev-insurance/ivans/archive/

		elif [ $ENV = "test" ]; then
		  gcloud auth activate-service-account svcdatamigrationdwtest@dw-test-196023.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/IntegTools-3e64a42e9ec1.json --project=dw-test-196023
		  BUCKET_NAME=gs://dw-test-insurance/pcomp/current/
		  ARCHIVE_BUCKET=gs://dw-test-insurance/pcomp/archive/
		elif [ $ENV = "prod" ]; then
		  gcloud auth activate-service-account dw-prod-cloud-sdk@aaa-mwg-dwprod.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/dw-prod-1d813c739a76.json --project=aaa-mwg-dwprod
		  BUCKET_NAME=gs://dw-prod-insurance/pcomp/current/
		  ARCHIVE_BUCKET=gs://dw-prod-insurance/pcomp/archive/
		fi

		FILE_FLAG='1'
		# Start to write to the logfile and start the job

		echo $current_date ": Process Started"                                     > $logfile
		echo $current_date ": GCS current bucket ="$BUCKET_NAME                   >> $logfile
		echo $current_date ": GCS archive bucket ="$ARCHIVE_BUCKET                >> $logfile
		echo $current_date ": .DAT file of AS400 ="$raw_filename                     >> $logfile
		echo $current_date ": JOBNAME="$JOBNAME                                   >> $logfile

		# Copy file from mount point to GCS bucket current folder

		gsutil cp $raw_filename $BUCKET_NAME                          >> $logfile

		RETCODE=$?
		if [ $RETCODE == 0 ]
		then
		   echo "File has been copied from MOUNT_POINT to GCS's Current Folder"   >> $logfile
		else
		   echo "ERROR: Issue in copying file from MOUNT_POINT to GCS's Current Folder" >> $logfile
		   exit 0
		fi

		echo '///////////////////////////////////////'                             >> $logfile

		# Processing of AS400 .DAT file and data load to Bigquery
		filename=$(echo $raw_filename | rev | cut -d"/" -f1 | rev)  >> $logfile
        python ${scriptdir}/load_as400_segments_to_bq_dataflow.py --config "config.properties" --productconfig "ivans.properties" --env ${ENV} --separator "|" --stripheader "0" --stripdelim "0" --addaudit "1" --writeDeposition "WRITE_APPEND" --system "AS400" --input "${BUCKET_NAME}${filename}" >> $logfile
		
		RETCODE=$?
		if [ $RETCODE == 0 ]
		then
		   echo "DataFlow job run is successful"                                     >> $logfile
		else
		   echo "ERROR: DataFlow job has failed"                                    >> $logfile
		   exit 0
		fi

		echo '///////////////////////////////////////'                             >> $logfile


		# Move all files from current to archive folder

		gsutil mv $BUCKET_NAME* $ARCHIVE_BUCKET                                      >> $logfile

		RETCODE=$?
		if [ $RETCODE == 0 ]
		then
		   echo "All files has been moved from current folder to archive folder successfully">> $logfile
		else
		   echo "ERROR: Issue in moving the files from current folder to archive folder"    >> $logfile
		   exit 0
		fi

		echo '///////////////////////////////////////'                             >> $logfile

	fi

	echo '///////////////////////////////////////'                                 >> $logfile

done

if [ $FILE_FLAG == 1 ]
then
	echo "Process ran successfully" >> $logfile
else	
	echo "ERROR: Couldn't find the file having current date in filename"       >> $logfile
fi


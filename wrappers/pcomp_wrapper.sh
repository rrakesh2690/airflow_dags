#!/bin/sh
#Created By : Atul Guleria
#Created Date : 07/18/2018
#Wrapper Script for loading INSURANCE PCOMP data


ENV=$1

######################################################################################################
#This will have file path and file name for log file
######################################################################################################

LOG_FILE_PATH=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/logs
LOG_FILE_NAME=PCOMP_`date +"%m.%d.%Y"`.log

######################################################################################################
#Setting up Scripts and Config directory path
######################################################################################################

pythonscriptdir=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/util
projectconfig="config.properties"
productconfig="pcomp.properties"

echo '/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////' >> $LOG_FILE_PATH/$LOG_FILE_NAME

echo 'PCOMP process started for date' `date +"%m.%d.%Y"` > $LOG_FILE_PATH/$LOG_FILE_NAME

date= date +"%m.%d.%Y"

######################################################################################################
#Setting up current and archive bucket path for the corresponding environment
######################################################################################################

if [ $ENV = "dev" ]; then
  gcloud auth activate-service-account svcsdkeclipse@aaadata-181822.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/AAAData-69668e42a7cf.json --project=aaadata-181822
  BUCKET_NAME=gs://dw-dev-insurance/pcomp/current
  ARCHIVE_BUCKET=gs://dw-dev-insurance/pcomp/archive
elif [ $ENV = "test" ]; then
  gcloud auth activate-service-account svcdatamigrationdwtest@dw-test-196023.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/IntegTools-3e64a42e9ec1.json --project=dw-test-196023
  BUCKET_NAME=gs://dw-test-insurance/pcomp/current
  ARCHIVE_BUCKET=gs://dw-test-insurance/pcomp/archive
elif [ $ENV = "prod" ]; then
  gcloud auth activate-service-account dw-prod-cloud-sdk@aaa-mwg-dwprod.iam.gserviceaccount.com --key-file=/app/Informatica/INFA/server/infa_shared/Scripts/GCP/GCPDWH/gcp_key/dw-prod-1d813c739a76.json --project=aaa-mwg-dwprod
  BUCKET_NAME=gs://dw-prod-insurance/pcomp/current
  ARCHIVE_BUCKET=gs://dw-prod-insurance/pcomp/archive
fi

######################################################################################################
#Setting up Stage table, Main table, Mount point, SQL Files and Skip rows variables list
######################################################################################################

STG_TABLES=( PCOMP_COMPUP_STG PCOMP_EXIGEN_AUTO_STG PCOMP_FLOOD_STG PCOMP_HOMEINCENTIVE_STG PCOMP_CEABLUEC_STG PCOMP_HOMEOWNERS_STG AUTO_AS400_STG AUTO_MPA_DAILY_STG HO_LEGACY_STG HO_HISTORY_STG HO_HISTORY_STG HO_LEGACY_XSDLYSTB_STG )

MAIN_TABLES=( PCOMP_INSURANCE_TRANSACTION PCOMP_INSURANCE_TRANSACTION PCOMP_INSURANCE_TRANSACTION PCOMP_INSURANCE_TRANSACTION PCOMP_INSURANCE_TRANSACTION PCOMP_INSURANCE_TRANSACTION AUTO_AS400 AUTO_MPA_DAILY HO_LEGACY HO_HISTORY HO_2K8_RECLASSIFY HO_LEGACY )

MOUNT_POINT=( /app/GC_SDK/sys_ig/IETXNPup /app/GC_SDK/sys_ig/IETXNExigen /app/GC_SDK/sys_ig/flood /app/GC_SDK/sys_ig/IETXNAS400 /app/GC_SDK/sys_ig/Bluecod /app/GC_SDK/sys_ig/IETXNHDES /app/GC_SDK/sys_ig/IETXNAS400 /app/GC_SDK/sys_ig/IETXNExigen /app/GC_SDK/sys_ig/IETXNExigen /app/GC_SDK/sys_ig/IETXNAS400 /app/GC_SDK/sys_ig/IETXNAS400 /app/GC_SDK/sys_ig/IETXNHDES )

SQL_FILES=( insert_compup_pcomp.sql insert_exigen_pcomp.sql insert_flood_pcomp.sql insert_homeinc_pcomp.sql insert_ceabluec_pcomp.sql insert_homeown_pcomp.sql insert_auto_as400.sql insert_auto_mpa_daily.sql insert_ho_legacy.sql insert_ho_history.sql insert_ho_2k8_reclassify.sql insert_ho_legacythru_xsdlystb.sql)

skipLeadingRows=( 0 0 1 1 1 0 1 0 0 1 1 1 )

######################################################################################################
#All the logic will be there in the for loop including following steps in sequence:
#1)Copying the file from mount point to GCS
#2)Running python script for loading from csv to stage tables in bigquery
#3)Running python script for loding from stage tables to main tables in bigquery
#1)Copying the file from current to archive bucket in GCS
######################################################################################################


for index in ${!STG_TABLES[@]}; do
    echo ${STG_TABLES[$index]}
    echo ${MAIN_TABLES[$index]}
    echo ${MOUNT_POINT[$index]}
    echo ${SQL_FILES[$index]}

######################################################################################################	
#Getting file names for respective tables and Checking whether file is there at mount point or not
######################################################################################################


	echo '/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////' >> $LOG_FILE_PATH/$LOG_FILE_NAME


if [ ${STG_TABLES[$index]} = "PCOMP_EXIGEN_AUTO_STG" ]; then
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "*4002*`date +"%m.%d.%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)
elif [ ${STG_TABLES[$index]} = "AUTO_MPA_DAILY_STG" ]; then
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "*4006*`date +"%m.%d.%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)
elif [ ${STG_TABLES[$index]} = "HO_LEGACY_STG" ]; then
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "*4218*`date +"%m.%d.%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)
elif [ ${STG_TABLES[$index]} = "PCOMP_FLOOD_STG" ]; then
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "*`date +"%m%d%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)	
elif [ ${STG_TABLES[$index]} = "PCOMP_HOMEINCENTIVE_STG" ]; then
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "*Qry*`date +"%m.%d.%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)	
elif [ ${STG_TABLES[$index]} = "AUTO_AS400_STG" ]; then
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "Home*Auto*`date +"%m.%d.%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)	
elif [ ${STG_TABLES[$index]} = "HO_HISTORY_STG" ]; then
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "Home*Home*`date +"%m.%d.%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)
elif [ ${STG_TABLES[$index]} = "PCOMP_HOMEOWNERS_STG" ]; then
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "*XSDLYSTA*`date +"%m.%d.%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)	
elif [ ${STG_TABLES[$index]} = "HO_LEGACY_XSDLYSTB_STG" ]; then
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "*XSDLYSTB*`date +"%m.%d.%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)	
else
	FILE_NAME=$(find ${MOUNT_POINT[$index]} -iname "*`date +"%m.%d.%Y"`*csv" -print | rev | cut -d"/" -f1 | rev)
fi
    
if [ -z $FILE_NAME ]; then
	echo 'ERROR: File has not arrived on Mount Point' >> $LOG_FILE_PATH/$LOG_FILE_NAME
	exit 0
else
echo $FILE_NAME
fi
	
######################################################################################################	
#Using gsutil to copy files from mount point to GCS	
######################################################################################################

	echo 'Copying file from mount point to GCS' >> $LOG_FILE_PATH/$LOG_FILE_NAME
    gsutil cp ${MOUNT_POINT[$index]}/$FILE_NAME $BUCKET_NAME >> $LOG_FILE_PATH/$LOG_FILE_NAME
	RETCODE=$?
	if [ $RETCODE = 0 ]; then 
	echo 'Copying file from mount point to GCS successful' >> $LOG_FILE_PATH/$LOG_FILE_NAME
	else
	echo 'ERROR: Copying file from mount point to GCS failed' >> $LOG_FILE_PATH/$LOG_FILE_NAME	
    exit 0
	fi
    
	echo '/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////' >> $LOG_FILE_PATH/$LOG_FILE_NAME

	
######################################################################################################	
#Running python script for loading file to stage table
######################################################################################################

	echo 'Loading stage table from csv file' >> $LOG_FILE_PATH/$LOG_FILE_NAME

    python ${pythonscriptdir}/loadcsvtobq.py --config ${projectconfig} --productconfig ${productconfig} --env ${ENV} --targettable ${STG_TABLES[$index]} --writeDeposition "WRITE_TRUNCATE" --filename $FILE_NAME --separator ","  --skipleadingrows ${skipLeadingRows[$index]}  --addcols "0"  --addauditcols "0" >> $LOG_FILE_PATH/$LOG_FILE_NAME
	RETCODE=$?
	if [ $RETCODE = 0 ]; then 
	echo 'csv to stage table load successful' >> $LOG_FILE_PATH/$LOG_FILE_NAME
	else
	echo 'ERROR: csv to stage table load failed' >> $LOG_FILE_PATH/$LOG_FILE_NAME	
    exit 0
	fi

	echo '/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////' >> $LOG_FILE_PATH/$LOG_FILE_NAME

######################################################################################################	
#Running python script for loading main table from stage table
######################################################################################################

	echo 'Loading main table from stage table' >> $LOG_FILE_PATH/$LOG_FILE_NAME
    python ${pythonscriptdir}/runSqlFiles.py --config ${projectconfig} --productconfig ${productconfig} --env ${ENV}  --sqlfile ${SQL_FILES[$index]} --tablename ${MAIN_TABLES[$index]} >> $LOG_FILE_PATH/$LOG_FILE_NAME
	RETCODE=$?
	if [ $RETCODE = 0 ]; then 
	echo 'stage to main table load successful' >> $LOG_FILE_PATH/$LOG_FILE_NAME
	else
	echo 'ERROR: stage to main table load failed' >> $LOG_FILE_PATH/$LOG_FILE_NAME	
    exit 0
	fi	

	echo '/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////' >> $LOG_FILE_PATH/$LOG_FILE_NAME

	
######################################################################################################
#Using gsutil to copy files from current to archive folder in GCS	
######################################################################################################

	echo 'Copying file from current to archive' >> $LOG_FILE_PATH/$LOG_FILE_NAME
	gsutil mv $BUCKET_NAME/$FILE_NAME $ARCHIVE_BUCKET >> $LOG_FILE_PATH/$LOG_FILE_NAME
	if [ $RETCODE = 0 ]; then  
	echo 'Copying file from current to archive successful' >> $LOG_FILE_PATH/$LOG_FILE_NAME
	else
	echo 'ERROR: Copying file from current to archive failed' >> $LOG_FILE_PATH/$LOG_FILE_NAME	
    exit 0
	fi

	echo '/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////' >> $LOG_FILE_PATH/$LOG_FILE_NAME
	

done
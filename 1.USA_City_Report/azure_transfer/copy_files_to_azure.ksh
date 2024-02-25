############################################################
# Developed By: Deepak Narayan                             #
# Developed Date: 23-02-2024                               #
# Script Name: copy_files_to_azure.ksh                     #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################

#Please ensure a new SAS URL is generated from the Container as the current one is only VALID until : 24/02/2024 7:40 AM AEST

# Declare a variable to hold the unix script name.
JOBNAME="copy_files_to_azure.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d-%H-%M-%S')

#Define a Log File where logs would be generated
LOGFILE="/home/deepaknrn/softwares/azure/${JOBNAME}_${date}.log"

###########################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

### Define Local Directories
LOCAL_OUTPUT_PATH="/home/deepaknrn/Spark_Learning_Projects/1.USA_City_Report/src/main/python/staging"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/fact

### Define SAS URLs

citySasUrl="https://prescpipeline.blob.core.windows.net/dimension-city/${bucket_subdir_name}?st=2024-02-23T12:24:11Z&se=2024-02-23T20:24:11Z&si=writeaccess&spr=https&sv=2022-11-02&sr=c&sig=hYX6fdYG9lTmD1koRPxlpEHD5BQdkq8YiOfY6FsgCnQ%3D"
prescSasUrl="https://prescpipeline.blob.core.windows.net/presc/${bucket_subdir_name}?st=2024-02-23T12:34:35Z&se=2024-02-23T20:34:35Z&si=write&spr=https&sv=2022-11-02&sr=c&sig=DWlNNsa8qdVd0tqyy3mgltBH3QAAoyyOXio0t76bczg%3D"

### Push City  and Fact files to Azure.
azcopy copy "${LOCAL_CITY_DIR}/*" "$citySasUrl"
azcopy copy "${LOCAL_FACT_DIR}/*" "$prescSasUrl"

echo "The ${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
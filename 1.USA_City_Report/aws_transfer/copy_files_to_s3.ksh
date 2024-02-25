############################################################
# Developed By: Deepak Narayan                             #
# Developed Date: 23-02-2024                               #
# Script Name: copy_files_to_s3                            #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="copy_files_to_s3.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d-%H-%M-%S')

#Define a Log File where logs would be generated
LOGFILE="/home/deepaknrn/Spark_Learning_Projects/1.USA_City_Report/aws_transfer/${JOBNAME}_${date}.log"

###########################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

LOCAL_OUTPUT_PATH="/home/deepaknrn/Spark_Learning_Projects/1.USA_City_Report/src/main/python/staging"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/fact


### Push City  and Fact files to s3.
for file in ${LOCAL_CITY_DIR}/*.*
do
  aws s3 --profile myprofile cp ${file} "s3://prescpipelinedeepaknrn/dimension_city/$bucket_subdir_name/"
  echo "City File $file is pushed to S3."
done

for file in ${LOCAL_FACT_DIR}/*.*
do
  aws s3 --profile myprofile cp ${file} "s3://prescpipelinedeepaknrn/fact/$bucket_subdir_name/"
  echo "Presc file $file is pushed to s3."
done

echo "The ${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
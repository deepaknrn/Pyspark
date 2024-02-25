Pyspark End to End Project Real time Implementation 
This project is based on the following course in Udemy
Course : https://www.udemy.com/course/end-to-end-pyspark-real-time-project-implementation-spark/learn/lecture/30228208#overview

Report#1 :
USA City Report 
1. Number of distinct prescribers for each city 
2. Total trx_cnt prescribed in each city 
3. Number of zips in each city 
4. Do not report a city if no prescriber is assigned to 

File type : json 
Compression Type : bzip2 
Location Type : S3
Number of split files : 1

Report#2 :
USA Prescriber Report 
Get Top 5 Prescribers with highest TRX_CNT in each state. Consider the prescribers only with working experience from 20 to 50 years.

File type : orc 
Compression Type : snappy 
Location Type : S3
Number of split files : 2

Folder Structure:
\src\main\python\bin -> contains all the .py files required for project
\src\main\python\config -> contains the configuration files required for the project
\src\main\python\lib -> contains any libraries required for the project (project dependencies .jars), connectivity to postgreSQL
\src\main\python\logs -> python logging using custom module
\src\main\python\sql -> contains any validation .sql scripts for the project
\src\main\python\staging -> folder that contains the input files used for the project
\src\main\python\util -> contains the custom logger framework file

Tech Stack : 
Hadoop HDFS , Hadoop YARN , Amazon S3 , Azure Blob , Python , Pyspark, PostgreSQL , Google Cloud 

Note : 
a) Ensure the Target Folders in WINDOWS are empty/not available before writing to TARGET
b) Ensure the Target Folders in HDFS are empty/not available before writing to TARGET
c) Ensure the log file ("run_presc_pipeline.log") is cleared before attempting to run (WINDOWS/HDFS/GCP)
d) Unzip the file USA_Presc_Medicare_Data_12021.zip in the folder \src\main\python\staging\fact (Github allows only 100MB source file uploads)

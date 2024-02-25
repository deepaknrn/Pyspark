'''This script is used to set the environment variables required for the Project'''

import os

#Set the environment variables
#os.environ['env']='test' #WINDOWS
os.environ['env']='prod' #HDFS
os.environ['header']='True'
os.environ['infer_schema']='True'

#Get the environment variables
envn=os.environ['env']
header=os.environ['header']
infer_schema=os.environ['infer_schema']

#Set the other Variables
appName="USA Prescriber City Report"
#current_path=os.getcwd() #WINDOWS  Get the current Path in Windows
current_path=r"C:\Users\IDME\PycharmProjects\Spark_Learning_Projects\1.USA_City_Report\src\main\python"   #If runninng spark-submit , use current_path hardcoded instead of using os.getcwd() which will reflect to a different path altogether
#current_path_hdfs="/opt/app/sand_box/idme/to_deploy/Learning/1.USA_City_Report/src/main/python/"

#staging_dimension_city=current_path+"\staging\dimension_city"  Unable to read path's starting with \\ https://issues.apache.org/jira/browse/HADOOP-8087 hence kept the files in local Windows C:\ Directory instead of Desktop
#staging_fact=current_path+"\staging\\fact" Unable to read path's starting with \\ https://issues.apache.org/jira/browse/HADOOP-8087 hence kept the files in local Windows C:\ Directory instead of Desktop

'''Input Paths-WINDOWS'''
staging_dimension_city="C:\\File\\dimension_city"  #Place the file in the folder : C:\File\dimension_city
staging_fact="C:\\File\\dimension_fact"           #Place the file in the folder : C:\File\dimension_fact

'''Output Paths-WINDOWS'''
file_path_city="C:\\File\\output_city"  #Ensure this folder doesnt exist in Windows -> https://stackoverflow.com/questions/42068903/spark-error-output-directory-file-already-exists
file_path_fact="C:\\File\\output_fact"    #Ensure this folder doesnt exist in Windows -> https://stackoverflow.com/questions/42068903/spark-error-output-directory-file-already-exists

'''Input Paths-HDFS'''
#staging_dimension_city="hdfs://0.0.0.0:19000/hdfs_windows/Spark_Learning_Projects_Input_Files/dimension_city"
staging_dimension_city_hdfs="/hdfs_windows/Spark_Learning_Projects_Input_Files/dimension_city"
staging_fact_hdfs="/hdfs_windows/Spark_Learning_Projects_Input_Files/dimension_fact"

'''Output Paths -HDFS'''
file_path_city_hdfs="/hdfs_windows/Spark_Learning_Projects_Output_Files/output_city"
file_path_fact_hdfs="/hdfs_windows/Spark_Learning_Projects_Output_Files/output_fact"

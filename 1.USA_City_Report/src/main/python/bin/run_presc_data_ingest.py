import get_all_variables as gav
import logging,logging.config

##Load the Logging configuration file
log_path = gav.current_path + "\\util\\usa_city_report.conf"  #WINDOWS
#log_path = gav.current_path + "/util/usa_city_report.conf"  #HDFS
logging.config.fileConfig(fname=log_path)
#Create a Custom Logger
logger=logging.getLogger(__name__)

def load_files(spark,file_dir,file_format,header,infer_schema):
    try:
        logger.info("load_files() function has been started")
        logger.info(f"spark.read.format{file_format}.load{file_dir}")
        if file_format=="parquet":
                #file_dir='r"' + 'file:///'+ file_dir + '"'
                df=spark.read.format(file_format).load(r"file:///C:\File\dimension_city\us_cities_dimension.parquet")
        elif file_format=="csv":
                #file_dir='r"' + 'file:///'+ file_dir + '"'
                df=spark.read.format(file_format).options(header=header,infer_schema=infer_schema).load(r"file:///C:\File\dimension_fact\USA_Presc_Medicare_Data_12021.csv")
    except Exception as argument:
        logger.error("Error in the method load_files(), Please check the stack trace",exc_info=True)
        raise
    else:
        logger.info(f"The input file '{file_dir}' has been loaded to the spark dataframe. The load_files() function is completed successfully ")
        return df

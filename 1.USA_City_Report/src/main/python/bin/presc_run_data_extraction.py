import get_all_variables as gav
import logging,logging.config

##Load the Logging configuration file
log_path = gav.current_path + "\\util\\usa_city_report.conf"  #WINDOWS
#log_path = gav.current_path + "/util/usa_city_report.conf"  #HDFS
logging.config.fileConfig(fname=log_path)
#Create a Custom Logger
logger=logging.getLogger("presc_run_data_extraction")

def extract_files_city(df,dfName,format,filepath,split_no,headerReq,compressionType):
    try:
        logger.info(f" Extraction extract_files() method has been started successfully for dataframe '{dfName}' ")
        df.coalesce(split_no).write.format(format).save(r"file:///C:\File\output_city",header=headerReq,compression=compressionType)
    except Exception as argument:
        logger.error(f" Error in calling the method extract_files() . Please check the stack trace for further information")
        raise
    else:
        logger.info(f" Extraction extract_files() method has been completed successfully for dataframe '{dfName}'")
        pass

def extract_files_fact(df,dfName,format,filepath,split_no,headerReq,compressionType):
    try:
        logger.info(f" Extraction extract_files() method has been started successfully for dataframe '{dfName}' ")
        df.coalesce(split_no).write.format(format).save(r"file:///C:\File\output_fact",header=headerReq,compression=compressionType)
    except Exception as argument:
        logger.error(f" Error in calling the method extract_files() . Please check the stack trace for further information")
        raise
    else:
        logger.info(f" Extraction extract_files() method has been completed successfully for dataframe '{dfName}'")
        pass


def extract_files_city_hdfs(df,dfName,format,filepath,split_no,headerReq,compressionType):
    try:
        logger.info(f" Extraction extract_files() method has been started successfully for dataframe '{dfName}' ")
        df.coalesce(split_no).write.format(format).save(r"/hdfs_windows/Spark_Learning_Projects_Output_Files/output_city",header=headerReq,compression=compressionType)
    except Exception as argument:
        logger.error(f" Error in calling the method extract_files() . Please check the stack trace for further information")
        raise
    else:
        logger.info(f" Extraction extract_files() method has been completed successfully for dataframe '{dfName}'")
        pass

def extract_files_fact_hdfs(df,dfName,format,filepath,split_no,headerReq,compressionType):
    try:
        logger.info(f" Extraction extract_files() method has been started successfully for dataframe '{dfName}' ")
        df.coalesce(split_no).write.format(format).save(r"/hdfs_windows/Spark_Learning_Projects_Output_Files/output_fact",header=headerReq,compression=compressionType)
    except Exception as argument:
        logger.error(f" Error in calling the method extract_files() . Please check the stack trace for further information")
        raise
    else:
        logger.info(f" Extraction extract_files() method has been completed successfully for dataframe '{dfName}'")
        pass
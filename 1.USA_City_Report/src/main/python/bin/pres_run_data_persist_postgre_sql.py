import get_all_variables as gav
import logging,logging.config

##Load the Logging configuration file
log_path = gav.current_path + "\\util\\usa_city_report.conf"  #WINDOWS
#log_path = gav.current_path + "/util/usa_city_report.conf"  #HDFS
logging.config.fileConfig(fname=log_path)
#Create a Custom Logger
logger=logging.getLogger("presc_run_data_persist_postgre_sql")

def extract_files_city_postgre_sql(df,dfName,mode="overwrite"):
    try:
        logger.info(f" Extraction extract_files() method has been started successfully for dataframe '{dfName}' ")
        df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/prespipeline") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "dimension_city") \
            .mode("append") \
            .option("user", "prescpipeline_user") \
            .option("password", "deepak123") \
            .save()
    except Exception as argument:
        logger.error(f" Error in calling the method extract_files() . Please check the stack trace for further information")
        raise
    else:
        logger.info(f" Extraction extract_files() method has been completed successfully for dataframe '{dfName}'")
        pass

def extract_files_fact_postgre_sql(df,dfName,mode="overwrite"):
    try:
        logger.info(f" Extraction extract_files() method has been started successfully for dataframe '{dfName}' ")
        df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/prespipeline") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "dimension_fact") \
            .mode("append") \
            .option("user", "prescpipeline_user") \
            .option("password", "deepak123") \
            .save()
    except Exception as argument:
        logger.error(f" Error in calling the method extract_files() . Please check the stack trace for further information")
        raise
    else:
        logger.info(f" Extraction extract_files() method has been completed successfully for dataframe '{dfName}'")
        pass



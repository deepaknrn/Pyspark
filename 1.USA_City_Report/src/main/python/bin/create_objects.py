from pyspark.sql import SparkSession

import get_all_variables as gav
import logging,logging.config

##Load the Logging configuration file
log_path = gav.current_path + "\\util\\usa_city_report.conf"  #WINDOWS
#log_path = gav.current_path + "/util/usa_city_report.conf"  #HDFS
logging.config.fileConfig(fname=log_path)
#Create a Custom Logger
logger=logging.getLogger(__name__)

#Create a Spark Object
def get_spark_object(envn,appName):
    try:
        logger.info(f"get_spark_object() has been started. The '{envn}' envn is used. ")
        if envn=='test':
            master='local' #work in local cluster using pycharm
        else:
            master='yarn'  #upload code to yarn cluster and run using spark-submit
        spark=SparkSession\
                .builder\
                .appName(appName)\
                .master(master)\
                .getOrCreate()
    except Exception as argument:
            logger.error("Error in the method get_spark_object(). Please check the stack trace " + str(argument),exc_info=True)
            raise
    else:
            logger.info("get_spark_object() has been executed. Spark Object created.")
            return spark
    finally:
        pass


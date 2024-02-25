import get_all_variables as gav
import logging,logging.config


##Load the Logging configuration file
log_path = gav.current_path + "\\util\\usa_city_report.conf"  #WINDOWS
#log_path = gav.current_path + "/util/usa_city_report.conf"  #HDFS
logging.config.fileConfig(fname=log_path)
#Create a Custom Logger
logger=logging.getLogger(__name__)

def get_current_date(spark):
    try:
        current_date_df=spark.sql(""" SELECT current_date""")
        logger.info("Validate the spark object creation by printing the Current Date" + str(current_date_df.collect()))
    except Exception as argument:
        logger.error("Error in the method get_current_date(). Please check the Stack Trace" + str(argument),exc_info=True)
        raise
    else:
        logger.info("Spark object has been validated. Spark Object is ready")
    finally:
        pass


def validate_dfcount(df,dfName):
    try:
        logger.info(f"The Dataframe Validation by count df_count() is started for dataframe '{dfName}' ")
        df_count=df.count()
        logger.info(f" The Dataframe '{dfName}' count is : " + str(df_count))
        logger.info(f" The Dataframe '{dfName}' schema is :" + str(df.printSchema()))
    except Exception as argument:
        logger.error(f" Error in the method - validate_dfcount() . Please check the stack Trace", exc_info=True)
        raise
    else:
        logger.info(f" The Dataframe '{dfName}' validation has been completed successfully.")


def df_top10_rec(df,dfName):
    try:
        logger.info(f"The Dataframe Validation by top 10 records df_top10_rec() is started for dataframe '{dfName}' ")
        logger.info(f"The Dataframe top 10 records are : ")
        df_top10_rec=df.limit(10)
        df_top10_rec_s=df.show(10,truncate=False)
        #df_pandas=df.limit(10).toPandas() #Converting dataframe to Python Dataframe
        #logger.info("\n \t " + df_pandas.toString(index=False)) #Converting pandas df to String to print in the log
        logger.info(f"\n \t '{df_top10_rec}'")
        #logger.info(f"\n \t"+ str(df.show(10,truncate=False)))
    except Exception as argument:
        logger.error("Error in the method - df_top10_rec(). Please check the stack trace",exc_info=True)
        raise
    else:
        logger.info(f"The Dataframe Validation by top10 records df_top_rec() is completed for dataframe '{dfName}'")


def df_print_schema(df,dfName):
    try:
        logger.info(f"The Dataframe Schema Validation for Dataframe '{dfName}'")
        sc=df.schema.fields
        logger.info(f"The Dataframe '{dfName}' schema is ")
        for i in sc:
            logger.info(f"\t '{i}' ")
    except Exception as argument:
        logger.error(f"Error in the method df_print_schema(). Please check the stack trace",exc_info=True)
        raise
    else:
        logger.info(f" The Dataframe Schema Validation is completed")

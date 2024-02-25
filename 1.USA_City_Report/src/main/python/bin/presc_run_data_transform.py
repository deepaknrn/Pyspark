#City Report

#Transform Logics
 #1. Calculate the number of zips in each file
 #2. Calculate the number of distinct prescribers assigned for each city
 #3. Calculate total TRX_CNT prescribed for each city
 #4. Do not report a city in the final report if no prescriber is assigned to it

#Output Layout -> City Name ,State Name,Country Name,City Population,Number of Zips,Prescriber Counts,Total Trx Counts
import get_all_variables as gav
import logging,logging.config
from pyspark.sql.functions import udf,upper,size, countDistinct, sum, dense_rank, col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import countDistinct,sum
from pyspark.sql.window import Window

##Load the Logging configuration file
log_path = gav.current_path + "\\util\\usa_city_report.conf"  #WINDOWS
#log_path = gav.current_path + "/util/usa_city_report.conf"  #HDFS
logging.config.fileConfig(fname=log_path)
#Create a Custom Logger
logger=logging.getLogger("presc_run_data_transform")


# 1. Calculate the number of zips in each file

@udf(returnType=IntegerType())
def input_zip(column):
    return len(column.split(" "))

def city_report(df_city_sel,df_fact_sel,dfName):
    try:
        logger.info(f"city_report() function has been started for dataframe '{dfName}'")
        df_city_split=df_city_sel.withColumn('zip_count',input_zip(df_city_sel.zips))

#2. Calculate the number of distinct prescribers assigned for each city
#3. Calculate total TRX_CNT prescribed for each city

        df_fact_distinct_presc_cnt=df_fact_sel.groupBy(df_fact_sel.presc_state, df_fact_sel.presc_city).agg(countDistinct("presc_id").alias("presc_counts"),
                                                                                                 sum("trx_cnt").alias("total_trx_cnt"))
#4. Do not report a city in the final report if no prescriber is assigned to it
        df_city_join=df_city_split.join(df_fact_distinct_presc_cnt,((df_city_split.city==df_fact_distinct_presc_cnt.presc_city) & (df_city_split.state_id==df_fact_distinct_presc_cnt.presc_state)),'inner')

        df_city_final_sel=df_city_join.select("city","state_name","county_name","population","zip_count","presc_counts","total_trx_cnt")

    except Exception as argument:
        logger.error(f"Error in the method city_report() for dataframe '{dfName}' Please check the stack trace",
                     exc_info=True)
        raise
    else:
        logger.info(f"city_report() function has been completed for dataframe '{dfName}'")
        return df_city_final_sel


def prescriber_report(df_fact_sel):
    """
        # Prescriber Report:
        Top 5 Prescribers with highest trx_cnt per each state.
        Consider the prescribers only from 20 to 50 years of experience.
        Layout:
          Prescriber ID
          Prescriber Full Name
          Prescriber State
          Prescriber Country
          Prescriber Years of Experience
          Total TRX Count
          Total Days Supply
          Total Drug Cost
        """
    try:
        logger.info("Transform - top_5_Prescribers() is started...")
        spec = Window.partitionBy("presc_state").orderBy(col("trx_cnt").desc())
        df_presc_final = df_fact_sel.select("presc_id", "presc_fullname", "presc_state", "country_name", "years_of_exp","trx_cnt", "total_day_supply", "total_drug_cost") \
            .filter((df_fact_sel.years_of_exp >= 20) & (df_fact_sel.years_of_exp <= 50)) \
            .withColumn("dense_rank", dense_rank().over(spec)) \
            .filter(col("dense_rank") <= 5) \
            .select("presc_id", "presc_fullname", "presc_state", "country_name", "years_of_exp", "trx_cnt","total_day_supply", "total_drug_cost")
    except Exception as exp:
        logger.error("Error in the method - top_5_Prescribers(). Please check the Stack Trace. " + str(exp),
                     exc_info=True)
        raise
    else:
        logger.info("Transform - top_5_Prescribers() is completed...")
    return df_presc_final


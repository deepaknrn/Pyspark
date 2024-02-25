import get_all_variables as gav
import logging,logging.config
from pyspark.sql.functions import upper,lit,col,regexp_extract,concat_ws,coalesce,round,avg
from pyspark.sql.window import Window

##Load the Logging configuration file
log_path = gav.current_path + "\\util\\usa_city_report.conf"  #WINDOWS
#log_path = gav.current_path + "/util/usa_city_report.conf"  #HDFS
logging.config.fileConfig(fname=log_path)
#Create a Custom Logger
logger=logging.getLogger("presc_run_data_preprocessing")


def perform_data_clean_dfcity(df1,dfName):
        ### Clean the city dataframe
        ### Select only the required columns
        ### Apply upper case functions for columns - city , state and County
        try:
                logger.info(f"perform_data_clean() function has been started for dataframe '{dfName}'")
                df_city_sel=df1.select(df1.city,df1.state_id,df1.state_name,df1.county_name,df1.population,df1.zips)
                df_city_sel=df1.select(upper(df1.city).alias("city"),df1.state_id,upper(df1.state_name).alias("state_name"),upper(df1.county_name).alias("county_name"),df1.population,df1.zips)
        except Exception as argument:
                logger.error(f"Error in the method perform_data_clean() for dataframe '{dfName}' Please check the stack trace",exc_info=True)
                raise
        else:
                logger.info(f"perform_data_clean() function has been completed successfully for dataframe '{dfName}'")
                return df_city_sel

def perform_data_clean_dffact(df2,dfName):
        ### Clean the fact dataframe
        #1. Select only the required columns
        #2. Rename the columns
        #3. Add the country field "USA"
        #4. Clean years_of_exp field
        #5. Convert years_of_exp field from string to Number
        #6. Combine First and Last Name
        #7. Check and clean all null / nan values
        #8. Impute TRX_CNT where it is null as avg of trx_cnt for the prescriber
        try:
            logger.info(f"perform_data_clean() function has been started for dataframe '{dfName}'")
            #1. Select only the required columns
            df_fact_sel = df2.select(df2.npi,df2.nppes_provider_last_org_name,df2.nppes_provider_first_name,df2.nppes_provider_city,df2.nppes_provider_state,df2.specialty_description,df2.drug_name,df2.total_claim_count,df2.total_day_supply,df2.total_drug_cost,df2.years_of_exp)
            #2 Rename the columns
            df_fact_sel = df_fact_sel.select(df2.npi.alias("presc_id"), df2.nppes_provider_last_org_name.alias("presc_lname"), \
                                     df2.nppes_provider_first_name.alias("presc_fname"),
                                     df2.nppes_provider_city.alias("presc_city"), \
                                     df2.nppes_provider_state.alias("presc_state"),
                                     df2.specialty_description.alias("presc_spclt"), df2.years_of_exp, \
                                     df2.drug_name, df2.total_claim_count.alias("trx_cnt"), df2.total_day_supply, \
                                     df2.total_drug_cost)
            #Ignored the renaming part (.withColumnRenamed API)
            #3. Add the country field "USA"
            df_fact_sel = df_fact_sel.withColumn("country_name",lit("USA"))
            #years_of_exp looks like = 32.0 (having equal=, decimal points.0)
            #4. Clean years_of_exp field
            pattern="\d+"
            idx=0
            df_fact_sel = df_fact_sel.withColumn("years_of_exp",regexp_extract(col("years_of_exp"),pattern,idx))
            #5. Convert Years of _expl field
            df_fact_sel = df_fact_sel.withColumn("years_of_exp",col("years_of_exp").cast("int"))
            #6. Combine first and Last Name
            df_fact_sel = df_fact_sel.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
            df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")
            # 8 Delete the records where the PRESC_ID is NULL
            df_fact_sel = df_fact_sel.dropna(subset="presc_id")
            # 9 Delete the records where the DRUG_NAME is NULL
            df_fact_sel = df_fact_sel.dropna(subset="drug_name")
            #8. for an NPI that as total_claim_count as null , determine the average of that and populate
            spec = Window.partitionBy("presc_id")
            df_fact_sel = df_fact_sel.withColumn('trx_cnt', coalesce("trx_cnt", round(avg("trx_cnt").over(spec))))
            df_fact_sel = df_fact_sel.withColumn("trx_cnt", col("trx_cnt").cast('integer'))

        except Exception as argument:
            logger.error(f"Error in the method perform_data_clean() for dataframe '{dfName}' Please check the stack trace",exc_info=True)
            raise
        else:
            logger.info(f"perform_data_clean() function has been completed successfully for dataframe '{dfName}'")
            return df_fact_sel

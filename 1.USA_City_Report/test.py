from pyspark.sql import SparkSession
import logging

def main():

    print("Entering the Main Module")

    # Create a Spark Object
    spark = SparkSession.builder.master('local').appName('Test').getOrCreate()
    print("Spark Object has been created " + str(spark))

    # Create a Spark Context Object
    sc = spark.sparkContext
    print("Spark Context has been created" + str(sc))

if __name__=="__main__":
    main()
import unittest
import sys
sys.path.insert(0,r"C:\Users\IDME\PycharmProjects\Spark_Learning_Projects\1.USA_City_Report\src\main\python\bin")
from presc_run_data_transform import city_report

from pyspark.sql import SparkSession

#Define the Class for Unit Testing

class ZipTransactionCountCheck(unittest.TestCase):   #Inherit unittest Class (TestCase Module)
    def test_transactionCountcheck(self):
        spark = SparkSession.builder.master("local").appName("test_transformation_function").getOrCreate()
        df1=spark.read.csv(r"file:///C:\Users\IDME\PycharmProjects\Spark_Learning_Projects\1.USA_City_Report\src\main\python\unittest\city_data_riverside.csv")
        df2=spark.read.csv(r"file:///C:\Users\IDME\PycharmProjects\Spark_Learning_Projects\1.USA_City_Report\src\main\python\unittest\USA_Presc_Medicare_Data_2021.csv")
        df1_count=df1.count()
        df2_count=df2.count()
        #self.assertEqual(df1_count,df2_count) #Validating by passing arguments normally
        #self.assertEqual([1,1],[df1_count,df2_count]) #Validating by passing arguments via a list
        self.assertGreater(self,df2_count,df1_count)

if __name__ == '__main__':
    unittest.main()



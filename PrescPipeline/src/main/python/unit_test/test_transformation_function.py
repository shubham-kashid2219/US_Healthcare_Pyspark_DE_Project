import unittest
import os, sys
sys.path.insert(0,r"C:\Users\Dell\Desktop\Udemy Project\Pycharm Project\Projects\PrescPipeline\src\main\python\\bin")

from presc_run_data_transform import city_report
from pyspark.sql import SparkSession


class TransformTest(unittest.TestCase):

    def test_city_report_zip_trx_count_check(self):

        # Create Spark Object
        spark = SparkSession \
        .builder \
        .master('local') \
        .appName('Testing city report function for Zip counts and Txr counts') \
        .getOrCreate()

        df_city_sel = spark.read.load('city_data_riverside.csv', format='csv',header=True)
        df_fact_sel = spark.read.load('USA_Presc_Medicare_Data_2021.csv', format='csv',header=True)
        df_city_sel.show(truncate=False)
        df_fact_sel.show(truncate=False)

        # Call the city report function
        df_city_final=city_report(df_city_sel, df_fact_sel)
        df_city_final.show(truncate=False)

        # Extract the zip count and trx count
        zip_cnt= df_city_final.select("zip_count").first().zip_count
        trx_cnt= df_city_final.select("trx_counts").first().trx_counts

        # Calculate the zip count and trx count from the files manually
        # zip_count = 14
        # trx_count = 45

        # Perform unit testing
        self.assertEqual(14,zip_cnt)


if __name__ == '__main__':
    unittest.main()




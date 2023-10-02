import requests
import csv
import pandas as pd
from datetime import datetime
import json
import findspark
findspark.init()
from pyspark import SparkConf , SparkContext
from pyspark.sql import SparkSession ,Row
from pyspark.sql.functions import *

def connection():
    spark = SparkSession.builder. \
    master("local[4]"). \
    appName("DataPipelineExample"). \
    getOrCreate()


    return spark 

def process_data(spark , df):

    spark_df = spark.createDataFrame(df)

    simple_df = spark_df

    simple_df.cache()

    simple_df.dropDuplicates()

    numeric_columns = []
    string_columns = []

    for column in simple_df.columns:
        # Sütunun veri tipini kontrol edin (sayısal ise)
        if "IntegerType()" in str(simple_df.schema[column].dataType) or "DoubleType()" in str(simple_df.schema[column].dataType):
            numeric_columns.append(column)
        else:
            string_columns.append(column)


    for column in string_columns:
        simple_df = simple_df \
        .withColumn(column, lower(col(column))) \
        .withColumn(column , initcap(col(column))) \
        .withColumn(column , trim(col(column)))

    return  simple_df.toPandas()






    
if __name__ == "__main__":
    # CSV dosyasını oku ve Spark DataFrame'i al
    spark = connection()

    # DataFrame'i başka bir fonksiyona iletebilir ve işleyebilirsiniz
    process_df = process_data(spark)


    # SparkSession'ı kapatma
    spark.stop()

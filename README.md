# Databricks-Supermarket-Stock-Analysis


### Start a Spark Session
 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('supermarket').getOrCreate()


## Load Supermarket csv file
 
df = spark.read.csv('/FileStore/shared_uploads/supermarket_stock.csv', inferSchema=True, header=True)

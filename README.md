# Databricks-Supermarket-Stock-Analysis


### Start a Spark Session
 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('supermarket').getOrCreate()

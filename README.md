# Databricks-Supermarket-Stock-Analysis


## Start a Spark Session
 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('supermarket').getOrCreate()


## Load Supermarket csv file
 
df = spark.read.csv('/FileStore/shared_uploads/supermarket_stock.csv', inferSchema=True, header=True)


## Show column names
 
df.columns
Out[137]: ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']


## Show schema
 
df.printSchema()
root
 |-- Date: timestamp (nullable = true)
 |-- Open: double (nullable = true)
 |-- High: double (nullable = true)
 |-- Low: double (nullable = true)
 |-- Close: double (nullable = true)
 |-- Volume: integer (nullable = true)
 |-- Adj Close: double (nullable = true)


## Show summary
 
from pyspark.sql.functions import format_number
 
summary = df.describe()
summary.select(summary['summary'],
 format_number(summary['Open'].cast('float'), 2).alias('Open'),
 format_number(summary['High'].cast('float'), 2).alias('High'),
 format_number(summary['Low'].cast('float'), 2).alias('Low'),
 format_number(summary['Close'].cast('float'), 2).alias('Close'),
 format_number(summary['Volume'].cast('int'),0).alias('Volume')
 ).show()
 
+-------+--------+--------+--------+--------+----------+
|summary|    Open|    High|     Low|   Close|    Volume|
+-------+--------+--------+--------+--------+----------+
|  count|1,258.00|1,258.00|1,258.00|1,258.00|     1,258|
|   mean|   72.36|   72.84|   71.92|   72.39| 8,222,093|
| stddev|    6.77|    6.77|    6.74|    6.76| 4,519,780|
|    min|   56.39|   57.06|   56.30|   56.42| 2,094,900|
|    max|   90.80|   90.97|   89.25|   90.47|80,898,100|
+-------+--------+--------+--------+--------+----------+


## Print out the first 5 columns and round to 2 decimal place
 
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, round
from pyspark.sql.functions import to_date
 
 
df.withColumn("Open",round(df.Open.cast(DoubleType()),2)) \
.withColumn("High",round(df.High.cast(DoubleType()),2)) \
.withColumn("Low",round(df.Low.cast(DoubleType()),2)) \
.withColumn("Close",round(df.Close.cast(DoubleType()),2)) \
.withColumn("Adj Close",round(df["Adj Close"].cast(DoubleType()),2)) \
.show(5)
 
 
 
+-------------------+-----+-----+-----+-----+--------+---------+
|               Date| Open| High|  Low|Close|  Volume|Adj Close|
+-------------------+-----+-----+-----+-----+--------+---------+
|2012-01-03 00:00:00|59.97|61.06|59.87|60.33|12668800|    52.62|
|2012-01-04 00:00:00|60.21|60.35|59.47|59.71| 9593300|    52.08|
|2012-01-05 00:00:00|59.35|59.62|58.37|59.42|12768200|    51.83|
|2012-01-06 00:00:00|59.42|59.45|58.87| 59.0| 8069400|    51.46|
|2012-01-09 00:00:00|59.03|59.55|58.92|59.18| 6679300|    51.62|
+-------------------+-----+-----+-----+-----+--------+---------+
only showing top 5 rows


## Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day
 
df_hv = df.withColumn('HV Ratio', df['High']/df['Volume']).select(['HV Ratio'])
df_hv.show()
+--------------------+
|            HV Ratio|
+--------------------+
|4.819714653321546E-6|
|6.290848613094555E-6|
|4.669412994783916E-6|
|7.367338463826307E-6|
|8.915604778943901E-6|
|8.644477436914568E-6|
|9.351828421515645E-6|
| 8.29141562102703E-6|
|7.712212102001476E-6|
|7.071764823529412E-6|
|1.015495466386981E-5|
|6.576354146362592...|
| 5.90145296180676E-6|
|8.547679455011844E-6|
|8.420709512685392E-6|
|1.041448341728929...|
|8.316075414862431E-6|
|9.721183814992126E-6|
|8.029436027707578E-6|
|6.307432259386365E-6|
+--------------------+
only showing top 20 rows


## What day had the Peak High in Price?
 
df.orderBy(df['High'].desc()).select(['Date']).head(1)[0]['Date']
Out[142]: datetime.datetime(2015, 1, 13, 0, 0)


## What is the mean of the Close column?
 
from pyspark.sql.functions import mean
df.select(mean('Close')).show()
+-----------------+
|       avg(Close)|
+-----------------+
|72.38844998012726|
+-----------------+



## What is the max and min of the Volume column?
 
from pyspark.sql.functions import min, max
df.select(max('Volume'),min('Volume')).show()
+-----------+-----------+
|max(Volume)|min(Volume)|
+-----------+-----------+
|   80898100|    2094900|
+-----------+-----------+


## How many days was the Close lower than 60 dollars?
 
df.filter(df['Close'] < 60).count()
Out[145]: 81


## What percentage of the time was the High greater than 80 dollars ?
## In other words, (Number of Days High>80)/(Total Days in the dataset)
 
df.filter('High > 80').count() * 100/df.count()
Out[146]: 9.141494435612083


## What is the max High per year
 
from pyspark.sql.functions import year
from pyspark.sql.functions import to_date
 
year_df = df.withColumn('Year', year(df['Date']))
year_df.groupBy('Year').max()['Year', 'max(High)'].show()
+----+---------+
|Year|max(High)|
+----+---------+
|2015|90.970001|
|2013|81.370003|
|2014|88.089996|
|2012|77.599998|
|2016|75.190002|
+----+---------+


## What is the average Close for each Calendar Month?
## In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your
## result will have a value for each of these months.
 
### Create a new column Month from existing Date column
month_df = df.withColumn('Month', month(df['Date']))
### Group by month and take average of all other columns
month_df = month_df.groupBy('Month').mean()
### Sort by month
month_df = month_df.orderBy('Month')
### Display only month and avg(Close), the desired columns
month_df['Month', 'avg(Close)'].show()
+-----+-----------------+
|Month|       avg(Close)|
+-----+-----------------+
|    1|71.44801958415842|
|    2|  71.306804443299|
|    3|71.77794377570092|
|    4|72.97361900952382|
|    5|72.30971688679247|
|    6| 72.4953774245283|
|    7|74.43971943925233|
|    8|73.02981855454546|
|    9|72.18411785294116|
|   10|71.57854545454543|
|   11| 72.1110893069307|
|   12|72.84792478301885|
+-----+-----------------+


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
from pyspark.sql.window import Window
from pyspark.sql.functions import isnan, when, count, col, desc, row_number

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


sc = spark.sparkContext
#sqlContext = SparkConf().setAppName("the apache sparksql")

path = "../training_data/price/Nov_22_28_minute.csv"
      
df = spark.read.format("csv") \
            .option("inferSchema",'True') \
            .option("header",True) \
            .load(path)

df2 = df.withColumn("Date", df["Date"].substr(1, 16))


df3 = df2.withColumn("Date", F.regexp_replace('Date', ' |:', '-')).drop("_c0")
df3.show()


w = Window().orderBy(desc("Date"))
df4withnull = (df3.withColumn("Futureprice", F.lag("Close").over(w)))
df4withnull.show()

df4 = df4withnull.where(df4withnull['Date'] != '2021-11-29-00-00')
df4.show()

#check null
df4.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df4.columns]).show()

#head -500 BTCUSD_1hr.csv | grep "2021-11-29"
#1638144000000,2021-11-29 00:00:00,BTCUSD,57335.37,57999.0,57187.77,57815.97,78.568040748
#future_price = 57815.97

#df4_fill = df4.na.fill(future_price)
#df4_fill.show()

w = Window().orderBy(desc("Date"))
df5 = df4.withColumn("id", row_number().over(w))
#df5.show()

df5.createOrReplaceTempView("trainingdata")

results = spark.sql("select id, Date, Open, High, Low, Close, Volume, Futureprice from trainingdata")
results.show()

results.write.option("header", "true").mode("overwrite").csv("../training_data/price/processed_Nov_22_28_minute")

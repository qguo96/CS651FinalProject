from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from graphframes import *
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import pandas as pd

# Commend to run this
# spark-submit --master local[*] --packages graphframes:graphframes:0.8.2-spark3.0-s_2.12
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

#address_path = "addrsmulti.csv"
address_path = "work/addrs.csv"

#payer_address, payee_address, value, tx_hash, time
address_schema = StructType() \
      .add("payer_address",StringType(),True) \
      .add("payee_address",StringType(),True) \
      .add("value",	DoubleType(),True) \
      .add("tx_hash",StringType(),True) \
      .add("time",StringType(),True)
      
df_address = spark.read.format("csv") \
        .option("delimiter", ",") \
      .option("header", True) \
      .schema(address_schema) \
      .load(address_path)


df2 = df_address.withColumn("time", df_address["time"].substr(1, 16))
df3 = df2.withColumn("time", F.regexp_replace('time', 'T|:', '-'))

#df3.show()


# Creates a temporary view using the DataFrame
#df3.createOrReplaceTempView("address")
address = df3.createGlobalTempView('address')

dfwithalladress = spark.sql("(select time, payer_address as person from global_temp.address) union (select time, payee_address as person from global_temp.address)")
results = dfwithalladress.groupBy("person").agg(F.min("time")).\
            withColumnRenamed("min(time)", "time").groupBy("time").\
            agg(F.count("person")).withColumnRenamed("count(person)", "new_address")

results.coalesce(1).write.option("header", "true").mode("overwrite").csv("count_new_address")
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

table1 = "mean_degree.csv"
table2 = "transactionInfo.csv"

df1 = spark.read.format("csv") \
            .option("inferSchema",'True') \
            .option("header",True) \
            .load(table1)

df1.createOrReplaceTempView("T1")

df2 = spark.read.format("csv") \
            .option("inferSchema",'True') \
            .option("header",True) \
            .load(table2)

df2.createOrReplaceTempView("T2")


results = spark.sql("select T1.Date, mean_degree, num_tx, mean_tx_value from T1 inner join T2 on T1.Date = T2.Date")
results.show()

results.write.option("header", "true").mode("overwrite").csv("merged_info")
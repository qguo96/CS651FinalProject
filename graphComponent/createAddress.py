from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from graphframes import *
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext
#sqlContext = SparkConf().setAppName("the apache sparksql")

address_path = "addrs.csv"

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

#df_address.show()
# address with day
df2 = df_address.withColumn("time", df_address["time"].substr(1, 10))

# Creates a temporary view using the DataFrame
df2.createOrReplaceTempView("address")


# SQL can be run over DataFrames that have been registered as a table.
nodes = spark.sql("(select payer_address as id from address) union (select payee_address as id from address)")

#nodes.show()
#nodes_payer = spark.sql("select payer_address as src, payee_address as dst, time as relationship from address")

edges = spark.sql("select payer_address as src, payee_address as dst, time as relationship from address")

g_all = GraphFrame(nodes, edges)


inDegree = g_all.inDegrees

degrees = g_all.degrees.show()
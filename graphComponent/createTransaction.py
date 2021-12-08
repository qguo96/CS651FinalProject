from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext


transaction_path = "transactions.csv"

#transactions.csv	tx_hash tx_value, timestamp, num_inputs, num_outputs
transaction_schema = StructType() \
      .add("tx_hash",StringType(),True) \
      .add("tx_value",IntegerType(),True) \
      .add("timestamp",StringType(),True) \
      .add("num_inputs",IntegerType(),True) \
      .add("num_outputs",IntegerType(),True)
      
df_transaction = spark.read.format("csv") \
        .option("delimiter", ",") \
      .option("header", True) \
      .schema(transaction_schema) \
      .load(transaction_path)

#df_transaction.show()

df2= df_transaction.withColumn("timestamp", df_transaction["timestamp"].substr(1, 10))
df2.show()


df2.count()
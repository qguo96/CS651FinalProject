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
#sqlContext = SparkConf().setAppName("the apache sparksql")

address_path = "addrs copy.csv"
#address_path = "addrs.csv"

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
df2 = df_address.withColumn("time", df_address["time"].substr(1, 13))
df3 = df2.withColumn("time", F.regexp_replace('time', 'T', '-'))


# Creates a temporary view using the DataFrame
df3.createOrReplaceTempView("address")


#something new
dfwithalladress = spark.sql("(select time, payer_address as person from address) union (select time, payee_address as person from address)")
#dfwithalladress.show()
dfwithalladress.createOrReplaceTempView("allperson")

dfoldtime = spark.sql("select min(time) as time, person from allperson group by person")
#dfoldtime.show()
dfoldtime.createOrReplaceTempView("tableWithOldTime")
dfnewaddress = spark.sql("select time, count(person) as newAddress from tableWithOldTime group by time")
dfnewaddress.createOrReplaceTempView("newaddress")
#dfnewaddress.show()

unique_time = spark.sql("select distinct time from address")
unique_time.show()
time_list = unique_time.select(F.collect_set('time').alias('time')).first()['time'] 

time_degree_dict = {i:0 for i in time_list}

print(time_degree_dict)
# SQL can be run over DataFrames that have been registered as a table.
nodes = spark.sql("(select payer_address as id from address) union (select payee_address as id from address)")


#nodes.show()
edges = spark.sql("select payer_address as src, payee_address as dst, time as relationship from address")

g_all = GraphFrame(nodes, edges)

for each_time_frame in time_list:
      edge_condition = "relationship = '" + each_time_frame + "'"
      #print(edge_condition)
      mean_degree_each_time = g_all.filterEdges(edge_condition).dropIsolatedVertices().degrees.select(F.avg("degree")).collect()[0][0]
      time_degree_dict[each_time_frame] = mean_degree_each_time

df = pd.DataFrame(time_degree_dict.items(), columns=['Date', 'mean_degree'])

value = df.values.tolist()

column = list(df.columns)

spark_df = spark.createDataFrame(value,column)

spark_df.createOrReplaceTempView("meanDegree")

results = spark.sql("select Date, mean_degree, newAddress from meanDegree left join newaddress on meanDegree.Date = newaddress.time").toPandas()

#print(type(results))
#print(results)
results.to_csv("mean_degree.csv", index=False)
#results.write.option("header", "true").mode("overwrite").csv("mean_degree.csv")

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler,StandardScaler
from pyspark.ml.regression import LinearRegression, GeneralizedLinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import isnan, when, count, col, desc
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

# Python
import numpy as np
import pandas as pd
from itertools import product

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("simple") \
    .getOrCreate()

sc = spark.sparkContext

filename = "../training_data/price/processed_Nov_22_28_minute.csv"
dataset = spark.read.format("csv") \
          .option("inferSchema",'True') \
          .option("header",True) \
          .load(filename)

# Check the num of partitions
dataset.rdd.getNumPartitions()

#check null
dataset.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dataset.columns]).show()

feature_cols = dataset.columns 
non_feature_cols  = ['id', 'Date',"Futureprice"]
[feature_cols.remove(non_feature) for non_feature in non_feature_cols]

def trainSplit(dataSet, proportion):
    records_num = dataset.count()
    split_point = round(records_num * proportion)
    
    train_data = dataset.filter(F.col("id") < split_point)
    test_data = dataset.filter(F.col("id") >= split_point)
    
    return (train_data,test_data)

proportion = 0.9
train_data,test_data = trainSplit(dataset, proportion)
train_data.cache()
test_data.cache()

# Form a column contains all the features - VectorAssembler
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
#standard_scaler = StandardScaler(inputCol="words", outputCol="features", withStd=True, withMean=False)
#lr = LinearRegression(featuresCol="features",labelCol="Futureprice",maxIter=5)
#rf = RandomForestRegressor(featuresCol="features", labelCol="Futureprice")
glr = GeneralizedLinearRegression(featuresCol="features", labelCol="Futureprice")
#dt = DecisionTreeRegressor(featuresCol="features", labelCol="Futureprice")
#pipeline = Pipeline(stages=[vector_assembler, lr])
#gbt = GBTRegressor(featuresCol="features", labelCol="Futureprice")
pipeline = Pipeline(stages=[vector_assembler, glr])
#pipeline_model = pipeline.fit(train_data)

"""
paramGrid = ParamGridBuilder()\
    .addGrid(lr.regParam, np.arange(0,1,0.2).round(decimals=2)) \
    .addGrid(lr.fitIntercept, [False, True])\
    .addGrid(lr.elasticNetParam, np.arange(0,1,0.2).round(decimals=2))\
    .build()
"""
"""
paramGrid = ParamGridBuilder()\
    .addGrid(rf.numTrees, [3, 5, 10, 20, 30]) \
    .addGrid(rf.maxDepth, [3, 5, 10]) \
    .build()
"""

paramGrid = ParamGridBuilder()\
    .addGrid(glr.maxIter, [5, 10, 50, 80]) \
    .addGrid(glr.regParam, [0, 0.1, 0.2]) \
    .addGrid(glr.family, ["gaussian", "gamma"]) \
    .addGrid(glr.link, ["identity", "inverse"]) \
    .build()

"""
paramGrid = ParamGridBuilder()\
    .addGrid(dt.maxDepth, [3, 5, 10]) \
    .addGrid(dt.minInstancesPerNode, [1, 3, 5]) \
    .build()
"""
"""
paramGrid = ParamGridBuilder()\
    .addGrid(gbt.maxIter, [20, 40, 60]) \
    .addGrid(gbt.maxDepth, [5, 8, 10]) \
    .addGrid(gbt.stepSize, [0.1, 0.3, 0.5, 0.7]) \
    .build()
"""

# In this case the estimator is simply the linear regression.
# A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
tvs = TrainValidationSplit(estimator=pipeline,
                           estimatorParamMaps=paramGrid,
                           evaluator=RegressionEvaluator(labelCol="Futureprice", metricName='rmse')
                           trainRatio=0.8)
# Make predictions
pipeline_model = tvs.fit(train_data)

#pipeline_model = pipeline_model2.bestModel
predictions = pipeline_model.transform(test_data)
predictions.select("Date","Futureprice","prediction").show(5)


# Compute test error
rmse_evaluator = RegressionEvaluator(labelCol="Futureprice", predictionCol="prediction", metricName='rmse')
mae_evaluator = RegressionEvaluator(labelCol="Futureprice", predictionCol="prediction", metricName='mae')
mse_evaluator = RegressionEvaluator(labelCol="Futureprice", predictionCol="prediction", metricName='mse')
r2_evaluator = RegressionEvaluator(labelCol="Futureprice", predictionCol="prediction", metricName='r2')
var_evaluator = RegressionEvaluator(labelCol="Futureprice", predictionCol="prediction", metricName='var')

rmse = rmse_evaluator.evaluate(predictions)
mae = mae_evaluator.evaluate(predictions)
mse = mse_evaluator.evaluate(predictions)
var = var_evaluator.evaluate(predictions)
r2 = r2_evaluator.evaluate(predictions)

results = {
    "Model": "Linear Regression",
    "Proportion": proportion,
    "RMSE": rmse,
    "MAE": mae,
    "MSE": mse,
    "Variance": var,
    "R2": r2,
    
}

print(results)
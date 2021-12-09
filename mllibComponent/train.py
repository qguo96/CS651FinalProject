from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler,StandardScaler
from pyspark.ml.regression import LinearRegression, GeneralizedLinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import isnan, when, count, col, desc

# Python
import numpy as np
import pandas as pd
from itertools import product
#import time

# Graph packages
# https://plotly.com/python/getting-started/#jupyterlab-support
# https://plotly.com/python/time-series/
#import plotly.express as px

# Scikit-learn
#from sklearn.metrics import mean_absolute_percentage_error


# Start a SparkSession
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Bitcoin Prediction") \
    .getOrCreate()

sc = spark.sparkContext

filename = "../training_data/price/processed_Nov_22_28.csv"

dataset = spark.read.format("csv") \
          .option("inferSchema",'True') \
          .option("header",True) \
          .load(filename)

dataset.printSchema()
"""
root
 |-- Date: string (nullable = true)
 |-- Open: double (nullable = true)
 |-- High: double (nullable = true)
 |-- Low: double (nullable = true)
 |-- Close: double (nullable = true)
 |-- Volume: double (nullable = true)
 |-- Futureprice: double (nullable = true)
"""

# Check the num of partitions
dataset.rdd.getNumPartitions()

#check null
dataset.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dataset.columns]).show()

# Total rows
print(dataset.count()) #168

#2. Feature Engineering
# labels and features
feature_cols = dataset.columns 
# Gain the column list of features
non_feature_cols  = ['id', 'Date',"Futureprice"]
[feature_cols.remove(non_feature) for non_feature in non_feature_cols]

print(feature_cols)

# Form a column contains all the features - VectorAssembler
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

#2.2. Feature Standardization
# Standardize the "features" column
# Use this column "scaledFeatures" if the algorithm didn't have the build-in param: Standardization
standard_scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)

def trainSplit(dataSet, proportion):
    records_num = dataset.count()
    split_point = round(records_num * proportion)
    
    train_data = dataset.filter(F.col("id") < split_point)
    test_data = dataset.filter(F.col("id") >= split_point)
    
    return (train_data,test_data)

# Split the dataSet: Train(70%), test(30%)
proportion = 0.7
train_data,test_data = trainSplit(dataset, proportion)

# Cache it
train_data.cache()
test_data.cache()

# Number of train and test dataSets
print(f"Training data: {train_data.count()}\nTest data: {test_data.count()}")

# Train a LinearRegression model
lr = LinearRegression(featuresCol="features",labelCol="Futureprice",maxIter=5, regParam=0.0, elasticNetParam=0.8)

# Chain assembler and forest in a Pipeline
pipeline = Pipeline(stages=[vector_assembler, lr])
pipeline_model = pipeline.fit(train_data)

# Make predictions
predictions = pipeline_model.transform(test_data)
predictions.select("Date","Futureprice","prediction").show(5)


# Compute test error
rmse_evaluator = RegressionEvaluator(labelCol="Futureprice", predictionCol="prediction", metricName='rmse')
mae_evaluator = RegressionEvaluator(labelCol="Futureprice", predictionCol="prediction", metricName='mae')
r2_evaluator = RegressionEvaluator(labelCol="Futureprice", predictionCol="prediction", metricName='r2')
var_evaluator = RegressionEvaluator(labelCol="Futureprice", predictionCol="prediction", metricName='var')

rmse = rmse_evaluator.evaluate(predictions)
mae = mae_evaluator.evaluate(predictions)
var = var_evaluator.evaluate(predictions)
r2 = r2_evaluator.evaluate(predictions)

#discard
#predictions_pd = predictions.select("Futureprice","prediction").toPandas()
#mape = mean_absolute_percentage_error(predictions_pd["Futureprice"], predictions_pd["prediction"])

# Adjusted R-squared
n = predictions.count()
p = len(predictions.columns)
adj_r2 = 1-(1-r2)*(n-1)/(n-p-1)

# Use dict to store each result
results = {
    "Model": "Linear Regression",
    "Proportion": proportion,
    "RMSE": rmse,
    #"MAPE": mape,
    "MAE": mae,
    "Variance": var,
    "R2": r2,
    "Adjusted_R2": adj_r2,
}

print(results)


'''
Description: Use Grid Search to tune the Model 
Args:
    dataSet: The dataSet which needs to be splited
    proportion_lst: A list represents the split proportion
    feature_col: The column name of features
    label_col: The column name of label
    ml_model: The module to use
    params: Parameters which want to test 
    assembler: An assembler to dataSet
    scaler: A scaler to dataSet
Return: 
    results_df: The best result in a pandas dataframe
'''
def autoTuning(dataSet, proportion_lst, feature_col, label_col, ml_model, params, assembler, scaler):
    
    # Initialize the best result for comparison
    result_best = {"RMSE": float('inf')}
    
    # Try different proportions 
    for proportion in proportion_lst:
        # Split the dataSet
        train_data,test_data = trainSplit(dataSet, proportion)
    
        # Cache it
        train_data.cache()
        test_data.cache()
    
        # ALL combination of params
        param_lst = [dict(zip(params, param)) for param in product(*params.values())]
    
        for param in param_lst:
            # Chosen Model
            if ml_model == "LinearRegression":
                model = LinearRegression(featuresCol=feature_col, \
                                         labelCol=label_col, \
                                         maxIter=param['maxIter'], \
                                         regParam=param['regParam'], \
                                         elasticNetParam=param['elasticNetParam'])
            
            elif ml_model == "GeneralizedLinearRegression":
                model = GeneralizedLinearRegression(featuresCol=feature_col, \
                                                    labelCol=label_col, \
                                                    maxIter=param['maxIter'], \
                                                    regParam=param['regParam'], \
                                                    family=param['family'], \
                                                    link=param['link'])
            
            elif ml_model == "DecisionTree":
                model = DecisionTreeRegressor(featuresCol=feature_col, \
                                              labelCol=label_col, \
                                              maxDepth = param["maxDepth"], \
                                              seed=0)
            
            elif ml_model == "RandomForest":
                model = RandomForestRegressor(featuresCol=feature_col, \
                                              labelCol=label_col, \
                                              numTrees = param["numTrees"], \
                                              maxDepth = param["maxDepth"], \
                                              seed=0)
            
            elif ml_model == "GBTRegression":
                model = GBTRegressor(featuresCol=feature_col, \
                                     labelCol=label_col, \
                                     maxIter = param['maxIter'], \
                                     maxDepth = param['maxDepth'], \
                                     stepSize = param['stepSize'], \
                                     seed=0)
            
            # Chain assembler and model in a Pipeline
            pipeline = Pipeline(stages=[assembler, model])
            # Train a model and calculate running time
            #start = time.time()
            pipeline_model = pipeline.fit(train_data)
            #end = time.time()

            # Make predictions
            predictions = pipeline_model.transform(test_data)

            # Compute test error by several evaluators
            # https://spark.apache.org/docs/3.1.1/mllib-evaluation-metrics.html#regression-model-evaluation
            # https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/ml/evaluation/RegressionEvaluator.html
            rmse_evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName='rmse')
            mae_evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName='mae')
            r2_evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName='r2')
            var_evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName='var')
            
            #predictions_pd = predictions.select("NEXT_BTC_CLOSE","prediction").toPandas()
            #mape = mean_absolute_percentage_error(predictions_pd["NEXT_BTC_CLOSE"], predictions_pd["prediction"])
            
            rmse = rmse_evaluator.evaluate(predictions)
            mae = mae_evaluator.evaluate(predictions)
            var = var_evaluator.evaluate(predictions)
            r2 = r2_evaluator.evaluate(predictions)
            # Adjusted R-squared
            n = predictions.count()
            p = len(predictions.columns)
            adj_r2 = 1-(1-r2)*(n-1)/(n-p-1)
        
            # Use dict to store each result
            results = {
                "Model": ml_model,
                "Proportion": proportion,
                "Parameters": [list(param.values())],
                "RMSE": rmse,
                #"MAPE":mape,
                "MAE": mae,
                "Variance": var,
                "R2": r2,
                "Adjusted_R2": adj_r2,
                #"Time": end - start,
                "Predictions": predictions.select("Futureprice","prediction",'Date') 
            }
            
            # Only store the lowest RMSE
            if results['RMSE'] < result_best['RMSE']:
                result_best = results
                
        # Release Cache
        train_data.unpersist()
        test_data.unpersist()
        
    # Transform dict to pandas dataframe
    results_df = pd.DataFrame(result_best)
    return results_df

# Split proportion list
proportion_lst = [0.6, 0.7, 0.8, 0.9]

lr_params = {
    'maxIter' : [5, 10, 50, 80, 100], # max number of iterations (>=0), default:100
    'regParam' : np.arange(0,1,0.2).round(decimals=2),# regularization parameter (>=0), default:0.0
    'elasticNetParam' : np.arange(0,1,0.2).round(decimals=2) # the ElasticNet mixing parameter, [0, 1], default:0.0 
}
result_lr = autoTuning(dataset, proportion_lst, "features", "Futureprice", "LinearRegression", lr_params, vector_assembler ,standard_scaler)
print(result_lr)
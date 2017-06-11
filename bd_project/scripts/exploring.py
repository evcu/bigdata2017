import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
import atexit
import datetime
import sys
from csv import reader
from operator import add
from validation_utils import *
from myUtils import *
from pyspark.sql.functions import udf

from pyspark.mllib.regression import LabeledPoint
import numpy as np
from pyspark.sql.functions import rand
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import StandardScaler, StandardScalerModel
from pyspark.mllib.util import MLUtils

'''
# logistic regression
from pyspark.mllib.classification import LogisticRegressionWithLBFGS 
from sklearn.metrics import roc_curve,auc
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.evaluation import MulticlassMetrics
'''

# random forest
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.evaluation import MulticlassMetrics

# grad boost treees
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel

sc = SparkContext()
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")

fields = getFieldDic()
timestart = datetime.datetime.now()
(taxi_data,prefix) = readFiles2({2016:range(1,13),2015:range(1,13),2014:range(1,13),2013:range(1,13)},sc)

taxi_data = cleanByFields(taxi_data, ['tpep_pickup_datetime', tpep_dropoff_datetime])

taxi_train_df = sqlContext.createDataFrame (taxi_data,('vendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'rateCodeID', 'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount','extra','mta_tax','improvement_surcharge','tip_amount','tolls_amount','total_amount'))


def time_delta(pickup, dropoff): 
    end = datetime.datetime.strptime(dropoff, '%Y-%m-%dT%H:%M:%S.%f')
    start = datetime.datetime.strptime(pickup, '%Y-%m-%dT%H:%M:%S.%f')
    delta = (end-start).total_seconds()
    hour = start.hour
    return delta

def getHour(value):
    given_date = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    hour = given_date.hour
    return hour

data_tripTime = udf(time_delta, IntegerType())
taxi_train_df = taxi_train_df.withColumn("trip_time", data_tripTime(taxi_train_df.tpep_pickup_datetime, taxi_train_df.tpep_dropoff_datetime))

data_getHour = udf(getHour, IntegerType())
taxi_train_df = taxi_train_df.withColumn("pickup_hour", data_getHour(taxi_train_df.tpep_pickup_datetime)) 

'''
def getHour(value):
    given_date = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    hour = given_date.hour
    return hour

data_pickupHour = udf(getHour, IntegerType())
taxi_train_new_df = taxi_train_df.withColumn("pickup_hour", data_pickupHour(taxi_train_df"tpep_pickup_datetime"))
taxi_train_new_df.show()
'''

taxi_df_train_cleaned = taxi_train_df.drop('store_and_fwd_flag').drop('tpep_pickup_datetime').drop('tpep_dropoff_datetime').drop('pickup_longitude').drop('pickup_latitude').drop('dropoff_latitude').drop('dropoff_longitude').drop('total_amount').drop('tolls_amount').drop('mta_tax').drop('improvement_surcharge').filter("passenger_count > 0 and passenger_count < 10 AND payment_type in ('CSH', 'CRD') AND tip_amount >= 0 AND tip_amount < 200 AND fare_amount >= 1 AND fare_amount < 300 AND trip_distance > 0 AND trip_distance < 100" )

taxi_df_train_cleaned.cache()
taxi_df_train_cleaned.count()


#####
'''
sqlStatement = """
    SELECT *,
    CASE
    WHEN (pickup_hour >=2 AND pickup_hour <= 5) THEN "Late Night" 
    WHEN (pickup_hour >= 6 AND pickup_hour <= 11) THEN "AMRush" 
    WHEN (pickup_hour >= 12 AND pickup_hour <= 16) THEN "Afternoon"
    WHEN (pickup_hour >= 17 AND pickup_hour <= 21) THEN "PMRush"
    WHEN (pickup_hour >= 22 OR pickup_hour <= 1 ) THEN "Night"
    END as trafficTime_bins
    FROM taxi_train 
"""
taxi_df_train_with_newFeatures = sqlContext.sql(sqlStatement)


#spark.sql("select store_and_fwd_flag,count(*) from yellow_taxi group by Store_and_fwd_flag")
#spark.sql("select")

# cache away
taxi_df_train_with_newFeatures.cache()
taxi_df_train_with_newFeatures.count()
'''

## BUILD CATEGORICAL FEATURES ##    
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer

stringIndexer = StringIndexer(inputCol="vendorID", outputCol="vendor_index")
model = stringIndexer.fit(taxi_df_train_cleaned) 

indexed = model.transform(taxi_df_train_cleaned)
encoder = OneHotEncoder(dropLast=False, inputCol="vendor_index", outputCol="vendor_vec")
encoded1 = encoder.transform(indexed)

stringIndexer = StringIndexer(inputCol="rate_code", outputCol="rate_index")
model = stringIndexer.fit(encoded1)
indexed = model.transform(encoded1)
encoder = OneHotEncoder(dropLast=False, inputCol="rate_index", outputCol="rate_vec")
encoded2 = encoder.transform(indexed)

stringIndexer = StringIndexer(inputCol="payment_type", outputCol="payment_index")
model = stringIndexer.fit(encoded2)
indexed = model.transform(encoded2)
encoder = OneHotEncoder(dropLast=False, inputCol="payment_index", outputCol="payment_vec")
encoded3 = encoder.transform(indexed)

'''
stringIndexer = StringIndexer(inputCol="trafficTime_bins", outputCol="trafficTime_bins_index")
model = stringIndexer.fit(encoded3)
indexed = model.transform(encoded3)
encoder = OneHotEncoder(dropLast=False, inputCol="trafficTime_bins_index", outputCol="trafficTime_binsvec")
'''
encoded = encoder.transform(indexed)


# FUNCTIONS FOR BINARY CLASSIFICATION

def parseRowIndexingBinary(line):
    features = np.array([line.payment_index, line.vendor_index, line.rate_index, line.trafficTime_bins_index, line.pickup_hour, line.passenger_count, line.trip_time, line.trip_distance, line.fare_amount])
    labPt = LabeledPoint(line.tipped, features)
    return  labPt

def parseRowOneHotBinary(line):
    features = np.concatenate((np.array([line.pickup_hour, line.weekday, line.passenger_count, line.trip_time_in_secs, line.trip_distance, line.fare_amount]), line.vendorVec.toArray(), line.rateVec.toArray(), line.paymentVec.toArray(), line.TrafficTimeBinsVec.toArray()), axis=0)
    labPt = LabeledPoint(line.tipped, features)
    return  labPt

def parseRowIndexingRegression(line):
    features = np.array([line.paymentIndex, line.vendorIndex, line.rateIndex, line.TrafficTimeBinsIndex, line.pickup_hour, line.weekday, line.passenger_count, line.trip_time, line.trip_distance, line.fare_amount])

    labPt = LabeledPoint(line.tip_amount, features)
    return  labPt

def parseRowOneHotRegression(line):
    features = np.concatenate((np.array([line.pickup_hour, line.weekday, line.passenger_count, line.trip_time_in_secs, line.trip_distance, line.fare_amount]), line.vendorVec.toArray(), line.rateVec.toArray(), line.paymentVec.toArray(), line.TrafficTimeBinsVec.toArray()), axis=0)
    labPt = LabeledPoint(line.tip_amount, features)
    return  labPt


## SPLIT DATA ##

sample_size = 0.25; #test with sample of data
train_ = 0.75;
test_ = (1-train_);
#seed = 5767;
encoded_sample = encoded.sample(False, sample_size, seed=seed)

temp_rand = encoded_sample.select("*", rand(0).alias("rand"));
train_data, test_data = temp_rand.randomSplit([train_, test_], seed=seed);

indexed_train_bin = train_data.map(parseRowIndexingBinary)
indexed_test_bin = test_data.map(parseRowIndexingBinary)
oneHot_train_bin = train_data.map(parseRowOneHotBinary)
oneHot_test_bin = test_data.map(parseRowOneHotBinary)
indexed_train_reg = train_data.map(parseRowIndexingRegression)
indexed_test_reg = test_data.map(parseRowIndexingRegression)
oneHot_train_reg = train_data.map(parseRowOneHotRegression)
oneHot_test_reg = test_data.map(parseRowOneHotRegression)


## FEATURE SCALING ##

label = oneHot_train_reg.map(lambda x: x.label)
features = oneHot_train_reg.map(lambda x: x.features)
scaler = StandardScaler(withMean=False, withStd=True).fit(features)
data_temp_ = label.zip(scaler.transform(features.map(lambda x: Vectors.dense(x.toArray()))))
oneHot_train_reg_scaled = data_temp_.map(lambda x: LabeledPoint(x[0], x[1]))

label = oneHot_test_reg.map(lambda x: x.label)
features = oneHot_test_reg.map(lambda x: x.features)
scaler = StandardScaler(withMean=False, withStd=True).fit(features)
data_temp_ = label.zip(scaler.transform(features.map(lambda x: Vectors.dense(x.toArray()))))
oneHot_test_reg_scaled = data_temp_.map(lambda x: LabeledPoint(x[0], x[1]))


## CACHE-Y CACHE CACHE ##
indexed_train_bin.cache()
indexed_test_bin.cache()
oneHot_train_bin.cache()
oneHot_test_bin.cache()
indexed_train_reg.cache()
indexed_test_reg.cache()
oneHot_train_reg.cache()
oneHot_test_reg.cache()
oneHot_train_reg_scaled.cache()
oneHot_test_reg_scaled.cache()

'''
## LOGISTIC REGRESSION ##
logitModel = LogisticRegressionWithLBFGS.train(oneHot_train_bin, iterations=20, initialWeights=None, regParam=0.01, regType='l2', intercept=True, corrections=10, tolerance=0.0001, validateData=True, numClasses=2)

print("Coefficients: " + str(logitModel.weights))
print("Intercept: " + str(logitModel.intercept))

predictionAndLabels = oneHot_test_bin.map(lambda lp: (float(logitModel.predict(lp.features)), lp.label))
metrics = BinaryClassificationMetrics(predictionAndLabels)

print("Area under PR = %s" % metrics.areaUnderPR)
print("Area under ROC = %s" % metrics.areaUnderROC)

metrics = MulticlassMetrics(predictionAndLabels)

# Stats
precision = metrics.precision()
recall = metrics.recall()
f1Score = metrics.fMeasure()
print("Summary Stats")
print("Precision = %s" % precision)
print("Recall = %s" % recall)
print("F1 Score = %s" % f1Score)

## SAVE MODEL WITH DATE-STAMP
datestamp = unicode(datetime.datetime.now()).replace(' ','').replace(':','_');
logisticregressionfilename = "LogisticRegressionWithLBFGS_" + datestamp;
dirfilename = modelDir + logisticregressionfilename;
logitModel.save(sc, dirfilename);

# OUTPUT PROBABILITIES AND REGISTER TEMP TABLE
logitModel.clearThreshold();
predictionAndLabelsDF = predictionAndLabels.toDF()
predictionAndLabelsDF.registerTempTable("tmp_results");
'''

'''
## RANDOM FOREST ##

categoricalFeaturesInfo={0:2, 1:2, 2:6, 3:4}

rfModel = RandomForest.trainClassifier(indexed_train_bin, numClasses=2, categoricalFeaturesInfo=categoricalFeaturesInfo, numTrees=25, featureSubsetStrategy="auto",impurity='gini', maxDepth=5, maxBins=32)

print("Learned classification forest model:")
print(rfModel.toDebugString())

predictions = rfModel.predict(indexed_test_bin.map(lambda x: x.features))
predictionAndLabels = indexed_test_bin.map(lambda lp: lp.label).zip(predictions)

metrics = BinaryClassificationMetrics(predictionAndLabels)
print("Area under ROC = %s" % metrics.areaUnderROC)

datestamp = unicode(datetime.datetime.now()).replace(' ','').replace(':','_');
rfclassificationfilename = "RandomForestClassification_" + datestamp;
dirfilename = modelDir + rfclassificationfilename;

rfModel.save(sc, dirfilename);

# Convert to df
test_predictions = sqlContext.createDataFrame(predictionAndLabels)
test_predictions.registerTempTable("randomForest_results");
'''

## GRAD BOOSTED TREES ##

categoricalFeaturesInfo={0:2, 1:2, 2:6, 3:4}
gbtModel = GradientBoostedTrees.trainRegressor(indexed_train_reg, categoricalFeaturesInfo=categoricalFeaturesInfo, numIterations=10, maxBins=32, maxDepth = 4, learningRate=0.1)

predictions = gbtModel.predict(indexed_test_reg.map(lambda x: x.features))
predictionAndLabels = indexed_test_reg.map(lambda lp: lp.label).zip(predictions)

testMetrics = RegressionMetrics(predictionAndLabels)
print("RMSE = %s" % testMetrics.rootMeanSquaredError)
print("R-sqr = %s" % testMetrics.r2)

# Save model
datestamp = unicode(datetime.datetime.now()).replace(' ','').replace(':','_');
btregressionfilename = "GradientBoostingTreeRegression_" + datestamp;
dirfilename = modelDir + btregressionfilename;
gbtModel.save(sc, dirfilename)

# Convert to df
test_predictions = sqlContext.createDataFrame(predictionAndLabels)
test_predictions.registerTempTable("boostedTrees_results");

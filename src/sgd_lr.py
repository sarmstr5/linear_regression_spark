#!/usr/bin/python

#TODO choose appropriate attributes
#TODO decide if there are any necessary transformations
#TODO create pipeline
#TODO add crossvalidation, 80/20 10 folds
#TODO compare MSE and RMSE
#TODO run with L1
#TODO run with L2

#import findspark
#findspark.init()

# import spark stuff
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import Row

# import ml stuff
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import StandardScaler
from pyspark.ml import param

#import mllib
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
#from pyspark.mllib.feature import StandardScalar, StandardScalerModel

# import python stuff
from collections import defaultdict, Counter
import os
import csv
import sys
import pickle

# parse the data, convert str to floats and ints as appropriate
def create_df_from_rdd(line_list):
    # using longitude coordinates
    lo_dist = abs(float(line_list[5]) - float(line_list[7])) # double check
    # using latitude coordinates
    la_dist = abs(float(line_list[6]) - float(line_list[8]))
    # time of flight
    y = int(line_list[-1])
    return Row(flight_time=y, lat_dist=la_dist, long_dist=lo_dist)

# parse the data, convert str to floats and ints as appropriate
def parse_row_for_cv(line_list):
    # row_id, id, vendor_id, pickup_datetime, dropoff_datetime,
    # passenger_count, pickup_longitude, pickup_latitude, 
    # dropoff_longitude, dropoff_latitude, store_and_fwd_flag,
    # trip_duration
    long_dist = abs(float(line_list[6]) - float(line_list[8])) # double check
    lat_dist = abs(float(line_list[7]) - float(line_list[9]))
    y = int(line_list[-1])
    row_id = int(line_list[0])
    #not currently using but may need some of the value
    #x_values = [line_list[0], line_list[1], line_list[2],  line_list[3],  line_list[4],
    #          int(line_list[5]),  float(line_list[6]),  float(line_list[7]),
    #          float(line_list[8]), float(line_list[9]),  line_list[10]]

    return (row_id, [y, lat_dist, long_dist])

def save_rdd_to_disk(output_dir, output_fn, rdd):
    output_path = get_abs_file_path(output_dir, output_fn)
    rdd.saveAsTextFile(output_path)
    #save_default_dict(stripe_count_dict, output_path)
    #print("saving cooccurrence counts")

def get_reg_evals(predictions):
    metrics = RegressionMetrics(predictions)
    # mse, rmse, var
    return metrics.meanSquaredError, metrics.rootMeanSquaredError, metrics.explainedVariance

def build_model(rdd):
    k_folds = 10
    training_df = rdd.map(create_df_from_rdd)
    cv_step = [x / float(100) for x in range(1, 20, 5)]
    cv_batch_size = [x /float(10) for x in range(1, 10, 5)]
    regType= ["L1", "L2"]

    # lr model
    lr = LinearRegression(maxIter=10, regParam=0.3, solver='sgd')
    pipeline = Pipeline(stages=[lr])
    paramGrid = ParamGridBuilder() \
    #            .addGrid(lr.stepSize=cv_step) \
    #            .addGrid(lr.miniBatchFraction=cv_batch_size) \
    #            .addGrid(lr.updater=cv_batch_size)

    crossval = CrossValidator(\
        estimator=pipeline, \
        estimatorParamMaps=paramGrid, \
        evaluator=evaluator, \
        numFolds=k_folds)

    lm = crossval.fit(trainingData)
    predictions = lm.transform(test)
    predictions.show
    rmse = evaluator.evaluate(predictions)


    MSE = values_and_preds \
        .map(lambda x: (x[0] - x[1])**2) \
        .reduce(lambda x, y: x + y) / values_and_preds.count()
    results.append(MSE)

def cross_validate(rdd, k_folds, test_k):
    # take mod
    train = rdd.filter(lambda x: x[0] % k_folds != test_k) \
               .map(lambda x: LabeledPoint(x[1][0], (x[1][1:])))

    test = rdd.filter(lambda x: x[0] % k_folds == test_k)  \
               .map(lambda x: LabeledPoint(x[1][0], (x[1][1:])))
                    
    return train, test

def main():
    # input parameters
    if len(sys.argv) < 3:
        print("you didnt give directory inputs, using test file")
        input_dir = "test_input"
        input_fn = "processed_tiny.csv"
        input_file_path = os.path.join(input_dir, input_fn)
        #input_file_path = get_abs_file_path(input_dir, input_fn)
        output_fn="test"

    else:
        input_fn = sys.argv[1]
        output_fn = sys.argv[2]
        input_dir = "data"
        input_file_path = get_abs_file_path(input_dir, input_fn)

    # initialize spark
    conf = SparkConf().setMaster("local").setAppName("linear_regression.py")
    sc = SparkContext(conf = conf)

    #spark session alternative, might need to do findspark or change spark bin
    #spark = SparkSession.builder.master("local").appName("lr model") \
    #        .config("spark.executor.memory", "1gb").getorCreate()

    # read in file
    data = sc.textFile(input_file_path)

    # take out header
    header = data.first()
    data = data.filter(lambda x: x != header)

    # convert attributes to floats/ints and make key/values

    # attributes
    cv_step = [x / float(100) for x in range(1, 20, 5)]
    cv_batch_fraction = [x / float(100) for x in range(1, 11, 5)]
    regType= ["L1", "L2"]
    k_folds = 10

    mse_list = []
    step_list = []
    batch_fraction_list = []
    reg_type_type_list = []
    MSE_list = []

    parsed_rdd = data.map(parse_row_for_cv).persist()
    train_set_rdd, test_set_rdd = parsed_rdd.randomSplit([0.8, 0.2], seed=1234)
    train_set_rdd.persist()
    n_train = train_set_rdd.count()

    for step in cv_step:
        for i in range(kfolds):
            train_rdd, validate_rdd = train_set_rdd.map(cross_validate)

            # Build model
            lm = LinearRegressionWithSGD.train_rdd(train_rdd, iterations=10,
                                               step=step, miniBatchFraction=0.1,
                                               regParam=0.0, regType=None,
                                               intercept=True, validateData=True )

            # Evalute the model on training data
            values_and_preds = train_rdd.map(lambda x: (x.label, lm.predict(x.features)))
            # squares the error then adds all errors together divided by n
            MSE = values_and_preds \
                .map(lambda x: (x[0] - x[1])**2) \
                .reduce(lambda x, y: x + y) / n_train

            results.append(MSE)
            step_list.append(step)

    for i in zip(results,step_list):
        print("mean squar error = " + str(i))

    # Output results
    #output_dir = "output/spark"
    #save_rdd_to_disk(output_dir, output_fn, counts)
    #with open(output_fn, "w") as text_file:
    #    text_file.write(MSE)


if __name__ == "__main__":
    main()

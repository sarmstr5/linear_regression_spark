#!/usr/bin/python

#TODO choose appropriate attributes
#TODO decide if there are any necessary transformations
#TODO fix crossvalidation to be random

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
import numpy as np
import os
import csv
import sys
import pickle
import math
from itertools import izip, izip_longest
import time


# zipped to make rows - take ith element of each list
def train_results_to_disk(fn, zipped_metrics):
    with open(fn, 'a') as results:
        wr = csv.writer(results)
        for row in zipped_metrics:
            wr.writerow(row)

# this is a single row so no need to iterate
def test_results_to_disk(fn, metrics):
    with open(fn, 'a') as results:
        wr = csv.writer(results)
        wr.writerow(metrics)


# parse the data, convert str to floats and ints as appropriate
def parse_row_for_cv(line):
    #x_values = [line_list[0], line_list[1], line_list[2],  line_list[3],  line_list[4],
    #          int(line_list[5]),  float(line_list[6]),  float(line_list[7]),
    #          float(line_list[8]), float(line_list[9]),  line_list[10]]
    def get_params(line_list):
        long_dist = abs(float(line_list[6]) - float(line_list[8]))
        lat_dist = abs(float(line_list[7]) - float(line_list[9]))
        y = int(line_list[-1])
        row_id = int(line_list[0])
        return long_dist, lat_dist, y, row_id

    param_list = line.split(',')
    # row_id, id, vendor_id, pickup_datetime, dropoff_datetime,
    # passenger_count, pickup_longitude, pickup_latitude, 
    # dropoff_longitude, dropoff_latitude, store_and_fwd_flag,
    # trip_duration
    try:
        long_dist, lat_dist, y, row_id = get_params(param_list)
	
    # for some reason some of the files have utf-8	
    except ValueError:
        try:
            encoded_list = [str(i.encode('utf-8')) for i in param_list]
            long_dist, lat_dist, y, row_id = get_params(param_list)
		# that wasnt the issue
        except e:
            print("\n------------\n!!!!!!!!!********########\n---------------")
            print(line_list)
            print(e)

    return (row_id, [y, lat_dist, long_dist])

def get_lr_evals(predictions):
    metrics = RegressionMetrics(predictions)
    # MSE, RMSE, var, MAE
    return metrics.meanSquaredError, metrics.rootMeanSquaredError, \
            metrics.explainedVariance, metrics.meanAbsoluteError

def cv_split(rdd, k_folds, test_k):
    # use mod and row number to filter train and validation sets
	# if the row id mod k_folds equals the current validation fold add to validation fold
	# this has the issue that the same rows get grouped together
	# Helps that the data is randomly partitioned into test and train sets 
	# Can fix by randomizing the rows then adding row id in spark 
    train = rdd.filter(lambda x: x[0] % k_folds != test_k)
    test = rdd.filter(lambda x: x[0] % k_folds == test_k)
    return train, test

def convert_to_LabeledPoint(train_rdd, test_rdd):
	# LabeledPoints are needed for mllib linear regression model
	# rows are (row_id, [y, parameter_value1, parameter_value2...])
	# remove row id, no longer needed hence grabbing index 1
    train = train_rdd.map(lambda x: LabeledPoint(x[1][0], (x[1][1:])))
    test = test_rdd.map(lambda x: LabeledPoint(x[1][0], (x[1][1:])))
    return train, test

# result_tups == [(RMSEi, stepi, batch_fraci, reg_typei, reg_paramsi), ... ]
def get_best_params(min_RMSE, zipped_results):
    return [tup[1:] for tup in zipped_results if tup[0] == min_RMSE][0]

def evaluate_lm(train_set, test_set, step, batch_pct, reg, reg_param, iterations=100):
    # Evalute the model on training data
    lm = LinearRegressionWithSGD.train(train_set, iterations=iterations, \
                                       step=step, miniBatchFraction=batch_pct,\
                                       regType=reg, regParam=reg_param,\
                                       intercept=True, validateData=False )

    values_and_preds = test_set.map(lambda x: (x.label,
                                               float(lm.predict(x.features))))
    return get_lr_evals(values_and_preds)

def main():
    start_time = time.time()
    # input parameters
    # filenames not given use default
    if len(sys.argv) < 3:
        print("you didnt give directory inputs, using test file")
        input_dir = "test_input"
        input_fn = "tiny_processed"
        #input_fn = "thousand_processed"
        input_file_path = os.path.join(input_dir, input_fn+".csv")
        output_fn="test"

    # filenames given, assuming in hydra
    else:
        # expecting full filepath from bash
        input_fn = sys.argv[1]
        output_fn = sys.argv[2]
        input_dir = "data"
        input_file_path = os.path.join(input_dir, input_fn+".csv")
        print("\n________------------________\n")
        print(input_file_path)

    # initialize spark
    conf = SparkConf().setMaster("local").setAppName("linear_regression.py")
    sc = SparkContext(conf = conf)

    # read in file
    data = sc.textFile(input_file_path)

    # take out header
    header = data.first()
    data = data.filter(lambda x: x != header)

    # Optimization params
    SGD_run = True
    reg_run = True
    iterations = 100
    cv_step = [x / float(1000) for x in range(80, 120, 10)]

    # SGD params, how much of the data is looked at each step
	# range cant be done with floats so do range of ints then divide with list compression
    if SGD_run:
        #cv_batch_fraction = [x / float(100) for x in range(10, 21, 5)]
        cv_batch_fraction = [0.1]
    else:
        cv_batch_fraction = [1]

    # Regularization params
    if reg_run:
        cv_reg_param = [x / float(1000) for x in range(1, 500, 100)]
        regType= ["l1", "l2"]
    else:
        cv_reg_param = [0]
        regType= [None]
    
    # CV
    k_folds = 10

    # param lists
    steps, batch_fractions, reg_types, reg_params = [], [], [], []
	
	# metric lists
    MSE_results, RMSE_results, exp_vars, MAE_results = [], [], [], []
    MSE_avgs, RMSE_avgs, exp_var_avgs, MAE_avgs= [], [], [], []
    timings = []

    # returns ((row_id, [y, lat_dist, long_dist]), ...)
    parsed_rdd = data.map(parse_row_for_cv)
    # split rdd into train and test sets
    #train_set, test_set = parsed_rdd.randomSplit([0.8, 0.2], seed=1234)
    train_set, test_set = parsed_rdd.randomSplit([0.8, 0.2])
    train_set.cache()

    # run cross validation on linear regression model
    # SGD step (alpha), batch percent
#--------------------------------Start Grid Search-------------------------------------#
    for step in cv_step:
        for batch_pct in cv_batch_fraction:
            for reg in regType:
                for reg_param in cv_reg_param:
                    # Build model
                    cv_start = time.time()
                    for k in range(k_folds):
                        #-------------------Start of CV--------------------------#
                        # create CV sets
                        train_rdd, validate_rdd = cv_split(train_set, k_folds, k)
                        train_rdd, validate_rdd = convert_to_LabeledPoint(train_rdd,
                                                                        validate_rdd)
                        # find evaluation metrics
                        MSE, RMSE, exp_var, MAE = evaluate_lm(train_rdd, validate_rdd, 
                                                         step, batch_pct, reg,
                                                         reg_param, iterations)
						
						# store results
                        MSE_results.append(MSE)
                        RMSE_results.append(RMSE)
                        exp_vars.append(exp_var)
                        MAE_results.append(MAE)
                        #---------------------End of CV--------------------------#
 
                    # update eval lists
                    MSE_avgs.append(np.mean(MSE_results))
                    RMSE_avgs.append(np.mean(RMSE_results))
                    exp_var_avgs.append(np.mean(exp_vars))
                    MAE_avgs.append(np.mean(MAE_results))

                    # reset cv lists
                    MSE_results, RMSE_results, exp_vars = [], [], [] 

                    # update param lists
                    steps.append(step)
                    batch_fractions.append(batch_pct)
                    reg_types.append(reg)
                    reg_params.append(reg_param)

                    # update timings
                    timings.append(time.time() - cv_start) 
#--------------------------------End Grid Search-------------------------------------#

    # Finished Grid Search Cross Validation runs
    # save to disk
    fn = os.path.join("..","results","training_results.csv")
	
    # izip_longest to repeat file_path_name for each of the values
    train_results_to_disk(fn, izip_longest([input_fn], RMSE_avgs, 
                            MSE_avgs, steps, batch_fractions,
                            reg_types, reg_params, timings, MAE_avgs,
                            fillvalue=input_fn))

    # delete result lists to save RAM
    del timings, MSE_avgs, MSE_results, RMSE_results, exp_vars, MAE_avgs

    # next find best params
    min_train_RMSE = min(RMSE_avgs)

	# Create lazy zip to reduce memory
    # results == [(RMSEi, stepi, batch_fraci, reg_typei, reg_paramsi), ... ]
    zipped_results = izip(RMSE_avgs, steps, batch_fractions, reg_types, reg_params)
	
	# iterate through results and find the params with the min loss
    step, batch_pct, reg, reg_param = get_best_params(min_train_RMSE, zipped_results)

    # delete param lists to save RAM
    del RMSE_avgs, steps, batch_fractions, reg_types, reg_params

    # Now run tuned model vs test
    # convert RDDs to LabeledPoint RDDs to use with mllib, get lm eval metrics
    test_start = time.time()
    train_rdd, test_rdd = convert_to_LabeledPoint(train_set, test_set)
	
	# not sure if mllib caches rdd in the background or not
    train_rdd.cache()
    test_rdd.cache()
	
	# return test results
    MSE, RMSE, exp_var, MAE = evaluate_lm(train_rdd, test_rdd, step, batch_pct,
                                     reg, reg_param, iterations)

    # save test results to local disk
    fn = os.path.join("..","results", output_fn)
    test_results_to_disk(fn, (input_fn, MSE, RMSE, exp_var,
                              min_train_RMSE, step, batch_pct, reg,
                              reg_param, SGD_run, reg_run,
                              time.time() - test_start,
                              time.time() - start_time, MAE))

if __name__ == "__main__":
    main()

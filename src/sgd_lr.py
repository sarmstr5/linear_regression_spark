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
from itertools import izip, izip_longest
import time


# zipped to make rows - take ith element of each list
def train_results_to_disk(fn, zipped_metrics):
    with open(fn, 'a') as results:
        wr = csv.writer(results)
        for row in zipped_metrics:
            wr.writerow(row)

def test_results_to_disk(fn, metrics):
    with open(fn, 'a') as results:
        wr = csv.writer(results)
        wr.writerow(metrics)


# parse the data, convert str to floats and ints as appropriate
def parse_row_for_cv(line_list):
    line_list = line_list.split(',')
    # row_id, id, vendor_id, pickup_datetime, dropoff_datetime,
    # passenger_count, pickup_longitude, pickup_latitude, 
    # dropoff_longitude, dropoff_latitude, store_and_fwd_flag,
    # trip_duration
    try:
        long_dist = abs(float(line_list[6]) - float(line_list[8])) # double check
        lat_dist = abs(float(line_list[7]) - float(line_list[9]))
        y = int(line_list[-1])
        row_id = int(line_list[0])
    except ValueError:
        print("\n------------\n!!!!!!!!!********########\n---------------")
        print(line_list)
        raise ValueError

    #not currently using but may need some of the value
    #x_values = [line_list[0], line_list[1], line_list[2],  line_list[3],  line_list[4],
    #          int(line_list[5]),  float(line_list[6]),  float(line_list[7]),
    #          float(line_list[8]), float(line_list[9]),  line_list[10]]
    return (row_id, [y, lat_dist, long_dist])

def get_lr_evals(predictions):
    metrics = RegressionMetrics(predictions)
    # mse, rmse, var
    return metrics.meanSquaredError, metrics.rootMeanSquaredError, metrics.explainedVariance

def cv_split(rdd, k_folds, test_k):
    # use mod and row number to filter train and validation sets
    train = rdd.filter(lambda x: x[0] % k_folds != test_k)
    test = rdd.filter(lambda x: x[0] % k_folds == test_k)
    return train, test

def convert_to_LabeledPoint(train_rdd, test_rdd):
    # remove row id, no longer needed
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
        #input_fn = "tiny_processed"
        input_fn = "thousand_processed"
        input_file_path = os.path.join(input_dir, input_fn+".csv")
        #input_file_path = get_abs_file_path(input_dir, input_fn)
        output_fn="test"

    # filenames given, assuming in hydra
    else:
        # expecting full filepath from bash
        input_file_path = sys.argv[1]
        print("\n________------------________\n")
        print(input_file_path)
        output_fn = sys.argv[2]
        #input_file_path = os.path.join(input_dir, input_fn+".csv")

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
    reg_run = False
    iterations = 100
    cv_step = [x / float(100) for x in range(1, 10, 3)]

    #SGD params, how much of the data is looked at each step
    if SGD_run:
        cv_batch_fraction = [x / float(100) for x in range(1, 75, 25)]
    else:
        cv_batch_fraction = [1]

    # Regularization params
    if reg_run:
        cv_reg_param = [x / float(100) for x in range(1, 20, 5)]
        regType= ["l1", "l2"]
    else:
        cv_reg_param = [0]
        regType= [None]
    
    # CV
    k_folds = 10

    # metric lists
    steps, batch_fractions, reg_types, reg_params = [], [], [], []
    MSE_results, RMSE_results, exp_vars = [], [], [] 
    MSE_avgs, RMSE_avgs, exp_var_avgs = [], [], []
    timings = []

    # returns ((row_id, [y, lat_dist, long_dist]), ...)
    parsed_rdd = data.map(parse_row_for_cv)
    # split rdd into train and test sets
    train_set, test_set = parsed_rdd.randomSplit([0.8, 0.2], seed=1234)
    train_set.cache()

    # run cross validation on linear regression model
    # SGD step (alpha), batch percent
    for step in cv_step:
        for batch_pct in cv_batch_fraction:
            for reg in regType:
                for reg_param in cv_reg_param:
                    # Build model
                    cv_start = time.time()
                    for k in range(k_folds):
                        #----Start of CV----#
                        #print("on step and k_folds:{}\t{}\t{}\t{}".format(step, batch_pct, reg, k))

                        # create CV sets
                        train_rdd, validate_rdd = cv_split(train_set, k_folds, k)
                        train_rdd, validate_rdd = convert_to_LabeledPoint(train_rdd,
                                                                        validate_rdd)
                        # find evaluation metrics
                        MSE, RMSE, exp_var = evaluate_lm(train_rdd, validate_rdd, 
                                                         step, batch_pct, reg,
                                                         reg_param, iterations)

                        #SSE = evaluate_lm(train_rdd, validate_rdd, step,\
                        #                  batch_pct,  reg, reg_param, iterations)

                        #MSE = SSE / validate_rdd.count()
                        #RMSE = MSE**(0.5)
                        #exp_var = 0

                        MSE_results.append(MSE)
                        RMSE_results.append(RMSE)
                        exp_vars.append(exp_var)

                        #----End of CV----#

                    # update eval lists
                    MSE_avgs.append(np.mean(MSE_results))
                    RMSE_avgs.append(np.mean(RMSE_results))
                    exp_var_avgs.append(np.mean(exp_vars))

                    # reset cv lists
                    MSE_results, RMSE_results, exp_vars = [], [], [] 

                    # update param lists
                    steps.append(step)
                    batch_fractions.append(batch_pct)
                    reg_types.append(reg)
                    reg_params.append(reg_param)

                    # update timings
                    timings.append(time.time() - cv_start) 

    # Finished Grid Search Cross Validation runs
    # save to disk
    fn = os.path.join("..","results","training_results.csv")
    # izip longest to repeat file_path_name
    train_results_to_disk(fn, izip_longest([input_file_path], RMSE_avgs, 
                            MSE_avgs, steps, batch_fractions,
                            reg_types, reg_params, timings,
                            fillvalue=input_file_path))

    # delete result lists to save RAM
    del timings, MSE_avgs, MSE_results, RMSE_results, exp_vars 

    # next find best params, create lazy zip to reduce memory
    min_train_MSE = min(RMSE_avgs)

    # results == [(RMSEi, stepi, batch_fraci, reg_typei, reg_paramsi), ... ]
    zipped_results = izip(RMSE_avgs, steps, batch_fractions, reg_types, reg_params)
    step, batch_pct, reg, reg_param = get_best_params(min_train_MSE, zipped_results)

    # delete param lists to save RAM
    del RMSE_avgs, steps, batch_fractions, reg_types, reg_params

    # Now run tuned model vs test
    # convert RDDs to LabeledPoint RDDs to use with mllib, get lm eval metrics
    test_start = time.time()
    train_rdd, test_rdd = convert_to_LabeledPoint(train_set, test_set)
    train_rdd.cache()
    test_rdd.cache()
    MSE, RMSE, exp_var = evaluate_lm(train_rdd, test_rdd, step, batch_pct,
                                     reg, reg_param, iterations)

    # save test results to local disk
    fn = os.path.join("..","results", output_fn)
    test_results_to_disk(fn, (input_file_path, MSE, RMSE, exp_var,
                              min_train_MSE, step, batch_pct, reg,
                              reg_param, SGD_run, reg_run,
                              time.time() - test_start,
                              time.time() - start_time))

if __name__ == "__main__":
    main()

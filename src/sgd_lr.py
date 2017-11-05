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
import numpy as np
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
        print("\n------------\n!!!!!!!!!********########\n---------------")
        raise ValueError

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

def get_regr_evals(predictions):
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
    # use mod and row number to filter train and validation sets
    # remove row id, no longer needed
    train = rdd.filter(lambda x: x[0] % k_folds != test_k) \
               .map(lambda x: LabeledPoint(x[1][0], (x[1][1:])))

    test = rdd.filter(lambda x: x[0] % k_folds == test_k)  \
               .map(lambda x: LabeledPoint(x[1][0], (x[1][1:])))
    return train, test

def get_best_params(min_RMSE, result_tups):
    return [tup[1:] for tup in results_tups if tup[0] == min_RMSE]
    
def results_to_disk(fn, *argv):
    with open(fn, 'wr') as results:
        # zip to make rows - take ith element of each list
        for row in zip(*argv):
            results.write(",".join((str(col) for col in row)))

def evaluate_lm(train_set, test_set, step, batch_pct, reg, reg_param, iterations=100):
    # Evalute the model on training data
    lm = LinearRegressionWithSGD.train(train_set, iterations=iterations, \
                                       step=step, miniBatchFraction=batch_pct,\
                                       regType=reg, regParam=reg_param,\
                                       intercept=True, validateData=False )

    values_and_preds = test_set.map(lambda x: (x.label, lm.predict(x.features)))
    return get_regr_evals(values_and_preds)

    # Finds sum of squares, to calc MSE and RMSE
    #SSE = values_and_preds.map(lambda x: (x[0] - x[1])**2).reduce(lambda x, y: x + y) 
    #return SSE

def main():
    # input parameters
    # filenames not given use default
    if len(sys.argv) < 3:
        print("you didnt give directory inputs, using test file")
        input_dir = "test_input"
        input_fn = "thousand_processed.csv"
        input_file_path = os.path.join(input_dir, input_fn)
        #input_file_path = get_abs_file_path(input_dir, input_fn)
        output_fn="test"

    # filenames given
    else:
        input_fn = sys.argv[1]
        output_fn = sys.argv[2]
        input_dir = "data"
        input_file_path = os.path.join(input_dir, input_fn)

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


    # SGD params
    cv_step = [x / float(100) for x in range(1, 20, 3)]
    cv_batch_fraction = [x / float(100) for x in range(1, 100, 25)]
    cv_reg_param = [x / float(100) for x in range(1, 20, 5)]
    regType= ["l1", "l2"]
    iterations = 100
    
    # CV
    k_folds = 10

    # metric lists
    steps, batch_fractions, reg_types, reg_params = [], [], [], []
    MSE_results, RMSE_results, exp_vars = [], [], [] 
    MSE_avgs, RMSE_avgs, exp_var_avgs = [], [], []

    # returns ((row_id, [y, lat_dist, long_dist]), ...)
    parsed_rdd = data.map(parse_row_for_cv)
    # split rdd into train and test sets
    train_set, test_set = parsed_rdd.randomSplit([0.8, 0.2], seed=1234)
    train_set.persist()

    # run cross validation on linear regression model
    # SGD step (alpha), batch percent
    for step in cv_step:
        for batch_pct in cv_batch_fraction:
            for reg in regType:
                for reg_param in cv_reg_param:
                    # Build model
                    for k in range(k_folds):
                        #----Start of CV----#
                        #print("on step and k_folds:{}\t{}\t{}\t{}".format(step, batch_pct, reg, k))

                        # create CV sets
                        train_rdd, validate_rdd = cross_validate(train_set, k_folds, k)
                        validate_rdd.cache()
                        
                        # find evaluation metrics
                        MSE, RMSE, exp_var = evaluate_lm(train_rdd, validate_rdd, \
                                                         step, batch_pct, reg,
                                                         reg_param, iterations)

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

    # Finished Grid Search Cross Validation runs
    results_to_disk(RMSE_avgs, MSE_avgs, steps, batch_fractions, reg_types, reg_params)
    MSE_avgs, MSE_results, RMSE_results, exp_vars = None, None, None, None
    min_train_MSE = min(RMSE_avgs)

    step, batch_pct, reg, reg_param = find_best_params(min_train_MSE,\
                                                       zip(RMSE_avgs, steps,\
                                                            batch_fractions,\
                                                            reg_types, reg_params))

    # remove lists to reduce RAM
    RMSE_avgs, steps, batch_fractions, reg_types, reg_params = None, None, None, None, None

    MSE, RMSE, exp_var = evaluate_lm(train_set, test_set, step, batch_pct, \
                                     reg, reg_param, iterations)

    #MSE = SSE / test_set.count()
    #RMSE = MSE**(0.5)
    fn = "test_results.csv"
    results_to_disk(fn, MSE, RMSE, exp_var, min_train_MSE, step, batch_pct, reg, reg_param)

if __name__ == "__main__":
    main()

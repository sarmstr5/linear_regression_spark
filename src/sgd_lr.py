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
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.feature import StandardScalar, StandardScalerModel

# import python stuff
from collections import defaultdict, Counter
import os
import csv
import sys
import pickle

# parse the data, convert str to floats and ints as appropriate
def parse_row(line_list):
    # id, vendor_id, pickup_datetime, dropoff_datetime,
    # passenger_count, pickup_longitude, pickup_latitude, 
    # dropoff_longitude, dropoff_latitude, store_and_fwd_flag,
    # trip_duration
    long_dist = abs(float(line_list[5]) - float(line_list[7])) # double check
    lat_dist = abs(float(line_list[6]) - float(line_list[8]))
    y = int(line_list[-1])
    #not currently using but may need some of the value
    x_values = [line_list[0], line_list[1],  line_list[2],  line_list[3],
              int(line_list[4]),  float(line_list[5]),  float(line_list[6]),
              float(line_list[7]), float(line_list[8]),  line_list[9]]

    return LabeledPoint(y, (lat_dist, long_dist))

def save_rdd_to_disk(output_dir, output_fn, rdd):
    output_path = get_abs_file_path(output_dir, output_fn)
    rdd.saveAsTextFile(output_path)
    #save_default_dict(stripe_count_dict, output_path)
    #print("saving cooccurrence counts")

def cross_validate(rdd, train_percent=0.8, kfolds=10 ):
    for i in range(kfolds):
         hold_out = rdd.sample(False,1/kfolds)
         yield hold_out

def main():
    # input parameters
    if len(sys.argv) < 3:
        print("you didnt give directory inputs, using test file")
        input_dir = "test_input"
        input_fn = "tiny.csv"
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

    # read in file
    data = sc.textFile(input_file_path)

    # take out header
    header = data.first()
    data = data.filter(lambda x: x != header)

    # convert attributes to floats/ints and make key/values
    parsed_pair_rdd = data.map(parse_row)

    # attributes
    cv_step = [x / float(100) for x in range(1, 20, 5)]
    cv_batch_fraction = [x /float(10) for x in range(1, 10, 5)]
    regType= ["L1", "L2"]

    mse_list = []
    step_list = []
    batch_fraction_list = []
    reg_type_type_list = []
    MSE_list = []

    for step in cv_step:
        for i in range(kfolds):
            train_hold_out = cross_validate(parsed_pair_rdd)
            train = parsed_pair_rdd(lambda x: x not in train_hold_out)

            # Build model
            lm = LinearRegressionWithSGD.train(parsed_pair_rdd, iterations=10,
                                               step=step, miniBatchFraction=0.1,
                                               regParam=0.0, regType=None,
                                               intercept=True, validateData=True )

            # Evalute the model on training data
            values_and_preds = parsed_pair_rdd.map(lambda x: (x.label,
                                                              lm.predict(x.features)))
            MSE = values_and_preds \
                .map(lambda x: (x[0] - x[1])**2) \
                .reduce(lambda x, y: x + y) / values_and_preds.count()
            results.append(MSE)
    for i in zip(results,steps)
        print("mean squar error = " + str(results))
        fo

    # Output results
    #output_dir = "output/spark"
    #save_rdd_to_disk(output_dir, output_fn, counts)
    #with open(output_fn, "w") as text_file:
    #    text_file.write(MSE)


if __name__ == "__main__":
    main()

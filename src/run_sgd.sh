#!/bin/bash
fns='1000 10000 50000 100000 500000 1000000 1458645'
test_fns='test_input/thousand_processed.csv'
hdfs_dir='data/'
output_fn=test_results.csv
home_dir=$(eval echo "~$USER")
bash_time_output="$home_dir/hw3/results/time_trials.txt"

# might be better to save time results as a var then get append as row to csv
for fn in $fns
do
  echo "$hdfs_dir$fn"
  echo "#" $fn >> $bash_time_output
  { time spark-submit --master yarn --deploy-mode client sgd_lr.py "$hdfs$fn" $output_fn 2>&1; } 2>> $bash_time_output
done

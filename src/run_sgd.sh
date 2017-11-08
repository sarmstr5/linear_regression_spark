#!/bin/bash
fns='1000 10000 50000 100000 500000 1458645 1000000 '
test_fns='test_input/thousand_processed.csv'
hdfs_dir='data/'
output_fn=test_results.csv

home_dir=$(eval echo "~$USER")
time_file="$home_dir/hw3/results/time_trials.txt"
time_file2="$home_dir/hw3/results/time_trials2.txt"



# might be better to save time results as a var then get append as row to csv
for fn in $fns
do

  { /usr/bin/time -f "%e,%U,%S" spark-submit --master yarn --deploy-mode client sgd_lr.py "$hdfs$fn" $output_fn 2>&1; } 2>> $time_file2 
  printf "%s,%s\n" $fn >> $time_file

done

  #run_time=$({ /usr/bin/time -f "%e,%U,%S" sleep 1 2>&1;} )
# { time python $p_code $input_fn $output_fn 2>&1; } 2>> $bash_time_output

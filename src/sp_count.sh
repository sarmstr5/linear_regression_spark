#!/bin/bash
# run hadoop pairs process or time it

input_fn=sixteenth.csv
output_fn=sixteenth_run2
home_dir=$(eval echo "~$USER")

# input output dirs
input_path="$home_dir/programming/cs657/hw2/data/$input_fn"
#output_fn="$home_dir/hw2/output/spark/$input_fn"
bash_output_fn=pairs_hp_output.txt
bash_time_output=pairs_hp_time.txt

# python files
p_code="$home_dir/programming/cs657/hw2/src/spark_code/stripe_counts.py"

#wc
#local_path="/home/ubuntu/hw2/data/$input_fn"
echo $input_path
file_wc=$(cat $input_path | wc -l)

echo "#" $file_wc >> $bash_time_output

{ time python $p_code $input_fn $output_fn 2>&1; } 2>> $bash_time_output
echo "#" $output_fn >> $bash_output_fn

#!/usr/bin/python
import os
import csv
import sys

#r_fn = sys.argv[1] 
#w_fn = "../{}_processed.csv".format(r_fn.split(".csv")[0])

verbose = False
def get_abs_file_path(file_dir, fn):
    cur_dir = os.path.abspath(os.curdir)
    return os.path.normpath(os.path.join(cur_dir, "..", file_dir, fn))

r_fn = get_abs_file_path("data", "train.csv")
w_fn = get_abs_file_path("data", "train_processed.csv")
with open(w_fn, 'w') as output:
    wr = csv.writer(output)
    with open(r_fn, 'rt') as input:
        header = True
        i = 0
        for row in csv.reader(input):
            if verbose:
                print(row)
            if header:
                wr.writerow(['row_id'] + row)
                header = False
            else:
                wr.writerow([i] + row)
            i += 1


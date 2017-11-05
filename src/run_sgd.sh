#!/bin/bash
#spark-submit --master yarn --deploy-mode client sgd_lr.py
spark-submit --master yarn --deploy-mode client sgd_lr.py train_processed.csv full_run_test.csv
#spark-submit --master yarn --deploy-mode client sgd_lr.py train_processed.csv full_run.csv >> output.txt

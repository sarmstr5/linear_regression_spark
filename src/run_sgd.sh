!#/bin/bash
spark-submit --master yarn --deploy-mode client sgd_lr.py processed_train.csv full_run.csv
#spark-submit --master yarn --deploy-mode client sgd_lr.py processed_train.csv full_run.csv >> output.txt

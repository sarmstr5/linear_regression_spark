This program performs linear regression using GD/SGD to optimize its parameters
The best model is found by utilizing 10 cross validation that is then used on the "test" set.
The parameters that are searched on are:
	- GD step size (alpha)
	- SGD batch size
	- regularization calculation or type
	- regularization coefficient

Steps to run program
 - Download files from kaggle at https://www.kaggle.com/c/nyc-taxi-trip-duration/data
 - Put files in data folder
 - Run process_input/preprocessing_add_row_ids
	- Adds row ids to file
 - Change training file to 1458645.csv (number of rows in file)
 - Update GD/SGD parameters in sgd_lr.py
 - Run . single_sgd_run.sh or call python sgd_lr.py <input file name without extension> <output filename with ext>
 - To run multiple sgd_lr.py against multiple training sizes, use run_sgd.sh
	- The list fns must be file names of files in the data folder (minus the csv extention)

Folder structure:
Pseudo_code - holds project pseudo code
report - contains powerpoint with conclusions and graphs
results - training cross validaiton and test results
src - python code and scripts to run linear regression
src/not_used/ - miscellanous code that was not used for project
src/process_input - python code to process training data
test_input - small training sets to test code # not included for submission


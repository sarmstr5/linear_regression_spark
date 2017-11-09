class Main()
	Read in data file
	Convert data file to RDD
	For each row in RDD
		Processed_Data = Get_Params(row)
	Randomly split Processed_Data into Train and Test RDDs
	Initialize GD/SGD cv optimization parameter lists
	Run Grid Search over parameter lists
		Set linear model parameters
		Run Cross Validation with current linear model parameter set
			Split Train RDD into 9 training folds and 1 validation fold
			Combine training folds into cross validation training set
			Create linear model with parameters
			Train linear model on cross validation training set
			Calculate evaluation metrics of model on the validation fold
			Store evaluation metrics and model parameters
		Calculate average evaluation metrics across folds
		Store evaluation averages
		
	Find the optimal model with the min average loss
	Set linear model params to optimal cross validation model
	Train optimal model on Train RDD
	Evaluate optimal model on Test RDD
	Save results to disk
	
method Get_Features(row):
	Split each value delineated by commas
	Calculate Manahattan distance using lats and longs
	Return row_id, lat distance, long distance, n_passangers


# parse the data, convert str to floats and ints as appropriate
def create_df_from_rdd(line_list):
    # using longitude coordinates
    lo_dist = abs(float(line_list[5]) - float(line_list[7])) # double check
    # using latitude coordinates
    la_dist = abs(float(line_list[6]) - float(line_list[8]))
    # time of flight
    y = int(line_list[-1])
    return Row(flight_time=y, lat_dist=la_dist, long_dist=lo_dist)
:
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

def calculate_haversine_dist(lat1, lon1, lat2, lon2):
    # haverside distance for a sphere can be calculated via law of haversines
    # hav(c) = hav(a-b) + sin(a) * sin(b)* hav(C)
    #
	def convert_to_rads(deg):
		return deg * (math.pi/180)
	
    r_km = 6371; # Radius of the earth in km
    lat_dist = convert_to_rads(lat2-lat1)
    lon_dist = convert_to_rads(lon2-lon1)
    hav_a = math.sin(lat_dist / 2.0) * math.sin(lat_dist / 2.0) + \
        math.cos(convert_to_rads(lat1)) * math.cos(convert_to_rads(lat2)) *\
        math.sin(lon_dist / 2.0) * math.sin(lon_dist / 2.0)
    hav_c = 2 * math.atanh(math.sqrt(a), math.sqrt(1-a))
    return r_km * hav_c # distance in km
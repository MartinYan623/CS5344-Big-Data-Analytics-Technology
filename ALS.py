import sys, os
from pyspark import SparkContext
from pyspark.mllib.recommendation import Rating
from  pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import MatrixFactorizationModel
sc = SparkContext("local", "Simple App")
user_data = sc.textFile("ratings.dat")
rates = user_data.map(lambda x: x.split("::")[0:3])
print(rates.first())
rates_data = rates.map(lambda x: Rating(int(x[0]),int(x[1]),int(x[2])))
print(rates_data.first())
sc.setCheckpointDir('checkpoint/')
ALS.checkpointInterval = 2
model = ALS.train(ratings=rates_data, rank=20, iterations=5, lambda_=0.02)
print(model.predict(1,3))
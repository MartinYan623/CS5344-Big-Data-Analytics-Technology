from pyspark import SparkContext
from pyspark.mllib.recommendation import Rating
from  pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import MatrixFactorizationModel

sc = SparkContext("local", "Simple App")
user_data = sc.textFile("review_new.csv")
rates = user_data.map(lambda x: x.split(","))
rates_data = rates.map(lambda x: Rating(int(x[2]),int(x[0]),int(x[1])))
sc.setCheckpointDir('checkpoint/')
ALS.checkpointInterval = 2
model = ALS.train(ratings=rates_data, rank=20, iterations=20, lambda_=0.005)
userFeatures = model.userFeatures()
print(model.predict(2,13))
print(model.recommendProducts(1,5))

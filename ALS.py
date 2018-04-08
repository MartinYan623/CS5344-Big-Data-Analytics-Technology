from pyspark import SparkContext
from pyspark.mllib.recommendation import Rating
from pyspark.mllib.recommendation import ALS
import pandas as pd
from sklearn import cross_validation as cv
import time
import csv


if __name__=='__main__':

    # Read data set
    sc = SparkContext("local", "Simple App")
    df = pd.read_csv("review_new.csv", header=None)
    # Split data set as train data and test data
    train_data, test_data = cv.train_test_split(df, test_size=0.002)
    train_data.to_csv("train_data.csv", index=False, header=False)
    test_data.to_csv("test_data.csv", index=False, header=False)

    user_data = sc.textFile("train_data.csv")
    rates = user_data.map(lambda x: x.split(","))
    rates_data = rates.map(lambda x: Rating(int(x[2]), int(x[0]), int(x[1])))
    sc.setCheckpointDir('checkpoint/')
    ALS.checkpointInterval = 2
    time1 = time.time()
    model = ALS.train(ratings=rates_data, rank=30, iterations=20, lambda_=0.05)
    time2 = time.time()
    diff_time = time2 - time1
    print('Train model time:' + str(diff_time))
    userFeatures = model.userFeatures()

    user_id_list = train_data[2].tolist()
    business_id_list = train_data[0].tolist()
    restaurantData = test_data[test_data[0].isin(business_id_list)]
    restaurantData = restaurantData[restaurantData[2].isin(user_id_list)]

    # Predict star for test data
    sum = 0
    num = 0
    time1 = time.time()
    for line in restaurantData.itertuples():
        num = num + 1
        user_id = line[3]
        business_id = line[1]
        star = line[2]
        predict_star = model.predict(user_id, business_id)
        sum = sum + pow(star-predict_star, 2)
    time2 = time.time()
    diff_time = time2 - time1
    print('Predict time:' + str(diff_time))
    rmse = pow(sum/num, 0.5)
    print(rmse)


""" 
df=pd.read_csv("review_deleted.csv", header=0)
df.to_csv("review_new.csv", index=False, header=False)
user_id_predict_list = []
business_id_predict_list = []
user_id_predict_list.append(user_id)
business_id_predict_list.append(business_id)
print(user_id, business_id,model.predict(user_id,business_id))
dataframe = pd.DataFrame({'user_index': user_id_predict_list, 'business_index':business_id_predict_list, 'stars': star})
dataframe.to_csv('predict_result.csv', header=False, index=False, sep=',')
star.append(model.predict(user_id,business_id))
test_data.columns=['column']
test_data.columns=['business_index','stars','user_index']
print(model.recommendProductsForUsers(3).collect())
"""
"""
f = open('review.csv', 'r')
content = csv.reader(f)
lineNum = 0
for line in content:
    lineNum += 1
print(lineNum)  # lineNum就是你要的文件行数
f.close()
"""

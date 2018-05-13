# CS5344-Big-Data-Analytics-Technology

Main recommendation method : ALS (Alternating Least Square)

ALS has already been implemented by spark, we just need to use it directly.

You need to install the third library spark and import two important packages Rating and ALS.

The following code is aimed to use ALS method to train data and build model.

#model = ALS.train(ratings=rates_data, rank=15, iterations=20, lambda_=0.01)

There are three parameters: rank is the number of latent variables, iterations are the number of iteration and lambda is regularization parameter.

The following code is aimed to predict the score of every user to each business.

#predict_star = model.predict(user_id, business_id)

We use RMSE to value the performance of the model.
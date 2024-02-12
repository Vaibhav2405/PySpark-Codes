from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("ColaborativeFiltering").getOrCreate()

movieDF = spark.read.options(header='True', inferSchema='True').csv("/content/movies.csv")
ratingsDF = spark.read.options(header='True', inferSchema='True').csv("/content/ratings.csv")

ratings = ratingsDF.join(movieDF, "movieId", "left")

(train, test) = ratings.randomSplit([0.8, 0.2])

als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", nonnegative=True, implicitPrefs=False, coldStartStrategy="drop")

param_grid = ParamGridBuilder()\
    .addGrid(als.rank, [10, 50, 100, 150])\
    .addGrid(als.regParam, [.01, .05, .1, .15])\
    .build()

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

cv = CrossValidator(
    estimator=als,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=5
)

model = cv.fit(train)
bestModel = model.bestModel
testPredictions = bestModel.transform(test)
RMSE = evaluator(testPredictions)
print(RMSE)

recommendations = bestModel.recommendForAllUsers(5)

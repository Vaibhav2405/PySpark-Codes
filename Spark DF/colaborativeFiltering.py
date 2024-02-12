from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ColaborativeFiltering").getOrCreate()

movieDF = spark.read.options(header='True', inferSchema='True').csv("/content/movies.csv")
ratingsDF = spark.read.options(header='True', inferSchema='True').csv("/content/ratings.csv")

ratings = ratingsDF.join(movieDF, "movieId", "left")

ratings.show() #count is 100836

(train, test) = ratings.randomSplit([0.8, 0.2]) #data gets divided into 80% and 20% respectively.

train.show() #count is 80721
test.show() #count is 20115
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Practice").getOrCreate()

df = spark.read.options(header='True', inferSchema='True').csv("/content/StudentData.csv")
df.show()

df.dropDuplicates(["course"]).show()
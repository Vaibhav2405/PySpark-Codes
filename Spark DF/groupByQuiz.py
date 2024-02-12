from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("groupByQuiz").getOrCreate()
df = spark.read.mode("overwrite").options(header='True', inferSchema='True').csv("/content/StudentData.csv")

df.groupby(df.course).count().show()
df.groupby(df.course, df.gender, df.marks).count().show()
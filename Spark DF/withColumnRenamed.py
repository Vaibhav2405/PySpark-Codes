from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("withColumnRenamed").getOrCreate()

df = spark.read.options(header = 'True').csv("/content/StudentData,csv")
df.show()

df = df.withColumnRenamed('gender', 'sex').withColumnRenamed('email', 'EmailId')
df.show()

df.select(col('roll').alias('roll number')).show()

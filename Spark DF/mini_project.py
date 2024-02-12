from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, avg
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("miniProject").getOrCreate()

df = spark.read.options(header='True', inferSchema='True').csv("/content/OfficeDataProject.csv")

#1
df.count()

#2
df.dropDuplicates(["department"]).count()

#3
df.select("department").dropDuplicates(["department"]).show()

#4
df.groupBy("department").count().show()

#5
df.groupBy("state").count().show()

#6
df.groupBy("state", "department").count().show()

#7
df.groupby("department").agg(max("salary").alias("max_salary"), min("salary").alias("min_salary")).orderBy(col("max_salary").asc()).show()

#8




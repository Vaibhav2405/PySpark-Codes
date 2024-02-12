from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,IntegerType,StructType

spark = SparkSession.builder.appName("fistCode").getOrCreate()

df = spark.read.csv(r"D:\Python projects\mydata.csv")
df.show()

schema = StructType([StructField("age",IntegerType(), True),
                     StructField("gender", StringType(), True),
                     StructField("name", StringType(), True),
                     StructField("course", StringType(), True),
                     StructField("roll", StringType(), True),
                     StructField("marks", IntegerType(), True),
                     StructField("email", StructType(), True)])

spark = SparkSession.builder.appName("name").getOrCreate()
df = spark.read.option("header", True).schema(schema).csv("/content/StudentData.csv")
df.show(10)
df.printSchema()
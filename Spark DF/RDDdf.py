from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDDdf").getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("name", StringType(), True),
    StructField("course", StringType(), True),
    StructField("roll", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("email", StringType(), True),
])

df = spark.read.options(header='True').schema(schema).csv("/content/StudentData.csv")
df.show()
df.printSchema()
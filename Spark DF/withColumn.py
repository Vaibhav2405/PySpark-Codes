from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("withColumn").getOrCreate()

df = spark.read.options(header='True').csv("/content/StudentData.csv")
df.show()

df.withColumn('roll', col('roll').cast('String')).printSchema()
df.withColumn('marks', col('marks') + 10).show()
df.withColumn('updated marks', col('marks') - 10).show()

df.withColumn('Country', lit('IND')).show()
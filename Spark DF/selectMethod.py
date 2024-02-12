from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("selectMethod").getOrCreate()

df = spark.read.options(header='True').csv("/content/StudentData.csv")
df.show()

df.select('age', 'name', 'email').show()
df.select(df.name,df.marks,df.email).show()
df.select(df.columns('age', 'name', 'gender')).show()

df.select(col('name'), col('email')).show()

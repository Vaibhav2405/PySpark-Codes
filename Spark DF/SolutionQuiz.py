from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

spark = SparkSession.builder.appName("solutionQuiz").getOrCreate()
df = spark.read.options(header='True').csv("/content/StudentData.csv")
df.show()

df = df.withColumn("total_marks", lit(120))
df = df.withColumn("average", (col("marks")/col("total_marks"))*100)
df.show()

df_OOP = df.filter((df.course == "OOP") & (df.average > 80))
df_Cloud = df.filter((df.course == "Cloud") & (df.average > 60))

df_OOP.select('name', 'marks').show()
df_Cloud.select('name', 'marks').show()

result = df_OOP.union(df_Cloud)
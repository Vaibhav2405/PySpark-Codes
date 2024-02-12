from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("filterOperation").getOrCreate()
df = spark.read.options(header='True').csv("/content/StudentData.csv")
df.show()

df.filter(df.course == 'DB').show()
df.filter(col('course') == 'DB').show()

df.filter((df.course == 'DB') & (df.marks > 50)).show()

courses = ['DB', 'DSA', 'Cloud', 'OOP']
df.filter(df.course.isin(courses)).show()

df.filter(df.name.like('%H%')).show() #in between (.contains())
df.filter(df.name.like('H%')).show()  #at start (.startsWith())
df.filter(df.name.like('%H')).show()    #as end (.endsWith())

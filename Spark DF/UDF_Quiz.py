from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("UDF_Quiz").getOrCreate()
df = spark.read.options(header='True', inferSchema='True').csv("/content/OfficeData.csv")

def getIncrementSalary(salary,bonus,state):
    if state == 'NY':
        return ((salary/100)*10)+((bonus/100)*5)
    elif state == 'CA':
        return ((salary / 100) * 12) + ((bonus / 100) * 3)

totalIncrementUDF=udf(lambda salary,bonus,state: getIncrementSalary(salary,bonus,state), DoubleType() )

df1 = df.withColumn('incremented_salary', totalIncrementUDF(df.salary,df.bonus,df.state))

df1.withColumn('total_salary', (df1.salary+df1.bonus+df1.incremented_salary)).show()

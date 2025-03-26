
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType 


# initialize spark session 
spark = SparkSession.builder.appName('create DataFrame').getOrCreate()

# define schema  
schema = StructType([
    StructField('Name', StringType(), True),\
    StructField('Age', IntegerType(), True)
])

# sample data 
data = [('waldo', 29), ('bob', 18), ('ralph', 25)]

# create dataframe 
df = spark.createDataFrame(data, schema=schema)

df.printSchema()
# df.show()

# define UDF
def squared_number(n):
    return n ** 2

squared_udf = F.udf(squared_number, IntegerType())

df.withColumn('AgeSquared', squared_udf(F.col('Age'))).show()
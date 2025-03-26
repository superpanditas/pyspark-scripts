
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType


spark = SparkSession.builder.appName('popular-Movies').getOrCreate()

schema = StructType([
    StructField('userID', StringType(), True),\
    StructField('movieID', StringType(), True),\
    StructField('rating', IntegerType(), True),\
    StructField('timestamp', LongType(), True)
]) 

# load movies dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("/Users/mbp/Documents/data-engineer-pyspark/ml-100k/u.data")

moviesDFSorted = moviesDF.groupBy('userID').count().orderBy(F.desc('count'))

# grab the top 10
moviesDFSorted.show(10)

# stop the session 
spark.stop()
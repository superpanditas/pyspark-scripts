from pyspark.sql import SparkSession
from pyspark.sql import functions as F 

spark = SparkSession.builder.appName('friends-by-age').getOrCreate()
friendsByAge = spark.read.option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv("/Users/mbp/Documents/data-engineer-pyspark/ml-100k/fakefriends-header.csv")

friendsByAge.groupBy(F.col('age')).agg(F.round(F.avg("friends"), 2).alias(
"friend_avg")).sort(F.col("age")).show()

spark.stop()
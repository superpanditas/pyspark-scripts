
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.types import StructField, StructType, IntegerType, StringType


spark = SparkSession.builder.appName('most-popular-superheroe').getOrCreate()
sc = spark.sparkContext

# create schema 
schema = StructType([
    StructField('id', IntegerType(), True),\
    StructField('name', StringType(), True)
])

names = spark.read\
    .schema(schema)\
    .option('sep', ' ')\
    .csv('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/Marvel+Names.txt')

lines = spark.read.text('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/Marvel+Graph.txt')

conn = lines.withColumn('id', F.split(F.col('value'), ' ')[0])\
        .withColumn('connections', F.size(F.split(F.col('value'), ' ')) - 1)\
        .groupBy('id').agg(F.sum('connections').alias('connections'))

mostPopular = conn.orderBy(F.desc('connections')).first()
mostPopularName = names.filter(F.col('id') == mostPopular[0]).select('name').first()

print(mostPopularName[0] + " is the most popular superheroe with " + str(mostPopular[1]))
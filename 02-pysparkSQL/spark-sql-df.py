from pyspark.sql import SparkSession 
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()

people = spark.read.option("header", "true")\
    .option("inferSchema", "true")\
    .csv("/Users/mbp/Documents/data-engineer-pyspark/ml-100k/fakefriends-header.csv")

print("Here is our inferred schema")
people.printSchema()

print("Let's display the name column:")
people.select(F.col("name")).show()

print("Filter out anyone over 21")
people.filter(F.col("age") < 21 ).show()

print("Group by age")
people.groupBy(F.col("age")).count().show()

print("Make everyone 10 years older")
people.select(F.col("name"), F.col("age") + 10).show()

# objective: Getting the total spend by customer ID

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType

#  session 
spark = SparkSession.builder.appName('total_spend').getOrCreate()
schema = StructType([
    StructField('customerID', StringType(), True),\
    StructField('productID', StringType(), True),\
    StructField('amount', FloatType(), True)
])

# print Schema
df = spark.read.schema(schema).csv('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/customer-orders.csv')
df.printSchema()

totalAmount = df.groupBy('customerID')\
    .agg(F.sum('amount')\
    .alias('totalAmount'))

totalAmountSorted = totalAmount.withColumn('totalAmount', F.round(F.col('totalAmount'), 2)).sort('totalAmount')
totalAmountSorted.show(totalAmountSorted.count())
spark.stop()
'''
Question:
You are given a dataset containing sales data for different stores across various months. Each row contains the store name, the month, and the sales amount. Your task is to calculate the cumulative sales for each store, considering the monthly sales, using PySpark.

You should also:
Filter out stores with sales lower than 1000 in any month.
Calculate the total sales for each store over all months.
Sort the results by the total sales in descending order.
'''

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, LongType 

spark = SparkSession.builder.appName('Window Pattern').getOrCreate()

data = [ ("Store A", "2024-01", 800),
    ("Store A", "2024-02", 1200), ("Store A", "2024-03", 900), 
    ("Store B", "2024-01", 1500), ("Store B", "2024-02", 1600), 
    ("Store B", "2024-03", 1400), ("Store C", "2024-01", 700), 
    ("Store C", "2024-02", 1000), ("Store C", "2024-03", 800) ]

# create dataframe
df = spark.createDataFrame(data, ['store', 'month', 'sales'])

# filter out stores with sales lower than 1000 in any month 
df_filtered = df.filter(F.col('sales') >= 1000)

# define window specification to calculate cumulative sales 
windowSpec = Window.partitionBy('store').orderBy('month')

# calculate cumulative sales 
df_cumulative = df_filtered.withColumn('cumulativeSales', F.sum('sales').over(windowSpec))

# calculate total sales per store 
df_total_sales = df_cumulative.groupBy('store').agg(F.sum('sales').alias('totalSales'))

# join df_cumulative with df_total_sales 
df_final = df_cumulative.join(df_total_sales, on='store').select('store', 'totalSales', 'cumulativeSales').distinct()

# sort by total sales in descending order 
df_result = df_final.orderBy(F.col('totalSales').desc())

# show the result 
df_result.show(truncate=False)

spark.stop()
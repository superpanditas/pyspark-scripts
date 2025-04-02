'''
Case Study: Uber Eats 7-Day Moving Average of Daily Sales
You are an Analytics & Automation Manager at Uber Eats, and your team wants to track the 7-day moving average of daily sales per restaurant to monitor performance trends.
'''

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType
from pyspark.sql.window import Window 

spark = SparkSession.builder.appName('7-Moving Average').getOrCreate()

data = [
    (101, "R1", "2024-08-25", 120.00),
    (102, "R2", "2024-08-25", 200.00),
    (103, "R1", "2024-08-24", 100.00),
    (104, "R1", "2024-08-23", 80.00),
    (105, "R2", "2024-08-23", 150.00),
    (106, "R1", "2024-08-22", 90.00),
    (107, "R1", "2024-08-21", 85.00),
    (108, "R1", "2024-08-20", 110.00)
]

columns = ["order_id", "restaurant_id", "order_date", "sales_amount"]
df = spark.createDataFrame(data, columns)

# Cast order_date column to date 
df = df.withColumn('order_date', F.to_date('order_date'))

# Define window: 7-day moving average per restaurant 
window_spec = Window.partitionBy('restaurant_id').orderBy('order_date').rowsBetween(-6, 0)

# Calculate 7_day_moving_average
df = df.withColumn('7_day_moving_average', F.avg('sales_amount').over(window_spec))

# output
df.show()

# Stop session
spark.stop()
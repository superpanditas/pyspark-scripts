'''
Question:
Your company wants to anticipate staffing needs by identifying the top two busiest times of the week. Each day should be segmented into different parts using the following criteria:

Morning: Before 12 p.m. (not inclusive)
Early Afternoon: 12 - 15 p.m.
Late Afternoon: After 15 p.m. (not inclusive)

Your output should include the day and time of day combination for the two busiest times with the most orders, along with the number of orders.

For example, the results might include:
Friday Late Afternoon with 12 orders
Sunday Morning with 10 orders

If there is a tie in ranking, all tied results should be displayed.
'''

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
from pyspark.sql.window import Window 

spark = SparkSession.builder.appName('Timestamp Pattern').getOrCreate()

data = [ (1, 101, '2024-12-15 09:30:00'), (2, 102, '2024-12-15 11:45:00'),
        (3, 103, '2024-12-15 12:10:00'), (4, 104, '2024-12-15 13:15:00'), 
        (5, 105, '2024-12-15 14:20:00'), (6, 106, '2024-12-15 15:30:00'), 
        (7, 107, '2024-12-15 16:40:00'), (8, 108, '2024-12-16 09:50:00'), 
        (9, 109, '2024-12-16 10:30:00'), (10, 110, '2024-12-16 12:05:00'), 
        (11, 111, '2024-12-16 13:50:00'), (12, 112, '2024-12-16 14:15:00'), 
        (13, 113, '2024-12-16 15:30:00'), (14, 114, '2024-12-17 09:45:00'), 
        (15, 115, '2024-12-17 11:20:00'), (16, 116, '2024-12-17 12:25:00'), 
        (17, 117, '2024-12-17 13:30:00'), (18, 118, '2024-12-17 14:55:00'), 
        (19, 119, '2024-12-17 15:10:00'), (20, 120, '2024-12-18 10:40:00') ]

columns = ["order_id", "product_id", "timestamp"]

# Create dataframe 
df = spark.createDataFrame(data, columns)

# Convert timestamp to proper format 
df = df.withColumn('timestamp', F.col('timestamp').cast('timestamp'))

# Add day of the week and time period columns 
df = (
    df.withColumn('day_of_week', F.date_format(F.col('timestamp'), 'EEEE'))
    .withColumn(
        'time_period', F.when(F.hour(F.col('timestamp')) < 12, F.lit('Morning'))
        .when((F.hour(F.col('timestamp')) >= 12) & (F.hour(F.col('timestamp')) < 15), F.lit('Early Afternoon'))
        .otherwise(F.lit('Late Afternoon'))
    )
)

# Group by day_of_week and time_period, and count the number of orders 
time_period_orders = (
    df.groupBy('day_of_week', 'time_period')
    .agg(F.count('order_id').alias('order_count'))
)

# Add rank to find the top 2 busiest times
window_spec = Window.orderBy(F.col('order_count').desc())
ranked_orders = time_period_orders.withColumn("rank", F.rank().over(window_spec))

# Filter top 2 business times (consider ties)
output = ranked_orders.filter(F.col('rank') <= 2).orderBy(F.col('rank'), F.col('order_count').desc())

# Show result 
output.show()

# stop session
spark.stop()
'''
𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
You are working as a Data Engineer, and the company has a log system where timestamps are recorded for every user action (e.g., when the user logs in and logs out). Your manager wants to know how much time each user spends between log in and log out.
The system generates logs with login_timestamp and logout_timestamp columns. You need to calculate the difference between the logout_timestamp and login_timestamp in hours, minutes, and seconds. The result should be formatted like "HH:mm:ss".
'''

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

spark = SparkSession.builder.appName('Timestamp Difference').getOrCreate()

data = [ 
    (1, "2025-01-31 08:00:00", "2025-01-31 10:30:45"),
    (2, "2025-01-31 09:00:30", "2025-01-31 12:15:10"),
    (3, "2025-01-31 07:45:00", "2025-01-31 09:00:15") 
]

columns = ["user_id", "login_timestamp", "logout_timestamp"]

# create dataframe 
df = spark.createDataFrame(data, columns)

# convert timestamp columns to unix timestamp 
df = df.withColumn("login_time", F.unix_timestamp(F.col("login_timestamp")))
df = df.withColumn("logout_time", F.unix_timestamp(F.col('logout_timestamp')))

# calculate the difference between loging_time and logout_time in seconds 
df = df.withColumn("duration_seconds", F.col("logout_time") - F.col("login_time"))

# calculate hours, minutes, and seconds 
df = df.withColumn('hours', (F.col('duration_seconds') / 3600).cast('int'))
df = df.withColumn('minutes', ((F.col('duration_seconds') % 3600) / 60).cast('int'))
df = df.withColumn('seconds', (F.col('duration_seconds') % 60).cast('int'))

# format the duration column as HH:mm:SS
df = df.withColumn('formatted_duration',
    F.expr("lpad(hours, 2, '0') || ':' || lpad(minutes, 2, '0') || ':' || lpad(seconds, 2, '0')")
)

# show the result 
df.select('user_id', 'formatted_duration').show(truncate=False)

# stop session
spark.stop()
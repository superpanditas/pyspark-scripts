'''
Question:
Given a DataFrame containing employee details, write a PySpark code snippet to group employees by their department and 
calculate the average salary for each department.
'''

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 

spark = SparkSession.builder.appName("aggregate-data").getOrCreate()

data = [
    ("1", "HR", 50000),
    ("2", "IT", 75000),
    ("3", "Finance", 62000),
    ("4", "IT", 82000),
    ("5", "HR", 52000),
    ("6", "Finance", 60000)
]

columns = ['employee_id', 'department', 'salary']

# create dataframe
df = spark.createDataFrame(data, columns)

# calculate average salary by department
df_grouped = df.groupBy('department').agg(
    F.avg('salary').alias('salary_avg')
).orderBy(F.col('salary_avg').desc())

# show df_grouped 
df_grouped.show(truncate=False)

# stop session 
spark.stop()
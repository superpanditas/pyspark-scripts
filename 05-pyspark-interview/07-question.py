
'''
Question:
You are given an employee dataset with the following schema:
Emp_ID: Unique identifier for each employee
Name: Employee's name
Department: Department of the employee
Salary: Salary of the employee

Your task is to find the highest-paid employee in each department using PySpark.
'''

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 


spark = SparkSession.builder.appName('highest-paid').getOrCreate()

data =  [
 (101, "John", "IT", 60000),
 (102, "Mike", "HR", 55000),
 (103, "Sarah", "IT", 70000),
 (104, "Anna", "HR", 65000),
 (105, "David", "IT", 72000)
]

columns = ['employ_id', 'name', 'department', 'salary']

# create dataframe 
df = spark.createDataFrame(data, columns)

# highest paid 
df_highest_paid = (
    df
    .groupBy('department').agg(
        F.max('salary').alias('max_salary')
    )
    .withColumnRenamed('department', 'department_highest_paid')
)

# joining df and df_highest_paid
df = (
    df
    .join(df_highest_paid, 
        (df.department == df_highest_paid.department_highest_paid) & (df.salary == df_highest_paid.max_salary)
    )
    .select('employ_id', 'name', 'department', 'salary')
    .orderBy(F.col('salary').desc())
)

# show output 
df.show(truncate=False)

# stop session 
spark.stop()
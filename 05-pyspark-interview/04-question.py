
'''
Question:
Pivot columns into rows
'''

from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType

spark = SparkSession.builder.appName('Pivot columns into rows').getOrCreate()

data = [
    ("123", "francisco", 7.0, 8.0, 10.0),
    ("124", "leonardo", 9.0, 6.0, 9.0),
    ("125", "roberto", 5.0, 5.0, 5.0),
    ("126", "juan", 6.0, 10.0, 7.0)
]

columns = ["StudentID", "StudentName", "ScoreMath", "ScoreLanguage", "ScoreCoding"]

# create dataframe 
df = spark.createDataFrame(data, columns)

# pivot columns into rows
df_pivoted = df.selectExpr(
    "StudentID",
    "StudentName",
    "stack(3, 'ScoreMath', ScoreMath, 'ScoreLanguage', ScoreLanguage, 'ScoreCoding', ScoreCoding) as (Subject, Score)"
)

# show result
df_pivoted.show(truncate=False)

# stop session 
spark.stop()
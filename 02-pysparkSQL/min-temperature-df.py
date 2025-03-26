from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName('min-temperatures').getOrCreate()
schema = StructType([
    StructField("stationID", StringType(), True),\
    StructField("date", IntegerType(), True),\
    StructField("measureType", StringType(), True),\
    StructField("temperature", FloatType(), True)\
])

df = spark.read.schema(schema).csv("/Users/mbp/Documents/data-engineer-pyspark/ml-100k/1800.csv")
df.printSchema()

minTemps = df.filter(df.measureType == "TMIN")
stationsTemps = minTemps.select("stationID", "temperature")
minTempsByStation = stationsTemps.groupBy("stationID").min("temperature")

minTempsByStation.show()

# convert temperature to farenheit and sort the dataset 
minTempsByStationF = minTempsByStation\
    .withColumn("temperature", F.round(F.col("min(temperature)") * 0.1 * (9.0/5.0) * 32.0, 2))\
    .select('stationID', 'temperature')\
    .sort('temperature')

# after use a collect method it returns a list of rows
# output 
result = minTempsByStationF.show()



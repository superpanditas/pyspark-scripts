from pyspark.sql import SparkSession 
from pyspark.sql import Row 

# create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(lines):
    fields = lines.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode('utf-8')), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("/Users/mbp/Documents/data-engineer-pyspark/ml-100k/fakefriends.csv")
people = lines.map(mapper)
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("select * from people where age >= 13 and age <= 19")
for teen in teenagers.collect():
    print(teen)

teenagers.groupBy("age").count().orderBy("age").show()

spark.stop()
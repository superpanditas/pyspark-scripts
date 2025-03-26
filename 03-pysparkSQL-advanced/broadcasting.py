
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType, LongType
import codecs 

def loadMovieNames():
    movieNames = {}
    with codecs.open('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/u.item', 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName('popularMovies').getOrCreate()
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# create schema 
schema = StructType([
    StructField('userID', IntegerType(), True),\
    StructField('movieID', IntegerType(), True),\
    StructField('rating', IntegerType(), True),\
    StructField('timestamp', LongType(),  True)
])

# load movie data as dataframe 
moviesDF = spark.read.option('sep', '\t')\
        .schema(schema)\
        .csv('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/u.data')
    
movieCounts = moviesDF.groupBy('movieID').count()

def lookupName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = F.udf(lookupName)

# add a movie title column
moviesWithNames = movieCounts.withColumn('movieTitle', lookupNameUDF(F.col('movieID')))
# order by counts
sortedMoviesWithNames = moviesWithNames.orderBy(F.desc(F.col('count')))
# display top 10 
sortedMoviesWithNames.show(10)
# stop the session
spark.stop()
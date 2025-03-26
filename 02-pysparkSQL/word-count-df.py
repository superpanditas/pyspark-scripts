from pyspark.sql import SparkSession
from pyspark.sql import functions as F 

spark = SparkSession.builder.appName("word-count-df").getOrCreate()
inputBook = spark.read.text("/Users/mbp/Documents/data-engineer-pyspark/ml-100k/Book.txt")

words = inputBook.select(F.explode(F.split(inputBook.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(F.col("word") != "")
lowerCaseWords = wordsWithoutEmptyString.select(F.lower(wordsWithoutEmptyString.word).alias("word"))
wordsCounts = lowerCaseWords.groupBy("word").count()
wordCountSorted = wordsCounts.sort("count")
wordCountSorted.show()

spark.stop()
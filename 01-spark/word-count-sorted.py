from pyspark import SparkContext, SparkConf
import re

conf = SparkConf().setMaster('local').setAppName('word-count-sorted')
sc = SparkContext(conf = conf)

def normalizeWords(line):  
    return re.compile('\W+', re.UNICODE).split(line.lower())

text = sc.textFile('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/Book.txt')
words = text.flatMap(normalizeWords)
wordsCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordsCountSorted = wordsCounts.map(lambda pair: (pair[1],pair[0])).sortByKey()

results = wordsCountSorted.collect()

for result in results:
    
    print(result[0], result[1])
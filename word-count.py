import re 
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('word-count')
sc = SparkContext(conf = conf)

def normalizeWords(line):  
    return re.compile('\W+', re.UNICODE).split(line.lower())

text = sc.textFile('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/Book.txt')
words = text.flatMap(normalizeWords)
wordsCounts = words.countByValue()

for word, count in wordsCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(word, count)
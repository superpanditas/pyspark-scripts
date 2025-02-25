from pyspark import SparkContext, SparkConf 
import collections 

conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')
sc = SparkContext(conf = conf)

lines = sc.textFile('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(f'key: {key}, value {value}') 


from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('friends-by-age')
sc = SparkContext(conf = conf)


def parseLine(currLine):
    fields = currLine.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return (age, friends)

lines = sc.textFile('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/fakefriends.csv')
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avgByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

output = avgByAge.collect()
for elem in output:
    print(elem)


from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('min-temperature')
sc = SparkContext(conf = conf)

def parseLine(currLine):
    fields = currLine.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) * 32.0 # celsius to fahrenheit
    return (stationID, entryType, temperature)

lines = sc.textFile('/Users/mbp/Documents/data-engineer-pyspark/ml-100k/1800.csv')
rdd = lines.map(parseLine)
stationsTempsMin = rdd.filter(lambda x: 'TMIN' in x[1])
stationTemps = stationsTempsMin.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))

output = minTemps.collect()
for station in output:
    print(f'Station: {station[0]}  Temperature: {station[1]:.2f}')

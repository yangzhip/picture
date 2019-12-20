from pyspark import SparkContext
sc = SparkContext('local','spark')
lines = sc.textFile("hdfs://222.27.166.215:9000/home/spark-test/douban_movies.txt")
res = lines.flatMap(lambda x: x.split("\n"))\
    .map(lambda x: (x,1))
#ping fen fen bu
data = res.map(lambda x: x[0].split("|"))\
    .map(lambda x:(x[1],1))\
    .reduceByKey(lambda x,y:x+y)\
    .sortByKey()\
    .saveAsTextFile("hdfs://222.27.166.215:9000/home/spark-test/picture_data")

data1 = res.map(lambda x: x[0].split("|"))\
    .map(lambda x:x[2]).flatMap(lambda x:x.split(";")).map(lambda x:(x,1))\
    .reduceByKey(lambda x,y:x+y)\
    .saveAsTextFile("hdfs://222.27.166.215:9000/home/spark-test/picture_data1")
#yu yan fen bu
data2 = res.map(lambda x: x[0].split("|"))\
    .map(lambda x:x[4]).flatMap(lambda x:x.split(";")).map(lambda x:(x,1))\
    .reduceByKey(lambda x,y:x+y)\
    .saveAsTextFile("hdfs://222.27.166.215:9000/home/spark-test/picture_data2")



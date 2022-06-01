from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Students")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('file:///spark/datasets/StudentData.csv')
header = rdd.first()
rdd = rdd.filter(lambda x: x != header)
rdd2 = rdd.map(lambda x: x.split(',')).count()  # --> total students

rdd3 = rdd.map(lambda x: (x.split(',')[1], int(x.split(',')[5]))).reduceByKey(
    lambda x, y: x + y)  # --> total marks by gender

rdd4 = rdd.map(lambda x: int((x.split(',')[5]))).filter(lambda x: x > 50).count()  # --> total students passed
rdd5 = rdd.map(lambda x: int((x.split(',')[5]))).filter(lambda x: x < 50).count()  # --> total students failed

rdd6 = rdd.map(lambda x: x.split(',')[3]).map(lambda x: (x, 1)).reduceByKey(
    lambda x, y: x + y)  # --> total students enrolled per course

rdd7 = rdd.map(lambda x: (x.split(',')[3], int(x.split(',')[5]))).reduceByKey(
    lambda x, y: x + y)  # --> total marks per course

rdd8 = rdd.map(lambda x: (x.split(',')[3], (int(x.split(',')[5]), 1))).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1]))  # --> avg marks per course
rdd8 = rdd8.map(lambda x: (x[0], x[1][0] / x[1][1]))
rdd8 = rdd8.map(lambda x: (x[0], x[1][0] / x[1][1]))

rdd9 = rdd.map(lambda x: (x.split(',')[3], int(x.split(',')[5]))).reduceByKey(
    lambda x, y: x if x > y else y)  # --> max marks per course
rdd9 = rdd.map(lambda x: (x.split(',')[3], int(x.split(',')[5]))).reduceByKey(
    lambda x, y: x if x < y else y)  # --> min marks per course

rdd10 = rdd.map(lambda x: (x.split(',')[1], (int(x.split(',')[0]), 1))).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1]))  # --> avg age
rdd10 = rdd10.map(lambda x: (x[0], x[1][0] / x[1][1]))

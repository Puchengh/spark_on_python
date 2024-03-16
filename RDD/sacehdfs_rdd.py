from pyspark import SparkContext

sc = SparkContext()

data_list = [1, 2, 3, 4, 5, 5, 6, 7, 8]
rdd = sc.parallelize(data_list,numSlices=2)
# rdd2 = rdd.repartition(2)
rdd.saveAsTextFile('hdfs:///data/test_3')

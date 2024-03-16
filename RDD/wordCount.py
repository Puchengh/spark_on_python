# 行动算子的使用 action算子
from pyspark import SparkContext

sc = SparkContext()

# 读取文件
rdd = sc.textFile('hdfs:///data/datatest.txt')

rdd2 = rdd.flatMap(lambda x: x.split(',')).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1],ascending=False)
# rdd2 = rdd.map(lambda x: (x.split(','), 1))

res = rdd.collect()
res2 = rdd2.collect()

print(res)
print(res2)

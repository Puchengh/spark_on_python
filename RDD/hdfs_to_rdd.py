from pyspark import SparkContext

sc = SparkContext()

# 读取hdfs的文件数据转为rdd
# spark读取文件时候是按行读取的。
rdd1 = sc.textFile('hdfs:///data/data.txt')


rdd3 = sc.textFile('hdfs:///data/data.txt',minPartitions=2)

# 读取指定下面的所有文件
rdd2 = sc.textFile('hdfs:///data')
# 触发计算结果
res1 = rdd1.collect()
res2 = rdd2.collect()
res3 = rdd2.glom().collect()

print(res1)
print(res2)
print(res3)

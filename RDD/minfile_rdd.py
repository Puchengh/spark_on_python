from pyspark import SparkContext

sc = SparkContext()

rdd1 = sc.wholeTextFiles('hdfs:///data/')

# 每个小文件都会对应一个分区
# glom 查看转化后的分区信息
res = rdd1.glom().collect()

print(res)

# 结果 [['hadoop,spark,flink,zk', 'hive,sql,python,itcast'], ['hadoop,spark,flink,zk', 'hive,sql,python,itcast'], ['hadoop,spark,flink,zk', 'hive,sql,python,itcast'], ['hadoop,spark,flink,zk', 'hive,sql,python,itcast'], ['hadoop,spark,flink,zk', 'hive,sql,python,itcast'], ['hadoop,spark,flink,zk', 'hive,sql,python,itcast'], ['hadoop,spark,flink,zk', 'hive,sql,python,itcast']]

# 读取文件  ['65001,zhangsan,男,20,CS', '65002,zhangsan,女,18,MA', '65003,zhangsan,男,21,CS', '65004,zhangsan,女,19,CS']
from pyspark import SparkContext

sc = SparkContext()

rdd = sc.textFile('hdfs:///data/test')

# 2.转为二维列表
rdd_map = rdd.map(lambda x: x.split(','))

# 3.按照性别分组计算不同性别的年龄总和，需要性别和年龄总和 需要性别和年龄两个数据

table_rdd = rdd_map.map(lambda x: (x[2], int(x[3])))

res_rdd2 = table_rdd.reduceByKey(lambda x, y: x + y)

print(res_rdd2.collect())

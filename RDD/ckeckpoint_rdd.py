from pyspark import SparkContext, SparkConf
from pyspark.storagelevel import StorageLevel

conf = SparkConf().set('spark.shuffle.file.buffer', '64k').set('spark.shuffle.memoryFraction', '0.4')
sc = SparkContext(master='yarn', appName='shuffle_dome', conf=conf)
# 新checkpoint需要指定数据保存的位置 路径
sc.setCheckpointDir('hdfs:///checkpoint_data')

data_list = [1, 2, 3, 4, 5, 5, 6, 7, 8]

rdd1 = sc.parallelize(data_list)

rdd2 = rdd1.map(lambda x: x + 1)
# 可以checkpoint rdd2的数据 当rdd3失败时候可以从rdd2获取中间结果
rdd2.checkpoint()  # 需要action算子触发
# rdd2.collect()  # 会将数据缓存 rdd3触发行动算子 也可以触发checkpoint

rdd3 = rdd2.map(lambda x: x * 2)

print(rdd3.collect())

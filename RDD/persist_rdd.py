from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel

sc = SparkContext()

data_list = [1, 2, 3, 4, 5, 5, 6, 7, 8]

rdd1 = sc.parallelize(data_list)

rdd2 = rdd1.map(lambda x: x + 1)
# 可以缓存rdd2的数据 当rdd3失败时候可以从rdd2获取中间结果
# storageLevel 可以指定缓存界别
rdd2.persist(storageLevel=StorageLevel.MEMORY_ONLY_2)
# 触发缓存需要使用action算子
rdd2.collect()  # 会将数据缓存

rdd3 = rdd2.map(lambda x: x * 2)

print(rdd3.collect())

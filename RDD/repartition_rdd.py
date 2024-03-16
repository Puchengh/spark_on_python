# 分区的意义在于减少分区，如果增加分区数会增加空的分区
from pyspark import SparkContext

sc = SparkContext()

rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6])

map_rdd = rdd1.map(lambda x: x + 1)

# 修该rdd分区信息
repartition_rdd = map_rdd.repartition(2)  # 未加之前是四个分区  [[2], [3, 4], [5], [6, 7]]  加了之后是两个分区 [[2, 5, 6, 7], [3, 4]]

print(repartition_rdd.glom().collect())  # glom() 查看rdd的分区信息  根据分区算法来分区的

# 多个rdd操作
from pyspark import SparkContext

# sc = SparkContext(master='yarn')
# sc = SparkContext(master='spark://hadoop:7077')
sc = SparkContext()

rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([5, 6, 7, 4])

rdd_kv1 = sc.parallelize([('a', 1), ('b', 2), ('c', 3)])
rdd_kv2 = sc.parallelize([('c', 4), ('d', 5), ('e', 6)])

# rdd之间的合并
# rdd1合并rdd2 合并之后会返回结果  不会有去重操作
union_rdd = rdd1.union(rdd2).distinct()
union_rdd_kv = rdd_kv1.union(rdd_kv2).distinct()
print(union_rdd.collect())  # 结果:[1, 2, 3, 4, 5, 6, 7]
print(union_rdd_kv.collect())  # 结果:[('b', 2), ('c', 4), ('a', 1), ('e', 6), ('c', 3), ('d', 5)]

# kv形式的rdd进行关联  通过key关联的  join之后是无序的
# 内关联 相同key的会保留下来
# 左关联  左边的rdd的数据会被保留下来   如果右边的rdd有key会显示，没有对应的key会显示
# 右关联  右边的rdd的数据会被保留下来   如果左边的rdd有key会显示，没有对应的key会显示

join_rdd = rdd_kv1.join(rdd_kv2)
left_join_rdd = rdd_kv1.leftOuterJoin(rdd_kv2)
right_join_rdd = rdd_kv1.rightOuterJoin(rdd_kv2)
print(join_rdd.collect())  # 结果：[('c', (3, 4))]
print(left_join_rdd.collect())  # 结果：[('a', (1, None)), ('b', (2, None)), ('c', (3, 4))]
print(right_join_rdd.collect())  # 结果：[('e', (None, 6)), ('c', (3, 4)), ('d', (None, 5))]

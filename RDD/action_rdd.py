# 行动算子的使用 action算子
from pyspark import SparkContext

sc = SparkContext()

# python转化为rdd
rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
rdd_t = sc.parallelize(['a', 'b', 'c', 'a'])

# transformation 算子的转化之后执行触发计算

# collect 方法 触发计算获取所有计算结果 action算子计算完成返回的计算结果 不再是rdd了 不能在进行rdd操作了.
res = rdd.collect()
print(res)
# reduce方法，传递一个计算逻辑，对元素数据进行累加计算
# 可以不需要转化算子 直接进行累加计算，但是不能处理kv形式的数据
res1 = rdd.reduce(lambda x, y: x + y)
print(res1)

# count 获取rdd中元素的个数
res2 = rdd.count()
print(res2)

# take取指定数量的元素数据
res3 = rdd.take(3)
print(res3)

# 属于action的高级算子
from pyspark import SparkContext

sc = SparkContext()

rdd = sc.parallelize([1, 2, 3, 4, 5, 6])

# action 算子的使用
# first 取rdd中的第一个元素值
res = rdd.first()
print(res)  # 结果： 1

# top 先进行排序 从大到小的排序 取出指定数据量的元素数据
res = rdd.top(3)
print(res)  # 结果：[6, 5, 4]

# takeOrdered 先进行rdd的排序 从小到大的排序 指定取出对应数量
res = rdd.takeOrdered(3)
print(res)  # 结果：[1, 2, 3]

# takeSample  随机取值  三个参数  1.是否允许取重复值 True 允许  2.指定取值的个数  3.随机数种子, 可以指定任意值 没有区别 随机算法需要一个值，不指定的话会用默认值
res = rdd.takeSample(True, 3, 123)
print(res)

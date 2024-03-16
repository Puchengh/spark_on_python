from pyspark import SparkContext
import os

# sc = SparkContext()   #本地模式

sc = SparkContext(master='spark://hadoop:7077')  # standaload模式
# sc = SparkContext(master='yarn')

rdd_date = sc.parallelize([1, 2, 3, 4, 5, 6])

res = rdd_date.reduce(lambda x, y: x + y)

print(res)

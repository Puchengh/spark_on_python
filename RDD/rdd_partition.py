from pyspark import SparkContext

sc = SparkContext()

data_list = [1, 2, 3, 4, 5]

# numSlices 指定分区数量
rdd1 = sc.parallelize(data_list, numSlices=3)

res1 = rdd1.glom().collect()

print(res1)
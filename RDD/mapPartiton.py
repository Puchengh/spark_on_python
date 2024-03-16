# 通过mapPartiton方法可以去读一次数据，将rdd1中的数据全部读取出来，然后用迭代器以此进行计算 可以提升效率

from pyspark import SparkContext

sc = SparkContext()

rdd1 = sc.parallelize([1, 2, 3, 4])


# 使用mapPartition  需要传递一个迭代器

def funct(data):
    # data接受整个分区的数据 mappartition将整个分区的数据传递给data
    for i in data:
        # res = i + 1  # 对整个分区内的数据进行计算
        res = (i, 'itcast')
        yield res


# 输出计算结果
partition_rdd = rdd1.mapPartitions(funct)
print(partition_rdd.collect())

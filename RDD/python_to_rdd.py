# 1.导入sparkcontext类
from pyspark import SparkContext

# 2.初始化SparkContext类型  默认使用本地资源
sc = SparkContext()

# 3.将数据转为rdd数据
data_list = [1, 2, 3, 4, 5]
data_dict = {'a': 1, 'b': 2}
data_dict2 = {('a',1), ('b',2)}
data_set = {1, 2, 3, 4, 5}

# parallelize 将python数据转为rdd
# rdd中处理的kv数据列表中嵌套元组

rdd1 = sc.parallelize(data_list)
rdd2 = sc.parallelize(data_dict)
rdd3 = sc.parallelize(data_set)
rdd4 = sc.parallelize(data_dict2)

# 4.使用rdd的方法进行计算

# 5.触发计算过程
# collect 方法可以出发转化操作  然后读取转化后的rdd中的所有数据
res1 = rdd1.collect()
res2 = rdd2.collect()
res3 = rdd3.collect()
res4 = rdd4.collect()
# 6.查看计算结果

print(res1)
print(res2)
print(res3)
print(res4)

# 调用程序就相当于启动了一个driver进程

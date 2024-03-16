# 在udf函数中 对字段数据的处理是一行一行的处理，无法对整个字段中的所有数据一次性处理 无法完成聚合的操作
# udaf函数的实现需要借助pandas中的series类型数据 该类型的数据可以接收一整列的数据，完成对字段所有数据的聚合操作
# pandas是python的一个数据分析包(numpy,matlab) 是为数据分析工具提供的
# pandas是单机资源计算的  不合适大数据的计算场景
# pandas 的主要数据结构是Series(一维数据) 与 DataFrame(二维数据)
# 通过索引取值 行索引 列索引
# Series 代表一列数据只有行索引 取出某行数据
# DataFrame 有行有列 同时又行索引 列索引,通过列索引取出的值就转为Series数据
# spark中的dataframe row对象 row[0] 和schema信息(列的名字)

# spark dataframe是由一行一行的row对象构成 分布式计算
# pandas dataframe 是由一列一列的series构成的 单机计算
# Series 是一种类似一维数组的对象 它是由一组数据以及一组与之相关的数据标签(索引)组成

import pandas as pd

s1 = pd.Series([1, 2, 3, 4, 5])
s2 = pd.Series(['a', 'b', 'c', 'd'])

# 查看series数据 可以根据索引取值
# print(s1)
# print(s2)

# 手动指定索引 不使用自动生成的 数据量较大是使用自动生成的索引
# index指定索引的值
s3 = pd.Series([1, 2, 3, 4, 5], index=['a', 'b', 'c', 'd', 'e'])
print(s3['a'])

# series聚合方法操作
print(f'求和：{s3.sum()}')
print(f'求平均数：{s3.mean()}')
print(f'求最大值：{s3.max()}')
print(f'求最小值：{s3.min()}')
print(f'求最个数：{s3.count()}')

print('-' * 100)

# DataFrame 是一个表格型的数据结构，它含有一组有序的列，每列可以是不同的值类型（数值字符串，布尔型值），可以看做是一种二维表格型数据。
# 行索引是index 列索引是columns  他可以被看做由Series组成的字典（共同用一个索引）
# pandas的dataFrame和spark的dataFrame的转化
# pandas的数据在计算使用的是单机资源进行计算，想要进行分布式计算，利用多台计算机资源，此时就可以将pandas的dataFrame
# 列索引 可以通过取出一列数据
df = pd.DataFrame(
    [
        [1, '张三', 22],  # 代表一行数据
        [2, '李四', 18],  # 第二行数据
        [3, '王五', 19],
        [4, '赵六', 32],
        [5, 'python', 21]
    ]
)

# 展示df数据
# 注意pandas的df没有show()
print(df)

# 按照索引取值
col1 = df[1]  # 取出一列数据是Series类型
print(type(col1))
print(col1)

# 取出某列中的某行数据
# 第一个[1]指定取那列数据 第二个[2]指定取该列中的第一行数据
print(df[1][2])

# 列索引本质就是字段 为了方便取值 可以通过指定列索引给数据命名字段
print('-' * 100)
df2 = pd.DataFrame(
    [
        [1, '张三', 22],  # 代表一行数据
        [2, '李四', 18],  # 第二行数据
        [3, '王五', 19],
        [4, '赵六', 32],
        [5, 'python', 21]
    ],
    columns=['id', 'name', 'age']  # 指定列索引
)
print(df2)
print(df2['name'])

col2 = df2['age']
print(col2.sum())

print('-' * 100)
# 使用字典的方式定义df
df3 = pd.DataFrame({
    'id': [1, 2, 3, 4, 5, 6],
    'name': ['a', 'b', 'c', 'd', 'e', 'f'],
    'age': [20, 30, 22, 100, 12, 11],
})

print(df3)

print('-' * 100)
# pandas的DataFrame转化为spark中的DataFrame,就可以把单机资源的DataFrame转化为spark的DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# 生成saprksession对象
ss = SparkSession.builder.master('yarn').config('spark.sql.execution.arrow.pyspark.enabled','true').getOrCreate()

# 定义schema信息
schema_type = StructType().add('id', IntegerType()).add('name', StringType()).add('age', IntegerType())

# 将pandas的df转化为spark的df
spark_df = ss.createDataFrame(df3, schema_type)

# spark的df数据展示
spark_df.show()

# 将spark的df转化为pandas的df
pd_df = spark_df.toPandas()
print(pd_df)

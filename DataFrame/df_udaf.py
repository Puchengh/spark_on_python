# UDAF函数
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import pandas as pd

ss = SparkSession.builder.getOrCreate()

# 使用sparkcontext 读取学生文件信息

sc = ss.sparkContext  # 属于sparksession的属性

# 读取文件生成rdd数据
rdd = sc.textFile('hdfs:///data/test_df')

# 将rdd数据转化为dataframe
# 先将rdd转为二维嵌套
table_rdd = rdd.map(
    lambda x: [int(x.split(',')[0]), x.split(',')[1], x.split(',')[2], int(x.split(',')[3]), x.split(',')[4]])

# 定义schema_type信息  指定字段名和字段类型
schema_type = (StructType().
               add('id', IntegerType()).
               add('name', StringType()).
               add('gender', StringType()).
               add('age', IntegerType()).
               add('cls', StringType()))

df = table_rdd.toDF(schema_type)  # 相当于生成了一个df


# 自定义函数 完成对字段数据的求和
# 注册时候需要通过两步注册，1.使用装饰器进行装饰 可以使用pandas的方法 2.使用register方法
@F.pandas_udf(IntegerType())
def func(a: pd.Series) -> int:  # int指定返回指定类型
    # a 就是一个参数 该参数需要接受字段的整列数据 所以还需要对该参数指定类型 pd.Series是他的类型

    # 可以替换a.sum()
    # res = 0
    # for i in a:
    #     res += i
    return a.sum()


sum_func = ss.udf.register('sum_func', func)

# 使用 DSL方法使用
sum_df = df.select(sum_func('age').alias('sum_data'))

# 使用sql方法
df.createOrReplaceTempView('stu')
sum_df2 = ss.sql('select sum_func(age) from stu')

df.show()
sum_df.show()
sum_df2.show()

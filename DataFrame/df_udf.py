# 自定义函数
# 用户可以根据需求自己封装计算逻辑 对字段数据进行计算
# 内置函数 时spark提供的对字段操作的方法 split(字段)对字段中的数据进行切割 F.sum()会将该字段下的数据进行求和
# 实际业务中又能内置函数不满足计算需求 此时就需要自定义函数 完成字段数据的业务处理

# 函数分类
# udf函数 一进一出 传递一行数据返回一行计算的结果  可以自定义
# udaf函数 多进一出 传递多行返回一行  集合函数 借助pandas 可以自定义
# udtf函数 一进多出 传递一行返回多行 爆炸函数  不可以自定义

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# 生成sparksession对象
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

# ----------------------------------------register注册------------------------------------------------------------------
# # 自定义一个字符串拼接方法
# def func(a, b):  # 一次处理一行数据
#     # a,b两个接收参数
#     return a + '--' + b
#
#
# # 将自定义函数进行注册到spark
# # 参数 1.注册函数的名字  2.自定义的函数名 3.指定数据的返回类型
# # register注册后会返回一个注册的函数
# concat_func = ss.udf.register('concat_func', func, StringType())
#
# # 使用自定义函数 可以在dsl中使用
# udf_df = df.select(concat_func('name', 'gender').alias('concat'))
#
# # 也可以在sql语句中使用
# df.createOrReplaceTempView('stu')
# udf_df2 = ss.sql('select concat_func(name,gender) from stu')
#
#
# udf_df.show()
# udf_df2.show()


# ----------------------------------------使用内置函数提供的装饰器----------------------------------------------------------
from pyspark.sql import functions as F


# 自定义一个字符串拼接方法

@F.udf(StructType())
def func(a, b):  # 一次处理一行数据
    # a,b两个接收参数
    return a + '--' + b


# 直接使用func名调用就可以了
# 在装饰器中使用的时候不能在dsl中使用
udf_df = df.select(func('name', 'gender').alias('concat'))
udf_df.show()

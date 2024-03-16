# 内置函m模块 spark中的内置函数完成对字段数据的操作 和hive内置的操作基本一样
# 里面封装了不同的方法
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# ss = SparkSession.builder.getOrCreate()
# ss = SparkSession.builder.master('yarn').getOrCreate()   # 采用yarn管理
# ss = SparkSession.builder.master('hdfs://hadoop:7077').getOrCreate()  #采用standalone模式
# 指定计算任务名称
# ss = SparkSession.builder.master('yarn').appName('yarn_spark').getOrCreate()
ss = SparkSession.builder.master('yarn').appName('yarn_spark').config('spark.sql.shuffle.partitions','200').getOrCreate()

sc = ss.sparkContext

rdd = sc.parallelize(
    [[1, 'zhangsan', 20, '男', '1988-01-01', '1705155605'],
     [2, 'lisi', 20, '男', '1988-01-01', '1705155605'],
     [32, 'wangwu', 22, '男', '1988-01-01', '1705155605']
     ]
)

# 定义schema类型
schema_type = (StructType().add('id', IntegerType()).
               add('name', StringType()).
               add('age', IntegerType(), False).
               add('gender', StringType()).
               add('date', StringType()).
               add('unix_t', StringType()))

df = rdd.toDF(schema_type)

# 内置函数使用
# 符串操作
new_df1 = df.select(F.concat('name', 'gender'))  # select concat(name,gender) from df
new_df2 = df.select(F.concat_ws(',', 'name', 'gender'))  # select concat_ws(',',name,gender) from df
new_df3 = df.select(F.substring('name', 1, 2))  # 字符串的截取
new_df4 = df.select(F.split('date', '-')[0])  # 字符串的切割

# 时间操作
new_df5 = df.select(F.current_date())
new_df6 = df.select(F.current_timestamp())
new_df7 = df.select(F.unix_timestamp())  # 时间的字符串

# 将unix时间转化为标准时间
new_df8 = df.select(F.from_unixtime('unix_t', format='yyyyMMdd HH:mm:ss'))  # 时间的字符串
new_df9 = df.select(F.date_add('date', 1))  # 日期加减

new_df10 = df.select(F.format_number('age', 2))

# new_df1.show()
# new_df2.show()
# new_df3.show()
# new_df4.show()
# new_df5.show()
# new_df6.show()
# new_df7.show()
# new_df9.show()
# new_df10.show()


# 聚合方法 先分组在聚合
# 内置函数中有聚合操作方法 F.sum F.avg F.min
# 需要配合df中的agg方法一起使用
# 多个方式聚合计算的时候使用spark的内置函数

# 取别名 alias
new_df11 = df.groupBy('gender').agg(F.sum('age'), F.avg('age'), F.max(
    'age'))  # select gender,sum(age) from df group by gender  多个聚合后操作
new_df12 = df.select(F.round('age', 2))
new_df13 = df.select(F.round('age', 2).alias('roud_date'))

new_df11.show()
new_df12.show()
new_df13.show()

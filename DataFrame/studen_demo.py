# 处理学生数据
# 导入sparkSeesion

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

# 对df数据进行操作
# 指定查询的字段数据
select_df1 = df.select('name')  # 类似select name from df

select_df2 = df.select(['id', 'name', 'age'])

# 第二种指定方式
select_df3 = df.select(df['name'], df['gender'])

# 展示所有信息
# where的条件过滤
where_df1 = df.where('age >= 20')  # select * from df were age >= 20  过滤年龄大于20的数据

# 多个条件的与或非  聚合方法  sum,avg,min,max 和sql中的方法一样
where_df2 = df.where('age >= 20 and gender = "男"')  # select * from df were age >= 20 and gender = "男"

where_df3 = df.where('age >= 20 or gender = "男"')  # select * from df were age >= 20 or gender = "男"

groupBy_df1 = df.groupby(['gender', 'id']).sum('age')  # select gender,id,sum(age) from df group by gender,id
groupBy_df2 = df.groupby('gender').sum('age')  # select ender,sum(age) from df group by gender

# 分组后的过滤
where_groupBy_df3 = df.groupby('gender').sum('age').where(
    'sum(age) > 40')  # select ender,sum(age) from df group by gender having sum(age) > 40

# 排序
orderby_groupBy_df1 = df.select(['name', 'age']).orderBy('age',
                                                         ascending=False)  # select name,age from df order by age 默认是是升序

# 指定的返回的数量 spark不支持使用
limit_df1 = df.select('age').limit(2)

# 读取所有rdd中的学生信息数据
print(table_rdd.collect())

# df.show(2)  # 可以指定展示多少行数据 show()方法返回的结果就是打印的信息 而不是一个新的df
# # 查看df数据 不使用select过滤条件 直接进行show 就是对df当前的所有字段进行展示  默认展示前20行的数据
# df.show()
#
# select_df1.show()
# select_df2.show()
# select_df3.show()

# where_df1.show()
# where_df2.show()
# where_df3.show()
# groupBy_df1.show()

# where_groupBy_df3.show()

# orderby_groupBy_df1.show()
limit_df1.show()

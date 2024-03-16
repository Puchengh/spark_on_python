# 处理电影数据
# 导入sparkSeesion
# 数据连接 https://files.grouplens.org/datasets/movielens/ml-100k/u.data

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# 生成sparksession对象
ss = SparkSession.builder.master('yarn').config('spark.sql.shuffle.partitions', 2).getOrCreate()

# 使用sparkcontext 读取学生文件信息

sc = ss.sparkContext  # 属于sparksession的属性

# 读取文件生成rdd数据
rdd = sc.textFile('hdfs:///data/movie.txt')

# 将rdd数据转化为dataframe
# 先将rdd转为二维嵌套
table_rdd = rdd.map(lambda x: [int(x.split('\t')[0]), int(x.split('\t')[1]), int(x.split('\t')[2]), x.split('\t')[3]])

# print(table_rdd.take(20))
# 定义schema_type信息  指定字段名和字段类型
schema_type = (StructType().
               add('user_id', IntegerType()).
               add('movie_id', IntegerType()).
               add('score', IntegerType()).
               add('unix_time', StringType()))

df = table_rdd.toDF(schema_type)  # 相当于生成了一个df

# 需求实现
# 1.查询每个用户平均分
user_avg_df1 = df.groupBy('user_id').agg(F.avg('score').alias('avg_data'))

# 2.查询每个电影平均分
movie_avg_df1 = df.groupBy('movie_id').agg(F.avg('score').alias('avg_data'))
# 3.查询最高分电影中(>3)打分次数最多的用户，并且求出此人的平均分
# 分布计算
# first取出的数据就是一个Row对象
user_row_df = df.where('score > 3').groupBy('user_id').agg(F.count('movie_id').alias('count_data')).orderBy(
    'count_data', ascending=False).first()
# 取出最高电影的用户id
user_id = user_row_df[0]

max_userid_ag = user_avg_df1.where(user_avg_df1['user_id'] == user_id)

# 4.查询每个用户的平均分，最低打分，最高打分
min_max_df1 = df.groupBy('user_id').agg(F.min('score').alias('min_data'), F.max('score').alias('max_data'))

# 5.查询被评分超过100次的电影的平均分，排名TOP10
top10_df = df.groupBy('movie_id').agg(F.count('movie_id').alias('count_date'),
                                      F.avg('score').alias('avg_data')).where('count_date>100').orderBy('avg_data',
                                                                                                        ascending=False).limit(
    10)

# user_avg_df1.show(4)
# movie_avg_df1.show(4)
# max_userid_ag.show()
# min_max_df1.show()
top10_df.show()

# 分区树木
# 在进行sparkSql计算是 SparkSql中有一个catalyst引擎，会根据数量调整分区，但是在进行shuffle时候会将分区调整为200个，分区过大会存在浪费
# 在sparkSQL中当Job中产生shuffle时，默认的分区数（spark.sql.shuffle.partitions） 为200 在实际项目中要合理的设置
# ss = SparkSession.builder.master('yarn').config('spark.sql.shuffle.partitions', 2)l.getOrCreate()

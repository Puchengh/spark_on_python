# 处理电影数据
# 导入sparkSeesion
# 数据连接 https://files.grouplens.org/datasets/movielens/ml-100k/u.data

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# 生成sparksession对象
ss = SparkSession.builder.getOrCreate()
sc = ss.sparkContext
# 使用sparksession的read方法惊醒读取不同的数据源

# txt_df1 = ss.read.text('file:///root/movie.txt')
# txt_df2 = ss.read.text('file:///root/environment.yml')

# 读取csv文件的时候指定分隔符
# orc_df3 = ss.read.orc('file:///root/movie.txt', sep=',')
# txt_df1.show()
# txt_df2.show()

# 读取mysql的文件
mysql_df = ss.read.jdbc(url='jdbc:mysql://192.168.116.135:3306/test?charsetEncoding=UTF-8', table='table_name',
                        properties={'user': 'test', 'password': '123456', 'dirver': 'com.mysql.jdbc.Driver'})

mysql_df.show()

# ----------------------------------------------------------------------------------------------------------------------

rdd = sc.parallelize([[1, 'zhangsan', 20], [2, 'lisi', 20]])

# 定义schema类型
schema_type = StructType().add('id', IntegerType()).add('name', StringType()).add('age', IntegerType(), False)

df = rdd.toDF(schema_type)

# 写入的两种方式 overwrite和append
df.write.json('hdfs:///data/json_file', mode='overwrite')
# 写入orc文件
df.write.orc('hdfs:///data/orc_file', mode='overwrite')
# 写入csv文件
df.write.csv('hdfs:///data/csv_file', mode='overwrite', sep='\t')

mysql_df = df.write.jdbc(url='jdbc:mysql://192.168.116.135:3306/test?charsetEncoding=UTF-8', table='table_name',
                         properties={'user': 'test', 'password': '123456', 'dirver': 'com.mysql.jdbc.Driver'},
                         mode='overwrite')

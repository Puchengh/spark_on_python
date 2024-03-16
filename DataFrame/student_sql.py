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


# 使用sql语句操作df
# 使用sparkSession提供的方法
# 需要现将df数据指定一个表的名字
df.createOrReplaceTempView('stu')

sql_df = ss.sql('select name,gender from stu')

sql_df = ss.sql('select gender,sum(age) from stu group by gender')

sql_df.show()

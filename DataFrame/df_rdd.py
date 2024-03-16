from pyspark.sql import SparkSession
from pyspark.sql.types import *

# 生成SparkSession对象
ss = SparkSession.builder.getOrCreate()

# SparkSession中可以通过sparkContext获取sparkContext对象
sc = ss.sparkContext

# 生成RDD

rdd = sc.parallelize([[1, 'zhangsan', 20], [2, 'lisi', 20]])

# 定义schema类型
schema_type = StructType().add('id', IntegerType()).add('name', StringType()).add('age', IntegerType(), False)

# 将二维的rdd数据转化为dataFrame数据
df = rdd.toDF(schema_type)

df.show()

# 将df数据重新转化为rdd数据
# df有个rdd可以将df数据重新转化为二维rdd
new_rdd = df.rdd
print(new_rdd.collect())
map_rdd = new_rdd.map(lambda x: (x['name'], x['id'], x['age']))
print(map_rdd.collect())

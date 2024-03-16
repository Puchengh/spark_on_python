# 缓存和checkpoint
from pyspark.sql import SparkSession, Row
# 导入定义字段的类型
from pyspark.sql.types import *

# dataFrame的创建方法 是由sparksession提供的 所以需要生成sparksession对象

ss = SparkSession.builder.getOrCreate()  # 是一个固定的写法 就可以生成一个sparksession对象

sc = ss.sparkContext

# 进行checkpoint的存储位置
sc.setCheckpointDir('hdfs:///spark_check')

rdd = sc.parallelize([[1, 'zhangsan', 20], [2, 'lisi', 20]])

# 定义schema类型
schema_type = StructType().add('id', IntegerType()).add('name', StringType()).add('age', IntegerType(), False)

df = rdd.toDF(schema_type)

# df进行缓存
# df.persist()

# 进行checkPoint
df.checkpoint()

df.where('id=1').show()  # 在进行df计算时,如果计算错误，就直接读取df的缓存数据从新计算

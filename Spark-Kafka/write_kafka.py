from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *

# pd_df = pd.DataFrame({
#     'id': [1, 2, 3, 4],
#     'name': ["张三", "李四", "王五", "赵六"]
#
# })


pd_df = pd.DataFrame({
    'id': [1, 2, 3, 4],
    'name': ['张三', '李四', '王五', '赵六']
})

# 生成saprksession对象
ss = SparkSession.builder.getOrCreate()

# 定义schema信息
schema_type = StructType().add('id', IntegerType()).add('name', StringType())

# 创建spparksession对象
ss = SparkSession.builder.getOrCreate()
# 将pandas的df转为spark的df
df = ss.createDataFrame(pd_df, schema_type)

# 将学生信息写入kafka
# 将df的表结构转为kafka写入时需要的表结构
# key,value(必须) 类型是字符串,topic,partiton 。。。。
# 需要写入kafa中的数据必须是value字段名，并必须有该字段
df.createTempView("stu")
df2 = ss.sql('select name as value, "itcast6" as topic ,0 as partition from stu')
df2.show()
# 写入数据到kafka中
df2.write.format('kafka').option('kafka.bootstrap.servers','hadoop:9093').save()
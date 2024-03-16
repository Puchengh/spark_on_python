# 导入数据类型
from pyspark.sql.types import *

# 定义schema类型 使用structType类进行定义
# add是指定字段信息 有三个参数 1.字段名 2.字段信息 3.是否允许为空值  默认是True
schema_type = (StructType().
               add('id', IntegerType()).
               add('name', StringType()).
               add('age', IntegerType(), False))

# DataFrame创建
# 需要使用到sparkSession的类型创建 提供了创建df的方法
# sparkSession类是在SparkContext的基础上进行了封装，也就是SparkSession类中包含了SparkContext

from pyspark.sql import SparkSession, Row
# 导入定义字段的类型
from pyspark.sql.types import *

# dataFrame的创建方法 是由sparksession提供的 所以需要生成sparksession对象

ss = SparkSession.builder.getOrCreate()  # 是一个固定的写法 就可以生成一个sparksession对象

# 定义每行数据
r1 = Row(id=1, name='张三', age=20)
r2 = Row(id=2, name='李四', age=22)

# 定义字段类型
schema_type = StructType().add('id', IntegerType()).add('name', StringType()).add('age', IntegerType(), False)

# 创建dataFrame
# 使用SparkSession对象中的createDataFrame方法创建
# 两个参数 1,，指定行数据，将每行数据放入列表中  2，指定表信息
# 创建成功后就可以得到一个df类型的数据
df = ss.createDataFrame([r1, r2], schema_type)

# 查看df的表数据
df.show()

# 产看schema信息
# 使用类的方式可以定义类型
df.printSchema()

# 定义schema信息
# 1.shcema中的字段名和行中的字段名  是按照schema中数据顺序读取数据的
# 2.定义的字段类型要和数据本身数据类型保持一致
# 3.定义的字段数量要和每行中的字段数量保持一致

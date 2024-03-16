# 导入row类
from pyspark.sql import Row

# row表示dataframe的一行数据

r1 = Row(1, 'r1-zhang', 20)
r2 = Row(2, 'r2-lisi', 20)

# 从行中可以通过下标取值
print(r1[1])
print(r2[1])

r3 = Row(id=1, name='r3-zhang', age=20)
r4 = Row(id=2, name='r4-lisi', age=20)

# 从行中可以通过字段名取值
print(r3['name'])
print(r4['name'])

# Memory 内存
# 将计算的结果输出内存

from time import sleep

from pyspark.sql import SparkSession, functions as F

# 1、生成sparkSession对象
ss = SparkSession.builder.getOrCreate()

# 2、采用socket方式读取数据处理
df = ss.readStream. \
    format('socket'). \
    option('host', '192.168.116.128'). \
    option('port', 9999). \
    load()

# 3、 对数据进行处理
df2 = df.select(
    F.split('value', ',')[0].alias('id'),
    F.split('value', ',')[1].alias('name'),
    F.split('value', ',')[2].alias('age')
)

# 输出数到内存
# 输出内容时，可以指定一个queryname  相当于对内存中的表数据命名了一个表名
# awaitTermination 方法是等待数据流的数据，会阻塞当前进程结束
df2.writeStream. \
    outputMode('append'). \
    format('memory'). \
    queryName('stu'). \
    start()

# 使用sparksql的方法进行sql语句编写
while True:
    # 手动等待数据
    print('while')
    # sql返回一个df需要show输出结果
    # 会持续从内存中获取数据
    ss.sql('select * from stu').show()
    sleep(2)

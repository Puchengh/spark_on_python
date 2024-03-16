from pyspark.sql import SparkSession, functions as F

# 1、生成sparkSession对象
ss = SparkSession.builder.getOrCreate()

# 2、采用socket方式读取数据处理
df = ss.readStream. \
    format('socket'). \
    option('host', '192.168.116.128'). \
    option('port', 9999). \
    load()

# 在写入数据之前需要现将转化为kafka的表结构形式

df2 = df.select([F.split('value', ',')[0].alias('value'), F.split('value', ',')[1].alias('topic')])

# 查看结果信息
# df2.writeStream. \
#     outputMode('append'). \
#     format('console'). \
#     start(). \
#     awaitTermination()

# 写入
# key,value(必须),topic,offset,partition
df2.writeStream.\
    outputMode('append').\
    format('kafka').\
    option('kafka.bootstrap.servers','hadoop:9093').\
    option('checkpointLocation','hdfs://hadoop:9000/chpoint2').\
    start().\
    awaitTermination()

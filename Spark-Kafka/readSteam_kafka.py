# from pyspark.sql import SparkSession,functions as F
# ss = SparkSession.builder.getOrCreate()
#
# # 数据流方式读取kafka数据
# # option('subscribe','itcast6,itcast7').\
# df = ss.readStream.\
#     format('kafka').\
#     option('kafka.bootstrap.servers','hadoop:9093').\
#     option('subscribePattern','it.*').\
#     load()
#
# # 数据处理 将value转化为string类型  value原生的是byte array类型
# df2 = df.select(['key',F.expr('cast(value as string)'),'topic','partition','offset','timestamp','timestampType'])
#
# # 展示结果
# df2.writeStream.outputMode('append').format('console').start().awaitTermination()
#


# 离线计算时也可以从kafka中获取数据进行处理
# 在kafka的topic上，spark进行批处理时，代码运行一次只能读取一次

from pyspark.sql import SparkSession, functions as F

ss = SparkSession.builder.getOrCreate()

# 批数据读取
df = ss.read.format('kafka'). \
    option('kafka.bootstrap.servers', 'hadoop:9093'). \
    option('subscribePattern', 'itcast6'). \
    option('startingOffsets', '{"itcast6":{"0":1}}'). \
    option('endingOffsets', '{"itcast6":{"0":-1}}'). \
    load()

# 展示读取到一批数据
df2 = df.select(['key', F.expr('cast(value as string)'), 'topic', 'partition', 'offset', 'timestamp', 'timestampType'])
df2.show(50)

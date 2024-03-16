from pyspark.sql import SparkSession, functions as F

# 1、生成sparkSession对象
ss = SparkSession.builder.getOrCreate()

# 2、采用socket方式读取数据处理 获取source数据
df = ss.readStream. \
    format('socket'). \
    option('host', '192.168.116.128'). \
    option('port', 9999). \
    load()

# 3.1,数据的处理
# append模式
# app[end不会显示所有数据
# append的模式输出不支持聚合操作
# df2 = df.select(F.explode(F.split('value', ',')).alias('word'))

# df2.writeStream. \
#     outputMode('append'). \
#     format('console'). \
#     start(). \
#     awaitTermination()  # 等待数据的写入

# 3.2,数据的处理
# complete模式
# 会显示所有的信息,并且进行聚合计算
# df2 = df.select(F.explode(F.split('value', ',')).alias('word')).groupBy('word').count()
#
# df2.writeStream. \
#     outputMode('complete'). \
#     format('console'). \
#     start(). \
#     awaitTermination()  # 等待数据的写入

# 3.3,数据的处理
# update模式
# 当数据只有新增，没有聚合类似append；如果对数据进行聚合，只会显示更新的数据
# 没有聚合操作的时候和append是一样的 有聚合操作的时候只展示更新的数据 也不是全部展示内容
df2 = df.select(F.explode(F.split('value', ',')).alias('word')).groupBy('word').count()

df2.writeStream. \
    outputMode('update'). \
    format('console'). \
    start(). \
    awaitTermination()  # 等待数据的写入



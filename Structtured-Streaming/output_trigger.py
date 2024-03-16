from pyspark.sql import SparkSession, functions as F

# 1、生成sparkSession对象
ss = SparkSession.builder.getOrCreate()

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

# trigger(processingTime='10 seconds')  10秒钟之内全部处理完才会一次处理数据
# trigger(once=True) 一次性微批处理  once=True  默认是false, 只会执行一次，适用于初始化 关闭资源这种只执行一次的场景
# trigger(continuous='5 seconds') 以固定时间间隔连续处理,对固定间隔进行优化, 从而减低了延迟性的问题, 和固定间隔微批处理行为上很像，无论是否有数据，到指定间隔时间后都会触发
df2.writeStream.outputMode('append'). \
    format('console'). \
    trigger(processingTime='10 seconds'). \
    start(). \
    awaitTermination()

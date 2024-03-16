from pyspark.sql import SparkSession

# 1、生成sparkSession对象
ss = SparkSession.builder.getOrCreate()

# 2、采用socket方式读取数据处理
df = ss.readStream. \
    format('socket'). \
    option('host', '192.168.116.128'). \
    option('port', 9999). \
    load()

# 3、采用dsl方式，数据处理
df2 = df.where('value > 5')

# 4、输出结果
df2.writeStream. \
    outputMode('append'). \
    format('console'). \
    start(). \
    awaitTermination()

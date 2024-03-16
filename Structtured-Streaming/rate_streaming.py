from pyspark.sql import SparkSession

# 1、生成SparkSession对象
ss = SparkSession.builder.getOrCreate()

# 2、指定读取方式生成df
# rampUpTime 等待十秒之后才能生成数据
df = ss.readStream. \
    format('rate').\
    option('rowsPerSecond',5).\
    option('rampUpTime',10).\   
    load()

# 3、处理数据

# 4、展示结果
df.writeStream.\
    outputMode('append').\
    format('console').\
    start().\
    awaitTermination()
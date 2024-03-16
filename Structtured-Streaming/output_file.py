from pyspark.sql import SparkSession, functions as F

# 1、生成sparkSession对象
ss = SparkSession.builder.getOrCreate()

# 2、采用socket方式读取数据处理
df = ss.readStream. \
    format('socket'). \
    option('host', '192.168.116.128'). \
    option('port', 9999). \
    load()

df3 = df.select(
    F.split('value', ',')[0].alias('id'),
    F.split('value', ',')[1].alias('name'),
    F.split('value', ',')[2].alias('age')
)

# 4、输出结果
# start中  指定路径 path='hdfs:///file_data'
# 文件写入需要 option("checkpointLocation",'hdfs://hadoop:9000/file_data')
# trigger 写入数据时的时间间隔  5 seconds   5秒内的数据是不会写入，等到五秒中后，在将数据一起写入
# 1 minute 分钟为单位进行数据写入
# 本地将hdfs改为file
df3.writeStream.outputMode('append').option("checkpointLocation", 'hdfs://hadoop:9000/file_date2').format(
    'json').trigger(processingTime='1 minute').start(path='hdfs://hadoop:9000/file_date2').awaitTermination()

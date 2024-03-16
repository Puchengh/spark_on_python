from pyspark.sql import SparkSession, functions as F

# 生成sparksession对象
ss = SparkSession.builder.getOrCreate()

# 读取流数据
# ss.read 去读批数据
# format 指定读取数据源的方式 采用socket
# load载入读取信息  生成一个df对象
df = (ss.readStream.format('socket')
      .option('host', '192.168.116.128')
      .option('port', 9999)  # 服务器端口
      .load())

# df就是一个无界表数据，按照对应表的操作完成数据处理
# 可以使用内置函数进行相应的处理
# df.select(F.explode(F.split('value', ',')).alias('word')).groupBy('word').count()
# withColumn 将处理后的数据增加新的一列
df1 = df.withColumn('word',F.explode(F.split('value', ','))).groupBy('word').count()

# 输出结果信息
# outputMode 指定输出模式
# format 指定输出的位置  console 输出到终端,不支持聚合操作  complete 支持聚合计算
# start表示开始进行流数据处理
# awaitTermination  会等待接受流数据

(df1.writeStream
 .outputMode('complete')
 .format('console')
 .start()
 .awaitTermination())

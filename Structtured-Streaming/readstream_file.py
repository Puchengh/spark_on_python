from pyspark.sql import SparkSession, functions as F

# 生成sparksession对象
ss = SparkSession.builder.getOrCreate()

# 读取流数据
# ss.read 去读批数据
# format 指定读取数据源的方式 采用socket
# load载入读取信息  生成一个df对象
# path 指定读取文件的路径  file://  HDFS://

# 第一种读取文件的方式
# df = (ss.readStream.format('csv')
#       .schema('id int,first_name string,last_name string,email string,gender string,ip_address string')
#       .load(path='file:///root/MOCK_DATA'))

# 第二种读取文件的方式 建议使用第二种方式
df = ss.readStream.csv('file:///root/MOCK_DATA',seq=';',
                  schema='id int,first_name string,last_name string,email string,gender string,ip_address string')

# 输出结果信息
# outputMode 指定输出模式
# format 指定输出的位置  console 输出到终端,不支持聚合操作  complete 支持聚合计算
# start表示开始进行流数据处理
# awaitTermination  会等待接受流数据

(df.writeStream
 .outputMode('append')
 .format('console')
 .start()
 .awaitTermination())

from pyspark.sql import SparkSession, functions as F

# 1、生成sparkSession对象
ss = SparkSession.builder.getOrCreate()

# 2、采用socket方式读取数据处理
df = ss.readStream. \
    format('socket'). \
    option('host', '192.168.116.128'). \
    option('port', 9999). \
    load()

df2 = df.select(
    F.split('value', ',')[0].alias('id'),
    F.split('value', ',')[1].alias('name'),
    F.split('value', ',')[2].alias('age')
)

# 设置rdd分区数
df3 = df2.repartition(2)


# 定义函数
def func(row):
    # row参数接受df中的每行的row对象
    print(f"id:{row['id']},name:{row['name']}")


# 定义类
# 类名可以根据需求，自己定义
# 类中三个方法名必须是固定方法名
# open，process，close
class Writer():
    def open(self, partition_id, epoch_id):
        # 进行初始化相关的操作
        print('*' * 100)
        print('open方法')
        print(f'partition_id:{partition_id},epoch_id:{epoch_id}')

        # open方法必须返回一个True ，如果不返回，则不会调用process
        return True

    def process(self, row):
        # 进程方法, 每次处理数据都会执行
        # 接受df传递的数据，row参数是按照行对象接受的
        print('process方法')
        print(f"id:{row['id']},name:{row['name']}")

    def close(self, error):
        # 关闭方法, 一般来说, 仅当JVM 或者python进程再中间崩溃的时候,或者发生错误的时候 会被调度
        print('close')
        # 每次处理一个数据之后都会被调用一次，当有错误时，会将错误传递给error


# 输出结果
# foreach可以接受一个函数 然后将df2中的数据 按照行对象传递到自定义函数中，然后进行处理
# df3.writeStream.outputMode('append').foreach(Writer()).start().awaitTermination()

# foreach 可以接受一个函数，然后将df2中的数据，按照行对象传递到自定函数中，然后进行处理
# foreach 还可以接受一个类对象 Writer()
# sparksql的引擎在最后计算的时候转话为rdd的
# 当前数据是在rdd的0分区处理 但是rdd中默认有两个分区 还有一个一分区的数据 1分区内没有数据 所以输出的结果是没有数据信息的
df3.writeStream. \
    outputMode('append'). \
    foreach(Writer()). \
    start(). \
    awaitTermination()

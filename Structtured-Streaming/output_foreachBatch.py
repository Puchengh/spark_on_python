# foreachBatch 和 foreachBatch
# 区别：
# 1、foreach中可以传递函数，和对象     foreachBatch  只能传递函数
# 2、foreach传递的的数据是df的row对象数据    foreachBatc  传递的数据是整个df数据
from pyspark.sql import SparkSession, functions as F

# 1、生成sparkSession对象
ss = SparkSession.builder.getOrCreate()

# 2、采用socket方式读取数据处理
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


# 定义函数
def func(df, df_id):
    # df 参数接受df的整个数据
    df.show()
    df.write.json('hdfs://hadoop:9000/foreachbatch_file', mode='overwrite')  # 覆盖
    print(f'df_id:{df_id}')


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
        # 关闭方法, 一般来说, 仅当JVM 或者python进程再中间崩溃的时候, 会被调度
        print('close')
        # 每次处理一个数据之后都会被调用一次，当有错误时，会将错误传递给error


# 4、输出结果
# foreachBatch 可以接受一个函数，然后将df2传递给函数进行使用
df2.writeStream. \
    outputMode('append'). \
    foreachBatch(func). \
    start(). \
    awaitTermination()

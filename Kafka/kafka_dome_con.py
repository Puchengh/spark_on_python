# 生成消费者对象
from kafka import KafkaConsumer

# 第一个参数指定主题
consumer = KafkaConsumer('itcast6', bootstrap_servers=['hadoop:9093'])

# 对消费者循环遍历 持续获取数据
for message in consumer:
    print(f'数据：{message.value.decode("utf-8")}')
    print(f'主题：{message.topic}')
    print(f'分区：{message.partition}')
    print(f'偏移量：{message.offset}')
    print('---------------------------------------------------------------')
    print(' ')

# 消费者的代码会持续运行
consumer.commit_async()
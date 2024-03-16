from kafka import KafkaProducer

# 创建生产者
producer = KafkaProducer(bootstrap_servers=['hadoop:9092'])

# 使用生产者对象发送消息
# topic 指定对哪个主题发送消息
# value 指定发送的消息内容 内容是bytes类型
send_obj = producer.send(topic='itcast', value='hello itcast'.encode('utf-8'), partition=0)
# 在字符串前面加一个b
# producer.send(topic='itcast', value=b'hello itcast')
# 需要获取返回结果
data = send_obj.get()
print(f'主题信息:{data.topic}')
print(f'分区信息:{data.partition}')
print(f'偏移量信息:{data.offset}')
# 需要获取返回结果

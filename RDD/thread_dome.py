import threading


def func():
    print('计算任务方法')
    print(1111)


t = threading.Thread(target=func)

t.start()

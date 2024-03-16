# 迭代器 可以配合next一次取一个值
data = [1, 2, 3, 4]


# # py自带的迭代器  只能以此取值 不能进行其他计算
# iter_obj = iter(data)
#
# # 然后配合next方法取值
# print(next(iter_obj))


# 自定义迭代器(生成器)  固定的格式
def func(a):
    # a:接受迭代的数据 该数据一般都是可以遍历的
    for i in a:
        sum_data = i + 1
        yield sum_data  # yield关键词 类型return作用 将结果数据依次返回


func_iter = func(data)

# 配合next()的使用
print(next(func_iter))
print(next(func_iter))



# 通过mapPartiton方法可以去读一次数据，将rdd1中的数据全部读取出来，然后用迭代器以此进行计算 可以提升效率

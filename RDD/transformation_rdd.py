# 转化算子的使用
from pyspark import SparkContext

sc = SparkContext()

# python转化为rdd
rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
rdd_t = sc.parallelize(['a', 'b', 'c', 'a'])

# rdd的算子使用
# 1.map算子 主要的场景在于转化数据为k-v类型  方法中要传递lambda表达式 函数必须定义一个接收值  x就是一个接收值 会接受rdd中每个元素的数据
# x+1 就是编写的计算过程 对rdd的每个元素进行计算  map执行完成之后会返回一个新的rdd
rdd1 = rdd.map(lambda x: x + 1)
# 词频统计需要将单词转化为kv类型  [('a',1)]
rdd4 = rdd_t.map(lambda x: (x, 1))

# 2.flatmap 算子 读取rdd5中每个额元素的数据 将元素数据传递到匿名函数中 匿名函数必须要有一个接收值  faltamap会对返回的列表数据进行读取放入一个新的rdd中
# 处理二维嵌套数据  [[1,2],[3,4]]

rdd5 = sc.parallelize([['a', 'b'], ['c', 'a']])
rdd6 = sc.parallelize(['a,b,c,d'])
flatMap_rdd = rdd5.flatMap(lambda x: x)
flatMap_rdd1 = rdd6.flatMap(lambda x: x)  # 将字符串中每个内容都当成元素 结果的数据为  ['a', ',', 'b', ',', 'c', ',', 'd']
flatMap_rdd2 = rdd6.flatMap(lambda x: x.split(','))

# 3.过滤算子  filter算子 可以接受rdd中每个元素数据 然后传递给函数进行过滤 lambda 需要有一个接收值
rdd7 = sc.parallelize([1, 2, 3, 4])
rdd8 = sc.parallelize(['a', 'b', 'c', 'd', 'd'])

filter_rdd = rdd7.filter(lambda x: x % 2 == 0)
filter_rdd1 = rdd8.filter(lambda x: x in ('a', 'b'))

# 4.去重  不需要参数条件 生成的数据会打乱输入和输出的顺序
distinct_rdd = rdd8.distinct()

# 5.对数据进行分区  会读取rdd中每个元素数据 传递给函数使用 labmda需要一个接收值 然后指定分组规则  hash(x) % 2 对x中的元素数据
#     进行hash取余，将数据分成两组  余数相同的数据会放在一起 返回一个新的rdd，新的结果是[(k,v),(k,v)]  k = 0 或者 1 ，因为分成了两个分区

# 6. 对kv形式的数据处理
# 6.1 对kv形式的数据进行取值处理  mapValue  可以获取kv的value值部分传递给函数进行使用
group_rdd1 = rdd8.groupBy(lambda x: hash(x) % 2)
group_rdd = group_rdd1.mapValues(lambda x: list(x))

# 6.2 对kv形式的数据进行取值处理 ,系统key值的数据会放在一起 不需要传递函数
rdd9 = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('a', 1), ('b', 2)])
groupKey_rdd1 = rdd9.groupByKey()
groupKey_rdd = groupKey_rdd1.mapValues(lambda x: list(x))

# 6.3 对kv形式的数据进行分组 在进行聚合计算
# reduceByKey会将相同的key的数据放在一起  然后对每个key中对应的value进行累计加计算
# [('b', [2, 2]), ('c', [3]), ('a', [1, 1])]
# reduceByKey 会将分组后的数据 按照key值传递给函数进行计算
# lambda需要接受两个参数，后面编写累加计算
reduceByKey_rdd1 = rdd9.reduceByKey(lambda x, y: x + y)

# 6.4 对kv形式的数据进行分排序　　不需要指定函数　按照key排序  默认是升序  需要降序的话就是 ascending=false
sortByKey_rdd = rdd9.sortByKey()


# 7.排序算组 sortBy 可以指定按照那个数据进行排序  会将rdd中的元素数据传递给函数使用  lambda需要接受x 接受rdd中每个元素
# 如果元素是kv类型可以通过下标方式指定排序 x[0]  代表key  x[1]  代表value值   默认是升序  需要降序的话就是 ascending=false
sortBy_rdd = rdd9.sortBy(lambda x:x[1])

# 使用action算子触发计算 行动算子
res1 = rdd1.collect()
res2 = rdd4.collect()
res3 = flatMap_rdd.collect()
res4 = flatMap_rdd1.collect()
res5 = flatMap_rdd2.collect()
res7 = filter_rdd.collect()
res8 = filter_rdd1.collect()
res9 = distinct_rdd.collect()
res10 = group_rdd.collect()
res11 = groupKey_rdd.collect()
res12 = reduceByKey_rdd1.collect()
res13 = sortByKey_rdd.collect()
res14 = sortBy_rdd.collect()

print(res1)  # 结果:[2, 3, 4, 5, 6, 7]
print(res2)  # 结果:[('a', 1), ('b', 1), ('c', 1), ('a', 1)]
print(f'flatMap方法 {res3}')  # 结果:flatMap方法 ['a', 'b', 'c', 'a']
print(f'flatMap_rdd方法 {res4}')  # 结果:flatMap_rdd方法 ['a', ',', 'b', ',', 'c', ',', 'd']
print(f'flatMap2方法 {res5}')  # 结果:flatMap2方法 ['a', 'b', 'c', 'd']

print(f'filter_rdd方法 {res7}')  # 结果:filter_rdd方法 [2, 4]
print(f'filter_rdd1方法 {res8}')  # 结果:filter_rdd1方法 ['a', 'b']
print(f'distinct_rdd方法 {res9}')  # 结果:distinct_rdd方法 ['b', 'c', 'a', 'd']
print(f'group_rdd方法 {res10}')  # 结果:group_rdd方法 [(0, ['b', 'c', 'd', 'd']), (1, ['a'])]
print(f'groupKey_rdd方法 {res11}')  # 结果:groupKey_rdd方法 [('b', [2, 2]), ('c', [3]), ('a', [1, 1])]
print(f'reduceByKey_rdd1方法 {res12}')  # 结果:reduceByKey_rdd1方法 [('b', 4), ('c', 3), ('a', 2)]
print(f'sortByKey_rdd方法 {res13}')   #sortBy_rdd方法 [('a', 1), ('a', 1), ('b', 2), ('b', 2), ('c', 3)
print(f'sortBy_rdd方法 {res14}')  #sortBy_rdd方法 [('a', 1), ('a', 1), ('b', 2), ('b', 2), ('c', 3)]
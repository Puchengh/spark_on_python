# 在udf函数中 对字段数据的处理是一行一行的处理，无法对整个字段中的所有数据一次性处理 无法完成聚合的操作
# udaf函数的实现需要借助pandas中的series类型数据 该类型的数据可以接收一整列的数据，完成对字段所有数据的聚合操作
# pandas是python的一个数据分析包(numpy,matlab) 是为数据分析工具提供的
# pandas是单机资源计算的  不合适大数据的计算场景
# pandas 的主要数据结构是Series(一维数据) 与 DataFrame(二维数据)
# 通过索引取值 行索引 列索引
# Series 代表一列数据只有行索引 取出某行数据
# DataFrame 有行有列 同时又行索引 列索引,通过列索引取出的值就转为Series数据
# spark中的dataframe row对象 row[0] 和schema信息(列的名字)

# spark dataframe是由一行一行的row对象构成 分布式计算
# pandas dataframe 是由一列一列的series构成的 单机计算
# Series 是一种类似一维数组的对象 它是由一组数据以及一组与之相关的数据标签(索引)组成

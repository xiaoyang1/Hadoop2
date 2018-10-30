这个文件是用来学习多表连接的。涉及的问题主要有 数据倾斜等。
t_order:
id pid amount
1001 01 1
1002 02 2
1003 03 3

 t_product
pid pname
01 小米
02 华为
03 格力

将商品信息表中数据根据商品 pid 合并到订单数据表中:
式：
id pname amount
1001 小米 1
1004 小米 4
1002 华为 2
1005 华为 5
1003 格力 3
1006 格力 6

需求一： Reduce 端表合并（数据倾斜）
通过将关联条件作为 map 输出的 key，将两表满足 join 条件的数据并携带数据所来源的
文件信息，发往同一个 reduce task，在 reduce 中进行数据的串联。

需求二： map 端表合并（）

1） 分析
适用于关联表中有小表的情形；
可以将小表分发到所有的 map 节点，这样， map 节点就可以在本地对自己所读到的大
表数据进行合并并输出最终结果，可以大大提高合并操作的并发度，加快处理速度。

1） DistributedCacheDriver缓存文件
// 1 加载缓存数据
job.addCacheFile(new
URI("file:///e:/cache/pd.txt"));
//2 map端join的逻辑不需要reduce阶
段， 设置reducetask数量为0
job.setNumReduceTasks(0);
2） 读取缓存的文件数据
setup()方法中
// 1 获取缓存的文件
// 2 循环读取缓存文件一行
// 3 切割
// 4 缓存数据到集合
// 5 关流
map方法中
// 1 获取一行
// 2 截取
// 3 获取订单id
// 4 获取商品名称
// 5 拼接
// 6 写出
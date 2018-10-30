1. 需求
 无论hdfs 还是mapreduce ，对小文件的话效率都有损失， 当面临大量小文件场景时，就需要有相应的解决方案
 将多个小文件合并成一个文件SequenceFile， SequenceFile 里面存储着多个文件，存储的形式为 文件路径+名称为key，
 文件内容为 value。

2. 分析
小文件的优化无非一下几种方式：
（1）在数据采集的时候，就将小文件或小批数据合成大文件再上传hdfs。
（2）在业务处理之前，在HDFS上使用MapReduce程序对小文件进行合并
（3）在MapReduce处理时， 可采用CombineTextInputFormat 提高效率

3. 具体实现
本节采用自定义一个类继承InputFormat的方式，处理小文件。
（1）自定义一个类继承 FileInputFormat
（2）改写 RecordReader，实现一次读取一个完整文件封装为 KV
（3）在输出时使用 SequenceFileOutPutFormat 输出合并文件
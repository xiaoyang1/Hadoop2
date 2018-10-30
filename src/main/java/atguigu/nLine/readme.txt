这个是用来学习 NLineInputFormat 使用案例的。

1） 需求： 根据每个输入文件的行数来规定输出多少个切片。 例如每三行放入一个切片中。
2）输入数据：
banzhang ni hao
xihuan hadoop banzhang dc
banzhang ni hao
xihuan hadoop banzhang dc
banzhang ni hao
xihuan hadoop banzhang dc
banzhang ni hao
xihuan hadoop banzhang dc
banzhang ni hao
xihuan hadoop banzhang dcbanzhang ni hao
xihuan hadoop banzhang dc
3） 输出结果：
Number of splits:4
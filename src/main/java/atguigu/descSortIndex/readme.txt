这个包是用来实现倒排索引的，主要的目的是用来学习多job的工作方式，

需求：  3个文件： a.txt  b.txt  c.txt

a.txt   : atguigu pingping
          atguigu ss
          atguigu ss
b.txt   : atguigu pingping
          atguigu pingping
          pingping ss
c.txt   : atguigu ss
          atguigu pingping

第一次期待输出：
atguigu--a.txt 3
atguigu--b.txt 2
atguigu--c.txt 2
pingping--a.txt 1
pingping--b.txt 3
pingping--c.txt 1
ss--a.txt 2
ss--b.txt 1
ss--c.txt 1

第二次期望的输出结果：

atguigu c.txt-->2 b.txt-->2 a.txt-->3
pingping c.txt-->1 b.txt-->3 a.txt-->1
ss c.txt-->1 b.txt-->1 a.txt-->2
# top100
url top100
100GB url 文件，使用 1GB 内存计算出出现次数 top100 的 url 和出现的次数
1. 注意代码可读性，添加必要的注释（英文）
2. 注意代码风格与规范，添加必要的单元测试和文档
3. 注意异常处理，尝试优化性能

* 运行流程：
1. 首先运行gen.sh产生测试数据
2. 然后运行run.sh产生top100
3. 最后运行test.sh验证结果

* 总体思路：
1. 总的任务数目：1个读线程+n个写线程+1合并线程
2. 读线程每隔500M进行统计计数结果通过输出线程输出到part*，写任务全部结束后进行多路归并，合并url相同的元素
3. 将合并完成的url通过大小为100的小根堆统计top100

 * 文件设置介绍
 1. test.sh为对比直接用shell命令统计得到的top100与程序统计结果是否相同
 2. GenUrl.cpp为产生测试url程序，程序限制最长url为128字符，默认产生数据量大小为2G
 3. main.cpp为主程序设置读线程每次最多读取500M数据量
 4. run.sh为启动主程序脚本 gen.sh为产生数据脚本 

 ## 切分和统计排序示意图如下
 ![划分和排序统计](https://note.youdao.com/yws/public/resource/02c145b807e6eb212157191e353e9803/xmlnote/2E3F8C36ADB44C93AEA4F8CB4D207EFB/2036)

 ## 合并(merge)示意图
 ![merge示意图](https://note.youdao.com/yws/public/resource/7c59a0ce5773981be1489e6b28abade2/xmlnote/2FCCCDA5D5CB40438DB31B794C75038E/2054)

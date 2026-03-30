# TDengine 双活

## 1. 测试环境

TD1 -----taosx1 ------ TD2,     TD2 -----taosx2 ------ TD1 

## 2. 场景一： native 方式

S1 -------> TD1 or TD2
应用脚本S1： 向TD1写入数据，如果失败，则切换向TD2写入，如果失败，则切换向TD1写入。如此循环。
**结果：TD1故障一段时间，再重启TD1后，taosx同步任务不恢复正常。**

## 3. 场景二：restful 方式

S2 -------> taosadapter1------TD1 ， or  S2 -------> taosadapter2------TD2
应用基本S2：向taosadapter1写入数据，如果失败，则切换向taosadapter2写入，如果失败，则切换向taosadapter1写入。如此循环。
**结果：输入错误的参数，启动restful 卡住不返回。**

## 4. 场景三：native方式 + restful 方式

1、同时启动两个taosx 同步任务，分别是native方式和 restful方式；
2、创建一个新的db；
3、重启所有的taosx任务；
4、启动建超级表、子表和插入数据；
**结果：目的端没有同步数据**

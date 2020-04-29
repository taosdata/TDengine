# Telegraf写入TDengine的API程序

刚刚开发完Telegraf写入TDengine的APi程序，本文总结一下程序的设计思路，并在此基础上提出一种schemaless的写入TDengine的适配方法和对应的查询思路。

## Telegraf数据分析
Telegraf采集节点的数据后，按照数据的格式为measurement加上一系列的tags，再加上一系列的fields和timestamp，组成一条记录发出。

```
cpu,cpu=cpu-total,host=liutaodeMacBook-Pro.local usage_irq=0,usage_guest=0,usage_guest_nice=0,usage_iowait=0,usage_softirq=0,usage_steal=0,usage_user=10.55527763881941,usage_system=3.5767883941970986,usage_idle=85.86793396698349,usage_nice=0 1571663200000000000
```
上面是一条按照influxdb格式输出的记录，第一个字段是measurement, 然后接着两个tags, tags后面的空格来作为tags和fields的分隔；fields和timestamp之间也是用空格分隔。

```json
{
	"fields":{
	"usage_guest":0,
	"usage_guest_nice":0,
	"usage_idle":87.73726273726274,
	"usage_iowait":0,
	"usage_irq":0,
	"usage_nice":0,
	"usage_softirq":0,
	"usage_steal":0,
	"usage_system":2.6973026973026974,
	"usage_user":9.565434565434565
	},
	"name":"cpu",
	"tags":{
		"cpu":"cpu-total",
		"host":"liutaodeMacBook-Pro.local"
		},
	"timestamp":1571665100
}
```
如果按json格式输出，则一条json格式的记录如上。
上面的数据个看上去跟TDengine的记录格式十分类似，很自然的我们可以把name作为超级表名，tags作为tags，fields作为values，timestamp作为timestamp，对应上TDengine的数据格式，照理说应该非常好写入TDengine。但实际过程中发现telegraf在输出数据时，经常会遇到一个问题，就是name，tags格式一样的情况下，fields的格式不一样，fields里的名字，数量，都可能变化。这种变化不是随意变化，可能是两三种组合的变化。比如如下情况
```
swap,host=testhost1 out=0i,in=0i 1574663615000000000
swap,host=testhost1 total=4294967296i,used=3473670144i,free=821297152i,used_percent=80.877685546875 1574663615000000000
```
同一个时间点来的两条记录，name都是swap，tag都是host，但fields却完全不相同。

再比如
```
system,host=testhost1 uptime_format="5 days,  1:07" 1574663615000000000
system,host=testhost1 uptime=436070i 1574663615000000000
system,host=testhost1 load15=5.9521484375,n_cpus=4i,n_users=6i,load1=3.17138671875,load5=6.462890625 1574663615000000000
```
同一时间点来的三条记录，name都是system，tags都是host，但fields完全不同。

如果以name作为TDengine的超级表名，就会面临到表格的结构发生变化。并且，由于influxdb是schemaless的设计，他们能够很好的处理这种变化，不管fields如何变化，都能顺利写入。因此，很难保证telegraf后续产生的数据，fields发生会怎么变化。如何设计TDengine的存储表结构，是一个问题。

## TDengine的表结构设计思路
面对这种数据来源，我们一般可以用两种设计思路：
### 提前设计好超级表，包含所有fields, 这种就是schema方式
一种，是在对数据的行为有充分的了解后，提前设计好TDengine的表格式，将所有可能变化的fields都放到values中，提前创建好超级表；然后在插入数据时，把同一时间戳的所有fields都收集齐后，再组装成一条TDengine的SQL记录，写入TDengine。这种，是我们当前通常用到的方法，可以成为schema方式的写入。这种方法的优点是，比较符合我们TDengine的设计假设，values有多列，写入性能相对高一些。但也有明显的缺点，需要提前对数据做大量的分析，确定每个测量的格式，手动来写schema配置文件或手动在TDengine客户端创建超级表，对于节点很多的监控，这种设计会带来较大的工作量。

### 不提前设计超级表，设计一种建表规则，根据来的数据自动写入，类似influxdb的schemaless的方式
另外一种，就是根据收到的数据自动建表，只要符合name，tags，fields，timestamp的格式，都能顺利写的创建TDengine的表，并写入TDengine。本程序就是采用这种思路，将每一个fields单独拆开，和name，tags组合起来，形成一个单列的表。这样的超级表符合任何fields，对于任意fields都可以顺利写入。下面将沿着这个设计思路继续展开。

## 超级表
本程序以收到的原始数据name作为超级表名，原始数据中的tags作为tags，同时，额外增加两个tag，一个是发来请求的源IP，用来区分设备；另一个是field，这个tag的值是原始数据中fields的名称，用来表明这个超级表存的是哪个指标。以上面的system这个原始数据为例，则超级表结构为
```toml
stablename = system
tags = ['host','srcip','field']
values = ['timestamp','value']
```
其中，tags的类型都为binary(50),长度超过50的标签值都截断为50；field这个标签则的可能值则为
```toml
field : ['uptime_format','uptime','load15','n_cpus','n_users','load1'，'load5']
```
## value的类型
由于无法预知数据的类型，以及简化程序实现，我们将value的类型分成两类，一类是数值型，统一用double来存储；一类是字符串，统一用binary(256)的类型来存。由于所有field都要用同一个超级表来存，因此我一开始就为每个name创建了两个超级表，一个是数值型的超级表，表名就是name；另一个是字符串型的超级表，表名是name加上_str后缀。然后根据field的数据类型，如果是数值型，就用数值型的超级表来创建表；如果是字符串型的，就用name_str的超级表来创建表。
因此，超级表创建的时候会创建两倍的数据量
```
                              name                              |     created_time     |columns| tags  |  tables   |
====================================================================================================================
system                                                          | 19-11-22 21:48:10.205|      2|      3|         12|
system_str                                                      | 19-11-22 21:48:10.205|      2|      3|          2|
cpu                                                             | 19-11-22 21:48:10.225|      2|      4|        200|
cpu_str                                                         | 19-11-22 21:48:10.226|      2|      4|          0|
processes                                                       | 19-11-22 21:48:10.230|      2|      3|         16|
processes_str                                                   | 19-11-22 21:48:10.230|      2|      3|          0|
disk                                                            | 19-11-22 21:48:10.233|      2|      7|        357|
disk_str                                                        | 19-11-22 21:48:10.234|      2|      7|          0|
diskio                                                          | 19-11-22 21:48:10.247|      2|      4|         72|
diskio_str                                                      | 19-11-22 21:48:10.248|      2|      4|          0|
swap                                                            | 19-11-22 21:48:10.254|      2|      3|          7|
swap_str                                                        | 19-11-22 21:48:10.255|      2|      3|          0|
mem                                                             | 19-11-22 21:48:10.272|      2|      3|         61|
mem_str                                                         | 19-11-22 21:48:10.272|      2|      3|          0|
Query OK, 14 row(s) in set (0.000588s)

```
因此查询的时候，需要根据查询值的类型，选择不同的超级表来查询
比如，对于数值类型，查询n_cpus值的语句为
```sql
Select * from system where field = "n_cpus";
```

对于字符串类型，查询uptime_format的值的语句为
```sql
Select * from system_str where field = "uptime_format";
```

## 表的创建
对于每个field，程序为它创建了一个表，表名规则如下：
将原始数据的所有tags值加上源ip加上field的名称，组成一个长的字符串，然后进行MD5计算，输出的结果加上MD5_作为前缀，形成表名。
这种规则，确保了只要数据的tags等特征不变，表就不会发生变化。
```
...
md5_b26d30c2e07529ac309d836b3b222f15                            | 19-11-24 21:34:35.830|      2|processes                                                       |
md5_08147d718d4961368155f90432eab536                            | 19-11-22 21:48:10.748|      2|disk                                                            |
md5_105158abfca0bbf0d932cc74bfc7e136                            | 19-11-24 21:34:35.846|      2|mem                                                             |
md5_e6842b5c6b9744b7d5ce3510a4d54c98                            | 19-11-24 21:34:35.874|      2|disk                                                            |
md5_285fd02686e0bfee76d22505dd29f14c                            | 19-11-22 21:48:11.509|      2|disk                                                            |
md5_9317870bb00109353f6aef58ee2ee9e9                            | 19-11-24 21:34:35.919|      2|cpu                                                             |
Query OK, 727 row(s) in set (0.020405s)

```
因此在数据插入时，只要根据tags值和IP和field的名称，就能计算出表名，直接插入该表。
查询时，可以用超级表加上tag值和field值来查询，也很清晰便利。

因此，数值写入自动生成的sql语句如下：
```sql
insert into md5_285fd02686e0bfee76d22505dd29f14c values(1574663615000,375.2023040);
```
其中表名md5_285fd02686e0bfee76d22505dd29f14c是自动根据数据特征计算出来的，无需人工输入。

而查询则可以通过超级表来查询

## Schemaless写入方法
基于上面的实现，后续我们可以确定一个写入的语法，就可以不用提前设定schema，而根据接收到的数据，自动创建超级表，表，方便的写入TDengine了。
沿用现有的json格式，或者参考influxdb的语法，确定一个写入的语法，就可以实现influxdb的schemaless写入的能力。

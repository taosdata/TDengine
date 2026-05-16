# Data Generation according to taosd internal design

## 背景

本文档介绍对 taosBenchmark 的一些优化，这些优化能够根据  taosd 存储引擎的内部设计的特性生成数据，控制这些数据在存储引擎中的位置和分布，从而为测试存储引擎提供更好的帮助。

## 控制数据在存储引擎中位置

     taosBenchmark 可以通过选择不同参数，控制数据生成在不同位置下，主要有：

###    1、影响参数及计算原理

           **影响数据存储位置的几个参数说明：**
1、num_of_records_per_req 每批提交行数
2、childtable_count： 生成子表的个数，这个参数配置要结合下面的 vgroups insertRows 和  minRows 配置
3、vgroups：生成的 VNODE 个数，此参数和子表个数及 BUFFER 会影响落盘
  4、insert_rows: 写入每个子表的行数
  5、BUFFER:    一个 vnode 对应的落盘内存大小，默认96M，一般数据达到 2/3 就会触发落盘
  6、DURATION:  配置一个 .data 数据文件中存放几天的数据，默认是 10 天
  7、minRows:  块的最小行数
** **** 计算原理****：**
     上面提到的 7 个参数都要参与计算是否达到了落盘的阈值，  总的来说计算过程是一个估算的过程。由于在 VNODE 中的存储结构及空值、NULL等各种情况都会影响占用落盘内存大小，所以不能精准计算。
      BUFFER 实际占用大小 = vnode 上分配的表个数 * insert_rows  * 每行占用的空间大小 
      vnode 上分配的表的个数 = childtable_count / vgroups. 因为咱们使用的是一致性 HASH 算法，原则上会平均分配，所以可以使用平均值来大体估算
      每行占用的空间大小 = 写入数据的所有列长度和 * 估算倍数 
      估算倍数： 这是个估计值，目前是按 3 来计算的
      **落盘触发 = BUFFER 实际占用大小 / BUFFER 配置参数  >  2 / 3**

###    2、只在内存中生成数据

          配置文件位置： case\insertSuit1.json 
          使用此配置文件会生成的数据，正好只在内存中有数据，但还没有达到落盘阀值的一个数据场景。
          **配置文件参数说明：**
1、num_of_records_per_req 这里配置为 100， 这个值最好在 300 以下为好
2、childtable_count： 生成子表的个数，这里配置的是 1000，如果此值配置大后，要把 insert_rows 配小
3、vgroups：这里配置了 6 个 vnode, 这个值看你实际的需求情况可以调整
  4、insert_rows: 这里配置是的 50 行，目前这个值可配置的范围比较大 50 ~ 1000 行应该都是可以的
  5、BUFFER : 默认值 96M, 此值越大越不容易触发落盘 
**    落盘触发 = FLASE **

###   3、在内存 + stt 文件有数据

          配置文件位置： case\insertSuit2.json 
          使用此配置文件会生成的数据，只会在内存和 stt 文件中有数据存在。
   **配置文件参数说明：**
1、num_of_records_per_req 这里配置为 100， 这个值最好在 300 以下为好
2、childtable_count： 生成子表的个数，这里配置的是 100W, 为了和前一个有所变化大
3、vgroups：这里配置了 6 个 vnode, 这个值看你实际的需求情况可以调整
  4、insert_rows: 这里配置是的 10 行，
  5、BUFFER : 默认值 96M, 此值越大越不容易触发落盘 
  6、 minRows: 配置 100行 ， 这个参数是这里的关键， insert_rows < minRows ，保证都会落在 stt 中
**落盘触发 = TRUE** 

###    4、在内存 + stt  + 单个.data 文件有数据

          配置文件位置： case\insertSuit3.json 
    使用此配置文件会生成的数据，在内存和 stt 及  .data 文件中都有数据。
    但只有一个 .data 文件，也就是写入的数据控制在一个 duration 内。
   **配置文件参数说明：**
1、num_of_records_per_req 这里配置为 500
2、childtable_count： 生成子表的个数，这里配置的是 100
3、vgroups：这里配置了 6 个 vnode, 这个值看你实际的需求情况可以调整
  4、insert_rows: 这里配置是的 3w 行
  5、BUFFER : 默认值 96M, 此值越大越不容易触发落盘 
  6、DURATION:  10天，即10天内数据会生成在一个数据文件中
  7、timestamp_step ：每行之间时间步长，这里是10，单位是ms 。此项不可配置过大，不超过 10秒为好。
  上面的参数计算后，数据文件会生成在一个.data 文件中
**落盘触发 = TRUE**

###     5、在内存 + stt  + 多个.data 文件有数据

          配置文件位置： case\insertSuit4.json 
          使用此配置文件会生成的数据，在内存和 stt 及  .data 文件中都有数据。
   会有多个 .data 文件，也就是写入的数据超过了一个 duration 
   **配置文件参数说明：**
1、num_of_records_per_req 这里配置为 100
2、childtable_count： 这里配置的是 10
3、vgroups：这里配置了 6 个 vnode
  4、insert_rows: 这里配置是的 10w 行，
  5、BUFFER : 默认值 96M, 此值越大越不容易触发落盘 
  6、timestamp_step : 每行之间时间步长，这个值需要配置的比较大，这里配置的是 60000，保证 10 w 行数据生成的时间跨在多个 data 文件中
**落盘触发 = TRUE**

## 使用说明

1、git 上更新 taos_tolls 仓库代码  https://github.com/taosdata/taos-tools.git
2、切换到 develop分支
  3、按 github 上指引手册编译 taosBenchmark
  4、 配置文件在仓库主目录下的 case 中
  5、 taosBenchmark -f case/insertSuitX.json 即可运行相应的功能进行测试数据生成

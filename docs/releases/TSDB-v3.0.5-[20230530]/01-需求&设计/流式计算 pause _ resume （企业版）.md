# 流式计算 pause / resume （企业版）

### 1. User manual ：

PAUSE STREAM [IF EXISTS] stream_name;
RESUME STREAM [IF EXISTS] [IGNORE UNTREATED] stream_name;

### 2. 功能说明：

1.流计算暂停计算任务
PAUSE STREAM [IF EXISTS] stream_name;
没有指定IF EXISTS，如果该stream不存在，则报错；如果存在，则暂停流计算。指定了IF EXISTS，如果该stream不存在，则返回成功；如果存在，则暂停流计算

2.流计算恢复计算任务
RESUME STREAM [IF EXISTS] [IGNORE UNTREATED] stream_name;
如果指定IGNORE UNTREATED，则恢复流计算时，忽略流计算暂停期间加载的数据，注意：此时流计算的结果与批查询的结果不一致。没有指定IF EXISTS，如果该stream不存在，则报错，如果存在，则恢复流计算；指定了IF EXISTS，如果stream不存在，则返回成功；如果存在，则恢复流计算。

### 3. 示例：-

后续补充

### 4. 方案设计：

#### 4.1 暂停流计算

1.Parser增加新的指令，并发消息给流计算引擎
2.MNode添加新的消息类型 TDMT_MND_STOP_STREAM，以及相应的处理函数，在该函数里发消息给所有的stream task停止计算任务。
3.stream task收到消息后，处理如下：
3.1对于给流计算转发submit data，停止转发，记录已被operator获取（被stream scan上面的operator fetch走的数据）的数据的最大version；对于被operator获取的数据会继续计算直到结束，对于未被operator获取的数据，直接清空。
3.2对于fill history，暂停读取历史数据；对于已经读入到内存中的历史数据会继续计算直到结束。

#### 4.2 恢复流计算

1.Parser增加新的指令，并发消息给流计算引擎
2.MNode添加新的消息类型 TDMT_MND_RESUME_STREAM，以及相应的处理函数，在该函数里发消息给所有的stream task恢复计算任务。
3.stream task收到消息后，处理如下：
3.1对于给流计算转发submit data，如果忽略处理暂停期间收到的数据，则直接恢复转发submit data；如果不忽略，则从历史数据中读取指定version区间的数据
3.2对于fill history，如果忽略处理暂停期间收到的数据，则忽略未读取的历史数据，直接恢复转发submit data；如果不忽略，则继续处理物理数据

### 5. 自测案例：

后续补充

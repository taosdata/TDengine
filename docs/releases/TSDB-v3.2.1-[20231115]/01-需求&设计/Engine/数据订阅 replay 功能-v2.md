# 数据订阅 replay 功能-v2

v1 版本为我们自己理解的回放功能，废弃（[数据订阅 replay 功能-v1 (废弃)](https://taosdata.feishu.cn/wiki/EgBvwp1NqiYkwtkTDtYccWxFn1f) ）。
通过验证 Influxdb replay 功能后，制定如下文档，和Influxdb 功能一致。 

## 1. Influxdb kapacitor 回放功能验证

- 验证例子：
  - https://docs.influxdata.com/kapacitor/v1/introduction/getting-started/
- 相关文档：
  - https://docs.influxdata.com/kapacitor/v1/working/api/
  - https://docs.influxdata.com/kapacitor/v1/working/cli_client/
- kapacitor 相关概念理解
  - task 为一个任务，可以对任务里符合条件的数据报警。 
  - record 相当于 TDengine 的 wal，主要用来录制一段数据。可以录制一段任务里的数据。
  - replay 主要是将这段数据重放出来，跑一次 task，将报警写到 task 结果里，和之前 task 的报警结果做对比。
  - -real-clock 指定（real）表示使用录像的时间回放，录制60s，回放就要60s。不指定（fast）会一下子回放完毕。
  - -rec-time 指定表示 报警里使用的时间为录制录像的时间。不指定 报警里使用的时间为当前时间。
- 总结
在 TDengine 里，Task 任务即用户的sql语句，record 在TD里已经实现，即wal。replay 功能，快速播放目前的订阅功能已实现；真实时间间隔播放还没实现，需通过按照写入wal的时间间隔订阅出来来实现。

## 2. replay 功能说明

订阅增加 replay 功能，按照数据写入的时间间隔回放。
比如，如下时间写入三条数据
```sql
2023/09/22 00:00:00.000
2023/09/22 00:00:05.000
2023/09/22 00:00:08.000
```

则订阅出第一条数据 5s 后返回第二条数据，获取第二条数据 3s 后返回第三条数据。

## 3. 范围限制

1. 仅列订阅支持数据回放
   - 回放需要保证独立时间线。
   - 如果是子表订阅或者普通表订阅，只有一个vnode上有数据，保证是一个时间线。
   - 如果超级表订阅，则需保证该 DB 只有一个vnode，否则报错（因为多个vnode上订阅出的数据不在一个时间线上）。
2. 超级表和库订阅保证不了同一个时间线，所以不支持回放。

## 4. 配置策略

增加 enable.replay 参数，true表示开启订阅回放功能，false表示不开启订阅回放功能，默认不开启。
回放不支持进度保存，所以回放参数 enable.replay = true 时，auto commit 自动关闭。

## 5. 回放时间精度

因为数据回放本身需要处理时间，所以回放的精度存在几十ms的误差。

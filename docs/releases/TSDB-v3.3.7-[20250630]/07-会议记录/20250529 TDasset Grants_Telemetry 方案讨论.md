# 20250529 TDasset Grants/Telemetry 方案讨论

## 1. 会议信息

1. 会议人员：霍琳贺、关胜亮、李亚强
2. 会议时间：2024-05-29 15:30 - 16:00

## 2. 会议结论

### 2.1 Telemetry

现有的 taosX 等组件未上报 Telemetry 信息。TDasset、TDgpt、taosX、taos-explorer 等组件（以下简称 component）有三种可能的上报路径，最终选择方案三：
1. component -> taosd -> Telemetry Server：taosd 需开发 http 接口或者 SQL 接口，路径长，修改多
2. component -> taoskeeper -> Telemetry Server：并不是所有社区用户都部署 taoskeeper 
3. component -> Telemetry Server：各组件自行开发，简单好调整
Telemetry 接口如有调整，琳贺会尽快排期

### 2.2 Grants

1. 按照 [TDgpt & TDasset 授权项控制 RS](https://taosdata.feishu.cn/wiki/Aoh8w2ygFihs7pkHE2TcWiqon0b) 描述的需求开展
2. 实现方案参照 taosX 现有办法，不需要单独编写
3. 具体授权项引擎组会编写 FS 文档，预计下周三（6 月 4 日）可开始调试

### 2.3 Monitor

TDasset 的监控信息走如下路径，不需新增接口，taoskeeper 如有调整，琳贺会尽快排期
TDasset -> taosKeeper ->taosd

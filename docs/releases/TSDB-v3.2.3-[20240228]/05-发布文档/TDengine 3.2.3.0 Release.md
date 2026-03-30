# TDengine 3.2.3.0 Release

## 1. Release Date:  2024/2/28

### 1.1 User Manuals: [3.2.3.0 User Manuals](https://taosdata.feishu.cn/wiki/VlY7wrWfOiLe4Uk26eLc2sCMnBc) 

### 1.2 New Features

1. 流计算
   - ~~snode 支持远程备份~~
   - 支持 Count Window
2. taosd 监测
   - 重构 monitor 数据即 log 库，含 taosKeeper 和 TDinsight 适配
   - ~~健康状态监测~~
   - 慢查询监测
3. ~~权限管理：普通用户默认没有建库权限~~
4. 授权机制：流计算、多级存储、数据订阅功能默认关闭，但可以通过授权码单独激活

## 2. Improvements

1. ~~性能优化：副本变更（vgroup 并行）~~ (optional：取决于 snapshot 传输优化的测试结果）
2. last()/last_row() 性能优化
   - ~~重启后加载 last 缓存（现有行为是不加载）（TS-4176）~~ (需要看 b, d 效果再定)
   - 建立超级表和子表时自动建立 last 缓存 （现有行为是在第一次查询时） （TS-4177，TS-4178）
   - last()/last_row() 同时使用时也能够利用缓存 (TD-27003)
   - 有一列全为 NULL 时，last_row() 性能差 (TD-24422）
   - ~~数据都在 STT 文件中时，查询 last 性能比在 data 中慢 ~~~~ ~~~~(TD-25402)~~ （需要看 b, d 效果再定）
   - ~~数据都在 data 文件时，插入数据后再加缓存，查询 last 比以前没有缓存时要慢 (TD-25401）~~（需要看 b, d 效果再定）
3. 查询优化：
   - ~~nchar 类型匹配的性能优化~~
   - ~~tag 列和多个选择函数可以组合使用~~
   - ~~last(*)/last_row(*) 可以同时返回标签列~~
   - ~~ROUND() 支持四舍五入到指定的小数位~~
   - select count(*) 能够过滤出空表
4. ~~建表：从 CSV 文件批量建表~~
5. ~~流计算~~
   - ~~支持目标数据库的时间精度与源数据库的时间精度不一致~~
   - ~~支持增量 checkpoint （性能优化）~~

## 3. 运维优化

1. 减少写入阻塞 (Optional)
   - redistribute vgroup 的副本变更方式优化（采用 learner）
   - split vgroup 优化（Learner)
2. restore vnode 支持指定 vgroup ID (Optional)
3. 删除 dnode
   - ~~drop dnode force 在有 vnode offline 的情况下能够强行 drop ~~ (2023/12/20 更新：涉及到的事务机制复杂，改动很大，收益很小，只在极端情况下有用，此优化取消)
   - 没有 vnode 的 dnode 可以直接 drop ，无论是否在线
4. 增加 dnode
   - ~~增加对 dnode end point 的合法性检查~~
   - ~~增加对新增 dnode 的连通性检查 ~~（待定：目前增加 dnode 时 dnode 可以不在线。2023/12/20 更新：结合上面 3.b 可以强制drop 没有 vnode 的 dnode，无论其是否在线，此优化取消。）

## 4. 内部 （进行中但不具备发布条件 ）

1. 复合主键
2. Join
3. TSMA
4. 双副本

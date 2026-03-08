你现在接手 TDengine 仓库中的一个持续开发任务，请严格沿用已有设计和第三阶段留痕，不要重新发散设计。

仓库根目录
- /Projects/work/TDengine

必须先读这些文件
1. /Projects/work/TDengine/docs/data_repair/03-TSDB_repair/task_plan.md
2. /Projects/work/TDengine/docs/data_repair/03-TSDB_repair/findings.md
3. /Projects/work/TDengine/docs/data_repair/03-TSDB_repair/progress.md
4. /Projects/work/TDengine/docs/plans/2026-03-07-tsdb-force-repair-design.md
5. /Projects/work/TDengine/docs/plans/2026-03-07-tsdb-force-repair-plan.md

当前任务
- 第三阶段：TSDB 强制修复
- 范围严格限定为：
  - taosd -r --node-type vnode --file-type tsdb --mode force
- 仅做：
  - 单副本 vnode 的 TSDB 强制修复
- 不包含：
  - wal
  - meta/tdb
  - replica/copy
  - 其他 node type

必须遵守的边界
1. dmMain.c 只能做 repair 参数校验和执行放行
2. 不能在 dmMain.c 中直接调用 TSDB repair 逻辑
3. TSDB repair 必须挂在 tsdb open fs 链路
4. current.json 的 repair 回写必须走事务式 manifest 切换机制
   - 不能直接覆盖正式 current.json
   - 当前设计/实现应走 staged manifest + commit 路径
5. 当前 repair 应归属“manifest 事务切换机制”，而不是依赖 scan-and-fix 兜底

当前代码与留痕的关键状态
- 文件组级 repair 已完成并已有测试覆盖：
  - missing_stt -> drop_stt
  - missing_core -> drop_core_group
  - size_mismatch_core -> drop_core_group
  - 受影响文件组定向备份：manifest.json / repair.log / original/
  - current.json staged manifest crash-safe 提交已完成
- 块级 repair 第一批已挂在：
  - /Projects/work/TDengine/source/dnode/vnode/src/tsdb/tsdbFS2.c
- 为解决 libvnode.a 链接测试目标时缺少 dmRepair* accessor 的问题，已新增：
  - /Projects/work/TDengine/source/dnode/vnode/src/vnd/vnodeRepair.c
  - 并已挂到：/Projects/work/TDengine/source/dnode/vnode/CMakeLists.txt
- 当前块级用例位于：
  - /Projects/work/TDengine/test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
  - 用例名：test_tsdb_force_repair_rebuilds_core_from_valid_blocks

本会话已确认并完成的内容
1. 已先排除环境态阻塞，而没有先改 TSDB repair 主逻辑。
2. 已修复系统时区解析问题：
   - 文件：/Projects/work/TDengine/source/os/src/osTimezone.c
   - 修复点：从 /etc/localtime -> .../zoneinfo/UTC 解析系统时区时，原先会留下前导 /，导致 tzalloc(/UTC) failed。
   - 现在会在 truncateTimezoneString() 里去掉前导 /。
3. 已新增回归测试：
   - 文件：/Projects/work/TDengine/source/os/test/osTimeTests.cpp
   - 用例：truncateTimezoneStringRemovesLeadingSlash
4. 已修复 repair dispatch 输出缓冲问题：
   - 文件：/Projects/work/TDengine/source/dnode/vnode/src/tsdb/tsdbFS2.c
   - 在两处 printf("tsdb force repair dispatch...") 后增加 fflush(stdout)
5. 以上两类修复已经写入第三阶段留痕：
   - /Projects/work/TDengine/docs/data_repair/03-TSDB_repair/findings.md
   - /Projects/work/TDengine/docs/data_repair/03-TSDB_repair/progress.md

本会话实际验证结果（这些结论是重要上下文）
1. 最小入口验证通过：
   - cd /Projects/work/TDengine/test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k dispatches_in_open_fs
   - 结果：1 passed, 11 deselected
2. phase1 参数回归通过：
   - cd /Projects/work/TDengine/test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1
   - 结果：1 passed, 1 deselected
3. 在“未引入后续实验性测试改动”时，TSDB repair 全量回归曾收敛到：
   - 3 failed, 8 passed, 1 skipped
   - 剩余失败集中在：
     - test_tsdb_force_repair_removes_missing_head_data_sma_from_current
     - test_tsdb_force_repair_removes_missing_data_head_data_sma_from_current
     - test_tsdb_force_repair_removes_size_mismatch_head_data_sma_from_current
4. 这 3 条失败已确认不是纯输出采集问题：
   - repair.log 已落盘，且内容显示：
     - reason=missing_core 或 size_mismatch_core
     - action=drop_core_group
   - 但 current.json 中对应 fid 仍保留 head/data/sma
5. 这说明问题已收敛到：
   - 判定层/备份日志层已经命中 drop_core_group
   - 但 staged manifest / committed current.json 并没有体现该修改

本会话后半段做过的进一步实验（请务必知晓）
- 为了继续收敛，测试文件里做过一些“探索性”改动，当前工作区里这些改动还在：
  - /Projects/work/TDengine/test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
- 这些探索性改动包括：
  1. 给 _start_repair_process() 增加 extra_env=None 参数，修复它之前引用未定义 extra_env 的 bug。
  2. 新增 _wait_for_core_group_removed() helper，用于轮询 current.json 中某 fid 的 head/data/sma 是否被摘除。
  3. 把 3 条 core-group 用例改成：
     - repair 前先 tdDnodes.forcestop(1)
     - 再用 _start_repair_process() 启动 repair 子进程
     - 轮询 current.json
     - 最后 _stop_repair_process(proc)
- 注意：这批测试改动目前只是实验性收敛手段，还没有被最终验证为正确方向，不要默认它们就是最终方案。
- 如果你判断它们偏离问题本质，可以在说明原因后调整；但不要无痕覆盖，请把结论补记到第三阶段留痕。

当前最关键的新发现
1. 直接在沙箱内手工运行 taosd -r 会因为 bind 被 sandbox 拦截，出现 EPERM/Operation not permitted 噪音；这类判断不要当成业务结论。
2. 用“沙箱外”隔离启动 taosd -r 时，能观察到一个重要现象：
   - 某些 targeted repair 启动会被其它 broken vnode 干扰；
   - 在同一个 sim 里一旦累积多个损坏 vnode，非目标 vnode 可能先在 tsdbOpen() 里命中 Retry needed，从而污染观察结果。
3. 因此，后续继续调试时，强烈建议：
   - 尽量单用例、干净 sim 环境验证；
   - 不要在“已有多个失败注入残留”的 sim 上直接下产品结论。

你接手后建议的优先顺序
1. 先重新阅读上面 5 份文档和当前工作区代码。
2. 检查当前工作区是否包含以下关键改动：
   - /Projects/work/TDengine/source/os/src/osTimezone.c
   - /Projects/work/TDengine/source/os/test/osTimeTests.cpp
   - /Projects/work/TDengine/source/dnode/vnode/src/tsdb/tsdbFS2.c
   - /Projects/work/TDengine/test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
3. 先判断当前 test_tsdb_force_repair.py 中实验性改动是否保留：
   - 如果保留，先在干净 sim、沙箱外做单用例验证；
   - 如果不保留，说明原因，并补记到第三阶段留痕。
4. 真正的业务收敛点，仍优先盯：
   - /Projects/work/TDengine/source/dnode/vnode/src/tsdb/tsdbFS2.c
   - 特别是：
     - tsdbDispatchForceRepair()
     - tsdbRepairDropCoreOnTmpFSet()
     - tsdbRepairCommitStagedCurrent()
     - save_fs()
     - commit_edit()
5. 如果发现“drop_core_group 已写 repair.log，但 current.json 仍未去掉 head/data/sma”的真实根因，请做最小修复，不要推翻既有设计。
6. 每完成一批验证或修正，继续更新：
   - /Projects/work/TDengine/docs/data_repair/03-TSDB_repair/findings.md
   - /Projects/work/TDengine/docs/data_repair/03-TSDB_repair/progress.md

建议优先执行的验证命令
- 最小入口：
  - cd /Projects/work/TDengine/test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k dispatches_in_open_fs
- 3 条剩余 core-group 用例（建议单用例、干净 sim 环境逐条验证）：
  - cd /Projects/work/TDengine/test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k removes_missing_head_data_sma_from_current
  - cd /Projects/work/TDengine/test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k removes_missing_data_head_data_sma_from_current
  - cd /Projects/work/TDengine/test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k removes_size_mismatch_head_data_sma_from_current
- phase1 参数回归：
  - cd /Projects/work/TDengine/test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1

关于外部依赖/代理
- 如果构建或测试过程中 git/依赖访问失败，可以使用用户给的代理：
  - http://192.168.31.125:6789
- 本会话里已经用该代理完成过 cmake 构建；新会话如果需要，直接说明要用这个代理。

最后要求
- 不要重新设计大方向
- 以当前代码和留痕为准继续推进
- 在给出“完成/通过/修复”判断前，必须基于你自己重新跑出来的实际结果

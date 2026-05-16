# Viega PoC — OPC UA 断链缓存自动化测试

## 快速开始

```bash
# 运行全部测试（T1～T4）
sudo ./run-test.sh

# 运行单个用例
sudo ./run-test.sh --case=T2

# 自定义点位数
sudo ./run-test.sh --case=T3 --points=500
```

> 需要 `sudo` 权限，因为 pfctl 网络模拟需要 root。

## 前置条件

1. **192.168.2.139** 已部署 taosx + TDengine，Explorer 可访问（:6060）
2. **taosx-agent** 已编译：`/Users/yangzy/Projects/taosx/target/release/taosx-agent`
3. **agent.toml** 已配置（`/etc/taos/agent.toml`），包含正确的 endpoint 和 token
4. **OPC UA 模拟器** 已安装依赖：`cd /Users/yangzy/workspace/opcae-demo && npm install`

## 测试用例

| 用例 | persist_data_enable | 断链时长 | 目的 |
|------|---------------------|---------|------|
| T1 | false | 5 min | 对照组，确认不缓存则丢数据 |
| T2 | true  | 30 sec | 短断链，看 IPC retry 能否自愈 |
| T3 | true  | 5 min  | 核心测试 |
| T4 | true  | 30 min | 长断链恢复验证 |

## 测试流程（每个用例）

1. 启动 OPC UA 模拟器（本地）
2. 启动 taosx-agent（本地）
3. 通过 Explorer API 自动创建 OPC UA 任务
4. Warmup 2 分钟，确认数据正常写入
5. pfctl 断开网络 → 等待 N 秒
6. 恢复网络 → Cooldown 2 分钟
7. 验证数据完整性（CSV vs TDengine）
8. 清理任务，停止进程

## 输出结果

每次运行产生独立的结果目录：

```
results/20260512-180000/
├── summary.md              # 汇总对照表
├── T1/
│   ├── params.json         # 测试参数
│   ├── opcua-sim.log       # 模拟器日志
│   ├── opcua-logs/*.csv    # 模拟器数据 CSV（基准）
│   ├── agent.log           # agent 完整日志
│   ├── agent-events.txt    # agent 关键事件摘要
│   ├── verify-result.txt   # 数据完整性报告
│   ├── count_before.txt    # 断链前数据量
│   ├── count_after.txt     # 恢复后数据量
│   └── disconnect_*.txt    # 断链起止时间
├── T2/
│   └── ...
└── ...
```

## 注意事项

- 每个用例使用独立数据库（`viega_t1`、`viega_t2`...），互不干扰
- 脚本退出时自动恢复网络、停止所有进程（trap EXIT）
- 如果中途 Ctrl+C 退出，也会自动清理
- T4 用例（30 分钟断链）总耗时约 34 分钟，全部跑完约需 1.5 小时

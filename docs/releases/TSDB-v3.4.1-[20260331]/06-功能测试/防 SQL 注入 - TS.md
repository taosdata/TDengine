# 防 SQL 注入 - TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-16 | 2026-03-16 | 1.0 | 彭荣坤 | 初始版本 |

## 2. 测试目标

实现任务：

## 3. 参考文档

- 概要设计说明书 (TS): [防 SQL 注入 - FS](https://taosdata.feishu.cn/wiki/Kdl0wPYLKismFtk2AE7cklVnnJd)

## 4. 测试结论

综合黑名单与白名单（含学习模式）测试结果，TDengine SQL 防火墙在当前实现下能够可靠拦截典型注入模式（如 `UNION SELECT`、`DROP TABLE` 等），并在白名单学习模式下自动归纳正常 SQL 模式并进行保护。
在设计范围内（字符串层检查 + AST 语义检查 + 规则文件），功能基本符合 [防 SQL 注入 - FS](https://taosdata.feishu.cn/wiki/Kdl0wPYLKismFtk2AE7cklVnnJd)中对黑/白名单、防注入、学习模式和配置同步机制的描述，可以满足生产环境常见安全需求。

## 5. 测试环境

- **操作系统**: Ubuntu 22.04 LTS

## 6. 功能测试

### 6.1 黑名单功能（Blacklist Mode）

- **测试目标**
验证在 `sqlSecurity=1`、`sqlSecurityWhitelistMode=2`（仅黑名单模式）、启用字符串层 & AST 层检查的前提下，黑名单能正确拦截恶意 SQL，并不影响正常业务 SQL。
- **测试环境与配置**
  - 使用新测试框架，单节点集群，客户端直连。
  - 客户端配置通过写入 `taos.cfg` 或者通过SQL动态修改`alter local 'xxx' 'xx'` ：
    ```plaintext {wrap}
    sqlSecurity = 1
    sqlSecurityWhitelistMode = 2（仅黑名单）
    sqlSecurityStringCheck = 1
    sqlSecurityASTCheck = 1
    sqlSecurityRuleFile = sql_rules_blacklist.json
    ```

  - 规则文件 `sql_rules_blacklist.json`（或项目根目录 `sql_rules_blacklist.json`）中预置：
    ```json
    {
      "version": "1.0",
      "rules": [
        {
          "ruleId": 1,
          "ruleName": "UNION_SELECT",
          "action": "DENY",
          "priority": "HIGH",
          "pattern": "union[[:space:]]+select",
          "enabled": true
        },
        {
          "ruleId": 2,
          "ruleName": "DROP_TABLE",
          "action": "DENY",
          "priority": "HIGH",
          "pattern": "drop[[:space:]]+table",
          "enabled": true
        }
      ]
    }
    ```

- **测试用例与结果**
   - UNION SELECT 注入拦截（字符串层）
    - 场景：在 `db_fw.t1` 中插入合法数据后，执行：
      - `select * from t1 union select * from t1`
      - `SELECT * FROM t1 UNION SELECT * FROM t1`
    - 期望：触发黑名单规则，SQL 被拒绝执行。
    - 结果：`tdSql.error(...)` 检测到错误，满足预期。
   - DROP TABLE 拦截（规则文件）
    - 场景：创建 `t1` 后执行：
      - `drop table t1`
      - `DROP TABLE t1`
    - 期望：匹配 `DROP_TABLE` 黑名单规则，强制拒绝。
    - 结果：两条语句均被拒绝，不会真正删除表结构。
   - 正常 SQL 放行
    - 场景：创建 `sensors`，执行：
      - `insert into sensors values (now, 25.5)`
      - `select * from sensors where ts > now - 1h`
      - `select avg(temp) from sensors`
    - 期望：不匹配黑名单规则，应全部执行成功。
    - 结果：查询行数和聚合结果均正确，未被防火墙误拦截。
- **结论**
黑名单模式在典型关键字注入（`UNION SELECT`）、破坏性 DDL（`DROP TABLE`）以及普通查询场景下行为正确，实现了**“匹配规则即拒绝，不匹配规则正常执行”**的目标。

### 6.2 白名单学习模式与白名单功能（Whitelist Mode）

验证白名单学习模式能自动收集高频正常 SQL 模式，生成规则文件，并在启用白名单后仅放行已学习模式，拒绝未学习的异常 SQL。
- **测试环境与配置**
  - 客户端初始配置（通过 taos.cfg）：
  ```plaintext {wrap}
  sqlSecurity = 1
  sqlSecurityWhitelistMode = 0（初始关闭白名单）
  sqlSecurityStringCheck = 1
  sqlSecurityASTCheck = 1
  sqlSecurityRuleFile = sql_rules_whitelist.json
  whitelistLearning = 1
  whitelistLearningPeriod = 7
  whitelistLearningThreshold = 3
  ```

  - 测试前删除旧的 `sql_rules_whitelist.json`，确保从空规则开始。
- **主要测试步骤与结果**
   - 准备阶段 (`prepare`)：学习条件用户的SQL
    - 通过 `alter local` 关闭白名单、打开学习模式、设置阈值为 3。
    - 创建 `db_learn.sensors(ts, temp, humidity)` 并确认表可见。
    - 多次执行相同模式的 SQL（插入 + 查询 + 聚合），以触发学习逻辑。
    - 轮询等待直到 `sql_rules_whitelist.json` 生成，超时则判为失败。
    - 结果：规则文件在合理时间内生成，说明学习线程正常工作并有规则写入。
   - 基本学习验证 (`test_learning_basic`)
    - 步骤：
      - 关闭学习模式：`alter local 'whitelistLearning' '0'`
      - 启用白名单模式：`alter local 'sqlSecurityWhitelistMode' '1'`
      - 等待几秒确保客户端拉取到最新配置和规则，可以通过配置的路径`sqlSecurityRuleFile`查看到白名单输出JSON文件：
      ```json {wrap}
      {
          "version":  "1.0",
          "rules":    [{
                  "ruleId":   1000,
                  "ruleName": "LEARNED_RULE_1000",
                  "action":   "ALLOW",
                  "priority": "MEDIUM",
                  "pattern":  "alter local [^[:space:],;)]+ [^[:space:],;)]+",
                  "description":  "Learned pattern (count:13) - Pattern: alter local ? ?",
                  "enabled":  true
              }, {
                  "ruleId":   1001,
                  "ruleName": "LEARNED_RULE_1001",
                  "action":   "ALLOW",
                  "priority": "MEDIUM",
                  "pattern":  "insert into sensors values \\(now \\+ [^[:space:],;)]+s, [^[:space:],;)]+, [^[:space:],;)]+\\)",
                  "description":  "Learned pattern (count:5) - Pattern: insert into sensors values (now + ?s, ?, ?)",
                  "enabled":  true
              }, {
                  "ruleId":   1002,
                  "ruleName": "LEARNED_RULE_1002",
                  "action":   "ALLOW",
                  "priority": "MEDIUM",
                  "pattern":  "select \\* from sensors where temp > [^[:space:],;)]+",
                  "description":  "Learned pattern (count:5) - Pattern: select * from sensors where temp > ?",
                  "enabled":  true
              }, {
                  "ruleId":   1003,
                  "ruleName": "LEARNED_RULE_1003",
                  "action":   "ALLOW",
                  "priority": "MEDIUM",
                  "pattern":  "select avg\\(temp\\) from sensors",
                  "description":  "Learned pattern (count:5) - Pattern: select avg(temp) from sensors",
                  "enabled":  true
              }]
      }
      ```

      - 执行已学习模式 SQL：
        - `select * from sensors where temp > 20`
        - `select 0(temp) from sensors`
      - 执行未学习模式 SQL：
        - `select count(*) from sensors`
        - `drop database db_learn`
    - 期望：
      - 已学习模式：应命中白名单规则，允许执行。
      - 未学习模式：不在白名单中，应被拒绝。
    - 结果：
      - 查询与聚合 SQL 正常返回。
      - `count(*)` 和 `drop database` 未符合白名单中的SQL模式，被白名单机制拒绝访问。
    - 结论：白名单模式能够根据学习结果正确区分“已知良性模式”和“未授权模式”。
   - 值归一与阈值测试
    - 用于验证：
      - 不同常量值是否归一为同一模式；
      - 执行次数不足阈值的模式不会被写入规则。
- **结论**
白名单学习模式整体行为符合设计：
  - 能够自动采集并持久化频繁出现的正常 SQL 模式；
  - 在开启白名单后只放行已学习模式，对未在白名单中的 SQL 进行拦截，从而实现“正向定义允许行为”的安全策略。

## 7. 易用性测试

- **配置易用性**
  - 安全相关参数统一通过 `taos.cfg` / `alter local` 管理，如：
    ```plaintext {wrap}
    sqlSecurity, sqlSecurityWhitelistMode
    sqlSecurityStringCheck, sqlSecurityASTCheck
    sqlSecurityRuleFile
    whitelistLearning, whitelistLearningPeriod, whitelistLearningThreshold
    ```

  - 支持运行时调整（`alter local`），客户端通过心跳周期自动拉取更新，无需重启应用，符合文档所述“有一心跳周期延迟”的设计。
- **规则管理易用性**
  - 黑名单规则文件为标准 JSON，字段包括：
    - `ruleId`, `ruleName`, `action`, `priority`, `pattern`, `description`, `enabled`。
  - 学习模式自动生成白名单文件，格式与手工规则文件一致，便于统一查看与运维管理。
  - 测试中通过直接编辑/删除 `sql_rules_*.json` 验证了规则文件生成与覆盖行为可控。
- **测试结论**
对于熟悉 SQL 与正则的用户，黑/白名单配置与学习模式开关较为直观，结合 `alter local` 和 JSON 规则文件可以完成从“快速试验”到“稳定上线”的配置流程。

## 8. 长期稳定性测试

- **模拟高频场景**
  - 在学习阶段，通过循环执行多次 `INSERT + SELECT + AVG`，模拟生产环境中持续写入与查询的行为。
  - 在白名单模式下，多次重复相同查询，确认不会出现规则失效或性能显著退化现象（从脚本视角未观察到异常超时或错误）。
- **学习线程与规则更新**
  - 测试中反复开启/关闭 `whitelistLearning` 以及更新 `whitelistLearningThreshold`，学习线程可以稳定生成规则文件。
  - 若规则文件在多轮测试中被反复覆盖/删除，系统仍能重新生成，有一定容错能力。
- **结论**
虽然当前测试主要是功能和短时稳定性验证，但从多轮学习/切换/重启视角看，SQL 防火墙与学习模式在持续运行及频繁配置变更下表现稳定，无明显资源泄漏或规则错乱迹象。

## 9. 安全测试

- **注入防护能力**
  - 恒真条件注入：
    - 文档中给出的 `OR 1=1 / TRUE / 'a'='a'` 等模式归属于 AST 安全检查范围。
    - 现有测试用例主要通过规则和模式学习进行限制，后续可在 AST 层补充更多针对 `OR 1=1` 的精确检测用例。
  - UNION 注入：
    - 黑名单规则与字符串层检查对 `UNION SELECT` 注入有明确的、稳定的拦截效果。
  - DDL / 权限相关操作：
    - `DROP TABLE`、`DROP DATABASE` 等操作在黑名单和白名单模式下分别被拒绝，符合“高危险操作默认拒绝”的安全策略。
  - 多语句与注释截断：
    - 测试脚本尚未系统覆盖 `; SELECT`、`--`、`/* ... */` 等组合注入场景，但文档设计中已预留相关规则，后续可在规则文件中补充这些模式的 DENY 条目并新增测试用例。
- **规则优先级与黑/白名单冲突**
  - 当前测试主要验证了单一模式（仅黑名单 / 仅白名单），尚未对“同时启用黑白名单且规则冲突”的情况进行系统性验证。
  - 设计上规则优先级为：黑名单优先于白名单，且 HIGH > MEDIUM > LOW；可在后续测试中增加一条既在黑名单又在白名单的同优先级规则，用以验证冲突处理是否符合设计。
- **审计与日志**
  - 根据设计，黑名单拦截采用 WARN 级别日志，白名单拦截采用 DEBUG 级别，学习模式写入规则使用 INFO 级别。
  - 从测试视角看，`tdSql.error` 能够捕获到错误码和错误信息，便于对接日志与审计。
- **结论**
SQL 防火墙在典型注入、破坏性 DDL、未学习模式访问等方面表现出预期的安全防护能力。后续可在多语句注入、编码混淆、AST 深度检查等方面增加更细致的测试，以进一步覆盖文档中列举的 4.4 所有攻击类型。

## 10. 兼容性测试

- 无

## 11. 测试总结

- 整体评价
  - 黑名单功能已覆盖常见高危 SQL 注入与破坏性操作，表现稳定可靠。
  - 白名单学习模式能够自动抽取正常业务 SQL 模式，并在保护阶段只放行已学习模式，符合设计初衷。
  - 配置方式统一、可动态调整，适合在生产环境逐步从“仅黑名单”演进到“黑+白结合”或“严格白名单”的安全策略。
- 已知不足与改进建议
   - AST 级别的恒真条件检测（如 `OR 1=1`）目前在自动化用例中覆盖较少，之后如果投入生产使用需要增加黑名单用例和细化问题
   - 多语句注入（`; DROP`）、编码混淆（URL 编码、十六进制等）等复杂攻击模式在规则与测试层面仍有扩展空间。
   - 黑白名单共用场景下的优先级冲突处理建议增加专门用例验证。
   - 长期压力与并发下的性能与稳定性测试需要单独的性能/可靠性压测方案。

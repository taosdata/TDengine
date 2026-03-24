# 新增 Pulsar 数据源-TS

## 1. 测试目标

测试 Pulsar/Pulsar-Tuya 数据源是否能够正常采集数据并进入 tsdb。

## 2. 参考文档

JIRA:
TS-7448

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-12-04 | 0.1 | 张贵川 | 文档撰写 |

## 4. 测试结论

已完成测试：
1. 页面配置 Pulsar 数据源导入测试
2. 页面配置 Pulsar-Tuya 数据源导入测试
3. 命令行 Pulsa 数据导入测试
4. 命令行 Pulsa-Tuya 数据导入测试
5. Agent 模式下 Pulsar 数据导入测试
6. Pulsar 数据源同步的指标验证

## 5. 测试环境

- OS: ubuntu 22.04.01 LTS
- Browser: Chrome

## 6. 功能测试

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 通过 | 页面配置 Pulsar 数据源导入测试 | 页面测试： ![](./images/img_KqKEb89rpopTFOxwBZcc8cB4neg.png) ![](./images/img_Ko7TbYRukoq7QxxvsGFcPRB9nkj.png) ![](./images/img_M0LTb3r9PoAruSxXhjgcyzqsnud.png) | 1. 数据导入正常 |
| 2 | 通过 | 页面配置 Pulsar-Tuya 数据源导入测试 | 页面配置并导入数据： ![](./images/img_RfW4bctMyobfhexNsXwcObEHnYc.png) | 1. 数据正常导入 |
| 3 | 通过 | 命令行 Pulsa 数据导入测试 | 测试命令导入 Pulsar 数据： ```http {wrap} taosx run -f "pulsar://192.168.2.131:6650?batch_size=1000&busy_threshold=100%&char_encoding=UTF_8&consumer_name=c1&initial_position=Earliest&subscription=zgc&timeout=0ms&topics=persistent://public/default/pt-zgc" -t "taos+http://root:taosdata@192.168.2.131:6041/zgc" -p "@./docs/taosx/pulsar-parser.json" ``` | 1. 数据正常导入 |
| 4 | 通过 | 命令行 Pulsa-Tuya 数据导入测试 | 测试命令行模式下导入涂鸦数据： ```http {wrap} taosx run -f "pulsarTuya://mqe.tuyaus.com:7285?batch_size=1000&busy_threshold=100%&char_encoding=UTF_8&health_check_window_in_second=0s&initial_position=Earliest&max_errors_in_window=10&max_queue_length=1000&read_concurrency=0&timeout=0ms&tuya_access_id=49rmt4r5ukgu3rayuxcr&tuya_access_key=fbe6805862cc4527a90e782967c79b31&tuya_env=test" -t "taos+http://root:taosdata@tuya-test:6041/tuyadb" -p "@/root/tuya-parser.json" ``` | 1. 数据正常导入 |
| 5 | 通过 | Agent 模式下 Pulsar 数据导入测试 | 配置 agent 且数据正常导入： ![](./images/img_QHVnbm6VeolMKuxRSMHc5E4vnsd.png) 导入的 2007 条数据： ![](./images/img_Hr8jbnLI9okVY4x7nwOcVCzVnOv.png) | 1. 数据正常导入 |
| 6 | 通过 | Pulsar 数据源同步的指标验证 | 1. 打入5条数据验证，同步查看数据是否进入，指标是否正确，tsdb 数据是否正确； 页面指标： ![](./images/img_OipsbAEjcoQc5mxwtPMckoRKndd.png) tsdb 数据: ![](./images/img_PzRrbkAzwom2rXx6eF3crqNtnVd.png) 1. 再打入 1001 条数据，查看指标是否正确 页面指标： ![](./images/img_GyVebW5WooGETlxQLAlcgh8InZf.png) tsdb 数据： ![](./images/img_DiwDba6hJo1H9fxmVPIcYy7nnxe.png) | 1. 导入的5条数据均正常进入且对应指标正确 1. 再次导入的 1001 条数据均正常进入且对应指标正确 |



## 7. 易用性测试（可选）

无

## 8. 长期稳定性测试（可选）

无

## 9. 性能测试

无

## 10. 安全测试

无

## 11. 兼容性测试

兼容老版本

## 12. 已知问题和限制（可选）

 无

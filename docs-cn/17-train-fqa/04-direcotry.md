# 文件目录结构

安装 TDengine 后，默认会在操作系统中生成下列目录或文件：

| 目录/文件                 | 说明                                                                 |
| ------------------------- | -------------------------------------------------------------------- |
| /usr/local/taos/bin       | TDengine 可执行文件目录。其中的执行文件都会软链接到/usr/bin 目录下。 |
| /usr/local/taos/connector | TDengine 各种连接器目录。                                            |
| /usr/local/taos/driver    | TDengine 动态链接库目录。会软链接到/usr/lib 目录下。                 |
| /usr/local/taos/examples  | TDengine 各种语言应用示例目录。                                      |
| /usr/local/taos/include   | TDengine 对外提供的 C 语言接口的头文件。                             |
| /etc/taos/taos.cfg        | TDengine 默认[配置文件]                                              |
| /var/lib/taos             | TDengine 默认数据文件目录。可通过[配置文件]修改位置。                |
| /var/log/taos             | TDengine 默认日志文件目录。可通过[配置文件]修改位置。                |

## 可执行文件

TDengine 的所有可执行文件默认存放在 _/usr/local/taos/bin_ 目录下。其中包括：

- _taosd_：TDengine 服务端可执行文件
- _taos_：TDengine Shell 可执行文件
- _taosdump_：数据导入导出工具
- _taosBenchmark_：TDengine 测试工具
- remove.sh：卸载 TDengine 的脚本，请谨慎执行，链接到/usr/bin 目录下的**rmtaos**命令。会删除 TDengine 的安装目录/usr/local/taos，但会保留/etc/taos、/var/lib/taos、/var/log/taos。

:::note
2.4.0.0 版本之后的 taosBenchmark 和 taosdump 需要安装独立安装包 taosTools。

:::

:::tip
您可以通过修改系统配置文件 taos.cfg 来配置不同的数据目录和日志目录。

:::

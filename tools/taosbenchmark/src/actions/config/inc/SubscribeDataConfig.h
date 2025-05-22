#ifndef SUBSCRIBE_DATA_CONFIG_H
#define SUBSCRIBE_DATA_CONFIG_H

#include <string>
#include <vector>
#include <map>
#include <optional>
#include "ConnectionInfo.h"
#include "DataFormat.h"
#include "DataChannel.h"

struct SubscribeDataConfig {
    struct Source {
        ConnectionInfo connection_info; // 数据库连接信息
    } source;

    struct Control {
        DataFormat data_format;   // 数据格式化配置
        DataChannel data_channel; // 数据通道配置

        struct SubscribeControl {
            std::string log_path = "result.txt"; // 日志文件路径
            bool enable_dryrun = false;         // 是否启用模拟执行

            struct Execution {
                int consumer_concurrency = 1; // 并发消费者数量
                int poll_timeout = 1000;      // 轮询超时时间（毫秒）
            } execution;

            struct Topic {
                std::string name; // 主题名称
                std::string sql;  // 创建主题的 SQL 语句
            };
            std::vector<Topic> topics; // 订阅主题列表

            struct Commit {
                std::string mode = "auto"; // 提交模式（auto 或 manual）
            } commit;

            struct GroupID {
                std::string strategy;       // Group ID 生成策略（shared、independent、custom）
                std::optional<std::string> custom_id; // 自定义 Group ID（当 strategy 为 custom 时必需）
            } group_id;

            struct Output {
                std::string path;         // 数据文件保存路径
                std::string file_prefix;  // 数据文件前缀
                std::optional<int> expected_rows; // 每个消费者期望消费的行数
            } output;

            std::map<std::string, std::string> advanced; // 高级参数配置，键值对映射
        } subscribe_control;
    } control;
};

#endif // SUBSCRIBE_DATA_CONFIG_H
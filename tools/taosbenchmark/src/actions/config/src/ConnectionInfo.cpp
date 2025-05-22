#include "ConnectionInfo.h"

#include <vector>
#include <stdexcept>
#include <algorithm>

/**
 * 将字符串转换为小写
 */
static std::string to_lower(const std::string& str) {
    std::string lower_str = str;
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(), ::tolower);
    return lower_str;
}

/**
 * 解析 DSN 字符串并填充 host/port/user/password 字段
 */
void ConnectionInfo::parse_dsn(const std::string& input_dsn) {
    dsn = input_dsn;

    // 1. 检查协议头 "://"
    const size_t protocol_pos = dsn->find("://");
    if (protocol_pos == std::string::npos) {
        throw std::runtime_error("DSN invalid: missing '://' protocol separator");
    }

    // 2. 提取 host:port 部分（排除协议头后的内容）
    const std::string post_protocol = dsn->substr(protocol_pos + 3);
    const size_t path_start = post_protocol.find_first_of("?");
    const std::string host_port = post_protocol.substr(0, path_start);

    // 3. 解析 host 和 port
    size_t colon_pos = host_port.find(':');
    host = host_port.substr(0, colon_pos);
    if (colon_pos != std::string::npos) {
        const std::string port_str = host_port.substr(colon_pos + 1);
        try {
            port = std::stoi(port_str);
            if (port <= 0 || port > 65535) {  // 校验端口范围
                throw std::runtime_error("Invalid port number: " + port_str);
            }
        } catch (const std::exception& e) {
            throw std::runtime_error("Port parse error: " + std::string(e.what()));
        }
    }  // 未指定 port 则保留原值

    // 4. 解析查询参数
    const size_t query_start = post_protocol.find('?', path_start);
    if (query_start != std::string::npos) {
        const std::string query = post_protocol.substr(query_start + 1);
        std::vector<std::string> pairs;
        size_t pos = 0;

        // 分割键值对
        while (pos < query.size()) {
            const size_t end = query.find('&', pos);
            if (end == std::string::npos) {
                pairs.push_back(query.substr(pos));
                break;
            } else {
                pairs.push_back(query.substr(pos, end - pos));
                pos = end + 1;
            }
        }

        // 处理每个键值对
        for (const auto& pair : pairs) {
            const size_t eq_pos = pair.find('=');
            if (eq_pos == std::string::npos) continue;  // 忽略无效格式

            const std::string key = to_lower(pair.substr(0, eq_pos));
            std::string value = to_lower(pair.substr(eq_pos + 1));

            // 根据 key 映射到对应字段
            user = key;
            password = value;
        }
    }
}




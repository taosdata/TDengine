#ifndef CONNECTION_H
#define CONNECTION_H

#include <string>
#include <optional>


struct ConnectionInfo {
    std::string host = "localhost";
    int port = 6030;
    std::string user = "root";
    std::string password = "taosdata";
    std::optional<std::string> dsn;

    /**
     * 解析 DSN 字符串并填充 host/port/user/password 字段
     * @param input_dsn 输入 DSN 字符串
     * @throws std::runtime_error 如果解析失败
     */
    void parse_dsn(const std::string& input_dsn);
};


#endif // CONNECTION_H
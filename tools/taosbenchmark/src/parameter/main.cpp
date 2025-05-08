#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>

// 使用 nlohmann::json 别名
using json = nlohmann::json;

int main() {
    // 打开 JSON 文件
    std::ifstream file("../config.json");
    if (!file.is_open()) {
        std::cerr << "Failed to open config.json" << std::endl;
        return 1;
    }

    // 解析 JSON 文件
    json config;
    try {
        file >> config;
    } catch (const json::parse_error& e) {
        std::cerr << "JSON parse error: " << e.what() << std::endl;
        return 1;
    }

    // 输出解析结果
    std::cout << "Parsed JSON content:" << std::endl;
    std::cout << config.dump(4) << std::endl;

    // 示例：访问 JSON 数据
    if (config.contains("host")) {
        std::cout << "Host: " << config["host"].get<std::string>() << std::endl;
    }

    return 0;
}
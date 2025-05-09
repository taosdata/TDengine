#include "ParameterContext.h"
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <vector>


// 测试命令行参数解析
void test_commandline_merge() {
    ParameterContext ctx;
    const char* argv[] = {
        "dummy_program",
        "--host=127.0.0.1",
        "--port=6041",
        "--user=admin",
        "--password=taosdata"
    };
    ctx.merge_commandline(5, const_cast<char**>(argv));

    const auto& config = ctx.get_config_data().global;
    assert(config.host == "127.0.0.1");
    assert(config.port == 6041);
    assert(config.user == "admin");
    assert(config.password == "taosdata");
    std::cout << "Commandline merge test passed.\n";
}

// 测试环境变量合并
void test_environment_merge() {
    ParameterContext ctx;
    setenv("TAOS_HOST", "192.168.1.100", 1);
    setenv("TAOS_PORT", "6042", 1);
    ctx.merge_environment_vars();

    const auto& config = ctx.get_config_data().global;
    assert(config.host == "192.168.1.100");
    assert(config.port == 6042);
    std::cout << "Environment merge test passed.\n";
}

// 测试 YAML 配置合并
void test_yaml_merge() {
    ParameterContext ctx;

    // 模拟 YAML 配置
    YAML::Node config = YAML::Load(R"(
global:
  host: 10.0.0.1
  port: 6043
  user: root
  password: secret
concurrency: 4
jobs:
  - job_type: insert
    job_name: job1
    source:
      source_type: kafka
  - job_type: query
    job_name: job2
    source:
      connection:
        host: localhost
)");

    ctx.merge_yaml(config);
    const auto& data = ctx.get_config_data();

    // 验证全局配置
    assert(data.global.host == "10.0.0.1");
    assert(data.global.port == 6043);
    assert(data.concurrency == 4);

    // 验证作业解析
    assert(data.jobs.size() == 2);
    assert(std::holds_alternative<InsertJobConfig>(data.jobs[0]));
    assert(std::holds_alternative<QueryJobConfig>(data.jobs[1]));

    std::cout << "YAML merge test passed.\n";
}

// 测试参数优先级（命令行 > 环境变量 > YAML）
void test_priority() {
    ParameterContext ctx;

    // 设置环境变量
    setenv("TAOS_HOST", "env.host", 1);

    // 合并 YAML
    YAML::Node config = YAML::Load(R"(
global:
  host: yaml.host
)");
    ctx.merge_yaml(config);

    // 合并命令行参数
    const char* argv[] = {"dummy", "--host=cli.host"};
    ctx.merge_commandline(2, const_cast<char**>(argv));

    // 验证优先级
    assert(ctx.get_config_data().global.host == "cli.host");
    std::cout << "Priority test passed.\n";
}

int main() {
    test_commandline_merge();
    test_environment_merge();
    test_yaml_merge();
    test_priority();
    std::cout << "All tests passed!\n";
    return 0;
}

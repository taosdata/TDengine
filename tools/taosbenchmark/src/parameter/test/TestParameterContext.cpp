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

    const auto& conn_info = ctx.get_connection_info();
    assert(conn_info.host == "127.0.0.1");
    assert(conn_info.port == 6041);
    assert(conn_info.user == "admin");
    assert(conn_info.password == "taosdata");
    std::cout << "Commandline merge test passed.\n";
}

// 测试环境变量合并
void test_environment_merge() {
    ParameterContext ctx;
    setenv("TAOS_HOST", "192.168.1.100", 1);
    setenv("TAOS_PORT", "6042", 1);
    ctx.merge_environment_vars();

    const auto& conn_info = ctx.get_connection_info();
    assert(conn_info.host == "192.168.1.100");
    assert(conn_info.port == 6042);
    std::cout << "Environment merge test passed.\n";
}

// 测试 YAML 配置合并
void test_yaml_merge() {
    ParameterContext ctx;

    // 模拟 YAML 配置
    YAML::Node config = YAML::Load(R"(
global:
  connection_info: &db_conn
    host: 10.0.0.1
    port: 6043
    user: root
    password: secret
concurrency: 4
jobs:
  create-super-table:
    name: Create Super Table
    needs: []
    steps:
      - name: Create Super Table
        uses: actions/create-super-table
        with:
          connection_info: *db_conn
  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-super-table]
    steps:
      - name: Insert Data
        uses: actions/insert-data
        with:
          source:
            table_name:
              source_type: generator
              generator:
                prefix: s
                count: 10000
                from: 200
)");

    ctx.merge_yaml(config);
    const auto& data = ctx.get_config_data();

    // 验证全局配置
    assert(data.global.connection_info.host == "10.0.0.1");
    assert(data.global.connection_info.port == 6043);
    assert(data.concurrency == 4);

    // 验证作业解析
    assert(data.jobs.size() == 2);
    assert(data.jobs[0].key == "create-super-table");
    assert(data.jobs[0].name == "Create Super Table");
    assert(data.jobs[0].steps.size() == 1);

    assert(data.jobs[1].key == "insert-second-data");
    assert(data.jobs[1].name == "Insert Second-Level Data");
    assert(data.jobs[1].needs.size() == 1);


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
    assert(ctx.get_config_data().global.connection_info.host == "cli.host");
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

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
  database_info: &db_info
    name: testdb
    drop_if_exists: true
    precision: us
    properties: vgroups 20 replica 3 keep 3650
  super_table_info: &stb_info
    name: points
    columns: &columns_info
      - name: latitude
        type: float
      - name: longitude
        type: float
      - name: quality
        type: varchar(50)
    tags: &tags_info
      - name: type
        type: varchar(7)
      - name: name
        type: varchar(20)
      - name: department
        type: varchar(7)

concurrency: 4
jobs:
  create-database:
    name: Create Database
    needs: []
    steps:
      - name: Create Database
        uses: actions/create-database
        with:
          connection_info: *db_conn
          database_info:
            name: testdb
            drop_if_exists: true
            precision: us
            properties: vgroups 20 replica 3 keep 3650

  create-super-table:
    name: Create Super Table
    needs: [create-database]
    steps:
      - name: Create Super Table
        uses: actions/create-super-table
        with:
          connection_info: *db_conn
          database_info:
            name: testdb
          super_table_info:
            name: points
            columns: *columns_info
            tags: *tags_info

  create-second-child-table:
    name: Create Second Child Table
    needs: [create-super-table]
    steps:
      - name: Create Second Child Table
        uses: actions/create-child-table
        with:
          connection_info: *db_conn

  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
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
    assert(data.jobs.size() == 4);
    assert(data.jobs[0].key == "create-database");
    assert(data.jobs[0].name == "Create Database");
    assert(data.jobs[0].needs.size() == 0);
    assert(data.jobs[0].steps.size() == 1);
    assert(data.jobs[0].steps[0].name == "Create Database");
    assert(data.jobs[0].steps[0].uses == "actions/create-database");
    assert(std::holds_alternative<CreateDatabaseConfig>(data.jobs[0].steps[0].action_config));
    const auto& create_db_config = std::get<CreateDatabaseConfig>(data.jobs[0].steps[0].action_config);
    assert(create_db_config.connection_info.host == "10.0.0.1");
    assert(create_db_config.connection_info.port == 6043);
    assert(create_db_config.connection_info.user == "root");
    assert(create_db_config.connection_info.password == "secret");
    assert(create_db_config.database_info.name == "testdb");
    assert(create_db_config.database_info.drop_if_exists == true);
    assert(create_db_config.database_info.precision == "us");
    assert(create_db_config.database_info.properties == "precision us vgroups 20 replica 3 keep 3650");

    assert(data.jobs[1].key == "create-super-table");
    assert(data.jobs[1].name == "Create Super Table");
    assert(data.jobs[1].needs.size() == 1);
    assert(data.jobs[1].steps.size() == 1);
    assert(data.jobs[1].steps[0].name == "Create Super Table");
    assert(data.jobs[1].steps[0].uses == "actions/create-super-table");
    assert(std::holds_alternative<CreateSuperTableConfig>(data.jobs[1].steps[0].action_config));
    const auto& create_stb_config = std::get<CreateSuperTableConfig>(data.jobs[1].steps[0].action_config);
    assert(create_stb_config.database_info.name == "testdb");
    assert(create_stb_config.super_table_info.name == "points");
    assert(create_stb_config.super_table_info.columns.size() > 0);
    assert(create_stb_config.super_table_info.tags.size() > 0);

    assert(data.jobs[2].key == "create-second-child-table");
    assert(data.jobs[2].name == "Create Second Child Table");
    assert(data.jobs[2].needs.size() == 1);
    assert(data.jobs[2].steps.size() == 1);
    assert(data.jobs[2].steps[0].name == "Create Second Child Table");
    assert(data.jobs[2].steps[0].uses == "actions/create-child-table");

    assert(data.jobs[3].key == "insert-second-data");
    assert(data.jobs[3].name == "Insert Second-Level Data");
    assert(data.jobs[3].needs.size() == 1);
    assert(data.jobs[3].steps.size() == 1);
    assert(data.jobs[3].steps[0].name == "Insert Data");
    assert(data.jobs[3].steps[0].uses == "actions/insert-data");


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

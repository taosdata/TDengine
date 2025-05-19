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
          database_info:
            name: testdb
          super_table_info:
            name: points
          child_table_info:
            table_name:
              source_type: generator
              generator:
                prefix: s
                count: 10000
                from: 200
            tags:
              source_type: csv
              csv:
                file_path: /root/meta/cnnc_csv_1s.csv
                has_header: true
          batch:
            size: 1000
            concurrency: 10

  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
    steps:
      - name: Insert Second-Level Data
        uses: actions/insert-data
        with:
          # source
          source:
            table_name:
              source_type: generator
              generator:
                prefix: s
                count: 10000
                from: 200
            columns:
              source_type: csv
              csv:
                file_path: /root/data/cnnc_csv_1s/
                has_header: true

                timestamp_strategy:
                  strategy_type: original
                  original_config:
                    column_index: 0
                    precision: us

          # target
          target:
            target_type: tdengine
            tdengine:
              connection_info: *db_conn
              database_info:
                name: testdb
                precision: us
            
              super_table_info:
                name: points
                columns: *columns_info
                tags: *tags_info

          # control
          control: &insert_second_control
            data_format:
              format_type: sql
            data_channel:
              channel_type: native
            data_generation:
              interlace_mode:
                enabled: true
                rows: 60
              generate_threads: 8
              per_table_rows: 10000
            insert_control:
              per_request_rows: 10000
              auto_create_table: false
              insert_threads: 8
              thread_allocation: vgroup_binding
            time_interval:
              enabled: true
              interval_strategy: first_to_first

  query-super-table:
    name: Query Super Table
    needs:
      - create-second-child-table
      - create-minute-child-table
    steps:
      - name: query-super-table
        uses: actions/query-data
        with:
          # source
          source:
            connection_info: *db_conn
          
          # control
          control:
            data_format:
              format_type: sql
            data_channel:
              channel_type: native
            query_control:
              execution:
                mode: parallel_per_group
                threads: 10
                times: 50
                interval: 100
              query_type: super_table
              super_table:
                database_name: testdb
                super_table_name: points
                placeholder: ${child_table}
                templates:
                  - sql_template: select count(*) from ${child_table}
                    output_file: stb_result.txt

  subscribe-data:
    name: Subscribe Data
    needs:
      - create-second-child-table
      - create-minute-child-table
    steps:
      - name: subscribe-data
        uses: actions/subscribe-data
        with:
          # source
          source:
            connection_info: *db_conn
          # control
          control:
            data_format:
              format_type: sql
            data_channel:
              channel_type: native
            subscribe_control:
              execution:
                consumer_concurrency: 5
                poll_timeout: 500
              topics:
                - name: topic1
                  sql: select * from testdb.points
              commit:
                mode: auto
              group_id:
                strategy: custom
                custom_id: custom_group
              output:
                path: out
                file_prefix: subscribe_data_
                expected_rows: 10000
              advanced:
                client.id: benchmark_client
                auto.offset.reset: earliest
                msg.with.table.name: true
)");

    ctx.merge_yaml(config);
    const auto& data = ctx.get_config_data();

    // 验证全局配置
    assert(data.global.connection_info.host == "10.0.0.1");
    assert(data.global.connection_info.port == 6043);
    assert(data.concurrency == 4);

    // 验证作业解析
    // job: create-database
    assert(data.jobs.size() == 6);
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
    assert(create_db_config.database_info.properties == "vgroups 20 replica 3 keep 3650");

    // job: create-super-table
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

    // job: create-second-child-table
    assert(data.jobs[2].key == "create-second-child-table");
    assert(data.jobs[2].name == "Create Second Child Table");
    assert(data.jobs[2].needs.size() == 1);
    assert(data.jobs[2].steps.size() == 1);
    assert(data.jobs[2].steps[0].name == "Create Second Child Table");
    assert(data.jobs[2].steps[0].uses == "actions/create-child-table");
    assert(std::holds_alternative<CreateChildTableConfig>(data.jobs[2].steps[0].action_config));
    const auto& create_child_config = std::get<CreateChildTableConfig>(data.jobs[2].steps[0].action_config);
    assert(create_child_config.database_info.name == "testdb");
    assert(create_child_config.super_table_info.name == "points");
    assert(create_child_config.child_table_info.table_name.source_type == "generator");
    assert(create_child_config.child_table_info.table_name.generator.prefix == "s");
    assert(create_child_config.child_table_info.table_name.generator.count == 10000);
    assert(create_child_config.child_table_info.table_name.generator.from == 200);
    assert(create_child_config.child_table_info.tags.source_type == "csv");
    assert(create_child_config.child_table_info.tags.csv.file_path == "/root/meta/cnnc_csv_1s.csv");
    assert(create_child_config.child_table_info.tags.csv.has_header == true);
    assert(create_child_config.batch.size == 1000);
    assert(create_child_config.batch.concurrency == 10);

    // job: insert-second-data
    assert(data.jobs[3].key == "insert-second-data");
    assert(data.jobs[3].name == "Insert Second-Level Data");
    assert(data.jobs[3].needs.size() == 1);
    assert(data.jobs[3].needs[0] == "create-second-child-table");
    assert(data.jobs[3].steps.size() == 1);
    assert(data.jobs[3].steps[0].name == "Insert Second-Level Data");
    assert(data.jobs[3].steps[0].uses == "actions/insert-data");
    assert(std::holds_alternative<InsertDataConfig>(data.jobs[3].steps[0].action_config));
    const auto& insert_config = std::get<InsertDataConfig>(data.jobs[3].steps[0].action_config);

    assert(insert_config.source.table_name.source_type == "generator");
    assert(insert_config.source.table_name.generator.prefix == "s");
    assert(insert_config.source.table_name.generator.count == 10000);
    assert(insert_config.source.table_name.generator.from == 200);

    assert(insert_config.source.columns.source_type == "csv");
    assert(insert_config.source.columns.csv.file_path == "/root/data/cnnc_csv_1s/");
    assert(insert_config.source.columns.csv.has_header == true);
    assert(insert_config.source.columns.csv.timestamp_strategy.strategy_type == "original");
    assert(insert_config.source.columns.csv.timestamp_strategy.original_config.column_index == 0);
    assert(insert_config.source.columns.csv.timestamp_strategy.original_config.precision == "us");

    assert(insert_config.target.target_type == "tdengine");
    assert(insert_config.target.tdengine.connection_info.host == "10.0.0.1");
    assert(insert_config.target.tdengine.connection_info.port == 6043);
    assert(insert_config.target.tdengine.connection_info.user == "root");
    assert(insert_config.target.tdengine.connection_info.password == "secret");
    assert(insert_config.target.tdengine.database_info.name == "testdb");
    assert(insert_config.target.tdengine.database_info.precision == "us");
    assert(insert_config.target.tdengine.super_table_info.name == "points");
    assert(insert_config.target.tdengine.super_table_info.columns.size() > 0);
    assert(insert_config.target.tdengine.super_table_info.tags.size() > 0);

    assert(insert_config.control.data_format.format_type == "sql");
    assert(insert_config.control.data_channel.channel_type == "native");

    assert(insert_config.control.data_generation.interlace_mode.enabled == true);
    assert(insert_config.control.data_generation.interlace_mode.rows  == 60);
    assert(insert_config.control.data_generation.generate_threads == 8);
    assert(insert_config.control.data_generation.per_table_rows == 10000);

    assert(insert_config.control.insert_control.per_request_rows == 10000);
    assert(insert_config.control.insert_control.auto_create_table == false);
    assert(insert_config.control.insert_control.insert_threads == 8);
    assert(insert_config.control.insert_control.thread_allocation == "vgroup_binding");

    assert(insert_config.control.time_interval.enabled == true);
    assert(insert_config.control.time_interval.interval_strategy == "first_to_first");


    // job: query-super-table
    assert(data.jobs[4].key == "query-super-table");
    assert(data.jobs[4].name == "Query Super Table");
    assert(data.jobs[4].needs.size() == 2);
    assert(data.jobs[4].needs[0] == "create-second-child-table");
    assert(data.jobs[4].needs[1] == "create-minute-child-table");
    assert(data.jobs[4].steps.size() == 1);
    assert(data.jobs[4].steps[0].name == "query-super-table");
    assert(data.jobs[4].steps[0].uses == "actions/query-data");

    assert(std::holds_alternative<QueryDataConfig>(data.jobs[4].steps[0].action_config));
    const auto& query_config = std::get<QueryDataConfig>(data.jobs[4].steps[0].action_config);

    assert(query_config.source.connection_info.host == "10.0.0.1");
    assert(query_config.source.connection_info.port == 6043);
    assert(query_config.source.connection_info.user == "root");
    assert(query_config.source.connection_info.password == "secret");

    assert(query_config.control.data_format.format_type == "sql");
    assert(query_config.control.data_channel.channel_type == "native");

    const auto& query_control = query_config.control.query_control;
    assert(query_control.execution.mode == "parallel_per_group");
    assert(query_control.execution.threads == 10);
    assert(query_control.execution.times == 50);
    assert(query_control.execution.interval == 100);

    assert(query_control.query_type == "super_table");
    const auto& super_table = query_control.super_table;
    assert(super_table.database_name == "testdb");
    assert(super_table.super_table_name == "points");
    assert(super_table.placeholder == "${child_table}");
    assert(super_table.templates.size() == 1);
    assert(super_table.templates[0].sql_template == "select count(*) from ${child_table}");
    assert(super_table.templates[0].output_file == "stb_result.txt");


    // job: subscribe-super-table
    assert(data.jobs[5].key == "subscribe-data");
    assert(data.jobs[5].name == "Subscribe Data");
    assert(data.jobs[5].needs.size() == 2);
    assert(data.jobs[5].needs[0] == "create-second-child-table");
    assert(data.jobs[5].needs[1] == "create-minute-child-table");
    assert(data.jobs[5].steps.size() == 1);
    assert(data.jobs[5].steps[0].name == "subscribe-data");
    assert(data.jobs[5].steps[0].uses == "actions/subscribe-data");

    assert(std::holds_alternative<SubscribeDataConfig>(data.jobs[5].steps[0].action_config));
    const auto& subscribe_config = std::get<SubscribeDataConfig>(data.jobs[5].steps[0].action_config);

    assert(subscribe_config.source.connection_info.host == "10.0.0.1");
    assert(subscribe_config.source.connection_info.port == 6043);
    assert(subscribe_config.source.connection_info.user == "root");
    assert(subscribe_config.source.connection_info.password == "secret");

    assert(subscribe_config.control.data_format.format_type == "sql");
    assert(subscribe_config.control.data_channel.channel_type == "native");

    const auto& subscribe_control = subscribe_config.control.subscribe_control;
    assert(subscribe_control.execution.consumer_concurrency == 5);
    assert(subscribe_control.execution.poll_timeout == 500);
    assert(subscribe_control.topics.size() == 1);
    assert(subscribe_control.topics[0].name == "topic1");
    assert(subscribe_control.topics[0].sql == "select * from testdb.points");
    assert(subscribe_control.commit.mode == "auto");
    assert(subscribe_control.group_id.strategy == "custom");
    assert(subscribe_control.group_id.custom_id == "custom_group");
    assert(subscribe_control.output.path == "out");
    assert(subscribe_control.output.file_prefix == "subscribe_data_");
    assert(subscribe_control.output.expected_rows == 10000);
    assert(subscribe_control.advanced.at("client.id") == "benchmark_client");
    assert(subscribe_control.advanced.at("auto.offset.reset") == "earliest");
    assert(subscribe_control.advanced.at("msg.with.table.name") == "true");


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

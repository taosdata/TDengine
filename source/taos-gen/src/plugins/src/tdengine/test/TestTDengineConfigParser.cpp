#include "ConfigParser.hpp"
#include "TDengineConfigParser.hpp"
#include "TDengineRegistrar.hpp"
#include <iostream>
#include <sstream>
#include <cassert>
#include <yaml-cpp/yaml.h>

void test_TDengine() {
    std::string yaml = R"(
dsn: "taos+ws://root:taosdata@127.0.0.1:6041/test"
drop_if_exists: false
props: "precision 'ms' vgroups 4"
pool:
  enabled: true
  max_size: 10
  min_size: 2
  timeout: 1000
)";
    YAML::Node node = YAML::Load(yaml);
    TDengineConfig conn = node.as<TDengineConfig>();
    assert(conn.dsn == "taos+ws://root:taosdata@127.0.0.1:6041/test");
    assert(conn.drop_if_exists == false);
    assert(conn.properties.has_value() && *conn.properties == "precision 'ms' vgroups 4");
    assert(conn.pool.enabled == true);
    assert(conn.pool.max_size == 10);
    assert(conn.pool.min_size == 2);
    assert(conn.pool.timeout == 1000);
}


void test_InsertDataConfig_tdengine() {
    std::string yaml = R"(
target: tdengine
tdengine:
  dsn: "taos://root:taosdata@localhost:6030/test"
schema:
  name: test_schema
  columns:
    - name: ts
      type: BIGINT
format: sql
auto_create_table: true
timestamp_precision: us
concurrency: 8
queue_capacity: 2048
queue_warmup_ratio: 0.75
shared_queue: true
thread_affinity: true
thread_realtime: true
failure_handling:
  max_retries: 10
  retry_interval_ms: 200
  on_failure: skip
time_interval:
  enabled: true
  interval_strategy: fixed
  fixed_interval:
    base_interval: 100
checkpoint:
  enabled: true
  interval_sec: 5000
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig idc = node.as<InsertDataConfig>();
    assert(idc.target_type == "tdengine");
    auto* tc = get_plugin_config_mut<TDengineConfig>(idc.extensions, "tdengine");
    (void)tc;
    assert(tc != nullptr);
    assert(tc->dsn == "taos://root:taosdata@localhost:6030/test");
    assert(idc.schema.name == "test_schema");
    assert(idc.data_format.format_type == "sql");
    assert(idc.data_format.support_tags == true);

    auto* sf = get_format_opt_mut<SqlFormatOptions>(idc.data_format, "sql");
    (void)sf;
    assert(sf != nullptr);
    assert(sf->auto_create_table == true);

    assert(idc.timestamp_precision == "us");
    assert(idc.insert_threads == 8);
    assert(idc.queue_capacity == 2048);
    assert(idc.queue_warmup_ratio == 0.75);
    assert(idc.shared_queue == true);
    assert(idc.thread_affinity == true);
    assert(idc.thread_realtime == true);
    assert(idc.failure_handling.max_retries == 10);
    assert(idc.failure_handling.on_failure == "skip");
    assert(idc.time_interval.enabled == true);
    assert(idc.time_interval.fixed_interval.base_interval == 100);
    assert(idc.checkpoint_info.enabled == true);
    assert(idc.checkpoint_info.interval_sec == 5000);
}

int main() {
    register_tdengine_plugin_config_hooks();
    test_TDengine();
    test_InsertDataConfig_tdengine();

    std::cout << "All ConfigParser YAML tests passed!" << std::endl;
    return 0;
}
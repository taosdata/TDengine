#include "ConfigParser.hpp"
#include "InfluxDBConfigParser.hpp"
#include "InfluxDBRegistrar.hpp"
#include <iostream>
#include <cassert>
#include <yaml-cpp/yaml.h>

InfluxDBConfig* get_influxdb_config(InsertDataConfig& config) {
    return get_plugin_config_mut<InfluxDBConfig>(config.extensions, "influxdb");
}

InfluxDBFormatOptions* get_influxdb_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<InfluxDBFormatOptions>(config.data_format, "influxdb");
}

void test_InfluxDBConfig_basic() {
    std::string yaml = R"(
url: "http://influxdb.example.com:8086"
token: "my-secret-token"
org: "my-org"
bucket: "my-bucket"
)";
    YAML::Node node = YAML::Load(yaml);
    InfluxDBConfig cfg = node.as<InfluxDBConfig>();
    assert(cfg.url == "http://influxdb.example.com:8086");
    assert(cfg.token == "my-secret-token");
    assert(cfg.org == "my-org");
    assert(cfg.bucket == "my-bucket");
    assert(cfg.enabled == true);
    assert(cfg.get_sink_type() == "InfluxDB");
    assert(cfg.is_enabled() == true);
    assert(cfg.get_sink_info() == "InfluxDB(http://influxdb.example.com:8086/my-bucket)");

    std::cout << "test_InfluxDBConfig_basic PASSED\n";
}

void test_InfluxDBConfig_defaults() {
    std::string yaml = "{}";
    YAML::Node node = YAML::Load(yaml);
    InfluxDBConfig cfg = node.as<InfluxDBConfig>();
    assert(cfg.url == "http://localhost:8086");
    assert(cfg.token.empty());
    assert(cfg.org == "default");
    assert(cfg.bucket == "default");

    std::cout << "test_InfluxDBConfig_defaults PASSED\n";
}

void test_InfluxDBConfig_partial() {
    std::string yaml = R"(
token: "only-token"
bucket: "metrics"
)";
    YAML::Node node = YAML::Load(yaml);
    InfluxDBConfig cfg = node.as<InfluxDBConfig>();
    assert(cfg.url == "http://localhost:8086");
    assert(cfg.token == "only-token");
    assert(cfg.org == "default");
    assert(cfg.bucket == "metrics");

    std::cout << "test_InfluxDBConfig_partial PASSED\n";
}

void test_InfluxDBConfig_unknown_key() {
    std::string yaml = R"(
url: "http://localhost:8086"
unknown_key: "some_value"
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        InfluxDBConfig cfg = node.as<InfluxDBConfig>();
        (void)cfg;
        assert(false && "Should throw for unknown key");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unknown configuration key") != std::string::npos);
    }

    std::cout << "test_InfluxDBConfig_unknown_key PASSED\n";
}

void test_InfluxDBConfig_not_enabled_by_default() {
    InfluxDBConfig cfg;
    assert(!cfg.enabled);
    assert(!cfg.is_enabled());

    std::cout << "test_InfluxDBConfig_not_enabled_by_default PASSED\n";
}

void test_format_decoder_precision_valid() {
    register_influxdb_plugin_config_hooks();

    for (const auto& prec : {"ns", "us", "ms", "s"}) {
        std::string yaml = R"(
target: influxdb
influxdb:
  url: "http://localhost:8086"
  token: "test"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
precision: )" + std::string(prec);

        YAML::Node node = YAML::Load(yaml);
        InsertDataConfig idc = node.as<InsertDataConfig>();
        auto* fo = get_influxdb_format_options(idc);
        (void)fo;
        assert(fo != nullptr);
        assert(fo->precision == prec);
    }

    std::cout << "test_format_decoder_precision_valid PASSED\n";
}

void test_format_decoder_precision_invalid() {
    std::string yaml = R"(
target: influxdb
influxdb:
  url: "http://localhost:8086"
  token: "test"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
precision: "invalid"
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        InsertDataConfig idc = node.as<InsertDataConfig>();
        (void)idc;
        assert(false && "Should throw for invalid precision");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Invalid precision") != std::string::npos);
    }

    std::cout << "test_format_decoder_precision_invalid PASSED\n";
}

void test_format_decoder_batch_size() {
    std::string yaml = R"(
target: influxdb
influxdb:
  url: "http://localhost:8086"
  token: "test"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
batch_size: 1000
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig idc = node.as<InsertDataConfig>();
    auto* fo = get_influxdb_format_options(idc);
    (void)fo;
    assert(fo != nullptr);
    assert(fo->batch_size == 1000);

    std::cout << "test_format_decoder_batch_size PASSED\n";
}

void test_format_decoder_batch_size_invalid() {
    std::string yaml = R"(
target: influxdb
influxdb:
  url: "http://localhost:8086"
  token: "test"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
batch_size: -1
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        InsertDataConfig idc = node.as<InsertDataConfig>();
        (void)idc;
        assert(false && "Should throw for invalid batch_size");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("batch_size must be greater than 0") != std::string::npos);
    }

    std::cout << "test_format_decoder_batch_size_invalid PASSED\n";
}

void test_format_decoder_gzip() {
    std::string yaml = R"(
target: influxdb
influxdb:
  url: "http://localhost:8086"
  token: "test"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
gzip: true
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig idc = node.as<InsertDataConfig>();
    auto* fo = get_influxdb_format_options(idc);
    (void)fo;
    assert(fo != nullptr);
    assert(fo->gzip == true);

    std::cout << "test_format_decoder_gzip PASSED\n";
}

void test_format_decoder_defaults() {
    std::string yaml = R"(
target: influxdb
influxdb:
  url: "http://localhost:8086"
  token: "test"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig idc = node.as<InsertDataConfig>();
    auto* fo = get_influxdb_format_options(idc);
    (void)fo;
    assert(fo != nullptr);
    assert(fo->precision == "ns");
    assert(fo->batch_size == 5000);
    assert(fo->gzip == false);
    assert(fo->tbname_key.empty());
    assert(idc.data_format.format_type == "influxdb");
    assert(idc.data_format.support_tags == true);

    std::cout << "test_format_decoder_defaults PASSED\n";
}

void test_format_decoder_unknown_key() {
    std::string yaml = R"(
target: influxdb
influxdb:
  url: "http://localhost:8086"
  token: "test"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
unknown_option: "bad"
)";
    YAML::Node node = YAML::Load(yaml);
    try {
        InsertDataConfig idc = node.as<InsertDataConfig>();
        (void)idc;
        assert(false && "Should throw for unknown key");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unknown configuration key") != std::string::npos);
    }

    std::cout << "test_format_decoder_unknown_key PASSED\n";
}

void test_full_config_parsing() {
    std::string yaml = R"(
target: influxdb
influxdb:
  url: "http://myhost:8086"
  token: "my-token-abc"
  org: "my-org"
  bucket: "my-bucket"
schema:
  name: cpu
  columns:
    - name: usage
      type: FLOAT
precision: us
batch_size: 2000
gzip: true
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig idc = node.as<InsertDataConfig>();

    auto* ic = get_influxdb_config(idc);
    (void)ic;
    assert(ic != nullptr);
    assert(ic->url == "http://myhost:8086");
    assert(ic->token == "my-token-abc");
    assert(ic->org == "my-org");
    assert(ic->bucket == "my-bucket");

    auto* fo = get_influxdb_format_options(idc);
    (void)fo;
    assert(fo != nullptr);
    assert(fo->precision == "us");
    assert(fo->batch_size == 2000);
    assert(fo->gzip == true);

    assert(idc.schema.name == "cpu");

    std::cout << "test_full_config_parsing PASSED\n";
}

void test_format_decoder_tbname_key() {
    std::string yaml = R"(
target: influxdb
influxdb:
  url: "http://localhost:8086"
  token: "test"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
tbname_key: "device_id"
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig idc = node.as<InsertDataConfig>();
    auto* fo = get_influxdb_format_options(idc);
    (void)fo;
    assert(fo != nullptr);
    assert(fo->tbname_key == "device_id");

    std::cout << "test_format_decoder_tbname_key PASSED\n";
}

int main() {
    register_influxdb_plugin_config_hooks();

    test_InfluxDBConfig_basic();
    test_InfluxDBConfig_defaults();
    test_InfluxDBConfig_partial();
    test_InfluxDBConfig_unknown_key();
    test_InfluxDBConfig_not_enabled_by_default();
    test_format_decoder_precision_valid();
    test_format_decoder_precision_invalid();
    test_format_decoder_batch_size();
    test_format_decoder_batch_size_invalid();
    test_format_decoder_gzip();
    test_format_decoder_defaults();
    test_format_decoder_unknown_key();
    test_full_config_parsing();
    test_format_decoder_tbname_key();

    std::cout << "\nAll InfluxDB ConfigParser tests PASSED\n";
    return 0;
}

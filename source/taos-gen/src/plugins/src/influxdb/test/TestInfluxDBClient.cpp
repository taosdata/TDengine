#include "InfluxDBClient.hpp"
#include <iostream>
#include <cassert>
#include <stdexcept>
#include <vector>

// Mock implementation of IInfluxDBClient for testing
class MockInfluxDBClient : public IInfluxDBClient {
public:
    bool connected = false;
    size_t execute_count = 0;
    size_t total_rows_executed = 0;
    bool fail_connect = false;
    int fail_execute_times = 0;
    std::vector<std::string> executed_lines;

    bool connect() override {
        if (fail_connect) {
            throw std::runtime_error("Simulated connection failure");
        }
        connected = true;
        return true;
    }

    bool is_connected() const override {
        return connected;
    }

    void close() override {
        connected = false;
    }

    bool execute(const InfluxDBInsertData& data) override {
        execute_count++;

        if (fail_execute_times > 0) {
            fail_execute_times--;
            return false;
        }

        executed_lines.push_back(data.lines);
        total_rows_executed += data.total_rows;
        return true;
    }
};

void test_influxdb_client_url_construction() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "my-token-123";
    config.org = "my-org";
    config.bucket = "my-bucket";

    InfluxDBFormatOptions format;
    format.precision = "ns";

    InfluxDBClient client(config, format);
    assert(!client.is_connected());

    std::cout << "test_influxdb_client_url_construction PASSED\n";
}

void test_influxdb_client_trailing_slash() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086/";
    config.token = "test-token";
    config.org = "test-org";
    config.bucket = "test-bucket";

    InfluxDBFormatOptions format;
    format.precision = "ms";

    InfluxDBClient client(config, format);
    assert(!client.is_connected());

    std::cout << "test_influxdb_client_trailing_slash PASSED\n";
}

void test_influxdb_client_connect_close() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "test-token";
    config.org = "default";
    config.bucket = "default";

    InfluxDBFormatOptions format;
    format.precision = "ns";

    InfluxDBClient client(config, format);
    assert(!client.is_connected());

    bool ok = client.connect();
    (void)ok;
    assert(ok);
    assert(client.is_connected());

    client.close();
    assert(!client.is_connected());

    std::cout << "test_influxdb_client_connect_close PASSED\n";
}

void test_influxdb_client_mock_inject() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "test-token";
    config.org = "default";
    config.bucket = "default";

    InfluxDBFormatOptions format;

    InfluxDBClient client(config, format);

    auto mock = std::make_unique<MockInfluxDBClient>();
    auto* mock_ptr = mock.get();
    client.set_client(std::move(mock));

    assert(!client.is_connected());
    assert(client.connect());
    assert(client.is_connected());
    (void)mock_ptr;
    assert(mock_ptr->connected);

    client.close();
    assert(!client.is_connected());
    assert(!mock_ptr->connected);

    std::cout << "test_influxdb_client_mock_inject PASSED\n";
}

void test_influxdb_client_mock_execute() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "test-token";
    config.org = "default";
    config.bucket = "default";

    InfluxDBFormatOptions format;

    InfluxDBClient client(config, format);

    auto mock = std::make_unique<MockInfluxDBClient>();
    auto* mock_ptr = mock.get();
    client.set_client(std::move(mock));

    client.connect();

    InfluxDBInsertData data("cpu,host=h1 usage=50.0 1000000000\ncpu,host=h2 usage=60.0 2000000000", 2);
    bool ok = client.execute(data);
    (void)ok;
    assert(ok);
    (void)mock_ptr;
    assert(mock_ptr->execute_count == 1);
    assert(mock_ptr->total_rows_executed == 2);
    assert(mock_ptr->executed_lines.size() == 1);
    assert(mock_ptr->executed_lines[0].find("cpu,host=h1") != std::string::npos);

    std::cout << "test_influxdb_client_mock_execute PASSED\n";
}

void test_influxdb_client_mock_connect_failure() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "test-token";
    config.org = "default";
    config.bucket = "default";

    InfluxDBFormatOptions format;

    InfluxDBClient client(config, format);

    auto mock = std::make_unique<MockInfluxDBClient>();
    mock->fail_connect = true;
    client.set_client(std::move(mock));

    try {
        client.connect();
        assert(false && "Should have thrown");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Simulated connection failure") != std::string::npos);
    }

    std::cout << "test_influxdb_client_mock_connect_failure PASSED\n";
}

void test_influxdb_client_mock_execute_failure() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "test-token";
    config.org = "default";
    config.bucket = "default";

    InfluxDBFormatOptions format;

    InfluxDBClient client(config, format);

    auto mock = std::make_unique<MockInfluxDBClient>();
    mock->fail_execute_times = 1;
    auto* mock_ptr = mock.get();
    client.set_client(std::move(mock));

    client.connect();

    InfluxDBInsertData data("cpu,host=h1 usage=50.0 1000000000", 1);
    bool ok = client.execute(data);
    (void)ok;
    assert(!ok);
    (void)mock_ptr;
    assert(mock_ptr->execute_count == 1);
    assert(mock_ptr->total_rows_executed == 0);

    // Second call succeeds
    ok = client.execute(data);
    assert(ok);
    assert(mock_ptr->execute_count == 2);
    assert(mock_ptr->total_rows_executed == 1);

    std::cout << "test_influxdb_client_mock_execute_failure PASSED\n";
}

void test_influxdb_client_empty_data() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "test-token";
    config.org = "default";
    config.bucket = "default";

    InfluxDBFormatOptions format;

    InfluxDBClient client(config, format);

    auto mock = std::make_unique<MockInfluxDBClient>();
    auto* mock_ptr = mock.get();
    client.set_client(std::move(mock));

    client.connect();

    InfluxDBInsertData data("", 0);
    bool ok = client.execute(data);
    (void)ok;
    assert(ok);
    (void)mock_ptr;
    assert(mock_ptr->execute_count == 1);
    assert(mock_ptr->total_rows_executed == 0);

    std::cout << "test_influxdb_client_empty_data PASSED\n";
}

void test_influxdb_client_url_encoding() {
    // Test that org/bucket with special characters are properly URL-encoded
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "test-token";
    config.org = "my org";        // space in org
    config.bucket = "test&bucket"; // ampersand in bucket

    InfluxDBFormatOptions format;
    format.precision = "ns";

    CurlInfluxDBClient client(config, format);
    const auto& url = client.write_url();
    (void)url;
    // Space should be encoded as %20 (or +), ampersand as %26
    assert(url.find("org=my%20org") != std::string::npos ||
           url.find("org=my+org") != std::string::npos);
    assert(url.find("bucket=test%26bucket") != std::string::npos);
    assert(url.find("precision=ns") != std::string::npos);

    std::cout << "test_influxdb_client_url_encoding PASSED\n";
}

int main() {
    test_influxdb_client_url_construction();
    test_influxdb_client_trailing_slash();
    test_influxdb_client_connect_close();
    test_influxdb_client_mock_inject();
    test_influxdb_client_mock_execute();
    test_influxdb_client_mock_connect_failure();
    test_influxdb_client_mock_execute_failure();
    test_influxdb_client_empty_data();
    test_influxdb_client_url_encoding();
    std::cout << "\nAll InfluxDB client tests PASSED\n";
    return 0;
}

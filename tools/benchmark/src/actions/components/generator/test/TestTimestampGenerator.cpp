#include <iostream>
#include <cassert>
#include <chrono>
#include "TimestampGenerator.h"

void test_generate_single_timestamp() {
    TimestampGeneratorConfig config;
    config.start_timestamp = Timestamp{1000};
    config.timestamp_step = 10;
    config.timestamp_precision = "ms";

    TimestampGenerator generator(config);

    Timestamp ts = generator.generate();
    assert(ts == 1000);

    ts = generator.generate();
    assert(ts == 1010);

    std::cout << "test_generate_single_timestamp passed.\n";
}

void test_generate_multiple_timestamps() {
    TimestampGeneratorConfig config;
    config.start_timestamp = Timestamp{2000};
    config.timestamp_step = 5;
    config.timestamp_precision = "ms";

    TimestampGenerator generator(config);

    std::vector<Timestamp> timestamps = generator.generate(5);
    assert(timestamps.size() == 5);
    assert(timestamps[0] == 2000);
    assert(timestamps[1] == 2005);
    assert(timestamps[2] == 2010);
    assert(timestamps[3] == 2015);
    assert(timestamps[4] == 2020);

    std::cout << "test_generate_multiple_timestamps passed.\n";
}

void test_invalid_start_timestamp() {
    try {
        TimestampGeneratorConfig config;
        config.start_timestamp = std::string("invalid");
        config.timestamp_step = 10;
        config.timestamp_precision = "ms";

        TimestampGenerator generator(config);
        assert(false);
    } catch (const std::runtime_error& e) {
        std::cout << "test_invalid_start_timestamp passed.\n";
    }
}

void test_start_timestamp_now() {
    // Test for milliseconds precision
    {
        TimestampGeneratorConfig config;
        config.start_timestamp = std::string("now");
        config.timestamp_step = 10;
        config.timestamp_precision = "ms";

        auto now = std::chrono::system_clock::now();
        int64_t expected = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

        TimestampGenerator generator(config);
        Timestamp ts = generator.generate();

        assert(ts >= expected && ts <= expected + 1);
        std::cout << "test_start_timestamp_now (ms) passed.\n";
    }

    // Test for microseconds precision
    {
        TimestampGeneratorConfig config;
        config.start_timestamp = std::string("now");
        config.timestamp_step = 10;
        config.timestamp_precision = "us";

        auto now = std::chrono::system_clock::now();
        int64_t expected = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

        TimestampGenerator generator(config);
        Timestamp ts = generator.generate();

        assert(ts >= expected && ts < expected + 1'000);
        std::cout << "test_start_timestamp_now (us) passed.\n";
    }

    // Test for nanoseconds precision
    {
        TimestampGeneratorConfig config;
        config.start_timestamp = std::string("now");
        config.timestamp_step = 10;
        config.timestamp_precision = "ns";

        auto now = std::chrono::system_clock::now();
        int64_t expected = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

        TimestampGenerator generator(config);
        Timestamp ts = generator.generate();

        assert(ts >= expected && ts < expected + 1'000'000);
        std::cout << "test_start_timestamp_now (ns) passed.\n";
    }
}

int main() {
    test_generate_single_timestamp();
    test_generate_multiple_timestamps();
    test_invalid_start_timestamp();
    test_start_timestamp_now();

    std::cout << "All tests passed.\n";
    return 0;
}
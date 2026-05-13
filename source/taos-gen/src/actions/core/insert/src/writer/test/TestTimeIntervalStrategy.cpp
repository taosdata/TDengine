#include "TimeIntervalStrategy.hpp"
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <thread>

InsertDataConfig::TimeInterval create_test_config() {
    InsertDataConfig::TimeInterval config;
    config.enabled = true;
    config.interval_strategy = "fixed";
    config.wait_strategy = "sleep";
    config.fixed_interval.base_interval = 1000;
    config.dynamic_interval.min_interval = 100;
    config.dynamic_interval.max_interval = 5000;
    return config;
}

void test_timestamp_conversion() {
    auto config = create_test_config();

    // Test millisecond precision
    TimeIntervalStrategy strategy_ms(config, "ms");
    assert(strategy_ms.to_milliseconds(1000) == 1000);
    assert(strategy_ms.to_microseconds(1000) == 1000000);

    // Test microsecond precision
    TimeIntervalStrategy strategy_us(config, "us");
    assert(strategy_us.to_milliseconds(1000) == 1);
    assert(strategy_us.to_microseconds(1000) == 1000);

    // Test nanosecond precision
    TimeIntervalStrategy strategy_ns(config, "ns");
    assert(strategy_ns.to_milliseconds(1000000) == 1);
    assert(strategy_ns.to_microseconds(1000) == 1);

    // Test unknown precision throws
    bool thrown = false;
    try {
        TimeIntervalStrategy strategy_unknown(config, "sec");
        strategy_unknown.to_milliseconds(1000);
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    (void)thrown;
    assert(thrown);

    std::cout << "test_timestamp_conversion passed." << std::endl;
}

void test_fixed_interval() {
    auto config = create_test_config();
    TimeIntervalStrategy strategy(config, "ms");
    assert(strategy.fixed_interval_strategy() == 1000 * 1000);
    std::cout << "test_fixed_interval passed." << std::endl;
}

void test_clamp_interval() {
    auto config = create_test_config();
    TimeIntervalStrategy strategy(config, "ms");

    // Test minimum clamping
    assert(strategy.clamp_interval(50 * 1000) == 100 * 1000);

    // Test maximum clamping
    assert(strategy.clamp_interval(6000 * 1000) == 5000 * 1000);

    // Test within range
    assert(strategy.clamp_interval(1000 * 1000) == 1000 * 1000);

    // Test negative min/max (should not clamp)
    config.dynamic_interval.min_interval = -1;
    config.dynamic_interval.max_interval = -1;
    TimeIntervalStrategy strategy2(config, "ms");
    assert(strategy2.clamp_interval(10 * 1000) == 10 * 1000);
    assert(strategy2.clamp_interval(10000 * 1000) == 10000 * 1000);

    std::cout << "test_clamp_interval passed." << std::endl;
}

void test_wait_strategy() {
    auto config = create_test_config();
    TimeIntervalStrategy strategy(config, "ms");

    // Test 1: First write (should not wait)
    auto start_time = std::chrono::steady_clock::now();
    strategy.apply_wait_strategy(1000, 2000, 0, 0, true);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    (void)elapsed;
    assert(elapsed < 50); // Should return immediately

    // Test 2: Disabled config (should not wait)
    config.enabled = false;
    TimeIntervalStrategy disabled_strategy(config, "ms");
    start_time = std::chrono::steady_clock::now();
    disabled_strategy.apply_wait_strategy(1000, 2000, 0, 0, false);
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    assert(elapsed < 50); // Should return immediately

    // Test 3: Fixed interval with sleep strategy
    config.enabled = true;
    config.interval_strategy = "fixed";
    config.wait_strategy = "sleep";
    config.fixed_interval.base_interval = 500; // Use a smaller interval to speed up the test
    TimeIntervalStrategy sleep_strategy(config, "ms");

    start_time = std::chrono::steady_clock::now();
    sleep_strategy.apply_wait_strategy(2000, 3000, 1000, 1500, false);
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    assert(elapsed >= 450 && elapsed <= 550); // Allow 50ms error

    // Test 4: Fixed interval with busy wait strategy
    config.wait_strategy = "busy_wait";
    TimeIntervalStrategy busy_wait_strategy(config, "ms");

    start_time = std::chrono::steady_clock::now();
    busy_wait_strategy.apply_wait_strategy(2000, 3000, 1000, 1500, false);
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    assert(elapsed >= 450 && elapsed <= 550); // Allow 50ms error

    // Test 5: First-to-first strategy
    config.interval_strategy = "first_to_first";
    config.wait_strategy = "sleep";
    TimeIntervalStrategy first_to_first_strategy(config, "ms");

    start_time = std::chrono::steady_clock::now();
    first_to_first_strategy.apply_wait_strategy(2000, 3000, 1500, 2500, false);
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    assert(elapsed >= 450 && elapsed <= 550); // Should wait 500ms (2000-1500)

    // Test 6: Last-to-first strategy
    config.interval_strategy = "last_to_first";
    TimeIntervalStrategy last_to_first_strategy(config, "ms");

    start_time = std::chrono::steady_clock::now();
    last_to_first_strategy.apply_wait_strategy(3000, 4000, 1000, 2000, false);
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    assert(elapsed >= 950 && elapsed <= 1050); // Should wait 1000ms (3000-2000)

    // Test 7: Unknown strategy (should not wait)
    config.interval_strategy = "unknown";
    TimeIntervalStrategy unknown_strategy(config, "ms");
    start_time = std::chrono::steady_clock::now();
    unknown_strategy.apply_wait_strategy(1000, 2000, 0, 0, false);
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    assert(elapsed < 50);

    // Test 8: busy_wait with 0ms (should not wait)
    config.interval_strategy = "fixed";
    config.wait_strategy = "busy_wait";
    config.fixed_interval.base_interval = 0;
    TimeIntervalStrategy zero_wait_strategy(config, "ms");
    start_time = std::chrono::steady_clock::now();
    zero_wait_strategy.apply_wait_strategy(1000, 2000, 0, 0, false);
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    assert(elapsed < 50);

    std::cout << "test_wait_strategy passed." << std::endl;
}

void test_literal_strategy() {
    InsertDataConfig::TimeInterval config;
    config.enabled = true;
    config.interval_strategy = "literal";
    config.wait_strategy = "sleep";

    TimeIntervalStrategy strategy(config, "us");

    // Set current_start to "current time + 200ms"
    int64_t now_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    int64_t current_start = now_timestamp + 200000;

    auto start_time = std::chrono::steady_clock::now();
    strategy.apply_wait_strategy(current_start, 0, 0, 0, false);
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count();

    // The elapsed time should be approximately between 200 and 250 milliseconds
    std::cout << "in func test_literal_strategy elapsed: " << elapsed << " us" << std::endl;
    assert(elapsed >= 200000 && elapsed <= 250000);

    // Test is_literal_strategy
    assert(strategy.is_literal_strategy());
    config.enabled = false;
    TimeIntervalStrategy strategy2(config, "ms");
    assert(!strategy2.is_literal_strategy());

    std::cout << "test_literal_strategy passed." << std::endl;
}

void test_last_write_time() {
    auto config = create_test_config();
    TimeIntervalStrategy strategy(config, "ms");
    auto before = strategy.last_write_time();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    strategy.apply_wait_strategy(1000, 2000, 0, 0, false);
    auto after = strategy.last_write_time();
    (void)before;
    (void)after;
    assert(after > before);
    std::cout << "test_last_write_time passed." << std::endl;
}

void test_strategy_type() {
    auto config = create_test_config();
    config.interval_strategy = "fixed";
    TimeIntervalStrategy s1(config, "ms");
    assert(s1.strategy_type() == IntervalStrategyType::Fixed);
    config.interval_strategy = "first_to_first";
    TimeIntervalStrategy s2(config, "ms");
    assert(s2.strategy_type() == IntervalStrategyType::FirstToFirst);
    config.interval_strategy = "last_to_first";
    TimeIntervalStrategy s3(config, "ms");
    assert(s3.strategy_type() == IntervalStrategyType::LastToFirst);
    config.interval_strategy = "literal";
    TimeIntervalStrategy s4(config, "ms");
    assert(s4.strategy_type() == IntervalStrategyType::Literal);
    config.interval_strategy = "unknown";
    TimeIntervalStrategy s5(config, "ms");
    assert(s5.strategy_type() == IntervalStrategyType::Unknown);
    std::cout << "test_strategy_type passed." << std::endl;
}

int main() {
    test_timestamp_conversion();
    test_fixed_interval();
    test_clamp_interval();
    test_wait_strategy();
    test_literal_strategy();
    test_last_write_time();
    test_strategy_type();

    std::cout << "All TimeIntervalStrategy tests passed." << std::endl;
    return 0;
}
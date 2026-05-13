#include <iostream>
#include <cassert>
#include <stdexcept>
#include "TimestampCSVConfig.hpp"

void test_parse_time_offset_positive() {
    std::cout << "Running test_parse_time_offset_positive..." << std::endl;

    auto result = TimestampCSVConfig::OffsetConfig::parse_time_offset("+1y2M3d4h5m6s");
    auto [years, months, days, hours, minutes, seconds] = result;

    (void)years;
    (void)months;
    (void)days;
    (void)hours;
    (void)minutes;
    (void)seconds;

    assert(years == 1);
    assert(months == 2);
    assert(days == 3);
    assert(hours == 4);
    assert(minutes == 5);
    assert(seconds == 6);

    std::cout << "test_parse_time_offset_positive passed!" << std::endl;
}

void test_parse_time_offset_negative() {
    std::cout << "Running test_parse_time_offset_negative..." << std::endl;

    auto result = TimestampCSVConfig::OffsetConfig::parse_time_offset("-2y3M5d10h30m45s");
    auto [years, months, days, hours, minutes, seconds] = result;

    (void)years;
    (void)months;
    (void)days;
    (void)hours;
    (void)minutes;
    (void)seconds;

    assert(years == -2);
    assert(months == -3);
    assert(days == -5);
    assert(hours == -10);
    assert(minutes == -30);
    assert(seconds == -45);

    std::cout << "test_parse_time_offset_negative passed!" << std::endl;
}

void test_parse_time_offset_minutes_only() {
    std::cout << "Running test_parse_time_offset_minutes_only..." << std::endl;

    auto result = TimestampCSVConfig::OffsetConfig::parse_time_offset("+15m");
    auto [years, months, days, hours, minutes, seconds] = result;

    (void)years;
    (void)months;
    (void)days;
    (void)hours;
    (void)minutes;
    (void)seconds;

    assert(years == 0);
    assert(months == 0);
    assert(days == 0);
    assert(hours == 0);
    assert(minutes == 15);
    assert(seconds == 0);

    std::cout << "test_parse_time_offset_minutes_only passed!" << std::endl;
}

void test_parse_time_offset_months_uppercase() {
    std::cout << "Running test_parse_time_offset_months_uppercase..." << std::endl;

    auto result = TimestampCSVConfig::OffsetConfig::parse_time_offset("+6M");
    auto [years, months, days, hours, minutes, seconds] = result;

    (void)years;
    (void)months;
    (void)days;
    (void)hours;
    (void)minutes;
    (void)seconds;

    assert(years == 0);
    assert(months == 6);
    assert(days == 0);
    assert(hours == 0);
    assert(minutes == 0);
    assert(seconds == 0);

    std::cout << "test_parse_time_offset_months_uppercase passed!" << std::endl;
}

void test_parse_time_offset_mixed_units() {
    std::cout << "Running test_parse_time_offset_mixed_units..." << std::endl;

    auto result = TimestampCSVConfig::OffsetConfig::parse_time_offset("+1y6M15d12h30m45s");
    auto [years, months, days, hours, minutes, seconds] = result;

    (void)years;
    (void)months;
    (void)days;
    (void)hours;
    (void)minutes;
    (void)seconds;

    assert(years == 1);
    assert(months == 6);
    assert(days == 15);
    assert(hours == 12);
    assert(minutes == 30);
    assert(seconds == 45);

    std::cout << "test_parse_time_offset_mixed_units passed!" << std::endl;
}

void test_parse_time_offset_partial_units() {
    std::cout << "Running test_parse_time_offset_partial_units..." << std::endl;

    auto result = TimestampCSVConfig::OffsetConfig::parse_time_offset("+3d12h");
    auto [years, months, days, hours, minutes, seconds] = result;

    (void)years;
    (void)months;
    (void)days;
    (void)hours;
    (void)minutes;
    (void)seconds;

    assert(years == 0);
    assert(months == 0);
    assert(days == 3);
    assert(hours == 12);
    assert(minutes == 0);
    assert(seconds == 0);

    std::cout << "test_parse_time_offset_partial_units passed!" << std::endl;
}

void test_parse_time_offset_invalid_sign() {
    std::cout << "Running test_parse_time_offset_invalid_sign..." << std::endl;

    try {
        TimestampCSVConfig::OffsetConfig::parse_time_offset("1y2M3d");
        assert(false && "Should throw exception for missing sign");
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        assert(msg.find("Invalid sign character") != std::string::npos);
    }

    std::cout << "test_parse_time_offset_invalid_sign passed!" << std::endl;
}

void test_parse_time_offset_invalid_unit() {
    std::cout << "Running test_parse_time_offset_invalid_unit..." << std::endl;

    try {
        TimestampCSVConfig::OffsetConfig::parse_time_offset("+1x");
        assert(false && "Should throw exception for invalid unit");
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        assert(msg.find("Invalid time unit") != std::string::npos);
    }

    std::cout << "test_parse_time_offset_invalid_unit passed!" << std::endl;
}

void test_parse_time_offset_empty_string() {
    std::cout << "Running test_parse_time_offset_empty_string..." << std::endl;

    try {
        TimestampCSVConfig::OffsetConfig::parse_time_offset("");
        assert(false && "Should throw exception for empty string");
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        assert(msg.find("Empty offset string") != std::string::npos);
    }

    std::cout << "test_parse_time_offset_empty_string passed!" << std::endl;
}

void test_parse_time_offset_missing_number() {
    std::cout << "Running test_parse_time_offset_missing_number..." << std::endl;

    try {
        TimestampCSVConfig::OffsetConfig::parse_time_offset("+y");
        assert(false && "Should throw exception for missing number");
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        assert(msg.find("Missing number") != std::string::npos);
    }

    std::cout << "test_parse_time_offset_missing_number passed!" << std::endl;
}

void test_offset_config_relative() {
    std::cout << "Running test_offset_config_relative..." << std::endl;

    TimestampCSVConfig::OffsetConfig config("relative", std::string("+1y2M3d"), "ms");

    assert(config.offset_type == "relative");
    assert(config.parsed == true);

    auto [years, months, days, hours, minutes, seconds] = config.relative_offset;

    (void)years;
    (void)months;
    (void)days;
    (void)hours;
    (void)minutes;
    (void)seconds;

    assert(years == 1);
    assert(months == 2);
    assert(days == 3);
    assert(hours == 0);
    assert(minutes == 0);
    assert(seconds == 0);

    std::cout << "test_offset_config_relative passed!" << std::endl;
}

void test_timestamp_csv_config_basic() {
    std::cout << "Running test_timestamp_csv_config_basic..." << std::endl;

    TimestampCSVConfig config;
    (void)config;
    assert(config.enabled == false);
    assert(config.timestamp_index == 0);
    assert(!config.timestamp_precision.has_value());
    assert(!config.offset_config.has_value());

    std::cout << "test_timestamp_csv_config_basic passed!" << std::endl;
}

void test_timestamp_csv_config_with_offset() {
    std::cout << "Running test_timestamp_csv_config_with_offset..." << std::endl;

    TimestampCSVConfig config;
    config.enabled = true;
    config.timestamp_index = 1;
    config.timestamp_precision = "ms";
    config.offset_config = TimestampCSVConfig::OffsetConfig("relative", std::string("+5m30s"), "ms");

    assert(config.enabled == true);
    assert(config.timestamp_index == 1);
    assert(config.timestamp_precision.value() == "ms");
    assert(config.offset_config.has_value());
    assert(config.offset_config->offset_type == "relative");

    auto [years, months, days, hours, minutes, seconds] = config.offset_config->relative_offset;

    (void)years;
    (void)months;
    (void)days;
    (void)hours;
    (void)minutes;
    (void)seconds;

    assert(minutes == 5);
    assert(seconds == 30);

    std::cout << "test_timestamp_csv_config_with_offset passed!" << std::endl;
}

int main() {
    std::cout << "Running TimestampCSVConfig tests..." << std::endl;

    test_parse_time_offset_positive();
    test_parse_time_offset_negative();
    test_parse_time_offset_minutes_only();
    test_parse_time_offset_months_uppercase();
    test_parse_time_offset_mixed_units();
    test_parse_time_offset_partial_units();
    test_parse_time_offset_invalid_sign();
    test_parse_time_offset_invalid_unit();
    test_parse_time_offset_empty_string();
    test_parse_time_offset_missing_number();
    test_offset_config_relative();
    test_timestamp_csv_config_basic();
    test_timestamp_csv_config_with_offset();

    std::cout << "All TimestampCSVConfig tests passed!" << std::endl;
    return 0;
}

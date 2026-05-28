#include "LogUtils.hpp"
#include <cassert>
#include <iostream>
#include <fstream>
#include "FilesystemCompat.hpp"
#include <string>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>

bool log_file_contains(const std::string& log_file, const std::string& keyword) {
    std::ifstream fin(log_file);
    if (!fin.is_open()) return false;
    std::string line;
    while (std::getline(fin, line)) {
        if (line.find(keyword) != std::string::npos) return true;
    }
    return false;
}

void test_init_and_info_log() {
    std::string log_file = "testlog/test_info.log";
    if (fs::exists(log_file)) fs::remove(log_file);

    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::info("Hello Info Log");
    LogUtils::shutdown();
    assert(fs::exists(log_file));
    assert(log_file_contains(log_file, "Hello Info Log"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_init_and_info_log passed" << std::endl;
}

void test_debug_level_no_output() {
    std::string log_file = "testlog/test_debug.log";
    if (fs::exists(log_file)) fs::remove(log_file);
    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::debug("Debug message should not appear");
    LogUtils::info("Info message should appear");
    LogUtils::shutdown();
    assert(fs::exists(log_file));
    assert(!log_file_contains(log_file, "Debug message should not appear"));
    assert(log_file_contains(log_file, "Info message should appear"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_debug_level_no_output passed" << std::endl;
}

void test_warn_error_fatal_log() {
    std::string log_file = "testlog/test_warn_error_fatal.log";
    if (fs::exists(log_file)) fs::remove(log_file);
    LogUtils::init(LogUtils::Level::Debug, log_file, 1024 * 1024, 1);
    LogUtils::warn("Warn log");
    LogUtils::error("Error log");
    LogUtils::fatal("Fatal log");
    LogUtils::shutdown();
    assert(fs::exists(log_file));
    assert(log_file_contains(log_file, "Warn log"));
    assert(log_file_contains(log_file, "Error log"));
    assert(log_file_contains(log_file, "Fatal log"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_warn_error_fatal_log passed" << std::endl;
}

void test_log_file_directory_created() {
    std::string log_file = "testlog/subdir/test_create_dir.log";
    fs::path dir = fs::path(log_file).parent_path();
    if (fs::exists(dir)) fs::remove_all(dir);
    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::info("Check directory creation");
    LogUtils::shutdown();
    assert(fs::exists(dir));
    assert(fs::exists(log_file));
    assert(log_file_contains(log_file, "Check directory creation"));
    fs::remove(log_file);
    fs::remove_all("testlog");
    std::cout << "test_log_file_directory_created passed" << std::endl;
}

void test_set_level_runtime() {
    std::string log_file = "testlog/test_set_level.log";
    if (fs::exists(log_file)) fs::remove(log_file);

    LogUtils::init(LogUtils::Level::Warn, log_file, 1024 * 1024, 1);
    LogUtils::debug("Debug should not appear");
    LogUtils::info("Info should not appear");
    LogUtils::warn("Warn should appear");

    LogUtils::set_level(LogUtils::Level::Debug);
    LogUtils::debug("Debug should appear now");
    LogUtils::info("Info should appear now");

    LogUtils::shutdown();
    assert(fs::exists(log_file));
    assert(!log_file_contains(log_file, "Debug should not appear"));
    assert(!log_file_contains(log_file, "Info should not appear"));
    assert(log_file_contains(log_file, "Warn should appear"));
    assert(log_file_contains(log_file, "Debug should appear now"));
    assert(log_file_contains(log_file, "Info should appear now"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_set_level_runtime passed" << std::endl;
}


void test_fmt_info_log() {
    std::string log_file = "testlog/test_fmt_info.log";
    if (fs::exists(log_file)) fs::remove(log_file);

    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::info("Hello {} Log {}", "Fmt", 123);
    LogUtils::shutdown();
    assert(fs::exists(log_file));
    assert(log_file_contains(log_file, "Hello Fmt Log 123"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_fmt_info_log passed" << std::endl;
}

void test_fmt_debug_level_no_output() {
    std::string log_file = "testlog/test_fmt_debug.log";
    if (fs::exists(log_file)) fs::remove(log_file);
    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::debug("Debug {} should not appear {}", "fmt", 456);
    LogUtils::info("Info {} should appear {}", "fmt", 789);
    LogUtils::shutdown();
    assert(fs::exists(log_file));
    assert(!log_file_contains(log_file, "Debug fmt should not appear 456"));
    assert(log_file_contains(log_file, "Info fmt should appear 789"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_fmt_debug_level_no_output passed" << std::endl;
}

void test_fmt_warn_error_fatal_log() {
    std::string log_file = "testlog/test_fmt_warn_error_fatal.log";
    if (fs::exists(log_file)) fs::remove(log_file);
    LogUtils::init(LogUtils::Level::Debug, log_file, 1024 * 1024, 1);
    LogUtils::warn("Warn {} log", "fmt");
    LogUtils::error("Error {} log {}", "fmt", 1);
    LogUtils::fatal("Fatal {} log {}", "fmt", 2);
    LogUtils::shutdown();
    assert(fs::exists(log_file));
    assert(log_file_contains(log_file, "Warn fmt log"));
    assert(log_file_contains(log_file, "Error fmt log 1"));
    assert(log_file_contains(log_file, "Fatal fmt log 2"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_fmt_warn_error_fatal_log passed" << std::endl;
}

void test_fmt_set_level_runtime() {
    std::string log_file = "testlog/test_fmt_set_level.log";
    if (fs::exists(log_file)) fs::remove(log_file);

    LogUtils::init(LogUtils::Level::Warn, log_file, 1024 * 1024, 1);
    LogUtils::debug("Debug {} should not appear", "fmt");
    LogUtils::info("Info {} should not appear", "fmt");
    LogUtils::warn("Warn {} should appear", "fmt");

    LogUtils::set_level(LogUtils::Level::Debug);
    LogUtils::debug("Debug {} should appear now", "fmt");
    LogUtils::info("Info {} should appear now", "fmt");

    LogUtils::shutdown();
    assert(fs::exists(log_file));
    assert(!log_file_contains(log_file, "Debug fmt should not appear"));
    assert(!log_file_contains(log_file, "Info fmt should not appear"));
    assert(log_file_contains(log_file, "Warn fmt should appear"));
    assert(log_file_contains(log_file, "Debug fmt should appear now"));
    assert(log_file_contains(log_file, "Info fmt should appear now"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_fmt_set_level_runtime passed" << std::endl;
}

void test_logger_guard_basic() {
    std::string log_file = "testlog/test_logger_guard.log";
    if (fs::exists(log_file)) fs::remove(log_file);

    {
        LogUtils::LoggerGuard guard(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
        LogUtils::info("LoggerGuard info message");
        LogUtils::warn("LoggerGuard warn message");
    }

    assert(fs::exists(log_file));
    assert(log_file_contains(log_file, "LoggerGuard info message"));
    assert(log_file_contains(log_file, "LoggerGuard warn message"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_logger_guard_basic passed" << std::endl;
}

void test_logger_guard_set_level() {
    std::string log_file = "testlog/test_logger_guard_level.log";
    if (fs::exists(log_file)) fs::remove(log_file);

    {
        LogUtils::LoggerGuard guard(LogUtils::Level::Warn, log_file, 1024 * 1024, 1);
        LogUtils::info("Should not appear");
        LogUtils::warn("Should appear");
        guard.set_level(LogUtils::Level::Info);
        LogUtils::info("Should appear now");
    }

    assert(fs::exists(log_file));
    assert(!log_file_contains(log_file, "Should not appear"));
    assert(log_file_contains(log_file, "Should appear"));
    assert(log_file_contains(log_file, "Should appear now"));
    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_logger_guard_set_level passed" << std::endl;
}

void test_init_console_only() {
    LogUtils::init_console(LogUtils::Level::Info);

    // Just verify it doesn't crash and can log to console
    LogUtils::info("Console only message");
    LogUtils::warn("Console only warning");

    LogUtils::shutdown();
    std::cout << "test_init_console_only passed" << std::endl;
}

void test_init_console_only_no_file() {
    std::string log_file = "testlog/should_not_exist.log";
    if (fs::exists(log_file)) fs::remove(log_file);

    LogUtils::init_console(LogUtils::Level::Info);
    LogUtils::info("Console only, no file");
    LogUtils::flush();

    LogUtils::shutdown();

    // Log file should not be created
    assert(!fs::exists(log_file));
    std::cout << "test_init_console_only_no_file passed" << std::endl;
}

void test_reinit_logger() {
    std::string log_file1 = "testlog/test_reinit1.log";
    std::string log_file2 = "testlog/test_reinit2.log";
    if (fs::exists(log_file1)) fs::remove(log_file1);
    if (fs::exists(log_file2)) fs::remove(log_file2);

    // First initialization
    LogUtils::init_console(LogUtils::Level::Info);
    LogUtils::info("Console only message");

    // Re-initialize with file logging
    LogUtils::init(LogUtils::Level::Info, log_file1, 1024 * 1024, 1);
    LogUtils::info("First file message");

    // Re-initialize with different file
    LogUtils::init(LogUtils::Level::Info, log_file2, 1024 * 1024, 1);
    LogUtils::info("Second file message");

    LogUtils::shutdown();

    assert(fs::exists(log_file1));
    assert(fs::exists(log_file2));
    assert(log_file_contains(log_file1, "First file message"));
    assert(log_file_contains(log_file2, "Second file message"));

    fs::remove(log_file1);
    fs::remove(log_file2);
    fs::remove("testlog");
    std::cout << "test_reinit_logger passed" << std::endl;
}

void test_invalid_log_path() {
#if defined(_WIN32)
    // On Windows, use a path that is guaranteed to be invalid
    // (CON, NUL, etc. are reserved device names and cannot be used as directories)
    std::string log_file = "NUL\\invalid\\test.log";
#else
    std::string log_file = "/proc/invalid_path/test.log";
#endif

    try {
        LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
        assert(false && "Should throw exception for invalid path");
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        // Check for either "Invalid log file path" or "Failed to create log directory"
        bool valid_error = (msg.find("Invalid log file path") != std::string::npos) ||
                          (msg.find("Failed to create log directory") != std::string::npos);
        if (!valid_error) {
            std::cout << "Unexpected error message: " << msg << std::endl;
        }
        assert(valid_error);
        std::cout << "test_invalid_log_path passed" << std::endl;
    }
}

void test_create_log_directory() {
    std::string log_file = "testlog/deep/nested/dir/test.log";
    fs::path dir = fs::path(log_file).parent_path();
    if (fs::exists(dir)) fs::remove_all(dir);

    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::info("Test directory creation");
    LogUtils::shutdown();

    assert(fs::exists(dir));
    assert(fs::exists(log_file));
    assert(log_file_contains(log_file, "Test directory creation"));

    fs::remove_all("testlog");
    std::cout << "test_create_log_directory passed" << std::endl;
}

void test_console_only_debug_level() {
    LogUtils::init_console(LogUtils::Level::Debug);

    // Just verify it doesn't crash and can log at debug level
    LogUtils::debug("Debug message in console");
    LogUtils::info("Info message in console");

    LogUtils::shutdown();
    std::cout << "test_console_only_debug_level passed" << std::endl;
}

void test_console_only_then_file() {
    std::string log_file = "testlog/test_console_then_file.log";
    if (fs::exists(log_file)) fs::remove(log_file);

    // Start with console only
    LogUtils::init_console(LogUtils::Level::Info);
    LogUtils::info("Console only phase");

    // Switch to file logging
    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::info("File logging phase");

    LogUtils::shutdown();

    assert(fs::exists(log_file));
    assert(log_file_contains(log_file, "File logging phase"));
    // Console only message should not be in file
    assert(!log_file_contains(log_file, "Console only phase"));

    fs::remove(log_file);
    fs::remove("testlog");
    std::cout << "test_console_only_then_file passed" << std::endl;
}

void test_concurrent_logging_during_shutdown() {
    std::string log_file = "testlog/test_concurrent_shutdown.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);

    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);

    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;

    // Spawn threads that log continuously
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&stop, i]() {
            while (!stop.load(std::memory_order_relaxed)) {
                LogUtils::info("Thread {} logging", i);
                std::this_thread::yield();
            }
        });
    }

    // Let threads log for a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Cooperative shutdown: stop threads first, then shutdown logger
    stop.store(true, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }

    // Now safe to shutdown - no concurrent access
    LogUtils::shutdown();

    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_concurrent_logging_during_shutdown passed" << std::endl;
}

void test_logging_fallback_without_logger() {
    // Ensure no logger is active
    LogUtils::shutdown();

    // These should fall back to stdout/stderr without crashing
    LogUtils::debug("fallback debug {}", 1);
    LogUtils::info("fallback info {}", 2);
    LogUtils::warn("fallback warn {}", 3);
    LogUtils::error("fallback error {}", 4);
    LogUtils::fatal("fallback fatal {}", 5);

    // String versions
    LogUtils::debug("fallback debug str");
    LogUtils::info("fallback info str");
    LogUtils::warn("fallback warn str");
    LogUtils::error("fallback error str");
    LogUtils::fatal("fallback fatal str");

    std::cout << "test_logging_fallback_without_logger passed" << std::endl;
}

void test_repeated_shutdown_is_safe_after_reinit() {
    std::string log_file = "testlog/test_repeated_shutdown.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);

    LogUtils::init_console(LogUtils::Level::Info);
    LogUtils::info("phase1");

    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::info("phase2");

    LogUtils::shutdown();
    LogUtils::shutdown();

    assert(std::filesystem::exists(log_file));
    assert(log_file_contains(log_file, "phase2"));

    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_repeated_shutdown_is_safe_after_reinit passed" << std::endl;
}

int main() {
    test_init_and_info_log();
    test_debug_level_no_output();
    test_warn_error_fatal_log();
    test_log_file_directory_created();
    test_set_level_runtime();
    test_fmt_info_log();
    test_fmt_debug_level_no_output();
    test_fmt_warn_error_fatal_log();
    test_fmt_set_level_runtime();
    test_logger_guard_basic();
    test_logger_guard_set_level();

    test_init_console_only();
    test_init_console_only_no_file();
    test_reinit_logger();
    test_invalid_log_path();
    test_create_log_directory();
    test_console_only_debug_level();
    test_console_only_then_file();
    test_concurrent_logging_during_shutdown();
    test_logging_fallback_without_logger();
    test_repeated_shutdown_is_safe_after_reinit();

    std::cout << "All LogUtils tests passed!" << std::endl;
    return 0;
}
#include "LogUtils.hpp"
#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <memory>
#include <vector>
#include <filesystem>
#include <fstream>
#include <mutex>

namespace LogUtils {

std::shared_ptr<spdlog::logger> logger;
static std::mutex logger_lifecycle_mutex;

spdlog::level::level_enum to_spdlog_level(Level level) {
    switch (level) {
        case Level::Debug: return spdlog::level::debug;
        case Level::Info:  return spdlog::level::info;
        case Level::Warn:  return spdlog::level::warn;
        case Level::Error: return spdlog::level::err;
        case Level::Fatal: return spdlog::level::critical;
        default:           return spdlog::level::info;
    }
}

class LevelFullNameFormatter : public spdlog::custom_flag_formatter {
public:
    void format(const spdlog::details::log_msg& msg, const std::tm&, spdlog::memory_buf_t& dest) override {
        static const char* level_names[] = {
            "TRACE", "DEBUG", "INFO ", "WARN ", "ERROR", "FATAL", "OFF"
        };
        auto lvl = static_cast<size_t>(msg.level);
        if (lvl < sizeof(level_names) / sizeof(level_names[0])) {
            dest.append(level_names[lvl], level_names[lvl] + std::strlen(level_names[lvl]));
        } else {
            const char* info_str = "INFO ";
            dest.append(info_str, info_str + std::strlen(info_str));
        }
    }

    std::unique_ptr<spdlog::custom_flag_formatter> clone() const override {
        return std::make_unique<LevelFullNameFormatter>();
    }
};

void init_console(Level level) {
    std::lock_guard<std::mutex> lock(logger_lifecycle_mutex);

    if (logger) {
        logger->flush();
        spdlog::drop("taosgen_logger");
        logger.reset();
    }

    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

    std::vector<spdlog::sink_ptr> sinks{console_sink};
    logger = std::make_shared<spdlog::logger>("taosgen_logger", sinks.begin(), sinks.end());

    auto formatter = std::make_unique<spdlog::pattern_formatter>(
        "%Y-%m-%d %H:%M:%S.%f %t %X %v"
    );
    formatter->add_flag<LevelFullNameFormatter>('X');

    logger->set_formatter(std::move(formatter));
    logger->set_level(to_spdlog_level(level));
    logger->flush_on(spdlog::level::info);
    spdlog::register_logger(logger);
    spdlog::set_default_logger(logger);
}

void init(Level level, const std::string& log_file, size_t max_file_size, size_t max_files) {
    std::lock_guard<std::mutex> lock(logger_lifecycle_mutex);

    std::filesystem::path log_path(log_file);
    std::filesystem::path parent_dir = log_path.parent_path();

    if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
        try {
            std::filesystem::create_directories(parent_dir);
        } catch (const std::filesystem::filesystem_error& e) {
            throw std::runtime_error("Failed to create log directory '" + parent_dir.string() + "': " + e.what());
        }
    }

    // Validate log file path
    try {
        std::ofstream test_file(log_file, std::ios::app);
        if (!test_file.is_open()) {
            throw std::runtime_error("Cannot open log file: " + log_file);
        }
        test_file.close();
    } catch (const std::exception& e) {
        throw std::runtime_error("Invalid log file path '" + log_file + "': " + e.what());
    }

    // Shutdown existing logger if any
    if (logger) {
        logger->flush();
        spdlog::drop("taosgen_logger");
        logger.reset();
    }

    if (!spdlog::thread_pool()) {
        spdlog::init_thread_pool(8192, 1);
    }

    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(log_file, max_file_size, max_files);

    std::vector<spdlog::sink_ptr> sinks{console_sink, file_sink};
    logger = std::make_shared<spdlog::async_logger>(
        "taosgen_logger", sinks.begin(), sinks.end(),
        spdlog::thread_pool(), spdlog::async_overflow_policy::block);

    auto formatter = std::make_unique<spdlog::pattern_formatter>(
        "%Y-%m-%d %H:%M:%S.%f %t %X %v"
    );
    formatter->add_flag<LevelFullNameFormatter>('X');

    logger->set_formatter(std::move(formatter));
    logger->set_level(to_spdlog_level(level));
    logger->flush_on(spdlog::level::info);
    spdlog::register_logger(logger);
    spdlog::set_default_logger(logger);
}

void shutdown() {
    std::lock_guard<std::mutex> lock(logger_lifecycle_mutex);

    if (logger) {
        logger->flush();
        spdlog::drop("taosgen_logger");
        logger.reset();
    }
    spdlog::shutdown();
}

void set_level(Level level) {
    if (logger) logger->set_level(to_spdlog_level(level));
}

void flush() {
    if (logger) logger->flush();
}

void debug(const std::string& msg) {
    if (logger) {
        logger->debug(msg);
    } else {
        std::cout << "[DEBUG] " << msg << std::endl;
    }
}

void info(const std::string& msg) {
    if (logger) {
        logger->info(msg);
    } else {
        std::cout << "[INFO] " << msg << std::endl;
    }
}

void warn(const std::string& msg) {
    if (logger) {
        logger->warn(msg);
    } else {
        std::cout << "[WARN] " << msg << std::endl;
    }
}

void error(const std::string& msg) {
    if (logger) {
        logger->error(msg);
    } else {
        std::cerr << "[ERROR] " << msg << std::endl;
    }
}

void fatal(const std::string& msg) {
    if (logger) {
        logger->critical(msg);
    } else {
        std::cerr << "[FATAL] " << msg << std::endl;
    }
}

}
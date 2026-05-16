#pragma once

#include <string>

namespace ProcessUtils {
    double get_memory_usage_mb();
    std::string get_memory_usage_human_readable();
    double get_cpu_usage_percent();
    int get_thread_count();
    double get_system_free_memory_gb();
    std::string get_exe_directory();
}
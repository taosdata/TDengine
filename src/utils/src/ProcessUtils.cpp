#include "ProcessUtils.hpp"
#include <fstream>
#include <string>
#include <sstream>
#include <iomanip>
#include <atomic>
#include <mutex>
#include <vector>
#include <thread>

#if defined(_WIN32) || defined(_WIN64)
#include <Windows.h>
#include <tlhelp32.h>
#include <psapi.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#include <mach-o/dyld.h>
#include <unistd.h>
#include <limits.h>
#else
#include <unistd.h>
#include <limits.h>
#endif

namespace ProcessUtils {

double get_memory_usage_mb() {
#ifdef __linux__
    std::ifstream status("/proc/self/status");
    if (!status) return -1.0;

    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            std::istringstream iss(line);
            std::string key;
            long value = 0;
            iss >> key >> value;
            return value / 1024.0;
        }
    }
    return -1.0;
#elif defined(__APPLE__)
    mach_task_basic_info info;
    mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &infoCount) != KERN_SUCCESS)
        return -1.0;
    return static_cast<double>(info.resident_size) / (1024.0 * 1024.0);
#elif defined(_WIN32)
    PROCESS_MEMORY_COUNTERS pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
        return static_cast<double>(pmc.WorkingSetSize) / (1024.0 * 1024.0);
    }
    return -1.0;
#else
    return -1.0;
#endif
}

std::string get_memory_usage_human_readable() {
    double mem_mb = get_memory_usage_mb();
    if (mem_mb < 0) return "Unknown";
    std::ostringstream mem_usage_ss;
    mem_usage_ss << std::fixed << std::setprecision(2);
    std::string unit;
    double value;
    if (mem_mb < 1024.0) {
        value = mem_mb;
        unit = "MB";
    } else {
        value = mem_mb / 1024.0;
        unit = "GB";
    }
    mem_usage_ss << std::setw(6) << value << " " << unit;
    return mem_usage_ss.str();
}

int get_thread_count() {
#ifdef __linux__
    std::ifstream status("/proc/self/status");
    if (!status) return -1;

    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("Threads:", 0) == 0) {
            std::istringstream iss(line);
            std::string key;
            int value = 0;
            iss >> key >> value;
            return value;
        }
    }
    return -1;
#elif defined(__APPLE__)
    thread_act_array_t thread_list;
    mach_msg_type_number_t thread_count = 0;
    if (task_threads(mach_task_self(), &thread_list, &thread_count) != KERN_SUCCESS)
        return -1;

    for (mach_msg_type_number_t i = 0; i < thread_count; ++i)
        mach_port_deallocate(mach_task_self(), thread_list[i]);
    vm_deallocate(mach_task_self(), (vm_address_t)thread_list, thread_count * sizeof(thread_act_t));
    return static_cast<int>(thread_count);
#elif defined(_WIN32)
    DWORD process_id = GetCurrentProcessId();
    HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
    if (snapshot == INVALID_HANDLE_VALUE) return -1;

    THREADENTRY32 te;
    te.dwSize = sizeof(THREADENTRY32);
    int count = 0;

    if (Thread32First(snapshot, &te)) {
        do {
            if (te.th32OwnerProcessID == process_id) {
                count++;
            }
        } while (Thread32Next(snapshot, &te));
    }
    CloseHandle(snapshot);
    return count;
#else
    return -1;
#endif
}

double get_cpu_usage_percent() {
    static std::mutex mtx;
    static std::chrono::steady_clock::time_point prev_time = std::chrono::steady_clock::now();

#ifdef __linux__
    static long prev_proc = 0;
    // Get current process CPU time
    long proc = 0;
    std::ifstream self_stat("/proc/self/stat");
    if (!self_stat) return -1.0;

    std::string self_line;
    std::getline(self_stat, self_line);
    std::istringstream self_iss(self_line);
    std::vector<std::string> fields;
    std::string field;

    while (self_iss >> field) {
        fields.push_back(field);
    }

    if (fields.size() > 21) {
        try {
            long utime = std::stol(fields[13]);  // utime at position 14
            long stime = std::stol(fields[14]);  // stime at position 15
            proc = utime + stime;
        } catch (...) {
            return -1.0;
        }
    } else {
        return -1.0;
    }

    // Get current time
    auto now = std::chrono::steady_clock::now();

    // Calculate CPU usage
    std::lock_guard<std::mutex> lock(mtx);
    double percent = 0.0;

    if (prev_proc != 0) {
        // Calculate time interval (seconds)
        double interval = std::chrono::duration<double>(now - prev_time).count();

        if (interval > 0) {
            // Get system clock tick frequency (HZ)
            static long hz = sysconf(_SC_CLK_TCK);
            if (hz <= 0) hz = 100;  // Default value

            // Calculate CPU time delta (seconds)
            double proc_delta = (proc - prev_proc) / static_cast<double>(hz);

            // Calculate CPU usage (to match top command)
            percent = (proc_delta / interval) * 100.0;
        }
    }

    // Update state
    prev_proc = proc;
    prev_time = now;

    return percent;

#elif defined(__APPLE__)
    static uint64_t prev_total_usec = 0;

    task_thread_times_info_data_t info;
    mach_msg_type_number_t count = TASK_THREAD_TIMES_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_THREAD_TIMES_INFO, (task_info_t)&info, &count) != KERN_SUCCESS)
        return -1.0;

    uint64_t total_usec = info.user_time.seconds * 1000000ULL + info.user_time.microseconds +
                          info.system_time.seconds * 1000000ULL + info.system_time.microseconds;

    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mtx);
    double percent = 0.0;

    if (prev_total_usec != 0) {
        double interval = std::chrono::duration<double>(now - prev_time).count();
        double cpu_delta = (total_usec - prev_total_usec) / 1e6; // seconds
        if (interval > 0) {
            percent = (cpu_delta / interval) * 100.0;
        }
    }
    prev_total_usec = total_usec;
    prev_time = now;
    return percent;
#elif defined(_WIN32)
    static ULARGE_INTEGER prev_proc_total_time = {0};

    FILETIME proc_create, proc_exit, proc_kernel, proc_user;
    if (!GetProcessTimes(GetCurrentProcess(), &proc_create, &proc_exit, &proc_kernel, &proc_user))
        return -1.0;

    ULARGE_INTEGER proc_kernel_time, proc_user_time;
    proc_kernel_time.LowPart = proc_kernel.dwLowDateTime;
    proc_kernel_time.HighPart = proc_kernel.dwHighDateTime;
    proc_user_time.LowPart = proc_user.dwLowDateTime;
    proc_user_time.HighPart = proc_user.dwHighDateTime;

    ULONGLONG proc_total = proc_kernel_time.QuadPart + proc_user_time.QuadPart; // 100ns units

    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mtx);
    double percent = 0.0;

    if (prev_proc_total_time.QuadPart != 0) {
        double interval = std::chrono::duration<double>(now - prev_time).count();
        if (interval > 0) {
            double proc_delta_seconds = static_cast<double>(proc_total - prev_proc_total_time.QuadPart) / 1e7; // 100ns -> seconds
            percent = (proc_delta_seconds / interval) * 100.0;
        }
    }

    prev_proc_total_time.QuadPart = proc_total;
    prev_time = now;

    return percent;
#else
    return -1.0;
#endif
}

double get_system_free_memory_gb() {
#ifdef __linux__
    std::ifstream meminfo("/proc/meminfo");
    if (!meminfo) return -1.0;

    std::string line;
    long mem_available_kb = -1;
    while (std::getline(meminfo, line)) {
        if (line.rfind("MemAvailable:", 0) == 0) {
            std::istringstream iss(line);
            std::string key;
            iss >> key >> mem_available_kb;
            break;
        }
    }

    if (mem_available_kb != -1) {
        return static_cast<double>(mem_available_kb) / (1024.0 * 1024.0);
    }
    return -1.0;

#elif defined(__APPLE__)
    mach_port_t host_port = mach_host_self();
    mach_msg_type_number_t host_size = sizeof(vm_statistics64_data_t) / sizeof(integer_t);
    vm_size_t pagesize;
    vm_statistics64_data_t vm_stat;

    if (host_page_size(host_port, &pagesize) != KERN_SUCCESS) {
        return -1.0;
    }

    if (host_statistics64(host_port, HOST_VM_INFO64, (host_info64_t)&vm_stat, &host_size) != KERN_SUCCESS) {
        return -1.0;
    }

    // Available memory is generally considered free + inactive pages.
    double available_bytes = static_cast<double>(vm_stat.free_count + vm_stat.inactive_count) * static_cast<double>(pagesize);
    return available_bytes / (1024.0 * 1024.0 * 1024.0);

#elif defined(_WIN32)
    MEMORYSTATUSEX mem_status;
    mem_status.dwLength = sizeof(mem_status);
    if (GlobalMemoryStatusEx(&mem_status)) {
        // mem_status.ullAvailPhys is available physical memory
        return static_cast<double>(mem_status.ullAvailPhys) / (1024.0 * 1024.0 * 1024.0);
    }
    return -1.0;
#else
    return -1.0; // Not implemented for this platform
#endif
}

std::string get_exe_directory() {
#if defined(_WIN32) || defined(_WIN64)
    std::vector<wchar_t> wbuf(1024);
    DWORD len = 0;
    while (true) {
        len = GetModuleFileNameW(nullptr, wbuf.data(), static_cast<DWORD>(wbuf.size()));
        if (len == 0) {
            return "";
        }
        if (len < wbuf.size()) {
            break;
        }

        wbuf.resize(wbuf.size() * 2);
        if (wbuf.size() > (1 << 20)) {
            return "";
        }
    }
    std::wstring wpath(wbuf.data(), len);
    size_t pos = wpath.find_last_of(L"\\/");
    std::wstring wdir = (pos != std::wstring::npos) ? wpath.substr(0, pos + 1) : std::wstring(L".\\");

    int need = WideCharToMultiByte(CP_UTF8, 0, wdir.c_str(), static_cast<int>(wdir.size()), nullptr, 0, nullptr, nullptr);
    if (need <= 0) return "";
    std::string dir(need, '\0');
    WideCharToMultiByte(CP_UTF8, 0, wdir.c_str(), static_cast<int>(wdir.size()), dir.data(), need, nullptr, nullptr);
    return dir;
#elif defined(__APPLE__)
    uint32_t size = PATH_MAX;
    std::vector<char> buf(size);
    if (_NSGetExecutablePath(buf.data(), &size) != 0) {
        buf.resize(size);
        if (_NSGetExecutablePath(buf.data(), &size) != 0) {
            return "";
        }
    }
    std::string path(buf.data());
    size_t pos = path.find_last_of('/');
    if (pos != std::string::npos) {
        path.erase(pos + 1);
    } else {
        path.push_back('/');
    }
    return path;
#else
    std::vector<char> buf(PATH_MAX);
    while (true) {
        ssize_t count = readlink("/proc/self/exe", buf.data(), buf.size());
        if (count == -1) {
            return "";
        }
        if (static_cast<size_t>(count) < buf.size()) {
            std::string path(buf.data(), buf.data() + count);
            size_t pos = path.find_last_of('/');
            if (pos != std::string::npos) {
                path.erase(pos + 1);
            } else {
                path.push_back('/');
            }
            return path;
        }
        buf.resize(buf.size() * 2);
        if (buf.size() > (1 << 20)) {
            return "";
        }
    }
#endif
}

} // namespace ProcessUtils
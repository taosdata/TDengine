#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

#if !defined(_WIN32)
#include <csignal>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

static std::string find_taosgen_bin() {
    if (const char* env = std::getenv("TAOSGEN_BIN")) {
        if (env[0] != '\0' && fs::exists(env)) {
            return fs::absolute(env).string();
        }
    }

    std::vector<std::string> candidates = {
        "./src/taosgen",
        "../src/taosgen",
        "./taosgen",
        "src/taosgen",
        "../build/src/taosgen",
#if defined(_WIN32)
        "./src/Debug/taosgen.exe",
        "./src/Release/taosgen.exe",
        "../src/Debug/taosgen.exe",
        "../src/Release/taosgen.exe",
        "src/Debug/taosgen.exe",
        "src/Release/taosgen.exe",
#endif
    };

    for (const auto& c : candidates) {
        if (fs::exists(c)) {
            return fs::absolute(fs::path(c)).string();
        }
    }

    return {};
}

static std::string find_project_root() {
    if (const char* env = std::getenv("TSGEN_ROOT")) {
        if (env[0] != '\0' && fs::exists(fs::path(env) / "src/benchmark.cpp")) {
            return env;
        }
    }

    fs::path p = fs::current_path();
    for (int i = 0; i < 6; ++i) {
        if (fs::exists(p / "src/benchmark.cpp")) {
            return p.string();
        }
        if (p.has_parent_path()) {
            p = p.parent_path();
        } else {
            break;
        }
    }
    return {};
}

static std::string shell_cd(const std::string& dir) {
#if defined(_WIN32)
    return "cd /d \"" + dir + "\" && ";
#else
    return "cd \"" + dir + "\" && ";
#endif
}

static fs::path find_build_dir_from_bin(const fs::path& bin_path) {
#if defined(_MSC_VER)
    return bin_path.parent_path().parent_path().parent_path();
#else
    return bin_path.parent_path().parent_path();
#endif
}

static int run_cmd(const std::string& cmd) {
    std::cout << "[RUN] " << cmd << std::endl;
    return std::system(cmd.c_str());
}

void test_help_command() {
    const auto bin = find_taosgen_bin();
    assert(!bin.empty() && "taosgen binary not found; set TAOSGEN_BIN");

    const std::string cmd = "\"" + bin + "\" --help";
    int rc = run_cmd(cmd);
    (void)rc;
    assert(rc == 0 && "taosgen --help should exit 0");
    std::cout << "test_help_command passed\n";
}

void test_config_command() {
    const auto bin = find_taosgen_bin();
    assert(!bin.empty() && "taosgen binary not found; set TAOSGEN_BIN");

    const auto root = find_project_root();
    assert(!root.empty() && "project root not found; set TSGEN_ROOT");

    const fs::path bin_path = fs::path(bin);
    const fs::path build_dir = find_build_dir_from_bin(bin_path);

    const std::string cfg = (fs::path(root) / "conf/tdengine-csv.yaml").string();
    const std::string cmd = shell_cd(build_dir.string()) + "\"" + bin + "\" -v -c \"" + cfg + "\"";
    int rc = run_cmd(cmd);
    (void)rc;
    assert(rc == 0 && "taosgen -c <root>/conf/tdengine-csv.yaml should exit 0");
    std::cout << "test_config_command passed\n";
}

void test_unknown_argument() {
    const auto bin = find_taosgen_bin();
    assert(!bin.empty() && "taosgen binary not found; set TAOSGEN_BIN");

    const std::string cmd = "\"" + bin + "\" --unknown-arg";
    int rc = run_cmd(cmd);
    (void)rc;
    assert(rc != 0 && "taosgen with unknown args should exit non-zero");
    std::cout << "test_unknown_argument passed\n";
}

void test_sigterm_handling() {
    const auto bin = find_taosgen_bin();
    assert(!bin.empty() && "taosgen binary not found; set TAOSGEN_BIN");

#if defined(_WIN32)
    std::cout << "test_sigterm_handling skipped on Windows\n";
#else
    const auto root = find_project_root();
    assert(!root.empty() && "project root not found; set TSGEN_ROOT");

    const fs::path bin_path = fs::path(bin);
    const fs::path build_dir = find_build_dir_from_bin(bin_path);

    pid_t pid = fork();
    if (pid == 0) {
        execl(bin.c_str(), bin.c_str(), static_cast<char*>(nullptr));
        _exit(127);
    }

    assert(pid > 0 && "fork failed");
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    int kill_rc = kill(pid, SIGTERM);
    (void)kill_rc;
    assert(kill_rc == 0 && "Failed to send SIGTERM");

    int status = 0;
    waitpid(pid, &status, 0);

    if (WIFSIGNALED(status)) {
        int sig = WTERMSIG(status);
        (void)sig;
        assert(sig == SIGTERM || sig == SIGABRT || sig == SIGSEGV);
    } else if (WIFEXITED(status)) {
        assert(WEXITSTATUS(status) != 0 && "taosgen should exit non-zero on SIGTERM");
    } else {
        assert(false && "Unexpected child status");
    }

    std::cout << "test_sigterm_handling passed\n";
#endif
}

int main() {
    test_help_command();
    test_config_command();
    test_unknown_argument();
    test_sigterm_handling();
    std::cout << "All Benchmark command tests passed.\n";
    return 0;
}
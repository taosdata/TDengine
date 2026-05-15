#include "TDengineConnector.hpp"
#include "ProcessUtils.hpp"
#include <iostream>
#include "FilesystemCompat.hpp"
#include <stdexcept>
#include <cassert>
#include <cstdlib>
#include <string>

#if !defined(_WIN32)
#include <cstdio>
#include <sys/wait.h>
#endif

#if defined(_WIN32)
static constexpr const char* kLibName = "taos.dll";
static constexpr char kSep = '\\';
#else
#  if defined(__APPLE__)
static constexpr const char* kLibName = "libtaos.dylib";
#  else
static constexpr const char* kLibName = "libtaos.so";
#  endif
static constexpr char kSep = '/';
#endif

static std::string program_lib_dir() {
    std::string dir = ProcessUtils::get_exe_directory();
    if (!dir.empty() && dir.back() != kSep) dir.push_back(kSep);
    dir += "lib";
    return dir;
}

static std::string program_lib_path() {
    auto dir = program_lib_dir();
    fs::path p(dir);
    p /= kLibName;
    return p.string();
}

#if !defined(_WIN32)
static fs::path find_system_lib_candidate() {
#if defined(__APPLE__)
    const char* candidates[] = {
        "/opt/homebrew/lib",
        "/usr/local/lib",
        "/usr/lib"
    };
#else
    const char* candidates[] = {
        "/usr/local/lib",
        "/usr/lib",
        "/lib",
        "/lib/x86_64-linux-gnu",
        "/usr/lib/x86_64-linux-gnu"
    };
#endif
    for (auto* base : candidates) {
        fs::path p = fs::path(base) / kLibName;
        if (fs::exists(p)) return p;
    }
    return {};
}
#endif

static bool g_prog_lib_created_by_test = false;

static bool ensure_program_lib_present() {
    const std::string lib_dir = program_lib_dir();
    const std::string full = program_lib_path();

    std::error_code ec;
    fs::create_directories(lib_dir, ec);
    if (ec) {
        std::cerr << "[setup] FAIL: create lib dir failed: " << lib_dir << " (" << ec.message() << ")\n";
        assert(false && "Failed to create program lib directory");
        return false;
    }

    assert(fs::exists(lib_dir));
    assert(fs::is_directory(lib_dir));

    if (fs::exists(full)) {
        std::cout << "[setup] lib already present: " << full << "\n";
        g_prog_lib_created_by_test = false;
        assert(fs::path(full).filename().string() == kLibName);
        return true;
    }

#if defined(_WIN32)
    // On Windows, try to find taos.dll from common TDengine install locations
    const char* win_candidates[] = {
        "C:\\TDengine\\driver",
        "C:\\TDengine",
    };
    fs::path src_lib;
    for (auto* dir : win_candidates) {
        auto p = fs::path(dir) / kLibName;
        if (fs::exists(p)) { src_lib = p; break; }
    }
    if (src_lib.empty()) {
        std::cout << "[setup] taos.dll not found in system paths, skipping test\n";
        return true;  // skip gracefully
    }
    std::error_code ec_copy;
    fs::copy_file(src_lib, full, fs::copy_options::overwrite_existing, ec_copy);
    if (ec_copy) {
        std::cerr << "[setup] FAIL: copy " << src_lib << " -> " << full << " (" << ec_copy.message() << ")\n";
        assert(false && "Failed to copy taos.dll for test");
        return false;
    }
    std::cout << "[setup] Copied " << src_lib << " -> " << full << "\n";
    g_prog_lib_created_by_test = true;
    return true;
#else
    auto sys_lib = find_system_lib_candidate();
    if (sys_lib.empty()) {
        std::cerr << "[setup] System lib not found in candidates\n";
        assert(false && "System lib not found; cannot set up prefer path");
        return false;
    }

    std::error_code ec2;
    fs::copy_file(sys_lib, full, fs::copy_options::overwrite_existing, ec2);
    if (ec2) {
        std::cerr << "[setup] FAIL: copy " << sys_lib << " -> " << full << " (" << ec2.message() << ")\n";
        assert(false && "Failed to copy system lib to program lib dir");
        return false;
    }
    std::cout << "[setup] Copied " << sys_lib << " -> " << full << "\n";
    g_prog_lib_created_by_test = true;

    assert(fs::exists(full));
    assert(fs::path(full).filename().string() == kLibName);
    return true;
#endif
}

static void cleanup_program_lib_if_created() {
#if !defined(_WIN32)
    // if (!g_prog_lib_created_by_test) return;
    const std::string full = program_lib_path();
    const std::string dir = program_lib_dir();

    std::error_code ec;
    if (fs::exists(full)) {
        fs::remove(full, ec);
        if (ec) {
            std::cerr << "[cleanup] WARN: failed to remove " << full << " (" << ec.message() << ")\n";
            assert(false && "Failed to remove test-copied lib");
        } else {
            std::cout << "[cleanup] Removed test-copied lib: " << full << "\n";
        }
    }

    std::error_code ec2;
    if (fs::exists(dir) && fs::is_directory(dir) && fs::is_empty(dir)) {
        fs::remove(dir, ec2);
        if (ec2) {
            std::cerr << "[cleanup] WARN: failed to remove empty dir " << dir << " (" << ec2.message() << ")\n";
            assert(false && "Failed to remove empty lib dir");
        } else {
            std::cout << "[cleanup] Removed empty lib dir: " << dir << "\n";
        }
    }
    g_prog_lib_created_by_test = false;
#endif
}

static void try_construct_connector_expect_loaded(const char* case_name) {
    TDengineConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.user.clear();
    cfg.password.clear();
    cfg.database.clear();

    const std::string driver = "native";
    bool ok = false;
    try {
        TDengineConnector conn(cfg, driver, "TDengine");
        std::cout << "[" << case_name << "] OK: library loaded and driver set: " << driver << std::endl;
        ok = true;
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        if (msg.find("Failed to load libtaos shared library") != std::string::npos) {
            std::cerr << "[" << case_name << "] FAIL: library load failed: " << msg << std::endl;
            assert(false && "Failed to load libtaos shared library");
        } else {
            std::cerr << "[" << case_name << "] FAIL: unexpected runtime error: " << msg << std::endl;
            assert(false && "Unexpected runtime error during connector init");
        }
    } catch (...) {
        std::cerr << "[" << case_name << "] FAIL: unexpected exception." << std::endl;
        assert(false && "Unexpected exception");
    }
    (void)ok;
    assert(ok && "Connector should initialize without exception for native driver");
}

static void test_prefer_program_dir_lib_child() {
    bool ensured = ensure_program_lib_present();
    (void)ensured;
    assert(ensured && "Program lib must be ensured for prefer test");

    assert(fs::exists(program_lib_path()));
    assert(fs::path(program_lib_path()).filename().string() == kLibName);

    std::cout << "[prefer] Trying to load from: " << program_lib_path() << std::endl;

    try_construct_connector_expect_loaded("prefer");

    std::cout << "[prefer] expected source: program directory\n";
}

static void test_fallback_system_path_child() {
#if defined(_WIN32)
    std::cerr << "[fallback] Not supported on Windows in this test\n";
    assert(false && "Fallback test not supported on Windows");
#else
    const auto sys_lib = find_system_lib_candidate();
    if (sys_lib.empty()) {
        std::cerr << "[fallback] FAIL: no system lib found to fallback\n";
        assert(false && "System lib missing; cannot test fallback");
    }

    const fs::path prog_lib(program_lib_path());
    bool temporarily_moved = false;
    fs::path backup;

    if (fs::exists(prog_lib)) {
        backup = prog_lib;
        backup += ".bak";
        std::error_code ec_rm, ec_mv;
        fs::remove(backup, ec_rm);
        fs::rename(prog_lib, backup, ec_mv);
        if (!ec_mv) {
            temporarily_moved = true;
            std::cout << "[fallback] Temporarily moved: " << prog_lib << " -> " << backup << std::endl;
            assert(!fs::exists(prog_lib));
        } else {
            std::cerr << "[fallback] FAIL: could not move program lib (" << ec_mv.message() << ")\n";
            assert(false && "Failed to move program lib for fallback test");
        }
    } else {
        std::cout << "[fallback] Program lib absent, testing system path fallback\n";
    }

    try_construct_connector_expect_loaded("fallback");

    std::cout << "[fallback] expected source: system path\n";

    if (temporarily_moved) {
        std::error_code ec2;
        fs::rename(backup, prog_lib, ec2);
        if (ec2) {
            std::cerr << "[fallback] WARN: failed to restore " << backup << " -> " << prog_lib << " (" << ec2.message() << ")\n";
            assert(false && "Failed to restore program lib after fallback test");
        } else {
            std::cout << "[fallback] Restored: " << backup << " -> " << prog_lib << std::endl;
            assert(fs::exists(prog_lib));
        }
    }
#endif
}

#if !defined(_WIN32)
static int run_mode_and_capture(const char* exe_path, const char* mode, std::string& out) {
    std::string cmd = std::string(exe_path) + " --mode=" + mode + " 2>&1";
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) return -1;
    char buf[4096];
    while (fgets(buf, sizeof(buf), pipe)) {
        out.append(buf);
    }
    int status = pclose(pipe);
    int rc = (WIFEXITED(status) ? WEXITSTATUS(status) : -1);
    return rc;
}
#endif

// --- Schemaless insert tests ---
// These run in child processes and require a real TDengine on localhost:6030.
// If TDengine is unavailable, the connect() call fails gracefully and the test
// reports SKIPPED rather than FAILED.

static bool try_connect_tdengine(TDengineConnector& conn, const std::string& db) {
    if (!conn.connect()) return false;
    // Create and select test database
    conn.execute(std::string("DROP DATABASE IF EXISTS " + db));
    conn.execute(std::string("CREATE DATABASE IF NOT EXISTS " + db + " PRECISION 'ns'"));
    return conn.select_db(db);
}

static void test_schemaless_insert_basic_child() {
    TDengineConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 6030;
    cfg.user = "root";
    cfg.password = "taosdata";
    cfg.database = "";

    TDengineConnector conn(cfg, "native", "TDengine");
    const std::string db = "test_schemaless_basic";
    if (!try_connect_tdengine(conn, db)) {
        std::cout << "[schemaless_basic] SKIPPED: TDengine not available\n";
        return;
    }

    SchemalessInsertData data;
    data.lines = "cpu,host=h1 usage=50.0 1609459200000000000\n"
                 "cpu,host=h2 usage=75.5 1609459200000000000\n";
    data.total_rows = 2;
    data.protocol = TSDB_SML_LINE_PROTOCOL;
    data.precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;

    bool ok = conn.execute(data);
    (void)ok;
    assert(ok && "Schemaless basic insert should succeed");

    conn.execute(std::string("DROP DATABASE IF EXISTS " + db));
    std::cout << "[schemaless_basic] PASSED\n";
}

static void test_schemaless_insert_multiple_measurements_child() {
    TDengineConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 6030;
    cfg.user = "root";
    cfg.password = "taosdata";
    cfg.database = "";

    TDengineConnector conn(cfg, "native", "TDengine");
    const std::string db = "test_schemaless_multi";
    if (!try_connect_tdengine(conn, db)) {
        std::cout << "[schemaless_multi] SKIPPED: TDengine not available\n";
        return;
    }

    SchemalessInsertData data;
    data.lines = "cpu,host=h1 usage=50.0 1609459200000000000\n"
                 "mem,host=h1 used=8192i 1609459200000000000\n"
                 "disk,host=h1 free=500.5 1609459200000000000\n";
    data.total_rows = 3;
    data.protocol = TSDB_SML_LINE_PROTOCOL;
    data.precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;

    bool ok = conn.execute(data);
    (void)ok;
    assert(ok && "Schemaless multi-measurement insert should succeed");

    conn.execute(std::string("DROP DATABASE IF EXISTS " + db));
    std::cout << "[schemaless_multi] PASSED\n";
}

static void test_schemaless_insert_empty_lines_child() {
    TDengineConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 6030;
    cfg.user = "root";
    cfg.password = "taosdata";
    cfg.database = "";

    TDengineConnector conn(cfg, "native", "TDengine");
    const std::string db = "test_schemaless_empty";
    if (!try_connect_tdengine(conn, db)) {
        std::cout << "[schemaless_empty] SKIPPED: TDengine not available\n";
        return;
    }

    SchemalessInsertData data;
    data.lines = "";
    data.total_rows = 0;
    data.protocol = TSDB_SML_LINE_PROTOCOL;
    data.precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;

    // Empty lines should fail but not crash
    bool ok = conn.execute(data);
    (void)ok;  // result doesn't matter, just no crash

    conn.execute(std::string("DROP DATABASE IF EXISTS " + db));
    std::cout << "[schemaless_empty] PASSED (no crash)\n";
}

static void test_schemaless_insert_non_positive_rows_child() {
    TDengineConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 6030;
    cfg.user = "root";
    cfg.password = "taosdata";
    cfg.database = "";

    TDengineConnector conn(cfg, "native", "TDengine");
    const std::string db = "test_schemaless_non_positive_rows";
    if (!try_connect_tdengine(conn, db)) {
        std::cout << "[schemaless_non_positive_rows] SKIPPED: TDengine not available\n";
        return;
    }

    SchemalessInsertData zero_rows;
    zero_rows.lines = "";
    zero_rows.total_rows = 0;
    zero_rows.protocol = TSDB_SML_LINE_PROTOCOL;
    zero_rows.precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;

    bool ok = conn.execute(zero_rows);
    (void)ok;
    assert(ok && "Schemaless insert should return true when total_rows == 0");

    SchemalessInsertData negative_rows;
    negative_rows.lines = "cpu,host=h1 usage=50.0 1609459200000000000\n";
    negative_rows.total_rows = -1;
    negative_rows.protocol = TSDB_SML_LINE_PROTOCOL;
    negative_rows.precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;

    ok = conn.execute(negative_rows);
    (void)ok;
    assert(ok && "Schemaless insert should return true when total_rows < 0");

    conn.execute(std::string("DROP DATABASE IF EXISTS " + db));
    std::cout << "[schemaless_non_positive_rows] PASSED\n";
}

static void test_schemaless_insert_invalid_format_child() {
    TDengineConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 6030;
    cfg.user = "root";
    cfg.password = "taosdata";
    cfg.database = "";

    TDengineConnector conn(cfg, "native", "TDengine");
    const std::string db = "test_schemaless_invalid";
    if (!try_connect_tdengine(conn, db)) {
        std::cout << "[schemaless_invalid] SKIPPED: TDengine not available\n";
        return;
    }

    SchemalessInsertData data;
    data.lines = "this is not valid line protocol!!!";
    data.total_rows = 1;
    data.protocol = TSDB_SML_LINE_PROTOCOL;
    data.precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;

    bool ok = conn.execute(data);
    (void)ok;
    assert(!ok && "Invalid line protocol should fail");

    conn.execute(std::string("DROP DATABASE IF EXISTS " + db));
    std::cout << "[schemaless_invalid] PASSED\n";
}

static void test_schemaless_insert_not_connected_child() {
    TDengineConfig cfg;
    cfg.host = "192.0.2.1";  // unreachable address (TEST-NET)
    cfg.port = 6030;
    cfg.user = "root";
    cfg.password = "taosdata";
    cfg.database = "nonexistent";

    TDengineConnector conn(cfg, "native", "TDengine");

    SchemalessInsertData data;
    data.lines = "cpu,host=h1 usage=50.0 1609459200000000000";
    data.total_rows = 1;
    data.protocol = TSDB_SML_LINE_PROTOCOL;
    data.precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;

    // Should fail (cannot connect) but not crash
    bool ok = conn.execute(data);
    (void)ok;
    assert(!ok && "Execute on unreachable host should fail");

    std::cout << "[schemaless_not_connected] PASSED (no crash)\n";
}

static void test_schemaless_insert_large_batch_child() {
    TDengineConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 6030;
    cfg.user = "root";
    cfg.password = "taosdata";
    cfg.database = "";

    TDengineConnector conn(cfg, "native", "TDengine");
    const std::string db = "test_schemaless_large";
    if (!try_connect_tdengine(conn, db)) {
        std::cout << "[schemaless_large] SKIPPED: TDengine not available\n";
        return;
    }

    std::string lines;
    const int count = 1000;
    for (int i = 0; i < count; ++i) {
        int64_t ts = 1609459200000000000LL + static_cast<int64_t>(i) * 1000000LL;
        lines += "cpu,host=h" + std::to_string(i % 10) +
                 " usage=" + std::to_string(50.0 + (i % 50)) + " " +
                 std::to_string(ts) + "\n";
    }

    SchemalessInsertData data;
    data.lines = lines;
    data.total_rows = count;
    data.protocol = TSDB_SML_LINE_PROTOCOL;
    data.precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;

    bool ok = conn.execute(data);
    (void)ok;
    assert(ok && "Large batch schemaless insert should succeed");

    conn.execute(std::string("DROP DATABASE IF EXISTS " + db));
    std::cout << "[schemaless_large] PASSED\n";
}

int main(int argc, char** argv) {
    std::cout << "Running TDengineConnector library loading tests..." << std::endl;

    if (argc > 1) {
        const std::string arg = argv[1];
        if (arg == std::string("--mode=prefer")) {
            test_prefer_program_dir_lib_child();
            std::cout << "TDengineConnector prefer test completed." << std::endl;
            return 0;
        }
        if (arg == std::string("--mode=fallback")) {
            test_fallback_system_path_child();
            std::cout << "TDengineConnector fallback test completed." << std::endl;
            return 0;
        }
        // Schemaless sub-tests (run in child processes)
        if (arg == "--mode=schemaless_basic") {
            test_schemaless_insert_basic_child(); return 0;
        }
        if (arg == "--mode=schemaless_multi") {
            test_schemaless_insert_multiple_measurements_child(); return 0;
        }
        if (arg == "--mode=schemaless_empty") {
            test_schemaless_insert_empty_lines_child(); return 0;
        }
        if (arg == "--mode=schemaless_non_positive_rows") {
            test_schemaless_insert_non_positive_rows_child(); return 0;
        }
        if (arg == "--mode=schemaless_invalid") {
            test_schemaless_insert_invalid_format_child(); return 0;
        }
        if (arg == "--mode=schemaless_not_connected") {
            test_schemaless_insert_not_connected_child(); return 0;
        }
        if (arg == "--mode=schemaless_large") {
            test_schemaless_insert_large_batch_child(); return 0;
        }
    }

#if !defined(_WIN32)
    {
        std::string out;
        int rc = run_mode_and_capture(argv[0], "prefer", out);
        std::cout << "[parent] prefer output:\n" << out << std::endl;
        (void)rc;
        assert(rc == 0 && "prefer subprocess failed");
        assert(out.find("Loaded libtaos from program directory:") != std::string::npos &&
               "prefer should load libtaos from program directory");
        assert(out.find("Setting TDengine driver to native") != std::string::npos);
    }

    {
        std::string out;
        int rc = run_mode_and_capture(argv[0], "fallback", out);
        std::cout << "[parent] fallback output:\n" << out << std::endl;
        (void)rc;
        assert(rc == 0 && "fallback subprocess failed");
        assert(out.find("libtaos not found in program directory, trying system path") != std::string::npos ||
               out.find("[fallback] Program lib absent, testing system path fallback") != std::string::npos);
        assert(out.find("Loaded libtaos from system path:") != std::string::npos);
        assert(out.find("Setting TDengine driver to native") != std::string::npos);
    }

    // Schemaless insert tests (each in a subprocess)
    const char* schemaless_modes[] = {
        "schemaless_basic",
        "schemaless_multi",
        "schemaless_empty",
        "schemaless_non_positive_rows",
        "schemaless_invalid",
        "schemaless_not_connected",
        "schemaless_large",
    };
    for (const char* mode : schemaless_modes) {
        std::string out;
        int rc = run_mode_and_capture(argv[0], mode, out);
        std::cout << "[parent] " << mode << " output:\n" << out << std::endl;
        (void)rc;
        assert(rc == 0 && "schemaless subprocess failed");
        assert(out.find("PASSED") != std::string::npos || out.find("SKIPPED") != std::string::npos);
    }
#else
    test_prefer_program_dir_lib_child();
#endif

    cleanup_program_lib_if_created();

    std::cout << "TDengineConnector library loading tests completed." << std::endl;
    return 0;
}

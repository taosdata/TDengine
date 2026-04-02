#pragma once

#include "DatabaseConnector.hpp"
#include <taos.h>
#if defined(_WIN32)
  #include <windows.h>
#elif defined(__linux__) || defined(__APPLE__)
  #include <dlfcn.h>
#else
  #error "Dynamic library loading is only supported on Windows, Linux, and macOS."
#endif
#include <mutex>
#include <map>

#if defined(_WIN32)
  #define DYNLIB_HANDLE HMODULE
  #define DYNLIB_LOAD(lib) LoadLibraryA(lib)
  #define DYNLIB_SYM(handle, sym) GetProcAddress(handle, sym)
  #define DYNLIB_CLOSE(handle) FreeLibrary(handle)
#elif defined(__linux__) || defined(__APPLE__)
  #define DYNLIB_HANDLE void*
  #define DYNLIB_LOAD(lib) dlopen(lib, RTLD_LAZY)
  #define DYNLIB_SYM(handle, sym) dlsym(handle, sym)
  #define DYNLIB_CLOSE(handle) dlclose(handle)
#else
  #error "Dynamic library loading is only supported on Windows, Linux, and macOS."
#endif

class TDengineConnector : public DatabaseConnector {
public:
    TDengineConnector(const TDengineConfig& conn_info,
                          const std::string& driver_type,
                          const std::string& display_name);

    ~TDengineConnector() override;

    bool connect() override;
    void close() noexcept override;

    bool select_db(const std::string& db_name) override;
    bool prepare(const std::string& sql) override;
    bool execute(const std::string& sql) override;
    bool execute(const SqlInsertData& data) override;
    bool execute(const StmtV2InsertData& data) override;

    bool is_connected() const override;
    bool is_valid() const override;
    void reset_state() noexcept override;

protected:
    TDengineConfig conn_info_;
    std::string driver_type_;
    std::string display_name_;

    bool is_connected_{false};
    std::string current_db_;
    std::string prepare_sql_;
    TAOS* conn_{nullptr};
    TAOS_STMT* stmt_{nullptr};

private:
    void init_driver();

    DYNLIB_HANDLE taos_lib_handle_{nullptr};

    // Base API
    using taos_options_func = int (*)(TSDB_OPTION, const void*, ...);
    using taos_errstr_func = const char* (*)(TAOS_RES*);
    using taos_errno_func = int (*)(TAOS_RES*);
    using taos_connect_func = TAOS* (*)(const char*, const char*, const char*, const char*, uint16_t);
    using taos_query_func = TAOS_RES* (*)(TAOS*, const char*);
    using taos_free_result_func = void (*)(TAOS_RES*);
    using taos_select_db_func = int (*)(TAOS*, const char*);
    using taos_close_func = void (*)(TAOS*);

    // STMT2 API
    using taos_stmt2_init_func = TAOS_STMT2* (*)(TAOS*, TAOS_STMT2_OPTION*);
    using taos_stmt2_prepare_func = int (*)(TAOS_STMT2*, const char*, unsigned long);
    using taos_stmt2_bind_param_func = int (*)(TAOS_STMT2*, TAOS_STMT2_BINDV*, int32_t);
    using taos_stmt2_exec_func = int (*)(TAOS_STMT2*, int*);
    using taos_stmt2_close_func = int (*)(TAOS_STMT2*);
    using taos_stmt2_error_func = char* (*)(TAOS_STMT2*);

    // Member function pointer
    taos_options_func taos_options_{nullptr};
    taos_errstr_func taos_errstr_{nullptr};
    taos_errno_func taos_errno_{nullptr};
    taos_connect_func taos_connect_{nullptr};
    taos_query_func taos_query_{nullptr};
    taos_free_result_func taos_free_result_{nullptr};
    taos_select_db_func taos_select_db_{nullptr};
    taos_close_func taos_close_{nullptr};

    taos_stmt2_init_func taos_stmt2_init_{nullptr};
    taos_stmt2_prepare_func taos_stmt2_prepare_{nullptr};
    taos_stmt2_bind_param_func taos_stmt2_bind_param_{nullptr};
    taos_stmt2_exec_func taos_stmt2_exec_{nullptr};
    taos_stmt2_close_func taos_stmt2_close_{nullptr};
    taos_stmt2_error_func taos_stmt2_error_{nullptr};

    static std::mutex driver_init_mutex_;
    static std::map<std::string, std::once_flag> driver_init_flags;
};
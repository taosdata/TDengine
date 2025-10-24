#ifndef PITR_E2E_TEST_H
#define PITR_E2E_TEST_H

#include <stdint.h>
#include <stdbool.h>
#include "bitmap_engine.h"
#include "backup_coordinator.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// 数据量限制常量 - 必须保证测试数据量在1GB以内
// ============================================================================
#define MAX_DATA_SIZE_GB 1
#define MAX_DATA_SIZE_BYTES (MAX_DATA_SIZE_GB * 1024ULL * 1024ULL * 1024ULL)
#define ESTIMATED_BLOCK_SIZE_BYTES 1024  // 每个数据块约1KB
#define ESTIMATED_SNAPSHOT_OVERHEAD 0.1  // 快照开销10%
#define ESTIMATED_RECOVERY_OVERHEAD 0.2  // 恢复点开销20%

// PITR测试配置
typedef struct {
    uint32_t snapshot_interval_ms;      // 快照间隔（毫秒）
    uint32_t recovery_points;            // 恢复点数量
    uint64_t data_block_count;           // 数据块数量
    uint32_t concurrent_writers;         // 并发写入线程数
    uint32_t test_duration_seconds;      // 测试持续时间（秒）
    bool enable_disorder_test;           // 是否启用乱序测试
    bool enable_deletion_test;           // 是否启用删除测试
    const char* test_data_path;          // 测试数据路径
    const char* snapshot_path;           // 快照存储路径
    const char* recovery_path;           // 恢复数据路径
} SPitrTestConfig;

// PITR测试状态
typedef struct {
    uint64_t snapshots_created;          // 已创建快照数量
    uint64_t recovery_points_verified;   // 已验证恢复点数量
    uint64_t data_consistency_checks;    // 数据一致性检查次数
    uint64_t disorder_handled;           // 处理的乱序数据数量
    uint64_t deletion_handled;           // 处理的删除操作数量
    uint64_t total_test_time_ms;         // 总测试时间（毫秒）
    bool test_passed;                    // 测试是否通过
    char error_message[512];             // 错误信息
} SPitrTestStatus;

// 快照信息
typedef struct {
    int64_t timestamp;                   // 快照时间戳
    uint64_t snapshot_id;                // 快照ID
    uint64_t block_count;                // 块数量
    uint64_t data_size_bytes;            // 数据大小（字节）
    char metadata_path[256];             // 元数据路径
    char data_path[256];                 // 数据路径
    bool is_valid;                       // 快照是否有效
} SSnapshotInfo;

// 恢复点信息
typedef struct {
    int64_t recovery_timestamp;          // 恢复时间戳
    uint64_t recovery_id;                // 恢复点ID
    SSnapshotInfo* base_snapshot;        // 基础快照
    uint64_t incremental_blocks;         // 增量块数量
    uint64_t expected_block_count;       // 期望的块数量
    bool recovery_successful;            // 恢复是否成功
} SRecoveryPoint;

// 数据一致性检查结果
typedef struct {
    uint64_t expected_blocks;            // 期望的块数量
    uint64_t actual_blocks;              // 实际的块数量
    uint64_t mismatched_blocks;          // 不匹配的块数量
    uint64_t missing_blocks;             // 缺失的块数量
    uint64_t extra_blocks;               // 多余的块数量
    double consistency_percentage;        // 一致性百分比
    bool is_consistent;                  // 是否一致
    char details[512];                   // 详细信息
} SDataConsistencyResult;

// 乱序数据处理结果
typedef struct {
    uint64_t total_events;               // 总事件数
    uint64_t ordered_events;             // 有序事件数
    uint64_t disorder_events;            // 乱序事件数
    uint64_t reordered_events;           // 重排序事件数
    uint64_t dropped_events;             // 丢弃事件数
    double disorder_ratio;               // 乱序比例
    bool handling_successful;            // 处理是否成功
} SDisorderHandlingResult;

// 删除操作处理结果
typedef struct {
    uint64_t total_deletions;            // 总删除操作数
    uint64_t successful_deletions;       // 成功删除数
    uint64_t failed_deletions;           // 失败删除数
    uint64_t recovered_deletions;        // 恢复的删除数
    double deletion_success_rate;        // 删除成功率
    bool deletion_handling_successful;   // 删除处理是否成功
} SDeletionHandlingResult;

// PITR测试器
typedef struct SPitrTester SPitrTester;

// 创建PITR测试器
SPitrTester* pitr_tester_create(const SPitrTestConfig* config);

// 销毁PITR测试器
void pitr_tester_destroy(SPitrTester* tester);

// 运行完整的PITR E2E测试
int pitr_tester_run_full_test(SPitrTester* tester);

// 运行快照创建测试
int pitr_tester_run_snapshot_test(SPitrTester* tester);

// 运行时间点恢复测试
int pitr_tester_run_recovery_test(SPitrTester* tester);

// 运行乱序数据处理测试
int pitr_tester_run_disorder_test(SPitrTester* tester);

// 运行删除覆盖一致性测试
int pitr_tester_run_deletion_consistency_test(SPitrTester* tester);

// 运行边界条件测试
int pitr_tester_run_boundary_test(SPitrTester* tester);

// 获取测试状态
int pitr_tester_get_status(SPitrTester* tester, SPitrTestStatus* status);

// 获取快照列表
int pitr_tester_get_snapshots(SPitrTester* tester, SSnapshotInfo* snapshots, uint32_t max_count, uint32_t* actual_count);

// 获取恢复点列表
int pitr_tester_get_recovery_points(SPitrTester* tester, SRecoveryPoint* recovery_points, uint32_t max_count, uint32_t* actual_count);

// 验证数据一致性
int pitr_tester_verify_consistency(SPitrTester* tester, int64_t timestamp, SDataConsistencyResult* result);

// 生成测试报告
int pitr_tester_generate_report(SPitrTester* tester, const char* report_path);

// 重置测试器状态
int pitr_tester_reset(SPitrTester* tester);

// 设置测试配置
int pitr_tester_set_config(SPitrTester* tester, const SPitrTestConfig* config);

// 获取测试统计信息
int pitr_tester_get_statistics(SPitrTester* tester, void* stats);

// 时间点恢复测试
int pitr_tester_run_time_point_recovery(SPitrTester* tester, int64_t timestamp);

// 数据一致性验证
int pitr_tester_verify_data_consistency(SPitrTester* tester);

// 边界条件测试
int pitr_tester_run_boundary_tests(SPitrTester* tester);

// 性能测试
int pitr_tester_run_performance_test(SPitrTester* tester);

// 测试辅助函数

// 创建测试数据
int pitr_create_test_data(const char* data_path, uint64_t block_count, uint32_t concurrent_writers);



// 验证快照完整性
bool pitr_verify_snapshot_integrity(const SSnapshotInfo* snapshot);

// 比较两个时间点的数据
int pitr_compare_data_at_timestamps(const char* data_path, int64_t timestamp1, int64_t timestamp2, SDataConsistencyResult* result);

// 模拟网络延迟和乱序
int pitr_simulate_network_disorder(const char* data_path, double disorder_ratio, uint32_t max_delay_ms);

// 模拟删除操作
int pitr_simulate_deletions(const char* data_path, uint64_t deletion_count, double success_rate);

// 性能基准测试
int pitr_run_performance_benchmark(const char* test_name, void (*test_func)(void*), void* test_data, uint32_t iterations);

// 内存使用监控
int pitr_monitor_memory_usage(size_t* peak_memory, size_t* current_memory);

// 时间测量工具
typedef struct {
    struct timespec start_time;
    struct timespec end_time;
    char operation_name[64];
} SPitrTimer;

// 开始计时
SPitrTimer* pitr_timer_start(const char* operation_name);

// 停止计时
void pitr_timer_stop(SPitrTimer* timer);

// 获取耗时（毫秒）
double pitr_timer_get_elapsed_ms(SPitrTimer* timer);

// 打印计时结果
void pitr_timer_print_result(SPitrTimer* timer);

// 默认测试配置
extern const SPitrTestConfig PITR_DEFAULT_CONFIG;

// 测试结果常量
#define PITR_TEST_SUCCESS 0
#define PITR_TEST_FAILED -1
#define PITR_TEST_INVALID_CONFIG -2
#define PITR_TEST_TIMEOUT -3
#define PITR_TEST_MEMORY_ERROR -4

#ifdef __cplusplus
}
#endif

#endif // PITR_E2E_TEST_H

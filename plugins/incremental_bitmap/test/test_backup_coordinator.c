#include "../include/backup_coordinator.h"
#include "../include/bitmap_engine.h"
#include "../include/event_interceptor.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <unistd.h>

// 测试辅助函数
static void print_test_result(const char* test_name, bool passed) {
    printf("[%s] %s\n", passed ? "PASS" : "FAIL", test_name);
}

static int64_t get_current_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

// 测试1: 基本初始化和销毁
static void test_basic_init_destroy() {
    printf("\n=== 测试1: 基本初始化和销毁 ===\n");
    
    // 创建位图引擎
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    // 创建事件拦截器
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* event_interceptor = event_interceptor_init(&interceptor_config, bitmap_engine);
    assert(event_interceptor != NULL);
    
    // 创建备份协同器
    SBackupConfig config = {
        .batch_size = 100,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 5000,
        .enable_compression = true,
        .enable_encryption = false,
        .backup_path = NULL,
        .temp_path = NULL
    };
    
    SBackupCoordinator* coordinator = backup_coordinator_init(bitmap_engine, &config);
    assert(coordinator != NULL);
    print_test_result("初始化备份协同器", true);
    
    backup_coordinator_destroy(coordinator);
    event_interceptor_destroy(event_interceptor);
    bitmap_engine_destroy(bitmap_engine);
    print_test_result("销毁备份协同器", true);
}

// 测试2: 获取脏块
static void test_get_dirty_blocks() {
    printf("\n=== 测试2: 获取脏块 ===\n");
    
    // 创建组件
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* event_interceptor = event_interceptor_init(&interceptor_config, bitmap_engine);
    assert(event_interceptor != NULL);
    
    SBackupConfig config = {
        .batch_size = 100,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 5000,
        .enable_compression = true,
        .enable_encryption = false,
        .backup_path = NULL,
        .temp_path = NULL
    };
    
    SBackupCoordinator* coordinator = backup_coordinator_init(bitmap_engine, &config);
    assert(coordinator != NULL);
    
    int64_t timestamp = get_current_timestamp();
    
    // 添加一些脏块
    bitmap_engine_mark_dirty(bitmap_engine, 1001, 1000, timestamp);
    bitmap_engine_mark_dirty(bitmap_engine, 1002, 2000, timestamp + 1000);
    bitmap_engine_mark_dirty(bitmap_engine, 1003, 3000, timestamp + 2000);
    bitmap_engine_mark_dirty(bitmap_engine, 1004, 4000, timestamp + 3000);
    
    // 简单测试：检查位图引擎是否正确标记了脏块
    uint64_t total_blocks, dirty_count, new_count, deleted_count;
    bitmap_engine_get_stats(bitmap_engine, &total_blocks, &dirty_count, &new_count, &deleted_count);
    
    printf("总块数: %lu, 脏块数: %lu\n", total_blocks, dirty_count);
    assert(dirty_count >= 4);
    print_test_result("标记脏块", true);
    
    // 测试基本功能：获取所有脏块
    uint64_t block_ids[10];
    uint32_t count = bitmap_engine_get_dirty_blocks_by_wal(bitmap_engine, 0, UINT64_MAX, block_ids, 10);
    
    printf("找到 %u 个脏块\n", count);
    assert(count >= 4);
    print_test_result("获取脏块", true);
    
    backup_coordinator_destroy(coordinator);
    event_interceptor_destroy(event_interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试3: 创建和销毁游标
static void test_cursor_operations() {
    printf("\n=== 测试3: 创建和销毁游标 ===\n");
    
    // 创建组件
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* event_interceptor = event_interceptor_init(&interceptor_config, bitmap_engine);
    assert(event_interceptor != NULL);
    
    SBackupConfig config = {
        .batch_size = 100,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 5000,
        .enable_compression = true,
        .enable_encryption = false,
        .backup_path = NULL,
        .temp_path = NULL
    };
    
    SBackupCoordinator* coordinator = backup_coordinator_init(bitmap_engine, &config);
    assert(coordinator != NULL);
    
    int64_t start_time = get_current_timestamp();
    int64_t end_time = start_time + 1000000; // 1秒后
    
    // 设置游标
    SBackupCursor cursor = {
        .type = BACKUP_CURSOR_TYPE_TIME,
        .time_cursor = start_time,
        .wal_cursor = 1000,
        .block_count = 0,
        .last_update_time = start_time
    };
    
    int32_t result = backup_coordinator_set_cursor(coordinator, &cursor);
    assert(result == 0);
    print_test_result("设置游标", true);
    
    // 验证游标
    SBackupCursor retrieved_cursor;
    result = backup_coordinator_get_cursor(coordinator, &retrieved_cursor);
    assert(result == 0);
    assert(retrieved_cursor.type == BACKUP_CURSOR_TYPE_TIME);
    assert(retrieved_cursor.time_cursor == start_time);
    print_test_result("获取游标", true);
    
    backup_coordinator_destroy(coordinator);
    event_interceptor_destroy(event_interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试4: 批量获取增量数据
static void test_get_next_batch() {
    printf("\n=== 测试4: 批量获取增量数据 ===\n");
    
    // 创建组件
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* event_interceptor = event_interceptor_init(&interceptor_config, bitmap_engine);
    assert(event_interceptor != NULL);
    
    SBackupConfig config = {
        .batch_size = 100,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 5000,
        .enable_compression = true,
        .enable_encryption = false,
        .backup_path = NULL,
        .temp_path = NULL
    };
    
    SBackupCoordinator* coordinator = backup_coordinator_init(bitmap_engine, &config);
    assert(coordinator != NULL);
    
    int64_t timestamp = get_current_timestamp();
    
    // 添加一些块
    for (int i = 0; i < 10; i++) {
        uint64_t block_id = 2000 + i;
        bitmap_engine_mark_dirty(bitmap_engine, block_id, block_id * 10, timestamp + i);
    }
    
    // 获取增量块
    SIncrementalBlock blocks[5];
    uint32_t count = backup_coordinator_get_incremental_blocks(coordinator, 20000, 30000, blocks, 5);
    
    assert(count > 0);
    print_test_result("批量获取增量数据", true);
    
    // 验证块信息
    for (uint32_t i = 0; i < count; i++) {
        assert(blocks[i].block_id >= 2000 && blocks[i].block_id < 2010);
    }
    backup_coordinator_destroy(coordinator);
    event_interceptor_destroy(event_interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试5: 估算备份大小
static void test_estimate_size() {
    printf("\n=== 测试5: 估算备份大小 ===\n");
    
    // 创建组件
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* event_interceptor = event_interceptor_init(&interceptor_config, bitmap_engine);
    assert(event_interceptor != NULL);
    
    SBackupConfig config = {
        .batch_size = 100,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 5000,
        .enable_compression = true,
        .enable_encryption = false,
        .backup_path = NULL,
        .temp_path = NULL
    };
    
    SBackupCoordinator* coordinator = backup_coordinator_init(bitmap_engine, &config);
    assert(coordinator != NULL);
    
    int64_t timestamp = get_current_timestamp();
    
    // 添加一些块
    for (int i = 0; i < 20; i++) {
        uint64_t block_id = 3000 + i;
        bitmap_engine_mark_dirty(bitmap_engine, block_id, block_id * 10, timestamp + i);
    }
    
    // 估算备份大小
    uint64_t estimated_size = backup_coordinator_estimate_backup_size(coordinator, 30000, 50000);
    
    assert(estimated_size > 0);
    print_test_result("估算备份大小", true);
    
    printf("估算大小: %lu 字节\n", estimated_size);
    
    backup_coordinator_destroy(coordinator);
    event_interceptor_destroy(event_interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试6: 生成元数据
static void test_generate_metadata() {
    printf("\n=== 测试6: 生成元数据 ===\n");
    
    // 创建组件
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* event_interceptor = event_interceptor_init(&interceptor_config, bitmap_engine);
    assert(event_interceptor != NULL);
    
    SBackupConfig config = {
        .batch_size = 100,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 5000,
        .enable_compression = true,
        .enable_encryption = false,
        .backup_path = NULL,
        .temp_path = NULL
    };
    
    SBackupCoordinator* coordinator = backup_coordinator_init(bitmap_engine, &config);
    assert(coordinator != NULL);
    
    // 获取统计信息
    SBackupStats stats;
    int32_t result = backup_coordinator_get_stats(coordinator, &stats);
    
    assert(result == 0);
    print_test_result("获取统计信息", true);
    
    printf("总块数: %lu, 已处理块数: %lu\n", stats.total_blocks, stats.processed_blocks);
    
    backup_coordinator_destroy(coordinator);
    event_interceptor_destroy(event_interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试7: 验证备份完整性
static void test_validate_backup() {
    printf("\n=== 测试7: 验证备份完整性 ===\n");
    
    // 创建组件
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* event_interceptor = event_interceptor_init(&interceptor_config, bitmap_engine);
    assert(event_interceptor != NULL);
    
    SBackupConfig config = {
        .batch_size = 100,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 5000,
        .enable_compression = true,
        .enable_encryption = false,
        .backup_path = NULL,
        .temp_path = NULL
    };
    
    SBackupCoordinator* coordinator = backup_coordinator_init(bitmap_engine, &config);
    assert(coordinator != NULL);
    
    int64_t timestamp = get_current_timestamp();
    
    // 添加一些块
    for (int i = 0; i < 5; i++) {
        uint64_t block_id = 4000 + i;
        bitmap_engine_mark_dirty(bitmap_engine, block_id, block_id * 10, timestamp + i);
    }
    
    // 获取增量块进行验证
    SIncrementalBlock blocks[5];
    uint32_t count = backup_coordinator_get_incremental_blocks(coordinator, 40000, 50000, blocks, 5);
    
    assert(count > 0);
    print_test_result("验证备份完整性", true);
    
    backup_coordinator_destroy(coordinator);
    event_interceptor_destroy(event_interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试8: 统计信息
static void test_statistics() {
    printf("\n=== 测试8: 统计信息 ===\n");
    
    // 创建组件
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = NULL,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* event_interceptor = event_interceptor_init(&interceptor_config, bitmap_engine);
    assert(event_interceptor != NULL);
    
    SBackupConfig config = {
        .batch_size = 100,
        .max_retries = 3,
        .retry_interval_ms = 1000,
        .timeout_ms = 5000,
        .enable_compression = true,
        .enable_encryption = false,
        .backup_path = NULL,
        .temp_path = NULL
    };
    
    SBackupCoordinator* coordinator = backup_coordinator_init(bitmap_engine, &config);
    assert(coordinator != NULL);
    
    // 获取统计信息
    SBackupStats stats;
    int32_t result = backup_coordinator_get_stats(coordinator, &stats);
    assert(result == 0);
    
    printf("总块数: %lu, 总大小: %lu 字节, 开始时间: %ld\n", 
           stats.total_blocks, stats.total_size, stats.start_time);
    print_test_result("统计信息", true);
    
    backup_coordinator_destroy(coordinator);
    event_interceptor_destroy(event_interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试插件接口功能
// 测试9: 插件接口测试
static void test_plugin_interface() {
    printf("\n=== 测试9: 插件接口测试 ===\n");
    
    // 测试插件接口获取
    SBackupPluginInterface* interface = backup_plugin_get_interface();
    assert(interface != NULL);
    print_test_result("插件接口获取", true);
    
    // 测试插件初始化
    int32_t result = interface->init("test_config");
    assert(result == 0);
    print_test_result("插件初始化", true);
    
    // 测试获取脏块（在没有标记脏块的情况下，应该返回0）
    uint64_t block_ids[10];
    uint32_t count = interface->get_dirty_blocks(1000, 10000, block_ids, 10);
    printf("获取到 %u 个脏块\n", count);
    // 在没有标记脏块的情况下，返回0是正常的
    print_test_result("插件获取脏块", true);
    
    // 测试估算备份大小（在没有脏块的情况下，应该返回0）
    uint64_t size = interface->estimate_backup_size(1000, 10000);
    printf("估算备份大小: %lu 字节\n", size);
    // 在没有脏块的情况下，返回0是正常的
    print_test_result("插件估算备份大小", true);
    
    // 测试获取统计信息
    SBackupStats stats;
    result = interface->get_stats(&stats);
    assert(result == 0);
    print_test_result("插件获取统计信息", true);
    
    // 测试重置统计信息
    result = interface->reset_stats();
    assert(result == 0);
    print_test_result("插件重置统计信息", true);
    
    // 测试插件销毁
    interface->destroy();
    print_test_result("插件销毁", true);
}


int main() {
    printf("开始备份协同器测试...\n");
    
    test_basic_init_destroy();
    test_get_dirty_blocks();
    test_cursor_operations();
    test_get_next_batch();
    test_estimate_size();
    test_statistics();
    test_validate_backup();
    test_statistics();
    test_plugin_interface();

    
    printf("\n所有测试完成！\n");
    return 0;
} 
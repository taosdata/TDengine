#include "../include/storage_engine_interface.h"
#include "../include/event_interceptor.h"
#include "../include/bitmap_engine.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

// 测试辅助函数
static void print_test_result(const char* test_name, bool passed) {
    printf("[%s] %s\n", passed ? "PASS" : "FAIL", test_name);
}

static int64_t get_current_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

// 事件回调函数
static void test_event_callback(const SBlockEvent* event, void* user_data) {
    printf("收到事件: 类型=%d, 块ID=%lu, WAL偏移量=%lu, 时间戳=%ld\n",
           event->event_type, event->block_id, event->wal_offset, event->timestamp);
}

// 测试1: 存储引擎接口注册和获取
static void test_interface_registration() {
    printf("\n=== 测试1: 存储引擎接口注册和获取 ===\n");
    
    // 注册Mock存储引擎
    extern int32_t register_mock_storage_engine(void);
    int32_t result = register_mock_storage_engine();
    assert(result == 0);
    print_test_result("注册Mock存储引擎", true);
    
    // 获取Mock存储引擎接口
    SStorageEngineInterface* mock_interface = get_storage_engine_interface("mock");
    assert(mock_interface != NULL);
    print_test_result("获取Mock存储引擎接口", true);
    
    // 检查接口名称
    const char* name = mock_interface->get_engine_name();
    assert(strcmp(name, "mock") == 0);
    print_test_result("检查接口名称", true);
    
    // 检查是否支持
    bool supported = mock_interface->is_supported();
    assert(supported == true);
    print_test_result("检查支持状态", true);
}

// 测试2: 存储引擎接口初始化和配置
static void test_interface_initialization() {
    printf("\n=== 测试2: 存储引擎接口初始化和配置 ===\n");
    
    // 获取Mock存储引擎接口
    SStorageEngineInterface* mock_interface = get_storage_engine_interface("mock");
    assert(mock_interface != NULL);
    
    // 配置存储引擎
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = test_event_callback,
        .callback_user_data = NULL,
        .event_buffer_size = 1000,
        .callback_threads = 2
    };
    
    // 初始化存储引擎
    int32_t result = mock_interface->init(&config);
    assert(result == 0);
    print_test_result("初始化存储引擎", true);
    
    // 安装事件拦截
    result = mock_interface->install_interception();
    assert(result == 0);
    print_test_result("安装事件拦截", true);
    
    // 卸载事件拦截
    result = mock_interface->uninstall_interception();
    assert(result == 0);
    print_test_result("卸载事件拦截", true);
    
    // 销毁存储引擎
    mock_interface->destroy();
    print_test_result("销毁存储引擎", true);
}

// 测试3: 事件触发和统计
static void test_event_triggering() {
    printf("\n=== 测试3: 事件触发和统计 ===\n");
    
    // 获取Mock存储引擎接口
    SStorageEngineInterface* mock_interface = get_storage_engine_interface("mock");
    assert(mock_interface != NULL);
    
    // 初始化并安装拦截
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = test_event_callback,
        .callback_user_data = NULL,
        .event_buffer_size = 1000,
        .callback_threads = 2
    };
    
    mock_interface->init(&config);
    mock_interface->install_interception();
    
    // 触发事件
    SStorageEvent event = {
        .event_type = STORAGE_EVENT_BLOCK_UPDATE,
        .block_id = 12345,
        .wal_offset = 1000,
        .timestamp = get_current_timestamp(),
        .user_data = NULL
    };
    
    int32_t result = mock_interface->trigger_event(&event);
    assert(result == 0);
    print_test_result("触发事件", true);
    
    // 获取统计信息
    uint64_t events_processed, events_dropped;
    result = mock_interface->get_stats(&events_processed, &events_dropped);
    assert(result == 0);
    assert(events_processed == 1);
    assert(events_dropped == 0);
    print_test_result("获取统计信息", true);
    
    // 清理
    mock_interface->uninstall_interception();
    mock_interface->destroy();
}

// 测试4: 与事件拦截器集成
static void test_event_interceptor_integration() {
    printf("\n=== 测试4: 与事件拦截器集成 ===\n");
    
    // 创建位图引擎
    SBitmapEngine* bitmap_engine = bitmap_engine_init();
    assert(bitmap_engine != NULL);
    
    // 创建事件拦截器
    SEventInterceptorConfig interceptor_config = {
        .enable_interception = true,
        .event_buffer_size = 1000,
        .callback_threads = 2,
        .callback = test_event_callback,
        .callback_user_data = NULL
    };
    
    SEventInterceptor* interceptor = event_interceptor_init(&interceptor_config, bitmap_engine);
    assert(interceptor != NULL);
    
    // 获取Mock存储引擎接口
    SStorageEngineInterface* mock_interface = get_storage_engine_interface("mock");
    assert(mock_interface != NULL);
    
    // 设置存储引擎接口
    int32_t result = event_interceptor_set_storage_interface(interceptor, mock_interface);
    assert(result == 0);
    print_test_result("设置存储引擎接口", true);
    
    // 安装存储引擎拦截
    result = event_interceptor_install_storage_interception(interceptor);
    assert(result == 0);
    print_test_result("安装存储引擎拦截", true);
    
    // 启动事件拦截器
    result = event_interceptor_start(interceptor);
    assert(result == 0);
    print_test_result("启动事件拦截器", true);
    
    // 手动触发测试事件
    result = event_interceptor_trigger_test_event(interceptor, EVENT_BLOCK_UPDATE, 
                                                12345, 1000, get_current_timestamp());
    assert(result == 0);
    print_test_result("手动触发测试事件", true);
    
    // 等待事件处理
    usleep(100000); // 100ms
    
    // 获取统计信息
    uint64_t events_processed, events_dropped;
    event_interceptor_get_stats(interceptor, &events_processed, &events_dropped);
    printf("事件拦截器统计: 处理=%lu, 丢弃=%lu\n", events_processed, events_dropped);
    print_test_result("事件拦截器统计", true);
    
    // 停止和清理
    event_interceptor_stop(interceptor);
    event_interceptor_uninstall_storage_interception(interceptor);
    event_interceptor_destroy(interceptor);
    bitmap_engine_destroy(bitmap_engine);
}

// 测试5: 接口列表功能
static void test_interface_listing() {
    printf("\n=== 测试5: 接口列表功能 ===\n");
    
    // 列出所有可用的存储引擎接口
    char* names[10];
    uint32_t actual_count;
    int32_t result = list_storage_engine_interfaces(names, 10, &actual_count);
    assert(result == 0);
    assert(actual_count >= 1); // 至少应该有mock接口
    
    printf("可用的存储引擎接口:\n");
    for (uint32_t i = 0; i < actual_count; i++) {
        printf("  - %s\n", names[i]);
        free(names[i]); // 释放内存
    }
    
    print_test_result("列出存储引擎接口", true);
}

int main() {
    printf("开始存储引擎接口测试...\n");
    
    test_interface_registration();
    test_interface_initialization();
    test_event_triggering();
    test_event_interceptor_integration();
    test_interface_listing();
    
    printf("\n所有测试完成！\n");
    return 0;
}


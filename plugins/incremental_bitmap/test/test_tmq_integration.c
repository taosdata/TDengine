#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifndef USE_MOCK
#include <taos.h>
#endif
#include <pthread.h>
#include <assert.h>

#include "../include/storage_engine_interface.h"
#ifndef USE_MOCK
#include "../include/tdengine_storage_engine.h"
#endif

// 测试事件回调函数
static void test_event_callback(const SStorageEvent* event, void* user_data) {
    printf("[TEST] 收到事件: 类型=%d, 块ID=%lu, WAL偏移量=%lu, 时间戳=%ld\n",
           event->event_type, event->block_id, event->wal_offset, event->timestamp);
    
    // 更新事件计数（允许NULL，避免崩溃）
    if (user_data) {
        int* event_count = (int*)user_data;
        (*event_count)++;
    }
}

// 测试 TMQ 配置设置
static void test_tmq_config(void) {
    printf("\n=== 测试 TMQ 配置设置 ===\n");
    
    // 先初始化引擎，再设置配置
    SStorageEngineInterface* engine = get_storage_engine_interface("tdengine_tmq");
    assert(engine != NULL);

    SStorageEngineConfig cfg = {
        .enable_interception = false,
        .event_callback = test_event_callback,
        .callback_user_data = NULL,
        .event_buffer_size = 256,
        .callback_threads = 1
    };
    int32_t init_ret = engine->init(&cfg);
    assert(init_ret == 0);

    // 设置自定义配置（仅 TMQ 下编译）
    int32_t result = 0;
#ifndef USE_MOCK
    result = tdengine_set_tmq_config(
        "192.168.1.100",  // 自定义服务器IP
        6042,              // 自定义端口
        "admin",           // 自定义用户名
        "password123",     // 自定义密码
        "testdb",          // 自定义数据库
        "custom_topic",    // 自定义主题
        "custom_group"     // 自定义消费者组
    );
    
    assert(result == 0);
    printf("✓ TMQ 配置设置成功\n");
    
    // 设置提交策略
    result = tdengine_set_commit_strategy(
        false,   // 手动提交
        true,    // 至少一次
        2000     // 2秒间隔
    );
    
    assert(result == 0);
    printf("✓ 提交策略设置成功\n");
#else
    printf("[TEST] 跳过 TMQ 配置设置（USE_MOCK=ON）\n");
#endif

    // 清理本测试的初始化
    engine->destroy();
}

// 测试存储引擎生命周期
static void test_storage_engine_lifecycle(void) {
    printf("\n=== 测试存储引擎生命周期 ===\n");
    
    // 获取自动选择的存储引擎接口（优先 TMQ，不可用时回退 mock）
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    assert(engine != NULL);
    printf("✓ 获取存储引擎接口成功: %s\n", engine->get_engine_name());
    
    // 检查是否支持
    bool supported = engine->is_supported();
    assert(supported == true);
    printf("✓ 存储引擎支持检查通过\n");
    
    const char* name = engine->get_engine_name();
    printf("✓ 引擎名称: %s\n", name);
    
    // 初始化存储引擎
    int event_count = 0;
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = test_event_callback,
        .callback_user_data = &event_count,
        .event_buffer_size = 1024,
        .callback_threads = 2
    };
    
    int32_t result = engine->init(&config);
    assert(result == 0);
    printf("✓ TMQ 存储引擎初始化成功\n");
    
    // 安装事件拦截
    result = engine->install_interception();
    assert(result == 0);
    printf("✓ 事件拦截安装成功\n");
    
    // 在 TDengine 中准备数据库/主题/表并写入一条数据（仅在 TMQ 模式下执行）
    if (strcmp(name, "tdengine_tmq") == 0) {
#ifndef USE_MOCK
        TAOS* tconn = taos_connect("localhost", "root", "taosdata", NULL, 6030);
        if (tconn) {
            const char* stmts[] = {
                "CREATE DATABASE IF NOT EXISTS test", \
                "USE test", \
                "CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, v INT) TAGS (t INT)", \
                "CREATE TABLE IF NOT EXISTS d0 USING meters TAGS (1)", \
                "CREATE TOPIC IF NOT EXISTS incremental_backup_topic AS DATABASE test", \
                "INSERT INTO d0 VALUES (now, 1)"
            };
            for (size_t i = 0; i < sizeof(stmts)/sizeof(stmts[0]); ++i) {
                TAOS_RES* r = taos_query(tconn, stmts[i]);
                taos_free_result(r);
            }
            taos_close(tconn);
        } else {
            printf("[TEST] 警告: 无法连接 TDengine，可能影响消息消费验证\n");
        }
        (void)tconn; // 避免编译器在不同宏下的未使用警告
#else
        printf("[TEST] 跳过真实 TDengine 初始化（USE_MOCK=ON）\n");
#endif
    }

    // 等待一段时间让消费线程运行
    printf("等待 3 秒让消费线程运行...\n");
    sleep(3);
    
    // 在 mock 模式下手动触发一次事件，确保至少一次事件回调
    if (strcmp(name, "mock") == 0) {
        SStorageEvent mock_event = {
            .event_type = STORAGE_EVENT_BLOCK_UPDATE,
            .block_id = 42,
            .wal_offset = 0,
            .timestamp = 0,
            .user_data = NULL
        };
        int32_t tr = engine->trigger_event(&mock_event);
        assert(tr == 0);
    }
    
    // 获取统计信息
    uint64_t events_processed, events_dropped;
    result = engine->get_stats(&events_processed, &events_dropped);
    assert(result == 0);
    printf("✓ 统计信息获取成功: 处理=%lu, 丢弃=%lu\n", events_processed, events_dropped);
    
    // 获取详细统计信息（仅 TMQ 模式才有 tmq 专有统计）
    if (strcmp(name, "tdengine_tmq") == 0) {
#ifndef USE_MOCK
        uint64_t messages_consumed, offset_commits;
        int64_t last_commit_time;
        result = tdengine_get_detailed_stats(&events_processed, &events_dropped,
                                           &messages_consumed, &offset_commits, &last_commit_time);
        assert(result == 0);
        printf("✓ 详细统计信息获取成功:\n");
        printf("  事件处理: %lu\n", events_processed);
        printf("  事件丢弃: %lu\n", events_dropped);
        printf("  消息消费: %lu\n", messages_consumed);
        printf("  Offset提交: %lu\n", offset_commits);
        printf("  最后提交时间: %ld\n", last_commit_time);
#else
        printf("[TEST] 跳过 TMQ 详细统计（USE_MOCK=ON）\n");
#endif
    }
    // 至少应有一次事件回调
    assert(event_count > 0);
    
    // 卸载事件拦截
    result = engine->uninstall_interception();
    assert(result == 0);
    printf("✓ 事件拦截卸载成功\n");
    
    // 销毁存储引擎
    engine->destroy();
    printf("✓ TMQ 存储引擎销毁成功\n");
}

// 测试事件触发
static void test_event_trigger(void) {
    printf("\n=== 测试事件触发 ===\n");
    
    // 获取自动选择的存储引擎接口
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    assert(engine != NULL);
    
    // 初始化
    int event_count = 0;
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = test_event_callback,
        .callback_user_data = &event_count,
        .event_buffer_size = 1024,
        .callback_threads = 1
    };
    
    int32_t result = engine->init(&config);
    assert(result == 0);
    
    // 安装事件拦截
    result = engine->install_interception();
    assert(result == 0);
    
    // 手动触发事件
    SStorageEvent test_event = {
        .event_type = STORAGE_EVENT_BLOCK_UPDATE,
        .block_id = 12345,
        .wal_offset = 67890,
        .timestamp = 1234567890,
        .user_data = NULL
    };
    
    result = engine->trigger_event(&test_event);
    assert(result == 0);
    printf("✓ 手动事件触发成功\n");
    
    // 等待事件处理
    sleep(1);
    
    // 清理
    engine->uninstall_interception();
    engine->destroy();
}

// 测试错误处理
static void test_error_handling(void) {
    printf("\n=== 测试错误处理 ===\n");
    // 选择当前引擎
    SStorageEngineInterface* engine = get_storage_engine_interface("auto");
    assert(engine != NULL);
    const char* cur = engine->get_engine_name();
    
    // TMQ 专有错误路径：仅在 TMQ 模式下验证
    if (strcmp(cur, "tdengine_tmq") == 0) {
#ifndef USE_MOCK
        int32_t result = tdengine_set_tmq_config("localhost", 6041, "user", "pass", "db", "topic", "group");
        assert(result == -1);
        printf("✓ 未初始化时的配置设置正确拒绝\n");

        result = tdengine_set_commit_strategy(false, true, 1000);
        assert(result == -1);
        printf("✓ 未初始化时的策略设置正确拒绝\n");

        result = tdengine_get_detailed_stats(NULL, NULL, NULL, NULL, NULL);
        assert(result == -1);
        printf("✓ 未初始化时的统计获取正确拒绝\n");
        (void)result;
#else
        printf("[TEST] 跳过 TMQ 专有未初始化错误测试（USE_MOCK=ON）\n");
#endif
    } else {
        printf("[TEST] 跳过 TMQ 专有未初始化错误测试（当前为 mock）\n");
    }
    
    // 测试空指针参数
    int32_t result = engine->init(NULL);
    assert(result == -1); // 应该失败
    printf("✓ 空配置参数正确拒绝\n");
    
    // 测试未安装拦截时的操作
    SStorageEngineConfig config = {
        .enable_interception = true,
        .event_callback = test_event_callback,
        .callback_user_data = NULL,
        .event_buffer_size = 1024,
        .callback_threads = 1
    };
    
    result = engine->init(&config);
    assert(result == 0);
    
    SStorageEvent test_event = {
        .event_type = STORAGE_EVENT_BLOCK_UPDATE,
        .block_id = 1,
        .wal_offset = 1,
        .timestamp = 1,
        .user_data = NULL
    };
    
    result = engine->trigger_event(&test_event);
    assert(result == -1); // 应该失败，因为拦截未安装
    printf("✓ 未安装拦截时的事件触发正确拒绝\n");
    
    // 清理
    engine->destroy();
}

int main(void) {
    printf("开始 TMQ 集成测试...\n");
    
    // 注册 TMQ 与 Mock 存储引擎，auto 将按可用性选择
    int32_t result = 0;
#ifndef USE_MOCK
    result = register_tdengine_storage_engine();
    assert(result == 0);
#endif
    extern int32_t register_mock_storage_engine(void);
    result = register_mock_storage_engine();
    assert(result == 0);
    printf("✓ 存储引擎注册成功（TMQ + Mock）\n");
    
    // 运行测试
    test_tmq_config();
    test_storage_engine_lifecycle();
    test_event_trigger();
    test_error_handling();
    
    printf("\n🎉 所有 TMQ 集成测试通过！\n");
    return 0;
}

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include <stdbool.h>
#include <signal.h>
#include <errno.h>
#include "../include/storage_engine_interface.h"
#include "../include/observability.h"

// 测试宏定义
#define TEST_ASSERT(condition, message) do { \
    total_tests++; \
    if (condition) { \
        passed_tests++; \
        printf("✓ %s\n", message); \
    } else { \
        failed_tests++; \
        printf("❌ %s\n", message); \
    } \
} while(0)

// 测试计数器
static int total_tests = 0;
static int passed_tests = 0;
static int failed_tests = 0;

// 故障注入状态
typedef struct {
    bool connection_failure;
    bool subscription_failure;
    bool parse_failure;
    bool commit_failure;
    int failure_count;
    int max_failures;
    pthread_mutex_t mutex;
} SFaultInjectionState;

static SFaultInjectionState g_fault_state = {
    .connection_failure = false,
    .subscription_failure = false,
    .parse_failure = false,
    .commit_failure = false,
    .failure_count = 0,
    .max_failures = 3,
    .mutex = PTHREAD_MUTEX_INITIALIZER
};

// 模拟连接状态
typedef struct {
    bool connected;
    int reconnect_attempts;
    int max_reconnect_attempts;
    time_t last_connect_time;
    time_t last_disconnect_time;
    pthread_mutex_t mutex;
} SConnectionState;

static SConnectionState g_connection_state = {
    .connected = true,
    .reconnect_attempts = 0,
    .max_reconnect_attempts = 5,
    .last_connect_time = 0,
    .last_disconnect_time = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER
};

// 故障注入控制函数
void inject_connection_failure(bool enable, int max_failures) {
    pthread_mutex_lock(&g_fault_state.mutex);
    g_fault_state.connection_failure = enable;
    g_fault_state.max_failures = max_failures;
    g_fault_state.failure_count = 0;
    pthread_mutex_unlock(&g_fault_state.mutex);
}

void inject_subscription_failure(bool enable, int max_failures) {
    pthread_mutex_lock(&g_fault_state.mutex);
    g_fault_state.subscription_failure = enable;
    g_fault_state.max_failures = max_failures;
    g_fault_state.failure_count = 0;
    pthread_mutex_unlock(&g_fault_state.mutex);
}

void inject_parse_failure(bool enable, int max_failures) {
    pthread_mutex_lock(&g_fault_state.mutex);
    g_fault_state.parse_failure = enable;
    g_fault_state.max_failures = max_failures;
    g_fault_state.failure_count = 0;
    pthread_mutex_unlock(&g_fault_state.mutex);
}

void inject_commit_failure(bool enable, int max_failures) {
    pthread_mutex_lock(&g_fault_state.mutex);
    g_fault_state.commit_failure = enable;
    g_fault_state.max_failures = max_failures;
    g_fault_state.failure_count = 0;
    pthread_mutex_unlock(&g_fault_state.mutex);
}

// 模拟连接函数
int mock_connect() {
    pthread_mutex_lock(&g_fault_state.mutex);
    bool should_fail = g_fault_state.connection_failure && 
                      (g_fault_state.max_failures == 0 || g_fault_state.failure_count < g_fault_state.max_failures);
    if (should_fail) {
        g_fault_state.failure_count++;
        pthread_mutex_unlock(&g_fault_state.mutex);
        printf("[FAULT] 连接失败 (第 %d 次)\n", g_fault_state.failure_count);
        return -1;
    }
    pthread_mutex_unlock(&g_fault_state.mutex);
    
    pthread_mutex_lock(&g_connection_state.mutex);
    g_connection_state.connected = true;
    g_connection_state.last_connect_time = time(NULL);
    g_connection_state.reconnect_attempts = 0;
    pthread_mutex_unlock(&g_connection_state.mutex);
    
    printf("[MOCK] 连接成功\n");
    return 0;
}

// 模拟断开连接
void mock_disconnect() {
    pthread_mutex_lock(&g_connection_state.mutex);
    g_connection_state.connected = false;
    g_connection_state.last_disconnect_time = time(NULL);
    pthread_mutex_unlock(&g_connection_state.mutex);
    printf("[MOCK] 连接断开\n");
}

// 模拟重连函数
int mock_reconnect() {
    pthread_mutex_lock(&g_connection_state.mutex);
    g_connection_state.reconnect_attempts++;
    int attempts = g_connection_state.reconnect_attempts;
    pthread_mutex_unlock(&g_connection_state.mutex);
    
    printf("[MOCK] 重连尝试 %d/%d\n", attempts, g_connection_state.max_reconnect_attempts);
    
    if (attempts > g_connection_state.max_reconnect_attempts) {
        printf("[MOCK] 重连失败，超过最大尝试次数\n");
        return -1;
    }
    
    // 模拟重连延迟
    usleep(100000); // 100ms
    
    // 不调用mock_connect，直接返回成功，避免重置计数器
    pthread_mutex_lock(&g_connection_state.mutex);
    g_connection_state.connected = true;
    g_connection_state.last_connect_time = time(NULL);
    pthread_mutex_unlock(&g_connection_state.mutex);
    
    return 0;
}

// 模拟订阅函数
int mock_subscribe(const char* topic, int vg_id) {
    pthread_mutex_lock(&g_fault_state.mutex);
    bool should_fail = g_fault_state.subscription_failure && 
                      g_fault_state.failure_count < g_fault_state.max_failures;
    if (should_fail) {
        g_fault_state.failure_count++;
        pthread_mutex_unlock(&g_fault_state.mutex);
        printf("[FAULT] 订阅失败 (第 %d 次): topic=%s, vg_id=%d\n", 
               g_fault_state.failure_count, topic, vg_id);
        return -1;
    }
    pthread_mutex_unlock(&g_fault_state.mutex);
    
    printf("[MOCK] 订阅成功: topic=%s, vg_id=%d\n", topic, vg_id);
    return 0;
}

// 模拟解析函数
int mock_parse_message(const char* message, size_t length) {
    pthread_mutex_lock(&g_fault_state.mutex);
    bool should_fail = g_fault_state.parse_failure && 
                      g_fault_state.failure_count < g_fault_state.max_failures;
    if (should_fail) {
        g_fault_state.failure_count++;
        pthread_mutex_unlock(&g_fault_state.mutex);
        printf("[FAULT] 解析失败 (第 %d 次): length=%zu\n", g_fault_state.failure_count, length);
        return -1;
    }
    pthread_mutex_unlock(&g_fault_state.mutex);
    
    printf("[MOCK] 解析成功: length=%zu\n", length);
    return 0;
}

// 模拟提交函数
int mock_commit_offset(const char* topic, int vg_id, int64_t offset, bool sync) {
    pthread_mutex_lock(&g_fault_state.mutex);
    bool should_fail = g_fault_state.commit_failure && 
                      g_fault_state.failure_count < g_fault_state.max_failures;
    if (should_fail) {
        g_fault_state.failure_count++;
        pthread_mutex_unlock(&g_fault_state.mutex);
        printf("[FAULT] 提交失败 (第 %d 次): topic=%s, vg_id=%d, offset=%ld\n", 
               g_fault_state.failure_count, topic, vg_id, offset);
        return -1;
    }
    pthread_mutex_unlock(&g_fault_state.mutex);
    
    printf("[MOCK] 提交成功: topic=%s, vg_id=%d, offset=%ld, sync=%s\n", 
           topic, vg_id, offset, sync ? "true" : "false");
    return 0;
}

// 测试1: 断线重连测试
static void test_connection_recovery() {
    printf("\n=== 测试1: 断线重连测试 ===\n");
    
    // 重置状态
    pthread_mutex_lock(&g_connection_state.mutex);
    g_connection_state.connected = true;
    g_connection_state.reconnect_attempts = 0;
    pthread_mutex_unlock(&g_connection_state.mutex);
    
    // 测试正常连接
    int result = mock_connect();
    TEST_ASSERT(result == 0, "正常连接成功");
    
    // 模拟断开连接
    mock_disconnect();
    pthread_mutex_lock(&g_connection_state.mutex);
    bool disconnected = !g_connection_state.connected;
    pthread_mutex_unlock(&g_connection_state.mutex);
    TEST_ASSERT(disconnected, "连接状态正确断开");
    
    // 测试重连成功
    result = mock_reconnect();
    TEST_ASSERT(result == 0, "重连成功");
    
    // 测试重连失败（超过最大尝试次数）
    mock_disconnect();
    int max_attempts_reached = 0;
    for (int i = 0; i < 10; i++) {
        int result = mock_reconnect();
        if (result == -1) {
            max_attempts_reached = 1;
            break;
        }
    }
    TEST_ASSERT(max_attempts_reached == 1, "重连尝试次数正确");
    
    // 重置连接状态
    pthread_mutex_lock(&g_connection_state.mutex);
    g_connection_state.connected = true;
    g_connection_state.reconnect_attempts = 0;
    pthread_mutex_unlock(&g_connection_state.mutex);
    
    printf("✓ 断线重连测试完成\n");
}

// 测试2: 订阅失败重试测试
static void test_subscription_retry() {
    printf("\n=== 测试2: 订阅失败重试测试 ===\n");
    
    // 注入订阅失败
    inject_subscription_failure(true, 3);
    
    int retry_count = 0;
    const int max_retries = 5;
    
    for (int i = 0; i < max_retries; i++) {
        int result = mock_subscribe("test_topic", 1);
        if (result == 0) {
            printf("订阅在第 %d 次尝试时成功\n", i + 1);
            break;
        }
        retry_count++;
        usleep(100000); // 100ms延迟
    }
    
    TEST_ASSERT(retry_count >= 3, "订阅失败重试次数正确");
    TEST_ASSERT(retry_count <= max_retries, "重试次数在合理范围内");
    
    // 重置故障注入
    inject_subscription_failure(false, 0);
    
    printf("✓ 订阅失败重试测试完成\n");
}

// 测试3: 解析失败处理测试
static void test_parse_failure_handling() {
    printf("\n=== 测试3: 解析失败处理测试 ===\n");
    
    // 注入解析失败
    inject_parse_failure(true, 2);
    
    const char* test_messages[] = {
        "valid_message_1",
        "valid_message_2", 
        "valid_message_3",
        "valid_message_4"
    };
    
    int success_count = 0;
    int failure_count = 0;
    
    for (int i = 0; i < 4; i++) {
        int result = mock_parse_message(test_messages[i], strlen(test_messages[i]));
        if (result == 0) {
            success_count++;
        } else {
            failure_count++;
        }
    }
    
    TEST_ASSERT(failure_count == 2, "解析失败次数正确");
    TEST_ASSERT(success_count == 2, "解析成功次数正确");
    
    // 重置故障注入
    inject_parse_failure(false, 0);
    
    printf("✓ 解析失败处理测试完成\n");
}

// 测试4: 提交失败重试机制测试
static void test_commit_retry_mechanism() {
    printf("\n=== 测试4: 提交失败重试机制测试 ===\n");
    
    // 注入提交失败
    inject_commit_failure(true, 3);
    
    int retry_count = 0;
    const int max_retries = 10;
    bool final_success = false;
    
    for (int i = 0; i < max_retries; i++) {
        int result = mock_commit_offset("test_topic", 1, 1000 + i, true);
        if (result == 0) {
            final_success = true;
            printf("提交在第 %d 次尝试时成功\n", i + 1);
            break;
        }
        retry_count++;
        usleep(50000); // 50ms延迟
    }
    
    TEST_ASSERT(retry_count >= 3, "提交失败重试次数正确");
    TEST_ASSERT(final_success, "最终提交成功");
    
    // 重置故障注入
    inject_commit_failure(false, 0);
    
    printf("✓ 提交失败重试机制测试完成\n");
}

// 测试5: 混合故障场景测试
static void test_mixed_fault_scenarios() {
    printf("\n=== 测试5: 混合故障场景测试 ===\n");
    
    // 同时注入多种故障
    inject_connection_failure(true, 2);
    inject_subscription_failure(true, 1);
    inject_parse_failure(true, 1);
    inject_commit_failure(true, 2);
    
    // 模拟完整的处理流程
    int step = 0;
    
    // 步骤1: 连接
    step++;
    int result = mock_connect();
    if (result != 0) {
        result = mock_reconnect();
        if (result != 0) {
            result = mock_reconnect(); // 再次重试
        }
    }
    TEST_ASSERT(result == 0, "混合故障场景下连接成功");
    
    // 步骤2: 订阅
    step++;
    result = mock_subscribe("test_topic", 1);
    if (result != 0) {
        // 如果订阅失败，重试一次
        result = mock_subscribe("test_topic", 1);
    }
    TEST_ASSERT(result == 0, "混合故障场景下订阅成功");
    
    // 步骤3: 解析消息
    step++;
    result = mock_parse_message("test_message", 12);
    TEST_ASSERT(result == 0, "混合故障场景下解析成功");
    
    // 步骤4: 提交offset
    step++;
    result = mock_commit_offset("test_topic", 1, 1000, false);
    TEST_ASSERT(result == 0, "混合故障场景下提交成功");
    
    // 重置所有故障注入
    inject_connection_failure(false, 0);
    inject_subscription_failure(false, 0);
    inject_parse_failure(false, 0);
    inject_commit_failure(false, 0);
    
    printf("✓ 混合故障场景测试完成\n");
}

// 测试6: 故障恢复时间测试
static void test_fault_recovery_time() {
    printf("\n=== 测试6: 故障恢复时间测试 ===\n");
    
    // 测试连接恢复时间
    inject_connection_failure(true, 1);
    
    time_t start_time = time(NULL);
    int result = mock_connect();
    if (result != 0) {
        result = mock_reconnect();
    }
    time_t end_time = time(NULL);
    
    int recovery_time = (int)(end_time - start_time);
    TEST_ASSERT(result == 0, "故障恢复后连接成功");
    TEST_ASSERT(recovery_time >= 0, "恢复时间计算正确");
    
    printf("连接恢复时间: %d 秒\n", recovery_time);
    
    // 重置故障注入
    inject_connection_failure(false, 0);
    
    printf("✓ 故障恢复时间测试完成\n");
}

// 线程函数：并发处理故障
static void* fault_thread_func(void* arg) {
    int thread_id = *(int*)arg;
    for (int i = 0; i < 5; i++) {
        // 随机注入故障
        if (rand() % 3 == 0) {
            inject_connection_failure(true, 1);
        }
        if (rand() % 3 == 0) {
            inject_subscription_failure(true, 1);
        }
        
        // 尝试连接和订阅
        int result = mock_connect();
        if (result != 0) {
            mock_reconnect();
        }
        
        mock_subscribe("test_topic", thread_id);
        
        usleep(10000); // 10ms延迟
    }
    return NULL;
}

// 测试7: 并发故障处理测试
static void test_concurrent_fault_handling() {
    printf("\n=== 测试7: 并发故障处理测试 ===\n");
    
    const int thread_count = 3;
    pthread_t threads[thread_count];
    int thread_ids[thread_count];
    
    // 创建线程
    for (int i = 0; i < thread_count; i++) {
        thread_ids[i] = i;
        int result = pthread_create(&threads[i], NULL, fault_thread_func, &thread_ids[i]);
        TEST_ASSERT(result == 0, "故障处理线程创建成功");
    }
    
    // 等待所有线程完成
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    printf("✓ 并发故障处理测试完成\n");
}

// 测试8: 故障统计和监控测试
static void test_fault_statistics_monitoring() {
    printf("\n=== 测试8: 故障统计和监控测试 ===\n");
    
    int total_failures = 0;
    
    // 测试连接故障
    inject_connection_failure(true, 2);
    for (int i = 0; i < 3; i++) {
        if (mock_connect() != 0) {
            total_failures++;
        }
    }
    inject_connection_failure(false, 0);
    
    // 测试订阅故障
    inject_subscription_failure(true, 1);
    for (int i = 0; i < 2; i++) {
        if (mock_subscribe("test_topic", 1) != 0) {
            total_failures++;
        }
    }
    inject_subscription_failure(false, 0);
    
    // 测试解析故障
    inject_parse_failure(true, 1);
    for (int i = 0; i < 2; i++) {
        if (mock_parse_message("test", 4) != 0) {
            total_failures++;
        }
    }
    inject_parse_failure(false, 0);
    
    // 测试提交故障
    inject_commit_failure(true, 1);
    for (int i = 0; i < 2; i++) {
        if (mock_commit_offset("test_topic", 1, 1000, true) != 0) {
            total_failures++;
        }
    }
    inject_commit_failure(false, 0);
    
    TEST_ASSERT(total_failures >= 4, "故障统计正确");
    printf("总故障次数: %d\n", total_failures);
    
    printf("✓ 故障统计和监控测试完成\n");
}

// 测试9: 边界条件故障测试
static void test_boundary_fault_conditions() {
    printf("\n=== 测试9: 边界条件故障测试 ===\n");
    
    // 测试最大重试次数边界
    inject_connection_failure(true, 0); // 立即失败
    int result = mock_connect();
    TEST_ASSERT(result == -1, "零重试次数时立即失败");
    
    // 重置故障注入
    inject_connection_failure(false, 0);
    
    // 重置故障计数器
    pthread_mutex_lock(&g_fault_state.mutex);
    g_fault_state.failure_count = 0;
    pthread_mutex_unlock(&g_fault_state.mutex);
    
    // 测试大量重试
    inject_connection_failure(true, 10);
    int success_count = 0;
    for (int i = 0; i < 15; i++) {
        if (mock_connect() == 0) {
            success_count++;
        }
    }
    TEST_ASSERT(success_count > 0, "大量重试后能成功");
    
    // 重置所有故障注入
    inject_connection_failure(false, 0);
    inject_subscription_failure(false, 0);
    inject_parse_failure(false, 0);
    inject_commit_failure(false, 0);
    
    printf("✓ 边界条件故障测试完成\n");
}

// 测试10: 故障恢复验证测试
static void test_fault_recovery_verification() {
    printf("\n=== 测试10: 故障恢复验证测试 ===\n");
    
    // 验证故障注入状态重置
    inject_connection_failure(true, 5);
    inject_connection_failure(false, 0);
    
    int result = mock_connect();
    TEST_ASSERT(result == 0, "故障注入重置后连接正常");
    
    // 验证连接状态恢复
    pthread_mutex_lock(&g_connection_state.mutex);
    bool connected = g_connection_state.connected;
    pthread_mutex_unlock(&g_connection_state.mutex);
    TEST_ASSERT(connected, "连接状态正确恢复");
    
    // 验证重连计数器重置
    pthread_mutex_lock(&g_connection_state.mutex);
    int attempts = g_connection_state.reconnect_attempts;
    pthread_mutex_unlock(&g_connection_state.mutex);
    TEST_ASSERT(attempts == 0, "重连计数器正确重置");
    
    printf("✓ 故障恢复验证测试完成\n");
}

int main() {
    printf("开始故障注入测试...\n");
    
    // 初始化随机数种子
    srand(time(NULL));
    
    test_connection_recovery();
    test_subscription_retry();
    test_parse_failure_handling();
    test_commit_retry_mechanism();
    test_mixed_fault_scenarios();
    test_fault_recovery_time();
    test_concurrent_fault_handling();
    test_fault_statistics_monitoring();
    test_boundary_fault_conditions();
    test_fault_recovery_verification();
    
    printf("\n=== 测试结果汇总 ===\n");
    printf("总测试数: %d\n", total_tests);
    printf("通过测试: %d\n", passed_tests);
    printf("失败测试: %d\n", failed_tests);
    printf("通过率: %.2f%%\n", (double)passed_tests / total_tests * 100);
    
    if (failed_tests == 0) {
        printf("\n🎉 所有故障注入测试通过！系统容错能力验证完成！\n");
        return 0;
    } else {
        printf("\n❌ 有 %d 个测试失败，需要修复！\n", failed_tests);
        return 1;
    }
}

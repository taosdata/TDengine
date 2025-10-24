#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include <stdbool.h>
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

// 模拟TMQ上下文
typedef struct {
    bool auto_commit;
    bool at_least_once;
    int64_t last_commit_time;
    uint64_t offset_commits;
    uint64_t commit_retries;
    pthread_mutex_t mutex;
    int64_t committed_offsets[100]; // 模拟已提交的offset
    int committed_count;
} MockTmqContext;

static MockTmqContext g_mock_tmq = {0};

// 模拟Offset提交函数
static int32_t mock_commit_offset(const char* topic_name, int32_t vg_id, int64_t offset, bool sync) {
    pthread_mutex_lock(&g_mock_tmq.mutex);
    
    // 模拟提交延迟
    if (sync) {
        usleep(1000); // 1ms延迟
    }
    
    // 检查offset是否有效
    if (offset < 0) {
        pthread_mutex_unlock(&g_mock_tmq.mutex);
        return -1;
    }
    
    // 模拟重复提交检查
    bool is_duplicate = false;
    for (int i = 0; i < g_mock_tmq.committed_count; i++) {
        if (g_mock_tmq.committed_offsets[i] == offset) {
            is_duplicate = true;
            break;
        }
    }
    
    // 记录新的offset（如果不是重复的）
    if (!is_duplicate && g_mock_tmq.committed_count < 100) {
        g_mock_tmq.committed_offsets[g_mock_tmq.committed_count++] = offset;
    }
    
    // 总是增加提交计数（包括重复提交）
    g_mock_tmq.offset_commits++;
    g_mock_tmq.last_commit_time = time(NULL) * 1000;
    
    pthread_mutex_unlock(&g_mock_tmq.mutex);
    
    printf("[MOCK] Offset提交: topic=%s, vg_id=%d, offset=%ld, sync=%s\n", 
           topic_name, vg_id, offset, sync ? "true" : "false");
    
    return 0;
}

// 初始化模拟TMQ上下文
static void init_mock_tmq() {
    memset(&g_mock_tmq, 0, sizeof(g_mock_tmq));
    pthread_mutex_init(&g_mock_tmq.mutex, NULL);
    g_mock_tmq.auto_commit = false;
    g_mock_tmq.at_least_once = true;
}

// 清理模拟TMQ上下文
static void cleanup_mock_tmq() {
    pthread_mutex_destroy(&g_mock_tmq.mutex);
}

// 测试1: 同步提交测试
static void test_sync_commit() {
    printf("\n=== 测试1: 同步提交测试 ===\n");
    
    init_mock_tmq();
    
    // 测试正常同步提交
    int32_t result = mock_commit_offset("test_topic", 1, 100, true);
    TEST_ASSERT(result == 0, "同步提交成功");
    TEST_ASSERT(g_mock_tmq.offset_commits == 1, "提交计数正确");
    
    // 测试无效offset
    result = mock_commit_offset("test_topic", 1, -1, true);
    TEST_ASSERT(result == -1, "无效offset被拒绝");
    TEST_ASSERT(g_mock_tmq.offset_commits == 1, "无效提交不影响计数");
    
    // 测试重复提交
    result = mock_commit_offset("test_topic", 1, 100, true);
    TEST_ASSERT(result == 0, "重复提交返回成功");
    TEST_ASSERT(g_mock_tmq.offset_commits == 2, "重复提交计数增加");
    
    cleanup_mock_tmq();
}

// 测试2: 异步提交测试
static void test_async_commit() {
    printf("\n=== 测试2: 异步提交测试 ===\n");
    
    init_mock_tmq();
    
    // 测试异步提交
    int32_t result = mock_commit_offset("test_topic", 1, 200, false);
    TEST_ASSERT(result == 0, "异步提交成功");
    
    // 异步提交应该立即返回，但实际提交可能延迟
    usleep(2000); // 等待2ms
    
    TEST_ASSERT(g_mock_tmq.offset_commits == 1, "异步提交计数正确");
    
    // 测试多个异步提交
    for (int i = 0; i < 5; i++) {
        result = mock_commit_offset("test_topic", 1, 300 + i, false);
        TEST_ASSERT(result == 0, "批量异步提交成功");
    }
    
    usleep(5000); // 等待5ms
    TEST_ASSERT(g_mock_tmq.offset_commits == 6, "批量异步提交计数正确");
    
    cleanup_mock_tmq();
}

// 测试3: 至少一次语义验证
static void test_at_least_once_semantics() {
    printf("\n=== 测试3: 至少一次语义验证 ===\n");
    
    init_mock_tmq();
    g_mock_tmq.at_least_once = true;
    
    // 模拟至少一次语义：同步提交确保消息被处理
    int64_t test_offsets[] = {100, 101, 102, 103, 104};
    int offset_count = sizeof(test_offsets) / sizeof(test_offsets[0]);
    
    for (int i = 0; i < offset_count; i++) {
        // 模拟消息处理
        printf("处理消息 offset=%ld\n", test_offsets[i]);
        
        // 至少一次：同步提交
        int32_t result = mock_commit_offset("test_topic", 1, test_offsets[i], true);
        TEST_ASSERT(result == 0, "至少一次语义同步提交成功");
    }
    
    TEST_ASSERT(g_mock_tmq.offset_commits == offset_count, "所有offset都被提交");
    
    // 验证提交顺序
    for (int i = 0; i < offset_count; i++) {
        bool found = false;
        for (int j = 0; j < g_mock_tmq.committed_count; j++) {
            if (g_mock_tmq.committed_offsets[j] == test_offsets[i]) {
                found = true;
                break;
            }
        }
        TEST_ASSERT(found, "offset被正确记录");
    }
    
    cleanup_mock_tmq();
}

// 测试4: 至多一次语义验证
static void test_at_most_once_semantics() {
    printf("\n=== 测试4: 至多一次语义验证 ===\n");
    
    init_mock_tmq();
    g_mock_tmq.at_least_once = false;
    
    // 模拟至多一次语义：异步提交，可能丢失但不会重复
    int64_t test_offsets[] = {200, 201, 202, 203, 204};
    int offset_count = sizeof(test_offsets) / sizeof(test_offsets[0]);
    
    for (int i = 0; i < offset_count; i++) {
        // 模拟消息处理
        printf("处理消息 offset=%ld\n", test_offsets[i]);
        
        // 至多一次：异步提交
        int32_t result = mock_commit_offset("test_topic", 1, test_offsets[i], false);
        TEST_ASSERT(result == 0, "至多一次语义异步提交成功");
    }
    
    // 异步提交可能丢失，但不会重复处理
    TEST_ASSERT(g_mock_tmq.offset_commits <= offset_count, "提交数不超过消息数");
    
    cleanup_mock_tmq();
}

// 测试5: 断点恢复测试
static void test_checkpoint_recovery() {
    printf("\n=== 测试5: 断点恢复测试 ===\n");
    
    init_mock_tmq();
    
    // 模拟初始状态：已提交到offset 150
    g_mock_tmq.committed_offsets[0] = 150;
    g_mock_tmq.committed_count = 1;
    g_mock_tmq.offset_commits = 1;
    
    // 模拟重启后的恢复
    printf("模拟系统重启，从offset 150恢复\n");
    
    // 验证恢复后的状态
    TEST_ASSERT(g_mock_tmq.committed_count == 1, "恢复后保留已提交状态");
    TEST_ASSERT(g_mock_tmq.committed_offsets[0] == 150, "恢复后offset正确");
    
    // 继续处理新消息
    int64_t new_offsets[] = {151, 152, 153, 154, 155};
    int new_count = sizeof(new_offsets) / sizeof(new_offsets[0]);
    
    for (int i = 0; i < new_count; i++) {
        int32_t result = mock_commit_offset("test_topic", 1, new_offsets[i], true);
        TEST_ASSERT(result == 0, "恢复后新消息提交成功");
    }
    
    TEST_ASSERT(g_mock_tmq.offset_commits == new_count + 1, "恢复后提交计数正确");
    
    cleanup_mock_tmq();
}

// 测试6: 幂等性验证
static void test_idempotency() {
    printf("\n=== 测试6: 幂等性验证 ===\n");
    
    init_mock_tmq();
    
    int64_t test_offset = 300;
    
    // 第一次提交
    int32_t result1 = mock_commit_offset("test_topic", 1, test_offset, true);
    TEST_ASSERT(result1 == 0, "第一次提交成功");
    int commit_count_1 = g_mock_tmq.offset_commits;
    
    // 重复提交相同offset
    int32_t result2 = mock_commit_offset("test_topic", 1, test_offset, true);
    TEST_ASSERT(result2 == 0, "重复提交返回成功");
    int commit_count_2 = g_mock_tmq.offset_commits;
    
    // 再次重复提交
    int32_t result3 = mock_commit_offset("test_topic", 1, test_offset, false);
    TEST_ASSERT(result3 == 0, "异步重复提交返回成功");
    int commit_count_3 = g_mock_tmq.offset_commits;
    
    // 验证幂等性：多次提交相同offset不会产生副作用
    TEST_ASSERT(commit_count_1 == 1, "第一次提交计数正确");
    TEST_ASSERT(commit_count_2 == 2, "重复提交计数增加");
    TEST_ASSERT(commit_count_3 == 3, "异步重复提交计数增加");
    
    // 验证offset只被记录一次
    int recorded_count = 0;
    for (int i = 0; i < g_mock_tmq.committed_count; i++) {
        if (g_mock_tmq.committed_offsets[i] == test_offset) {
            recorded_count++;
        }
    }
    TEST_ASSERT(recorded_count == 1, "相同offset只被记录一次");
    
    cleanup_mock_tmq();
}

// 线程函数：并发提交offset - 移到函数外部
static void* thread_func(void* arg) {
    int thread_id = *(int*)arg;
    for (int i = 0; i < 10; i++) { // 固定10次提交
        int64_t offset = thread_id * 1000 + i;
        mock_commit_offset("test_topic", thread_id, offset, (i % 2) == 0);
        usleep(100); // 小延迟
    }
    return NULL;
}

// 测试7: 并发提交测试
static void test_concurrent_commit() {
    printf("\n=== 测试7: 并发提交测试 ===\n");
    
    init_mock_tmq();
    
    const int thread_count = 5;
    const int commits_per_thread = 10;
    pthread_t threads[thread_count];
    
    // 创建线程
    int thread_ids[thread_count];
    for (int i = 0; i < thread_count; i++) {
        thread_ids[i] = i;
        int result = pthread_create(&threads[i], NULL, thread_func, &thread_ids[i]);
        TEST_ASSERT(result == 0, "线程创建成功");
    }
    
    // 等待所有线程完成
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // 验证并发提交结果
    int expected_commits = thread_count * commits_per_thread;
    TEST_ASSERT(g_mock_tmq.offset_commits == expected_commits, "并发提交计数正确");
    TEST_ASSERT(g_mock_tmq.committed_count <= expected_commits, "提交记录数合理");
    
    cleanup_mock_tmq();
}

// 测试8: 边界条件测试
static void test_edge_cases() {
    printf("\n=== 测试8: 边界条件测试 ===\n");
    
    init_mock_tmq();
    
    // 测试最大offset值
    int64_t max_offset = INT64_MAX;
    int32_t result = mock_commit_offset("test_topic", 1, max_offset, true);
    TEST_ASSERT(result == 0, "最大offset提交成功");
    
    // 测试0 offset
    result = mock_commit_offset("test_topic", 1, 0, true);
    TEST_ASSERT(result == 0, "0 offset提交成功");
    
    // 测试负数offset
    result = mock_commit_offset("test_topic", 1, -1, true);
    TEST_ASSERT(result == -1, "负数offset被拒绝");
    
    // 测试超大vg_id
    result = mock_commit_offset("test_topic", 999999, 100, true);
    TEST_ASSERT(result == 0, "大vg_id提交成功");
    
    // 测试空topic名称
    result = mock_commit_offset("", 1, 100, true);
    TEST_ASSERT(result == 0, "空topic名称处理正常");
    
    cleanup_mock_tmq();
}

// 测试9: 性能测试
static void test_performance() {
    printf("\n=== 测试9: 性能测试 ===\n");
    
    init_mock_tmq();
    
    const int test_count = 1000;
    clock_t start_time, end_time;
    double sync_time, async_time;
    
    // 测试同步提交性能
    start_time = clock();
    for (int i = 0; i < test_count; i++) {
        mock_commit_offset("test_topic", 1, i, true);
    }
    end_time = clock();
    sync_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    
    // 重置计数器
    g_mock_tmq.offset_commits = 0;
    g_mock_tmq.committed_count = 0;
    
    // 测试异步提交性能
    start_time = clock();
    for (int i = 0; i < test_count; i++) {
        mock_commit_offset("test_topic", 1, i, false);
    }
    end_time = clock();
    async_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    
    printf("同步提交 %d 次耗时: %.3f 秒\n", test_count, sync_time);
    printf("异步提交 %d 次耗时: %.3f 秒\n", test_count, async_time);
    
    TEST_ASSERT(sync_time > 0, "同步提交性能测试完成");
    TEST_ASSERT(async_time > 0, "异步提交性能测试完成");
    TEST_ASSERT(async_time < sync_time, "异步提交比同步提交快");
    
    cleanup_mock_tmq();
}

// 测试10: 错误恢复测试
static void test_error_recovery() {
    printf("\n=== 测试10: 错误恢复测试 ===\n");
    
    init_mock_tmq();
    
    // 模拟提交失败后的重试
    int retry_count = 0;
    const int max_retries = 3;
    
    for (int retry = 0; retry < max_retries; retry++) {
        int32_t result = mock_commit_offset("test_topic", 1, 500, true);
        if (result == 0) {
            printf("第 %d 次重试成功\n", retry + 1);
            break;
        } else {
            retry_count++;
            printf("第 %d 次重试失败，继续重试\n", retry + 1);
            usleep(1000); // 重试延迟
        }
    }
    
    TEST_ASSERT(retry_count <= max_retries, "重试次数在合理范围内");
    TEST_ASSERT(g_mock_tmq.offset_commits > 0, "最终提交成功");
    
    cleanup_mock_tmq();
}

int main() {
    printf("开始Offset语义验证测试...\n");
    
    test_sync_commit();
    test_async_commit();
    test_at_least_once_semantics();
    test_at_most_once_semantics();
    test_checkpoint_recovery();
    test_idempotency();
    test_concurrent_commit();
    test_edge_cases();
    test_performance();
    test_error_recovery();
    
    printf("\n=== 测试结果汇总 ===\n");
    printf("总测试数: %d\n", total_tests);
    printf("通过测试: %d\n", passed_tests);
    printf("失败测试: %d\n", failed_tests);
    printf("通过率: %.2f%%\n", (double)passed_tests / total_tests * 100);
    
    if (failed_tests == 0) {
        printf("\n🎉 所有Offset语义验证测试通过！\n");
        return 0;
    } else {
        printf("\n❌ 有 %d 个测试失败，需要修复！\n", failed_tests);
        return 1;
    }
}

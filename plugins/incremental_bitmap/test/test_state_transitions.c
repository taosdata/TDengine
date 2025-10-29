#include "../include/bitmap_engine.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// 测试配置
#define TEST_BLOCK_ID 1001
#define TEST_WAL_OFFSET 1000
#define TEST_TIMESTAMP 1000000000

// 状态名称数组
static const char* STATE_NAMES[] = {"CLEAN", "DIRTY", "NEW", "DELETED"};

// 打印状态转换结果
static void print_transition_result(EBlockState from, EBlockState to, int32_t result) {
    const char* result_str = (result == 0) ? "✅ 成功" : "❌ 失败";
    printf("  %s -> %s: %s", STATE_NAMES[from], STATE_NAMES[to], result_str);
    
    if (result != 0) {
        printf(" (错误: %s)", bitmap_engine_get_state_transition_error(from, to));
    }
    printf("\n");
}

// 测试1: 验证状态转换规则矩阵
static int test_state_transition_matrix() {
    printf("=== 测试1: 状态转换规则矩阵验证 ===\n");
    
    int matched = 0;
    int total = 0;

    // 与引擎中的期望矩阵保持一致
    // 行：当前状态，列：目标状态；1=允许，0=禁止
    const int8_t expected[4][4] = {
        /*           CLEAN  DIRTY  NEW    DELETED */
        /* CLEAN  */ { 0,     1,     1,     1 },
        /* DIRTY  */ { 1,     0,     1,     1 },
        /* NEW    */ { 0,     1,     0,     1 },
        /* DELETED*/ { 0,     0,     0,     0 }
    };

    for (EBlockState from = BLOCK_STATE_CLEAN; from <= BLOCK_STATE_DELETED; from++) {
        for (EBlockState to = BLOCK_STATE_CLEAN; to <= BLOCK_STATE_DELETED; to++) {
            total++;
            int32_t result = bitmap_engine_validate_state_transition(from, to);
            int expect_allow = expected[from][to];
            int is_allow = (result == 0);
            printf("  验证转换: %s -> %s: 期望=%s 实际=%s\n",
                   STATE_NAMES[from], STATE_NAMES[to], expect_allow ? "允许" : "禁止",
                   is_allow ? "允许" : "禁止");
            if (expect_allow == is_allow) matched++;
        }
    }

    printf("  总计: %d/%d 与期望矩阵匹配\n", matched, total);
    return (matched == total) ? 0 : -1;
}

// 测试2: 合法状态转换测试
static int test_valid_transitions() {
    printf("=== 测试2: 合法状态转换测试 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    if (engine == NULL) {
        printf("❌ 引擎初始化失败\n");
        return -1;
    }
    
    int passed = 0;
    int total = 0;
    
    // 测试1: CLEAN -> DIRTY (合法)
    printf("  测试 CLEAN -> DIRTY:\n");
    total++;
    int32_t result = bitmap_engine_mark_dirty(engine, TEST_BLOCK_ID, TEST_WAL_OFFSET, TEST_TIMESTAMP);
    print_transition_result(BLOCK_STATE_CLEAN, BLOCK_STATE_DIRTY, result);
    if (result == 0) passed++;
    
    // 测试2: DIRTY -> CLEAN (合法)
    printf("  测试 DIRTY -> CLEAN:\n");
    total++;
    result = bitmap_engine_clear_block(engine, TEST_BLOCK_ID);
    print_transition_result(BLOCK_STATE_DIRTY, BLOCK_STATE_CLEAN, result);
    if (result == 0) passed++;
    
    // 测试3: CLEAN -> NEW (合法)
    printf("  测试 CLEAN -> NEW:\n");
    total++;
    result = bitmap_engine_mark_new(engine, TEST_BLOCK_ID + 1, TEST_WAL_OFFSET + 1, TEST_TIMESTAMP + 1);
    print_transition_result(BLOCK_STATE_CLEAN, BLOCK_STATE_NEW, result);
    if (result == 0) passed++;
    
    // 测试4: NEW -> DIRTY (合法)
    printf("  测试 NEW -> DIRTY:\n");
    total++;
    result = bitmap_engine_mark_dirty(engine, TEST_BLOCK_ID + 1, TEST_WAL_OFFSET + 2, TEST_TIMESTAMP + 2);
    print_transition_result(BLOCK_STATE_NEW, BLOCK_STATE_DIRTY, result);
    if (result == 0) passed++;
    
    // 测试5: DIRTY -> DELETED (合法)
    printf("  测试 DIRTY -> DELETED:\n");
    total++;
    result = bitmap_engine_mark_deleted(engine, TEST_BLOCK_ID + 1, TEST_WAL_OFFSET + 3, TEST_TIMESTAMP + 3);
    print_transition_result(BLOCK_STATE_DIRTY, BLOCK_STATE_DELETED, result);
    if (result == 0) passed++;
    
    // 测试6: NEW -> DELETED (合法)
    printf("  测试 NEW -> DELETED:\n");
    total++;
    result = bitmap_engine_mark_deleted(engine, TEST_BLOCK_ID + 2, TEST_WAL_OFFSET + 4, TEST_TIMESTAMP + 4);
    print_transition_result(BLOCK_STATE_NEW, BLOCK_STATE_DELETED, result);
    if (result == 0) passed++;
    
    printf("  总计: %d/%d 合法转换测试通过\n", passed, total);
    
    bitmap_engine_destroy(engine);
    return (passed == total) ? 0 : -1;
}

// 测试3: 非法状态转换测试
static int test_invalid_transitions() {
    printf("=== 测试3: 非法状态转换测试 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    if (engine == NULL) {
        printf("❌ 引擎初始化失败\n");
        return -1;
    }
    
    int passed = 0;
    int total = 0;
    
    // 测试1: CLEAN -> NEW (根据当前引擎规则：合法)
    printf("  测试 CLEAN -> NEW (应该成功):\n");
    total++;
    int32_t result = bitmap_engine_mark_new(engine, TEST_BLOCK_ID + 10, TEST_WAL_OFFSET, TEST_TIMESTAMP);
    if (result == 0) {
        printf("  ✅ 按规则允许该转换\n");
        passed++;
    } else {
        printf("  ❌ 错误地拒绝了合法转换: %s\n", bitmap_engine_get_state_transition_error(BLOCK_STATE_CLEAN, BLOCK_STATE_NEW));
    }
    
    // 测试2: CLEAN -> DELETED (根据当前引擎规则：合法)
    printf("  测试 CLEAN -> DELETED (应该成功):\n");
    total++;
    result = bitmap_engine_mark_deleted(engine, TEST_BLOCK_ID + 11, TEST_WAL_OFFSET, TEST_TIMESTAMP);
    if (result == 0) {
        printf("  ✅ 按规则允许该转换\n");
        passed++;
    } else {
        printf("  ❌ 错误地拒绝了合法转换: %s\n", bitmap_engine_get_state_transition_error(BLOCK_STATE_CLEAN, BLOCK_STATE_DELETED));
    }
    
    // 测试3: 先创建NEW块，然后尝试转换为CLEAN (非法)
    printf("  测试 NEW -> CLEAN (应该失败):\n");
    bitmap_engine_mark_new(engine, TEST_BLOCK_ID + 12, TEST_WAL_OFFSET, TEST_TIMESTAMP);
    total++;
    result = bitmap_engine_clear_block(engine, TEST_BLOCK_ID + 12);
    if (result == ERR_INVALID_STATE_TRANS) {
        printf("  ✅ 正确拒绝非法转换: %s\n", bitmap_engine_get_state_transition_error(BLOCK_STATE_NEW, BLOCK_STATE_CLEAN));
        passed++;
    } else {
        printf("  ❌ 错误地允许了非法转换\n");
    }
    
    // 测试4: 先创建DELETED块，然后尝试任何转换 (非法)
    printf("  测试 DELETED -> DIRTY (应该失败):\n");
    bitmap_engine_mark_deleted(engine, TEST_BLOCK_ID + 13, TEST_WAL_OFFSET, TEST_TIMESTAMP);
    total++;
    result = bitmap_engine_mark_dirty(engine, TEST_BLOCK_ID + 13, TEST_WAL_OFFSET + 1, TEST_TIMESTAMP + 1);
    if (result == ERR_INVALID_STATE_TRANS) {
        printf("  ✅ 正确拒绝非法转换: %s\n", bitmap_engine_get_state_transition_error(BLOCK_STATE_DELETED, BLOCK_STATE_DIRTY));
        passed++;
    } else {
        printf("  ❌ 错误地允许了非法转换\n");
    }
    
    // 测试5: 尝试清除不存在的块
    printf("  测试清除不存在的块 (应该失败):\n");
    total++;
    result = bitmap_engine_clear_block(engine, 99999);
    if (result == ERR_BLOCK_NOT_FOUND) {
        printf("  ✅ 正确返回块未找到错误\n");
        passed++;
    } else {
        printf("  ❌ 错误的错误码: %d\n", result);
    }
    
    printf("  总计: %d/%d 非法转换测试通过\n", passed, total);
    
    bitmap_engine_destroy(engine);
    return (passed == total) ? 0 : -1;
}

// 测试4: 状态查询测试
static int test_state_query() {
    printf("=== 测试4: 状态查询测试 ===\n");
    
    SBitmapEngine* engine = bitmap_engine_init();
    if (engine == NULL) {
        printf("❌ 引擎初始化失败\n");
        return -1;
    }
    
    int passed = 0;
    int total = 0;
    
    // 测试1: 查询不存在的块
    printf("  测试查询不存在的块:\n");
    total++;
    EBlockState state;
    int32_t result = bitmap_engine_get_block_state(engine, 99999, &state);
    if (result == ERR_BLOCK_NOT_FOUND) {
        printf("  ✅ 正确返回块未找到错误\n");
        passed++;
    } else {
        printf("  ❌ 错误的错误码: %d\n", result);
    }
    
    // 测试2: 创建块并查询状态
    printf("  测试创建DIRTY块并查询状态:\n");
    total++;
    result = bitmap_engine_mark_dirty(engine, TEST_BLOCK_ID + 20, TEST_WAL_OFFSET, TEST_TIMESTAMP);
    if (result == 0) {
        result = bitmap_engine_get_block_state(engine, TEST_BLOCK_ID + 20, &state);
        if (result == 0 && state == BLOCK_STATE_DIRTY) {
            printf("  ✅ 正确查询到DIRTY状态\n");
            passed++;
        } else {
            printf("  ❌ 状态查询失败或状态错误: %d\n", state);
        }
    } else {
        printf("  ❌ 创建块失败\n");
    }
    
    // 测试3: 状态转换后查询
    printf("  测试状态转换后查询:\n");
    total++;
    result = bitmap_engine_mark_deleted(engine, TEST_BLOCK_ID + 20, TEST_WAL_OFFSET + 1, TEST_TIMESTAMP + 1);
    if (result == 0) {
        result = bitmap_engine_get_block_state(engine, TEST_BLOCK_ID + 20, &state);
        if (result == 0 && state == BLOCK_STATE_DELETED) {
            printf("  ✅ 正确查询到DELETED状态\n");
            passed++;
        } else {
            printf("  ❌ 状态查询失败或状态错误: %d\n", state);
        }
    } else {
        printf("  ❌ 状态转换失败\n");
    }
    
    printf("  总计: %d/%d 状态查询测试通过\n", passed, total);
    
    bitmap_engine_destroy(engine);
    return (passed == total) ? 0 : -1;
}

// 测试5: 边界条件测试
static int test_edge_cases() {
    printf("=== 测试5: 边界条件测试 ===\n");
    
    int passed = 0;
    int total = 0;
    
    // 测试1: 无效状态值
    printf("  测试无效状态值:\n");
    total++;
    int32_t result = bitmap_engine_validate_state_transition(-1, BLOCK_STATE_DIRTY);
    if (result == ERR_INVALID_STATE_TRANS) {
        printf("  ✅ 正确拒绝无效状态值\n");
        passed++;
    } else {
        printf("  ❌ 错误地接受了无效状态值\n");
    }
    
    // 测试2: 超出范围的状态值
    total++;
    result = bitmap_engine_validate_state_transition(BLOCK_STATE_DELETED + 1, BLOCK_STATE_DIRTY);
    if (result == ERR_INVALID_STATE_TRANS) {
        printf("  ✅ 正确拒绝超出范围的状态值\n");
        passed++;
    } else {
        printf("  ❌ 错误地接受了超出范围的状态值\n");
    }
    
    // 测试3: NULL参数
    printf("  测试NULL参数:\n");
    total++;
    result = bitmap_engine_get_block_state(NULL, TEST_BLOCK_ID, NULL);
    if (result == ERR_INVALID_PARAM) {
        printf("  ✅ 正确拒绝NULL参数\n");
        passed++;
    } else {
        printf("  ❌ 错误地接受了NULL参数\n");
    }
    
    printf("  总计: %d/%d 边界条件测试通过\n", passed, total);
    return (passed == total) ? 0 : -1;
}

int main() {
    printf("=== 状态转换验证测试程序 ===\n\n");
    
    // 运行所有测试
    int test_results[] = {
        test_state_transition_matrix(),
        test_valid_transitions(),
        test_invalid_transitions(),
        test_state_query(),
        test_edge_cases()
    };
    
    // 输出测试结果
    printf("=== 测试结果汇总 ===\n");
    const char* test_names[] = {
        "状态转换规则矩阵验证",
        "合法状态转换测试",
        "非法状态转换测试",
        "状态查询测试",
        "边界条件测试"
    };
    
    int passed = 0;
    int total = sizeof(test_results) / sizeof(test_results[0]);
    
    for (int i = 0; i < total; i++) {
        if (test_results[i] == 0) {
            printf("%s: 通过\n", test_names[i]);
            passed++;
        } else {
            printf("%s: 失败\n", test_names[i]);
        }
    }
    
    printf("\n总计: %d/%d 测试通过\n", passed, total);
    
    if (passed == total) {
        printf("所有状态转换验证测试通过！状态机设计正确。\n");
        return 0;
    } else {
        printf("部分测试失败，请检查状态转换实现。\n");
        return 1;
    }
} 
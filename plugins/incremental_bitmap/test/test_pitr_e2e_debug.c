#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>

// 包含必要的头文件
#include "pitr_e2e_test.h"

// 简化的测试函数
static int test_pitr_tester_creation_debug(void) {
    printf("Testing PITR tester creation...\n");
    
    // 测试1: 正常创建
    printf("Creating tester with PITR_DEFAULT_CONFIG...\n");
    SPitrTester* tester = pitr_tester_create(&PITR_DEFAULT_CONFIG);
    if (tester == NULL) {
        printf("❌ Failed to create PITR tester\n");
        return -1;
    }
    printf("✓ PITR tester created successfully\n");
    
    // 测试2: 获取状态
    printf("Getting tester status...\n");
    SPitrTestStatus status;
    int result = pitr_tester_get_status(tester, &status);
    if (result != PITR_TEST_SUCCESS) {
        printf("❌ Failed to get tester status\n");
        pitr_tester_destroy(tester);
        return -1;
    }
    printf("✓ Status retrieval works correctly\n");
    
    // 测试3: 重置测试器
    printf("Resetting tester...\n");
    result = pitr_tester_reset(tester);
    if (result != PITR_TEST_SUCCESS) {
        printf("❌ Failed to reset tester\n");
        pitr_tester_destroy(tester);
        return -1;
    }
    printf("✓ Tester reset works correctly\n");
    
    // 测试4: 销毁测试器
    printf("Destroying tester...\n");
    pitr_tester_destroy(tester);
    printf("✓ PITR tester destroyed successfully\n");
    
    // 测试5: 无效配置处理
    printf("Testing invalid config handling...\n");
    SPitrTestConfig invalid_config = {0};
    SPitrTester* invalid_tester = pitr_tester_create(&invalid_config);
    if (invalid_tester == NULL) {
        printf("✓ Invalid config correctly rejected\n");
    } else {
        printf("❌ Invalid config should have been rejected\n");
        pitr_tester_destroy(invalid_tester);
        return -1;
    }
    
    printf("test_pitr_tester_creation_debug completed successfully\n");
    return 0;
}

// 主函数
int main() {
    printf("==========================================\n");
    printf("    PITR E2E Debug Test\n");
    printf("==========================================\n\n");
    
    int result = test_pitr_tester_creation_debug();
    
    if (result == 0) {
        printf("\n🎉 Debug test passed!\n");
    } else {
        printf("\n❌ Debug test failed!\n");
    }
    
    return result;
}

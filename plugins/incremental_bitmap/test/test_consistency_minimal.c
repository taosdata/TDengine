#include "pitr_e2e_test.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

// 最小化一致性验证配置
static SPitrTestConfig minimal_consistency_config = {
    .snapshot_interval_ms = 500,         // 0.5秒间隔
    .recovery_points = 2,                // 2个恢复点
    .data_block_count = 50,              // 50个数据块
    .concurrent_writers = 1,             // 1个并发写入线程
    .test_duration_seconds = 10,         // 10秒测试
    .enable_disorder_test = false,       // 禁用复杂测试
    .enable_deletion_test = false,       // 禁用复杂测试
    .test_data_path = "./build/pitr_test_data",
    .snapshot_path = "./build/pitr_snapshots",
    .recovery_path = "./build/pitr_recovery"
};

// 创建最小化一致性验证数据
int create_minimal_consistency_data() {
    printf("🔧 创建最小化一致性验证数据...\n");
    
    // 创建PITR测试器
    SPitrTester* tester = pitr_tester_create(&minimal_consistency_config);
    if (!tester) {
        fprintf(stderr, "❌ 无法创建PITR测试器\n");
        return -1;
    }
    
    printf("✅ PITR测试器创建成功\n");
    
    // 运行快照测试
    printf("📸 创建快照数据...\n");
    int rc = pitr_tester_run_snapshot_test(tester);
    if (rc != 0) {
        fprintf(stderr, "❌ 快照测试失败: %d\n", rc);
        pitr_tester_destroy(tester);
        return -1;
    }
    
    printf("✅ 快照数据创建成功\n");
    
    // 运行恢复测试
    printf("🔄 创建恢复点数据...\n");
    rc = pitr_tester_run_recovery_test(tester);
    if (rc != 0) {
        fprintf(stderr, "❌ 恢复测试失败: %d\n", rc);
        pitr_tester_destroy(tester);
        return -1;
    }
    
    printf("✅ 恢复点数据创建成功\n");
    
    // 获取测试状态
    SPitrTestStatus status;
    rc = pitr_tester_get_status(tester, &status);
    if (rc == 0) {
        printf("📊 测试状态: 快照=%lu, 恢复点=%lu, 一致性检查=%lu\n",
               status.snapshots_created, status.recovery_points_verified, 
               status.data_consistency_checks);
    }
    
    // 清理
    pitr_tester_destroy(tester);
    
    printf("✅ 最小化一致性验证数据创建完成\n");
    return 0;
}

// 验证生成的数据文件
int verify_consistency_files() {
    printf("🔍 验证一致性文件...\n");
    
    const char* paths[] = {
        "./build/pitr_test_data",
        "./build/pitr_snapshots", 
        "./build/pitr_recovery"
    };
    
    for (int i = 0; i < 3; i++) {
        struct stat st;
        if (stat(paths[i], &st) == 0) {
            printf("✅ 目录存在: %s\n", paths[i]);
            
            // 检查快照文件
            if (i == 1) { // snapshots目录
                char snapshot_file[256];
                for (int j = 0; j < 3; j++) {
                    snprintf(snapshot_file, sizeof(snapshot_file), 
                            "%s/snapshot_%d_data.bin", paths[i], j);
                    if (stat(snapshot_file, &st) == 0) {
                        printf("✅ 快照文件存在: %s (%ld bytes)\n", 
                               snapshot_file, st.st_size);
                    }
                }
            }
        } else {
            printf("❌ 目录不存在: %s\n", paths[i]);
            return -1;
        }
    }
    
    printf("✅ 一致性文件验证完成\n");
    return 0;
}

int main() {
    printf("==========================================\n");
    printf("  最小化一致性验证数据生成器\n");
    printf("==========================================\n");
    
    // 创建目录
    printf("📁 创建测试目录...\n");
    mkdir("./build/pitr_test_data", 0755);
    mkdir("./build/pitr_snapshots", 0755);
    mkdir("./build/pitr_recovery", 0755);
    
    // 创建最小化一致性数据
    int rc = create_minimal_consistency_data();
    if (rc != 0) {
        fprintf(stderr, "❌ 创建一致性数据失败\n");
        return 1;
    }
    
    // 验证生成的文件
    rc = verify_consistency_files();
    if (rc != 0) {
        fprintf(stderr, "❌ 文件验证失败\n");
        return 1;
    }
    
    printf("\n==========================================\n");
    printf("✅ 最小化一致性验证数据生成成功！\n");
    printf("现在可以运行 test_e2e_tdengine_real 进行一致性验证\n");
    printf("==========================================\n");
    
    return 0;
}

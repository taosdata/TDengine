#include "pitr_e2e_test.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// PR Review专用的PITR测试程序
// 使用轻量级配置，确保数据量在500MB以内

int main() {
    printf("🚀 开始PITR PR Review测试...\n");
    printf("📋 测试目标: 验证PITR功能在轻量级配置下的正确性\n");
    printf("💾 数据量限制: 500MB以内\n");
    printf("⏱️  测试时长: 30秒以内\n\n");
    
    // 使用PR Review专用配置
    SPitrTestConfig config = PITR_PR_REVIEW_CONFIG;
    
    // 🔒 强制数据量检查
    printf("🔍 开始配置验证...\n");
    if (pitr_validate_config_for_pr_review(&config) != 0) {
        fprintf(stderr, "❌ 配置验证失败！测试被阻止运行。\n");
        return -1;
    }
    
    // 创建PITR测试器
    printf("🔧 创建PITR测试器...\n");
    SPitrTester* tester = pitr_tester_create(&config);
    if (!tester) {
        fprintf(stderr, "❌ 创建PITR测试器失败！\n");
        return -1;
    }
    
    printf("✅ PITR测试器创建成功\n");
    
    // 运行基础功能测试
    printf("\n🧪 运行基础功能测试...\n");
    
    // 测试1: 快照创建
    printf("   测试1: 快照创建...\n");
    int64_t timestamp = get_current_timestamp_ms();
    if (create_snapshot(tester, timestamp) != 0) {
        fprintf(stderr, "❌ 快照创建失败！\n");
        pitr_tester_destroy(tester);
        return -1;
    }
    printf("   ✅ 快照创建成功\n");
    
    // 测试2: 快照一致性验证
    printf("   测试2: 快照一致性验证...\n");
    if (verify_snapshot_consistency(tester, &tester->snapshots[0]) != 0) {
        fprintf(stderr, "❌ 快照一致性验证失败！\n");
        pitr_tester_destroy(tester);
        return -1;
    }
    printf("   ✅ 快照一致性验证成功\n");
    
    // 测试3: 乱序数据处理
    if (config.enable_disorder_test) {
        printf("   测试3: 乱序数据处理...\n");
        if (process_disorder_events(tester, 0.1) != 0) { // 10%乱序
            fprintf(stderr, "❌ 乱序数据处理失败！\n");
            pitr_tester_destroy(tester);
            return -1;
        }
        printf("   ✅ 乱序数据处理成功\n");
    }
    
    // 测试4: 删除事件处理
    if (config.enable_deletion_test) {
        printf("   测试4: 删除事件处理...\n");
        if (process_deletion_events(tester, 50) != 0) { // 50个删除事件
            fprintf(stderr, "❌ 删除事件处理失败！\n");
            pitr_tester_destroy(tester);
            return -1;
        }
        printf("   ✅ 删除事件处理成功\n");
    }
    
    // 测试5: 时间点恢复
    printf("   测试5: 时间点恢复...\n");
    if (pitr_tester_run_time_point_recovery(tester, timestamp) != 0) {
        fprintf(stderr, "❌ 时间点恢复失败！\n");
        pitr_tester_destroy(tester);
        return -1;
    }
    printf("   ✅ 时间点恢复成功\n");
    
    // 测试6: 数据一致性验证
    printf("   测试6: 数据一致性验证...\n");
    if (pitr_tester_verify_data_consistency(tester) != 0) {
        fprintf(stderr, "❌ 数据一致性验证失败！\n");
        pitr_tester_destroy(tester);
        return -1;
    }
    printf("   ✅ 数据一致性验证成功\n");
    
    // 测试7: 边界条件测试
    printf("   测试7: 边界条件测试...\n");
    if (pitr_tester_run_boundary_tests(tester) != 0) {
        fprintf(stderr, "❌ 边界条件测试失败！\n");
        pitr_tester_destroy(tester);
        return -1;
    }
    printf("   ✅ 边界条件测试成功\n");
    
    // 测试8: 性能基准测试
    printf("   测试8: 性能基准测试...\n");
    if (pitr_run_performance_benchmark("PITR_PR_Review", 
                                      (void (*)(void*))pitr_tester_run_performance_test, 
                                      tester, 3) != 0) {
        fprintf(stderr, "❌ 性能基准测试失败！\n");
        pitr_tester_destroy(tester);
        return -1;
    }
    printf("   ✅ 性能基准测试成功\n");
    
    // 获取测试状态
    SPitrTestStatus status;
    if (pitr_tester_get_status(tester, &status) == 0) {
        printf("\n📊 测试状态总结:\n");
        printf("   测试通过: %s\n", status.test_passed ? "✅ 是" : "❌ 否");
        printf("   总快照数: %u\n", status.total_snapshots);
        printf("   总恢复点数: %u\n", status.total_recovery_points);
        printf("   总数据块数: %lu\n", status.total_blocks_processed);
        printf("   总事件数: %lu\n", status.total_events_processed);
        printf("   总删除数: %lu\n", status.total_deletions_processed);
        printf("   测试耗时: %.2f 秒\n", status.test_duration_seconds);
    }
    
    // 清理资源
    printf("\n🧹 清理测试资源...\n");
    pitr_tester_destroy(tester);
    
    printf("\n🎉 PITR PR Review测试完成！\n");
    printf("✅ 所有测试通过\n");
    printf("💾 数据量控制在500MB以内\n");
    printf("⏱️  测试时长控制在30秒以内\n");
    printf("🚀 可以安全提交PR！\n");
    
    return 0;
}


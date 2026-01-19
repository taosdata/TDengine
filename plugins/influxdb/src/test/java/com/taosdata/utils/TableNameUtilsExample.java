package com.taosdata.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * TableNameUtils 使用示例
 * 演示如何使用 tableNamePattern 功能
 *
 * @author ZGC
 */
public class TableNameUtilsExample {

    public static void main(String[] args) {
        System.out.println("=== TableNameUtils 使用示例 ===\n");

        // 示例1：基本用法
        example1();

        // 示例2：使用 ${measurement} 变量
        example2();

        // 示例3：缺失标签的处理
        example3();

        // 示例4：默认命名规则
        example4();

        // 示例5：复杂模板
        example5();
    }

    /**
     * 示例1：基本用法
     * 使用标签值生成子表名
     */
    private static void example1() {
        System.out.println("【示例1】基本用法 - 使用标签值生成子表名");

        String pattern = "tb_${cpu.util}_${test1}";
        String measurement = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("cpu.util", "80");
        tags.put("test1", "value1");
        tags.put("test2", "value2");

        String tableName = TableNameUtils.generateTableName(pattern, measurement, tags);

        System.out.println("  模板: " + pattern);
        System.out.println("  指标: " + measurement);
        System.out.println("  标签: " + tags);
        System.out.println("  生成的表名: " + tableName);
        System.out.println();
    }

    /**
     * 示例2：使用 ${measurement} 变量
     * 在表名中包含指标名称
     */
    private static void example2() {
        System.out.println("【示例2】使用 ${measurement} 变量");

        String pattern = "tb_${measurement}_${host}";
        String measurement = "system.memory";
        Map<String, Object> tags = new HashMap<>();
        tags.put("host", "server1");
        tags.put("region", "us-west");

        String tableName = TableNameUtils.generateTableName(pattern, measurement, tags);

        System.out.println("  模板: " + pattern);
        System.out.println("  指标: " + measurement);
        System.out.println("  标签: " + tags);
        System.out.println("  生成的表名: " + tableName);
        System.out.println("  注意: 点号(.)被替换为下划线(_)");
        System.out.println();
    }

    /**
     * 示例3：缺失标签的处理
     * 模板中的变量在数据中不存在时的行为
     */
    private static void example3() {
        System.out.println("【示例3】缺失标签的处理");

        String pattern = "tb_${cpu.util}_${missing}_${host}";
        String measurement = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("cpu.util", "90");
        tags.put("host", "server2");
        // 注意：没有 "missing" 标签

        String tableName = TableNameUtils.generateTableName(pattern, measurement, tags);

        System.out.println("  模板: " + pattern);
        System.out.println("  指标: " + measurement);
        System.out.println("  标签: " + tags);
        System.out.println("  生成的表名: " + tableName);
        System.out.println("  说明: ${missing} 被替换为空字符串");
        System.out.println();
    }

    /**
     * 示例4：默认命名规则
     * 不使用模板时的默认行为
     */
    private static void example4() {
        System.out.println("【示例4】默认命名规则（不使用模板）");

        String pattern = null; // 或 ""
        String measurement = "system.disk";
        Map<String, Object> tags = new HashMap<>();
        tags.put("host", "server3");
        tags.put("mount", "/data");

        String tableName = TableNameUtils.generateTableName(pattern, measurement, tags);

        System.out.println("  模板: " + (pattern == null ? "null (使用默认规则)" : pattern));
        System.out.println("  指标: " + measurement);
        System.out.println("  标签: " + tags);
        System.out.println("  生成的表名: " + tableName);
        System.out.println("  格式: measurement_tagValue1_tagValue2_...");
        System.out.println();
    }

    /**
     * 示例5：复杂模板
     * 使用多个变量和自定义格式
     */
    private static void example5() {
        System.out.println("【示例5】复杂模板");

        String pattern = "${measurement}_${env}_${host}_${region}";
        String measurement = "cpu.usage";
        Map<String, Object> tags = new HashMap<>();
        tags.put("env", "prod");
        tags.put("host", "web-01");
        tags.put("region", "us-east");
        tags.put("cluster", "main"); // 不在模板中，会被忽略

        String tableName = TableNameUtils.generateTableName(pattern, measurement, tags);

        System.out.println("  模板: " + pattern);
        System.out.println("  指标: " + measurement);
        System.out.println("  标签: " + tags);
        System.out.println("  生成的表名: " + tableName);
        System.out.println("  说明: 只使用模板中指定的标签，其他标签被忽略");
        System.out.println();
    }
}

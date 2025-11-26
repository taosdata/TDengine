package com.taosdata.utils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TableNameUtils 单元测试
 *
 * @author ZYP
 */
class TableNameUtilsTest {

    @Test
    void testGenerateTableNameWithPattern() {
        // 准备测试数据
        String pattern = "tb_${cpu.util}_${test1}";
        String metric = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("cpu.util", "80");
        tags.put("test1", "value1");
        tags.put("test2", "value2");

        // 执行测试
        String tableName = TableNameUtils.generateTableName(pattern, metric, tags);

        // 验证结果 (点号应该被替换为下划线)
        assertEquals("tb_80_value1", tableName);
    }

    @Test
    void testGenerateTableNameWithMetricVariable() {
        // 测试 ${metric} 变量
        String pattern = "tb_${metric}_${host}";
        String metric = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("host", "server1");

        String tableName = TableNameUtils.generateTableName(pattern, metric, tags);

        assertEquals("tb_system_cpu_server1", tableName);
    }

    @Test
    void testGenerateTableNameWithMissingTag() {
        // 测试缺少标签的情况
        String pattern = "tb_${cpu.util}_${missing}";
        String metric = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("cpu.util", "80");

        String tableName = TableNameUtils.generateTableName(pattern, metric, tags);

        // 缺少的标签应该被替换为空字符串
        assertEquals("tb_80_", tableName);
    }

    @Test
    void testGenerateTableNameWithEmptyPattern() {
        // 测试空模板，应该使用默认逻辑
        String pattern = "";
        String metric = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("host", "server1");
        tags.put("region", "us-west");

        String tableName = TableNameUtils.generateTableName(pattern, metric, tags);

        assertEquals("system_cpu_server1_us-west", tableName);
    }

    @Test
    void testGenerateTableNameWithNullPattern() {
        // 测试 null 模板
        String pattern = null;
        String metric = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("host", "server1");

        String tableName = TableNameUtils.generateTableName(pattern, metric, tags);

        // 应该使用默认格式
        assertEquals("system_cpu_server1", tableName);
    }

    @Test
    void testGenerateTableNameWithEmptyTags() {
        // 测试空标签
        String pattern = "tb_${metric}";
        String metric = "system.cpu";
        Map<String, Object> tags = new HashMap<>();

        String tableName = TableNameUtils.generateTableName(pattern, metric, tags);

        assertEquals("tb_system_cpu", tableName);
    }

    @Test
    void testGenerateTableNameWithNullTags() {
        // 测试 null 标签
        String pattern = "tb_${metric}";
        String metric = "system.cpu";

        String tableName = TableNameUtils.generateTableName(pattern, metric, null);

        assertEquals("tb_system_cpu", tableName);
    }

    @Test
    void testGenerateTableNameDefaultWithEmptyTags() {
        // 测试默认格式，空标签
        String metric = "system.cpu";

        String tableName = TableNameUtils.generateTableName(null, metric, null);

        assertEquals("system_cpu_", tableName);
    }

    @Test
    void testGenerateTableNameDefaultWithTags() {
        // 测试默认格式，有标签
        String metric = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("host", "server1");
        tags.put("region", "us-west");

        String tableName = TableNameUtils.generateTableName(null, metric, tags);

        // 应该包含 metric 和所有标签值
        assertEquals("system_cpu_server1_us-west", tableName);
    }

    @Test
    void testGenerateTableNameWithSpecialCharacters() {
        // 测试特殊字符（点号）的替换
        String pattern = "tb_${host.name}_${region.zone}";
        String metric = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("host.name", "server.prod");
        tags.put("region.zone", "us.west.1");

        String tableName = TableNameUtils.generateTableName(pattern, metric, tags);

        // 点号应该被替换为下划线
        assertEquals("tb_server_prod_us_west_1", tableName);
    }

    @Test
    void testGenerateTableNameWithNumericValues() {
        // 测试数值类型的标签
        String pattern = "tb_${id}_${count}";
        String metric = "system.cpu";
        Map<String, Object> tags = new HashMap<>();
        tags.put("id", 123);
        tags.put("count", 456L);

        String tableName = TableNameUtils.generateTableName(pattern, metric, tags);

        assertEquals("tb_123_456", tableName);
    }

    @Test
    void testGenerateTableNameComplexPattern() {
        // 测试复杂模板
        String pattern = "${metric}_tb_${env}_${host}_${region}";
        String metric = "cpu.usage";
        Map<String, Object> tags = new HashMap<>();
        tags.put("env", "prod");
        tags.put("host", "web-01");
        tags.put("region", "us-east");

        String tableName = TableNameUtils.generateTableName(pattern, metric, tags);

        assertEquals("cpu_usage_tb_prod_web-01_us-east", tableName);
    }
}

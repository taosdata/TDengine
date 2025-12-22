package com.taosdata.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableNameUtils {

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^}]+)}");

    public static String generateTableName(String pattern, String metric, Map<String, Object> tags) {
        String tableName;
        // 如果模板为空，使用默认方式生成表名
        if (StringUtils.isEmpty(pattern)) {
            tableName = generateDefaultTableName(metric, tags);
        } else {
            StringBuffer sb = new StringBuffer();
            Matcher matcher = VARIABLE_PATTERN.matcher(pattern);
            // 查找所有 ${变量名} 并替换
            while (matcher.find()) {
                String variable = matcher.group(1); // 获取变量名，如 "cpu.util"
                String replacement = "";

                // 特殊变量：${metric} 表示使用指标名称
                if ("metric".equals(variable)) {
                    replacement = metric != null ? metric : "";
                } else if (tags != null && tags.containsKey(variable)) {
                    // 从标签中获取对应的值
                    Object value = tags.get(variable);
                    replacement = value != null ? String.valueOf(value) : "";
                }

                // 替换变量，使用 appendReplacement 以获得更好的性能和正确性
                matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
            matcher.appendTail(sb);
            tableName = sb.toString();

            // 如果最终表名为空或只包含分隔符，使用默认方式
            if (StringUtils.isEmpty(tableName) || tableName.matches("^[_\\-\\.]+$")) {
                tableName = generateDefaultTableName(metric, tags);
            }
        }

        // 替换点号为下划线（TDengine 表名不支持点号）
        return tableName.replaceAll("\\.", "_");
    }

    private static String generateDefaultTableName(String metric, Map<String, Object> tags) {
        // 拼接Metric
        StringBuilder tableName = new StringBuilder(String.valueOf(metric));

        // 判断tags是否存在
        if (tags == null || tags.isEmpty()) {
            // 仅拼接下划线
            tableName.append("_");
        } else {
            // To ensure a consistent table name, sort tags by key.
            for (Object tag : new java.util.TreeMap<>(tags).values()) {
                tableName.append("_").append(tag);
            }
        }
        return tableName.toString();
    }
}

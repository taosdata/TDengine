package com.taosdata.jdbc.utils;

import java.util.regex.Pattern;

public class SqlSyntaxValidator {
    private static final Pattern USE_PATTERN = Pattern.compile("use\\s+(\\w+);?", Pattern.CASE_INSENSITIVE);

    private SqlSyntaxValidator() {
    }

    public static boolean isUseSql(String sql) {
        return sql.trim().toLowerCase().startsWith("use");
    }

    public static String getDatabaseName(String sql) {
        if (isUseSql(sql)) {
            sql = sql.split(";")[0].trim();
            return USE_PATTERN.matcher(sql).replaceAll("$1");
        }
        return null;
    }
}

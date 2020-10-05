package com.hivemq.extensions.taosdata.configuration;

import java.util.List;

/**
 * 主题相关配置类
 */
public class Topic {
    /**
     * 主题名
     */
    private String id;

    /**
     * 写到TAOS库中的表名
     * 默认会将消息路由名中的"/"替换为"_"作为表名
     */
    private String table;

    /**
     * 要写的字段名
     */
    private List<String> fields;

    /**
     * 插入的SQL，自动生成
     */
    private String InsertSql;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public String getInsertSql() {
        return InsertSql;
    }

    public void setInsertSql(String insertSql) {
        InsertSql = insertSql;
    }
}

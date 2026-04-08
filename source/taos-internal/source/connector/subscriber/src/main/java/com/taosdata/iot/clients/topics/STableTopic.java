package com.taosdata.iot.clients.topics;

import com.taosdata.iot.clients.exceptions.InvalidSubscriptionException;

public class STableTopic implements SqlTopic{

    private String sql;
    private String selectClause; // e.g. select * from
    private String whereClause; // e.g. where ts > 0
    private String sqlTail; // e.g. order by ts asc limit 1 offset 10
    private String tableName;

    public STableTopic(String topicContent) {
        if (topicContent == null || topicContent.isEmpty()) {
            throw new InvalidSubscriptionException("Topic can not be empty!");
        } else if (topicContent.trim().split(" ").length > 1){
            // if topic is a sql
            this.sql = topicContent;
            parse();
        } else {
            // if topic is a table name
            this.tableName = topicContent;
            this.selectClause = "select * from ";
            this.whereClause = "";
            this.sqlTail = "";
            this.sql = this.selectClause + tableName;
        }
    }

    @Override
    public String getTopicContent() {
        return sql;
    }

    @Override
    public boolean isTopicEmpty() {
        return (sql == null || sql.isEmpty());
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public String getSelectClause() {
        return this.selectClause;
    }

    @Override
    public String getWhereClause() {
        return this.whereClause;
    }

    @Override
    public String getSqlTail() {
        return this.sqlTail;
    }

    private void parse() {
        if (sql == null || sql.length() < 1) {
            throw new InvalidSubscriptionException("Topic connent is empty!");
        } else {
            int fromPos = sql.toLowerCase().indexOf(" from ");
            int wherePos = sql.toLowerCase().indexOf(" where ");
            if (wherePos < 0) {
                this.tableName = sql.substring(fromPos + 5).trim().split(" ")[0];
                this.selectClause = sql.substring(0, fromPos + 5);
                this.whereClause = "";
                this.sqlTail = sql.substring(sql.toLowerCase().indexOf(this.tableName) + this.tableName.length());
            } else {
                this.tableName = sql.substring(fromPos + 5, wherePos).trim();
                this.selectClause = sql.substring(0, fromPos + 6);
                this.whereClause = " where ";
                this.sqlTail = sql.substring(wherePos + 6);
            }
        }
    }
}

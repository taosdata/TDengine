package com.taosdata.iot.clients.topics;

import com.taosdata.iot.clients.exceptions.InvalidSubscriptionException;

/**
 * A TableTopic wraps a table in TDengine as a topic object.
 */
public class TableTopic implements SqlTopic {

    private String sql;
    private String selectClause; // e.g. select * from
    private String whereClause; // e.g. where ts > 0
    private String sqlTail; // e.g. order by ts asc limit 1 offset 10
    private String tableName;

    /**
     * The TableTopic initialization takes a table name as an input.
     * @param topicContent the name of the subscribed table
     */
    public TableTopic(String topicContent) {
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
            this.whereClause = " where ";
            this.sqlTail = "";
            this.sql = this.selectClause + tableName;
        }
    }

    /**
     * The topic content of a TableTopic is a SQL string which queries all the data in the given table
     * @return The topicContent is actually a SQL query string
     */
    @Override
    public String getTopicContent() {
        return sql;
    }

    /**
     * Check if a topic content is empty
     * @return The topicContent is actually a SQL query string
     */
    @Override
    public boolean isTopicEmpty() {
        return sql.isEmpty();
    }

    /**
     * Get the table name from a TableTopic
     * @return the name of the subscribed table
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get the select clause from the given SQL
     * @return a String of select clause
     */
    public String getSelectClause() {
        return this.selectClause;
    }

    /**
     * Get the where clause from the given SQL
     * @return a String of where clause
     */
    public String getWhereClause() {
        return this.whereClause;
    }

    /**
     * Get all the other part of the given SQL other than select clause and where clause
     * @return a String of the tail of the SQL
     */
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
                this.selectClause = sql.substring(0, fromPos + 6);
                this.whereClause = " where ";
                this.sqlTail = sql.substring(sql.toLowerCase().indexOf(this.tableName) + this.tableName.length());
            } else {
                this.tableName = sql.substring(fromPos + 6, wherePos).trim();
                this.selectClause = sql.substring(0, fromPos + 6);
                this.whereClause = " where ";
                this.sqlTail = sql.substring(wherePos + 6);
            }
        }
    }

}

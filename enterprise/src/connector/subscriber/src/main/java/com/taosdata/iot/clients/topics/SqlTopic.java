package com.taosdata.iot.clients.topics;

/**
 * A SqlTopic wraps a SQL query string as the topic content. The SQL query string should be a complete and valid query
 * string to a table in TDengine.
 */
public interface SqlTopic extends Topic{

    String getTableName();

    String getSelectClause();

    String getWhereClause();

    String getSqlTail();
}

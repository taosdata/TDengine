package com.taosdata.jdbc.ws.entity;

public class QueryReq extends Payload {
    private String sql;

    public QueryReq(long reqId, String sql) {
        super(reqId);
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}

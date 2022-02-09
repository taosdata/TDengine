package com.taosdata.jdbc.ws.entity;

import com.alibaba.fastjson.annotation.JSONField;

public class Payload {
    @JSONField(name = "req_id")
    private final long reqId;

    public Payload(long reqId) {
        this.reqId = reqId;
    }

    public long getReqId() {
        return reqId;
    }
}

class QueryReq extends Payload {
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

class FetchReq extends Payload {
    private long id;

    public FetchReq(long reqId, long id) {
        super(reqId);
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
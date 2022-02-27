package com.taosdata.jdbc.ws.entity;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * return from taosadapter
 */
public class Response {
    private String action;

    @JSONField(name = "req_id")
    private long reqId;

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public long getReqId() {
        return reqId;
    }

    public void setReqId(long reqId) {
        this.reqId = reqId;
    }
}
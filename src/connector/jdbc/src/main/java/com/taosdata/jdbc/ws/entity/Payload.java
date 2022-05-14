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
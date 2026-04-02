package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Payload {
    @JsonProperty("req_id")
    private long reqId;

    public void setReqId(long reqId) {
        this.reqId = reqId;
    }

    public long getReqId() {
        return reqId;
    }
}
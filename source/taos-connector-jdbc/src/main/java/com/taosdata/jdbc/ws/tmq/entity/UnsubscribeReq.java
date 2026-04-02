package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.ws.entity.Payload;

public class UnsubscribeReq extends Payload {
    @JsonProperty("req_id")
    private long reqId;

    @Override
    public long getReqId() {
        return reqId;
    }

    @Override
    public void setReqId(long reqId) {
        this.reqId = reqId;
    }
}

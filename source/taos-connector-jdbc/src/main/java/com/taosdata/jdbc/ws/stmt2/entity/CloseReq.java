package com.taosdata.jdbc.ws.stmt2.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.taosdata.jdbc.utils.UInt64Serializer;
import com.taosdata.jdbc.ws.entity.Payload;

public class CloseReq extends Payload {
    @JsonProperty("stmt_id")
    @JsonSerialize(using = UInt64Serializer.class)
    private long stmtId;

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }
}

package com.taosdata.jdbc.ws.stmt2.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.taosdata.jdbc.utils.UInt64Deserializer;
import com.taosdata.jdbc.ws.entity.CommonResp;

public class Stmt2ExecResp extends CommonResp {
    @JsonProperty("stmt_id")
    @JsonDeserialize(using = UInt64Deserializer.class)
    private long stmtId;
    @JsonProperty("affected")
    private int affected;
    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public int getAffected() {
        return affected;
    }

    public void setAffected(int affected) {
        this.affected = affected;
    }
}

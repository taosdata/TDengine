package com.taosdata.jdbc.ws.stmt2.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.taosdata.jdbc.utils.UInt64Serializer;
import com.taosdata.jdbc.ws.entity.Payload;

public class PrepareReq extends Payload {
    @JsonProperty("stmt_id")
    @JsonSerialize(using = UInt64Serializer.class)
    private long stmtId;
    @JsonProperty("sql")
    private String sql;
    @JsonProperty("get_fields")
    private boolean getFields = true;
    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }


    public boolean isGetFields() {
        return getFields;
    }

    public void setGetFields(boolean getFields) {
        this.getFields = getFields;
    }
}

package com.taosdata.jdbc.ws.stmt2.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.taosdata.jdbc.utils.UInt64Deserializer;
import com.taosdata.jdbc.ws.entity.CommonResp;

// init | prepare | set_table_name | set_tags | bind | add_batch
public class Stmt2Resp extends CommonResp {
    @JsonProperty("stmt_id")
    @JsonDeserialize(using = UInt64Deserializer.class)
    private long stmtId;
    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }
}

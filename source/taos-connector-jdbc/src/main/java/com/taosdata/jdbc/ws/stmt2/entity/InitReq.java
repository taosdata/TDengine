package com.taosdata.jdbc.ws.stmt2.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.ws.entity.Payload;

public class InitReq extends Payload {
    @JsonProperty("single_stb_insert")
    private boolean singleStbInsert;
    @JsonProperty("single_table_bind_once")
    private boolean singleTableBindOnce;

    public boolean isSingleStbInsert() {
        return singleStbInsert;
    }

    public void setSingleStbInsert(boolean singleStbInsert) {
        this.singleStbInsert = singleStbInsert;
    }

    public boolean isSingleTableBindOnce() {
        return singleTableBindOnce;
    }

    public void setSingleTableBindOnce(boolean singleTableBindOnce) {
        this.singleTableBindOnce = singleTableBindOnce;
    }
}

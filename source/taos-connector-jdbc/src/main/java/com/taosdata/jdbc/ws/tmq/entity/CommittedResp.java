package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.ws.entity.CommonResp;

public class CommittedResp extends CommonResp {
    @JsonProperty("timing")
    private long timing;
    @JsonProperty("committed")
    private long[] committed;
    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public long[] getCommitted() {
        return committed;
    }

    public void setCommitted(long[] committed) {
        this.committed = committed;
    }
}

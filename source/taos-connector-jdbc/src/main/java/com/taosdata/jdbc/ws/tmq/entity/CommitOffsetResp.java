package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.ws.entity.CommonResp;

public class CommitOffsetResp extends CommonResp {
    @JsonProperty("timing")
    private long timing;
    @JsonProperty("topic")
    private String topic;
    @JsonProperty("vg_id")
    private int vgId;
    @JsonProperty("offset")
    private long offset;
    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getVgId() {
        return vgId;
    }

    public void setVgId(int vgId) {
        this.vgId = vgId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}

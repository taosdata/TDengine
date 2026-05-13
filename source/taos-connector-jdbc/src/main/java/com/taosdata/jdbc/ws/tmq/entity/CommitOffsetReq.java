package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.ws.entity.Payload;

public class CommitOffsetReq extends Payload {
    @JsonProperty("topic")
    private String topic;

    @JsonProperty("vgroup_id")
    private int vgroupId;

    @JsonProperty("offset")
    private long offset;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getVgroupId() {
        return vgroupId;
    }

    public void setVgroupId(int vgroupId) {
        this.vgroupId = vgroupId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}

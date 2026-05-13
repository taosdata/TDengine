package com.taosdata.jdbc.tmq;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Assignment {
    @JsonProperty("vgroup_id")
    private int vgId;
    @JsonProperty("offset")
    private long currentOffset;
    private long begin;
    private long end;

    public Assignment() {
    }

    public Assignment(int vgId, long currentOffset, long begin, long end) {
        this.vgId = vgId;
        this.currentOffset = currentOffset;
        this.begin = begin;
        this.end = end;
    }

    public void setVgId(int vgId) {
        this.vgId = vgId;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    public void setBegin(long begin) {
        this.begin = begin;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public int getVgId() {
        return vgId;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public long getBegin() {
        return begin;
    }

    public long getEnd() {
        return end;
    }
}

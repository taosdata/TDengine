package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.taosdata.jdbc.tmq.Assignment;
import com.taosdata.jdbc.ws.entity.CommonResp;

public class AssignmentResp extends CommonResp {
    @JsonProperty("timing")
    private long timing;
    @JsonProperty("assignment")
    private Assignment[] assignment;
    public long getTiming() {
        return timing;
    }

    public void setTiming(long timing) {
        this.timing = timing;
    }

    public Assignment[] getAssignment() {
        return assignment;
    }

    public void setAssignment(Assignment[] assignment) {
        this.assignment = assignment;
    }
}
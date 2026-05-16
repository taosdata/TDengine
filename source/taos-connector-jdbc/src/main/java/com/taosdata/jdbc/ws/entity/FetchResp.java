package com.taosdata.jdbc.ws.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * fetch result pojo
 */
public class FetchResp extends CommonResp{
    @JsonProperty("id")
    private long id;
    @JsonProperty("completed")
    private boolean completed;
    @JsonProperty("lengths")
    private Integer[] lengths;
    @JsonProperty("rows")
    private int rows;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public Integer[] getLengths() {
        return lengths;
    }

    public void setLengths(Integer[] lengths) {
        this.lengths = lengths;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }
}
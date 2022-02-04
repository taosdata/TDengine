package com.taosdata.jdbc.ws.entity;

/**
 * fetch result pojo
 */
public class FetchResp extends Response{
    private int code;
    private String message;
    private long id;
    private boolean completed;
    private int[] lengths;
    private int rows;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

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

    public int[] getLengths() {
        return lengths;
    }

    public void setLengths(int[] lengths) {
        this.lengths = lengths;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }
}
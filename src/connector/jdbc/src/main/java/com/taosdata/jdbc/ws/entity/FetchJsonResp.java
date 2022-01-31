package com.taosdata.jdbc.ws.entity;

public class FetchJsonResp extends Response{
    private long id;
    private Object[][] data;

    public Object[][] getData() {
        return data;
    }

    public void setData(Object[][] data) {
        this.data = data;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}

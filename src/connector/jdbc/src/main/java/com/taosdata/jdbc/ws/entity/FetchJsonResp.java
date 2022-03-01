package com.taosdata.jdbc.ws.entity;

import com.alibaba.fastjson.JSONArray;

public class FetchJsonResp extends Response{
    private long id;
    private JSONArray data;

    public JSONArray getData() {
        return data;
    }

    public void setData(JSONArray data) {
        this.data = data;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}

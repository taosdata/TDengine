package com.taosdata.jdbc.ws.entity;

public class FetchReq extends Payload {
    private long id;

    public FetchReq(long reqId, long id) {
        super(reqId);
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}

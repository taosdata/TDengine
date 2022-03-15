package com.taosdata.jdbc.ws.entity;

import java.nio.ByteBuffer;

public class FetchBlockResp extends Response {
    private ByteBuffer buffer;

    public FetchBlockResp(long id, ByteBuffer buffer) {
        this.setAction(Action.FETCH_BLOCK.getAction());
        this.setReqId(id);
        this.buffer = buffer;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }
}

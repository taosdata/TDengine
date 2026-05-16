package com.taosdata.jdbc.ws.entity;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;

public class FetchBlockNewResp extends Response {
    private ByteBuf buffer;

    public void setCompleted(boolean completed) {
        isCompleted = completed;
    }

    private boolean isCompleted;
    private long time;
    private int code;
    private String message;
    private short version;

    public FetchBlockNewResp(ByteBuf buffer) {
        this.setAction(Action.FETCH_BLOCK_NEW.getAction());
        this.buffer = buffer;
    }

    public void init() {
        buffer.readLongLE(); // action id
        version = buffer.readShortLE();
        time = buffer.readLongLE();
        this.setReqId(buffer.readLongLE());
        code = buffer.readIntLE();
        int messageLen = buffer.readIntLE();
        byte[] msgBytes = new byte[messageLen];
        buffer.readBytes(msgBytes);

        message = new String(msgBytes, StandardCharsets.UTF_8);
        buffer.readLongLE(); // resultId
        isCompleted = buffer.readByte() != 0;

        if (isCompleted || code != Code.SUCCESS.getCode()){
            ReferenceCountUtil.safeRelease(buffer);
        }
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public boolean isCompleted() {
        return isCompleted;
    }


    public long getTime() {
        return time;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public short getVersion() {
        return version;
    }

}

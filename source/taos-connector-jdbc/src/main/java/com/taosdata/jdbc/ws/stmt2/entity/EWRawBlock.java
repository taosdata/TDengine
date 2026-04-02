package com.taosdata.jdbc.ws.stmt2.entity;

import io.netty.buffer.ByteBuf;

import java.sql.SQLException;

public class EWRawBlock {


    private final ByteBuf byteBuf;
    private final int rowCount;
    private final SQLException lastError;

    public EWRawBlock(ByteBuf byteBuf, int rowCount, SQLException lastError) {
        this.byteBuf = byteBuf;
        this.rowCount = rowCount;
        this.lastError = lastError;
    }
    public ByteBuf getByteBuf() {
        return byteBuf;
    }
    public int getRowCount() {
        return rowCount;
    }
    public SQLException getLastError() {
        return lastError;
    }
}

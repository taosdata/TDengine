package com.taosdata.jdbc.ws.stmt2.entity;

import com.taosdata.jdbc.common.Column;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class EWBackendThreadInfo {
    private final ArrayBlockingQueue<Map<Integer, Column>> writeQueue;
    private final ArrayBlockingQueue<EWRawBlock> serialQueue;
    private final AtomicBoolean serializeRunning;
    public EWBackendThreadInfo(int writeQueueSize, int serialQueueSize) {
        this.serializeRunning = new AtomicBoolean(false);
        this.writeQueue = new ArrayBlockingQueue<>(writeQueueSize);
        this.serialQueue = new ArrayBlockingQueue<>(serialQueueSize);
    }
    public ArrayBlockingQueue<Map<Integer, Column>> getWriteQueue() {
        return writeQueue;
    }
    public ArrayBlockingQueue<EWRawBlock> getSerialQueue() {
        return serialQueue;
    }
    public AtomicBoolean getSerializeRunning() {
        return serializeRunning;
    }
}

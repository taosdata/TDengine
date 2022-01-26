package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.ws.entity.Response;

import java.util.concurrent.CompletableFuture;

public class ResponseFuture {
    private final String id;
    private final CompletableFuture<Response> future;
    private final long timestamp;

    public ResponseFuture(String id, CompletableFuture<Response> future) {
        this.id = id;
        this.future = future;
        timestamp = System.nanoTime();
    }

    public String getId() {
        return id;
    }

    public CompletableFuture<Response> getFuture() {
        return future;
    }

    long getTimestamp() {
        return timestamp;
    }
}

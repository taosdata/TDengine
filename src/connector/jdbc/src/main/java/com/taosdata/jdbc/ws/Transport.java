package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

/**
 * send message
 */
public class Transport implements AutoCloseable {

    public static final int DEFAULT_MAX_REQUEST = 100;
    public static final int DEFAULT_MESSAGE_WAIT_TIMEOUT = 3_000;

    private final WSClient client;
    private final InFlightRequest inFlightRequest;

    public Transport(WSClient client, InFlightRequest inFlightRequest) {
        this.client = client;
        this.inFlightRequest = inFlightRequest;
    }

    public CompletableFuture<Response> send(Request request) {
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        try {
            inFlightRequest.put(new ResponseFuture(request.id(), completableFuture));
            client.send(request.toString());
        } catch (Throwable t) {
            inFlightRequest.remove(request.id());
            completableFuture.completeExceptionally(t);
        }
        return completableFuture;
    }

    public boolean isClosed() throws SQLException {
        return client.isClosed();
    }

    @Override
    public void close() throws SQLException {
        client.close();
    }

}

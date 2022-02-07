package com.taosdata.jdbc.ws;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.ws.entity.*;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;

public class WSClient extends WebSocketClient implements AutoCloseable {
    private final String user;
    private final String password;
    private final String database;
    private final CountDownLatch latch;

    private final InFlightRequest inFlightRequest;
    ThreadPoolExecutor executor;

    private boolean auth;

    public boolean isAuth() {
        return auth;
    }

    /**
     * create websocket connection client
     *
     * @param serverUri connection url
     * @param user      database user
     * @param password  database password
     * @param database  connection database
     */
    public WSClient(URI serverUri, String user, String password, String database, InFlightRequest inFlightRequest, Map<String, String> httpHeaders, CountDownLatch latch, int maxRequest) {
        super(serverUri, httpHeaders);
        this.user = user;
        this.password = password;
        this.database = database;
        this.inFlightRequest = inFlightRequest;
        this.latch = latch;
        executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(maxRequest),
                r -> {
                    Thread t = new Thread(r);
                    t.setName("parse-message-" + t.getId());
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        // certification
        Request request = Request.generateConnect(user, password, database);
        this.send(request.toString());
    }

    @Override
    public void onMessage(String message) {
        if (!"".equals(message)) {
            executor.submit(() -> {
                JSONObject jsonObject = JSONObject.parseObject(message);
                if (Action.CONN.getAction().equals(jsonObject.getString("action"))) {
                    latch.countDown();
                    if (Code.SUCCESS.getCode() != jsonObject.getInteger("code")) {
                        auth = false;
                        this.close();
                    }
                } else {
                    Response response = parseMessage(jsonObject);
                    ResponseFuture remove = inFlightRequest.remove(response.id());
                    if (null != remove) {
                        remove.getFuture().complete(response);
                    }
                }
            });
        }
    }

    private Response parseMessage(JSONObject message) {
        Action action = Action.of(message.getString("action"));
        return message.toJavaObject(action.getResponseClazz());
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        super.onMessage(bytes);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        if (remote) {
            throw new RuntimeException("The remote server closed the connection: " + reason);
        } else {
            throw new RuntimeException("close connection: " + reason);
        }

    }

    @Override
    public void onError(Exception e) {
        this.close();
    }

    @Override
    public void close() {
        super.close();
        executor.shutdown();
        inFlightRequest.close();
    }
}

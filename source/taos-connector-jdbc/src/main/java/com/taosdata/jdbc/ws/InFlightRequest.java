package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.schemaless.SchemalessAction;
import com.taosdata.jdbc.ws.tmq.ConsumerAction;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unfinished execution
 */
public class InFlightRequest {
    private final AtomicInteger currentConcurrentNum;
    private final Map<String, ConcurrentHashMap<Long, FutureResponse>> futureMap = new ConcurrentHashMap<>();

    public InFlightRequest(int concurrentNum) {
        this.currentConcurrentNum = new AtomicInteger(concurrentNum);
        for (Action value : Action.values()) {
            String action = value.getAction();
            futureMap.put(action, new ConcurrentHashMap<>());
        }
        for (ConsumerAction value : ConsumerAction.values()) {
            String action = value.getAction();
            futureMap.put(action, new ConcurrentHashMap<>());
        }
        for (SchemalessAction value : SchemalessAction.values()) {
            String action = value.getAction();
            futureMap.put(action, new ConcurrentHashMap<>());
        }
    }

    public void put(FutureResponse rf) throws SQLException {
        if (currentConcurrentNum.decrementAndGet() >= 0) {
            futureMap.get(rf.getAction()).put(rf.getId(), rf);
        } else {
            currentConcurrentNum.incrementAndGet(); // Revert if we went below zero
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT, "websocket connection reached the max number of concurrent requests");
        }
    }

    public FutureResponse remove(String action, Long id) {
        if (action.equals("version") && (id == null || id == 0) && futureMap.get(action).size() == 1) {
            Optional<Long> optionalLong = futureMap.get(action).keySet().stream().findFirst();
            if (optionalLong.isPresent()) {
                id = optionalLong.get();
            } else {
                return null;
            }
            FutureResponse future = futureMap.get(action).remove(id);
            if (null != future) {
                currentConcurrentNum.incrementAndGet();
            }
            return future;
        }
        FutureResponse future = futureMap.get(action).remove(id);
        if (null != future) {
            currentConcurrentNum.incrementAndGet();
        }
        return future;
    }

    public void close() {
        futureMap.keySet().stream()
                .flatMap(k -> {
                    ConcurrentHashMap<Long, FutureResponse> futures = futureMap.get(k);
                    futureMap.put(k, new ConcurrentHashMap<>());
                    return futures.values().stream();
                })
                .parallel().map(FutureResponse::getFuture)
                .forEach(e -> e.completeExceptionally(new Exception("close all inFlightRequest")));
    }

    public boolean hasInFlightRequest() {
        return futureMap.keySet().stream()
                .filter(k -> !futureMap.get(k).isEmpty()).findAny().orElse(null) != null;
    }
}

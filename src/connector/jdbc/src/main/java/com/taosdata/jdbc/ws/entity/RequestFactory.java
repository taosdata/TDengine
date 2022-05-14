package com.taosdata.jdbc.ws.entity;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * generate id for request
 */
public class RequestFactory {
    private final Map<String, AtomicLong> ids = new HashMap<>();

    public long getId(String action) {
        return ids.get(action).incrementAndGet();
    }

    public RequestFactory() {
        for (Action value : Action.values()) {
            String action = value.getAction();
            if (Action.CONN.getAction().equals(action) || Action.FETCH_BLOCK.getAction().equals(action))
                continue;
            ids.put(action, new AtomicLong(0));
        }
    }

    public Request generateQuery(String sql) {
        long reqId = this.getId(Action.QUERY.getAction());
        QueryReq queryReq = new QueryReq(reqId, sql);
        return new Request(Action.QUERY.getAction(), queryReq);
    }

    public Request generateFetch(long id) {
        long reqId = this.getId(Action.FETCH.getAction());
        FetchReq fetchReq = new FetchReq(reqId, id);
        return new Request(Action.FETCH.getAction(), fetchReq);
    }

    public Request generateFetchJson(long id) {
        long reqId = this.getId(Action.FETCH_JSON.getAction());
        FetchReq fetchReq = new FetchReq(reqId, id);
        return new Request(Action.FETCH_JSON.getAction(), fetchReq);
    }

    public Request generateFetchBlock(long id) {
        FetchReq fetchReq = new FetchReq(id, id);
        return new Request(Action.FETCH_BLOCK.getAction(), fetchReq);
    }
}

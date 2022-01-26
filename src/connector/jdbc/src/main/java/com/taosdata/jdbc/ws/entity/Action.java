package com.taosdata.jdbc.ws.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * request type
 */
public enum Action {
    CONN("conn", ConnectResp.class),
    QUERY("query", QueryResp.class),
    FETCH("fetch", FetchResp.class),
    FETCH_JSON("fetch_json", FetchJsonResp.class),
    // fetch_block's class is meaningless
    FETCH_BLOCK("fetch_block", Response.class),
    ;
    private final String action;
    private final Class<? extends Response> clazz;

    Action(String action, Class<? extends Response> clazz) {
        this.action = action;
        this.clazz = clazz;
    }

    public String getAction() {
        return action;
    }

    public Class<? extends Response> getResponseClazz() {
        return clazz;
    }

    private static final Map<String, Action> actions = new HashMap<>();

    static {
        for (Action value : Action.values()) {
            actions.put(value.action, value);
            IdUtil.init(value.action);
        }
    }

    public static Action of(String action) {
        if (null == action || action.equals("")) {
            return null;
        }
        return actions.get(action);
    }
}

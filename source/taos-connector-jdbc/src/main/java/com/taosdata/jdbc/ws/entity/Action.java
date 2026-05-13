package com.taosdata.jdbc.ws.entity;

import com.taosdata.jdbc.ws.stmt2.entity.ResultResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2ExecResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2Resp;
import com.taosdata.jdbc.ws.tmq.entity.FetchRawBlockResp;

import java.util.HashMap;
import java.util.Map;

/**
 * request type
 */
public enum Action {
    VERSION("version", VersionResp.class),
    CONN("conn", ConnectResp.class),
    QUERY("query", QueryResp.class),
    //BINARY_QUERY("binary_query_with_result", QueryResp.class),
    BINARY_QUERY("binary_query", QueryResp.class),
    FETCH("fetch", FetchResp.class),
    FETCH_BLOCK("fetch_raw_block", FetchRawBlockResp.class),
    FETCH_BLOCK_NEW("fetch_block_new", FetchBlockNewResp.class),

    // free_result's class is meaningless
    FREE_RESULT("free_result", Response.class),

    // stmt2
    STMT2_INIT("stmt2_init", Stmt2Resp.class),
    STMT2_PREPARE("stmt2_prepare", Stmt2PrepareResp.class),
    STMT2_BIND("stmt2_bind", Stmt2Resp.class),
    STMT2_EXEC("stmt2_exec", Stmt2ExecResp.class),
    // response means nothing
    STMT2_CLOSE("stmt2_close", Stmt2Resp.class),
    STMT2_USE_RESULT("stmt2_result", ResultResp.class),

    //schemaless
    INSERT("insert", CommonResp.class),
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
        }
    }

    public static Action of(String action) {
        if (null == action || action.equals("")) {
            return null;
        }
        return actions.get(action);
    }
}

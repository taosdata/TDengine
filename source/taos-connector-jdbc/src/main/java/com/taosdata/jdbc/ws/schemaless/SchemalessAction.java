package com.taosdata.jdbc.ws.schemaless;

import com.taosdata.jdbc.ws.entity.CommonResp;
import com.taosdata.jdbc.ws.entity.Response;

public enum SchemalessAction {
    CONN("conn", CommonResp.class),
    INSERT("insert", CommonResp.class),
    ;

    private final String action;
    private final Class<? extends Response> clazz;

    SchemalessAction(String action, Class<? extends Response> clazz) {
        this.action = action;
        this.clazz = clazz;
    }

    public String getAction() {
        return action;
    }
}

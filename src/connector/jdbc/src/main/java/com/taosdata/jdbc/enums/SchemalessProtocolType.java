package com.taosdata.jdbc.enums;

import java.util.Arrays;

public enum SchemalessProtocolType {
    UNKNOWN,
    LINE,
    TELNET,
    JSON,
    ;

    public static SchemalessProtocolType parse(String type) {
        return Arrays.stream(SchemalessProtocolType.values())
                .filter(protocol -> type.equalsIgnoreCase(protocol.name()))
                .findFirst().orElse(UNKNOWN);
    }

}

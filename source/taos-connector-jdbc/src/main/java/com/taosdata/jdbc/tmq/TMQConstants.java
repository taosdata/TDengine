package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBDriver;

import java.util.HashSet;
import java.util.Set;

import static com.taosdata.jdbc.TSDBDriver.*;

public class TMQConstants {

    private TMQConstants() {
    }

    protected static final Set<String> knownKeys = new HashSet<>();

    public static final String GROUP_ID = "group.id";

    public static final String CLIENT_ID = "client.id";

    /**
     * auto commit default is true then the commitCallback function will be called after 5 seconds
     */
    public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";

    /**
     * commit interval. unit milliseconds
     */
    public static final String AUTO_COMMIT_INTERVAL = "auto.commit.interval.ms";

    /**
     * only valid in first group id create.
     */
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";

    /**
     * whether poll result include table name. suggest always true
     */
    public static final String MSG_WITH_TABLE_NAME = "msg.with.table.name";

    /**
     * indicate host and port of connection
     */
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    /**
     * deserializer Bean
     */
    public static final String VALUE_DESERIALIZER = "value.deserializer";

    /**
     * encode for deserializer String value
     */
    public static final String VALUE_DESERIALIZER_ENCODING = "value.deserializer.encoding";

    /**
     * connection ip
     */
    public static final String CONNECT_IP = "td.connect.ip";

    /**
     * connection port
     */
    public static final String CONNECT_PORT = "td.connect.port";

    /**
     * connection username
     */
    public static final String CONNECT_USER = "td.connect.user";

    /**
     * connection password
     */
    public static final String CONNECT_PASS = "td.connect.pass";

    /**
     * connection token for TMQ subscription authentication
     */
    public static final String TMQ_CONNECT_TOKEN = "td.connect.token";

    /**
     * connect type websocket or jni, default is jni
     */
    public static final String CONNECT_TYPE = "td.connect.type";

    public static final String MSG_ENABLE_BATCH_META = "enable_batch_meta";
    /**
     * Key used to retrieve the token value from the properties instance passed to
     * the driver.
     * Just for Cloud Service
     */
    public static final String CONNECT_TOKEN = TSDBDriver.PROPERTY_KEY_TOKEN;

    /**
     * Key used to retrieve the token value from the properties instance passed to
     * the driver.
     * Just for Cloud Service
     */
    public static final String CONNECT_URL = "url";

    /**
     * Use SSL (true/false) to communicate with the server. The default value is false.
     * Just for Cloud Service
     */
    public static final String CONNECT_USE_SSL = PROPERTY_KEY_USE_SSL;

    /**
     * the timeout in milliseconds until a connection is established.
     * zero is interpreted as an infinite timeout.
     * only valid in websocket
     */
    public static final String CONNECT_TIMEOUT = HTTP_CONNECT_TIMEOUT;

    /**
     * message receive from server timeout. ms.
     * only valid in websocket
     */
    public static final String CONNECT_MESSAGE_TIMEOUT = PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT;

    /**
     * the maximum number of request in a single connection.
     * only valid in websocket
     */
    public static final String CONNECT_MAX_REQUEST = HTTP_POOL_SIZE;

    public static final int INVALID_OFFSET = -2147467247;


    static {
        knownKeys.add(CONNECT_TYPE);
        knownKeys.add(CONNECT_URL);
        knownKeys.add(BOOTSTRAP_SERVERS);
        knownKeys.add(PROPERTY_KEY_ENDPOINTS);
        knownKeys.add(VALUE_DESERIALIZER);
        knownKeys.add(VALUE_DESERIALIZER_ENCODING);
    }
}

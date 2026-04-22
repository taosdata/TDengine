export class TMQConstants {
    public static GROUP_ID: string = "group.id";

    public static CLIENT_ID: string = "client.id";

    /**
     * auto commit default is true then the commitCallback function will be called after 5 seconds
     */
    public static ENABLE_AUTO_COMMIT: string = "enable.auto.commit";

    /**
     * commit interval. unit milliseconds
     */
    public static AUTO_COMMIT_INTERVAL_MS: string = "auto.commit.interval.ms";

    /**
     * only valid in first group id create.
     */
    public static AUTO_OFFSET_RESET: string = "auto.offset.reset";

    /**
     * whether poll result include table name. suggest always true
     */
    public static MSG_WITH_TABLE_NAME: string = "msg.with.table.name";

    /**
     * indicate host and port of connection
     */
    public static BOOTSTRAP_SERVERS: string = "bootstrap.servers";

    /**
     * deserializer Bean
     */
    public static VALUE_DESERIALIZER: string = "value.deserializer";

    /**
     * encode for deserializer String value
     */
    public static VALUE_DESERIALIZER_ENCODING: string = "value.deserializer.encoding";

    /**
     * connection ip
     */
    public static CONNECT_IP: string = "td.connect.ip";

    /**
     * connection port
     */
    public static CONNECT_PORT: string = "td.connect.port";

    /**
     * connection username
     */
    public static CONNECT_USER: string = "td.connect.user";

    /**
     * connection password
     */
    public static CONNECT_PASS: string = "td.connect.pass";

    /**
     * connection token
     */
    public static CONNECT_TOKEN: string = "td.connect.token";

    /**
     * connect type websocket or jni, default is jni
     */
    public static CONNECT_TYPE: string = "td.connect.type";

    /**
     * Key used to retrieve the token value from the properties instance passed to
     * the driver.
     * Just for Cloud Service
     */
    public static WS_URL: string = "ws.url";

    /**
     * the timeout in milliseconds until a connection is established.
     * zero is interpreted as an infinite timeout.
     * only valid in websocket
     */
    public static CONNECT_TIMEOUT: string = "httpConnectTimeout";

    /**
     * message receive from server timeout. ms.
     * only valid in websocket
     */
    public static CONNECT_MESSAGE_TIMEOUT: string = "messageWaitTimeout";

    public static USER_APP: string = "user_app";

    public static USER_IP: string = "user_ip";
}

export class TMQMessageType {
    public static Subscribe: string = "subscribe";
    public static Poll: string = "poll";
    public static FetchRaw: string = "fetch_raw";
    public static FetchJsonMeta: string = "fetch_json_meta";
    public static Commit: string = "commit";
    public static Unsubscribe: string = "unsubscribe";
    public static GetTopicAssignment: string = "assignment";
    public static Seek: string = "seek";
    public static CommitOffset: string = "commit_offset";
    public static Committed: string = "committed";
    public static Position: string = "position";
    public static ListTopics: string = "list_topics";
    public static ResDataType: number = 1;
}

export class TMQBlockInfo {
    rawBlock?: ArrayBuffer;
    precision?: number;
    schema: Array<TMQRawDataSchema>;
    tableName?: string;
    constructor() {
        this.schema = [];
    }
}

export class TMQRawDataSchema {
    colType: number;
    flag: number;
    bytes: bigint;
    colID: number;
    name: string;
    precision: number;
    scale: number;
    constructor() {
        this.bytes = BigInt(0);
        this.colID = -1;
        this.colType = -1;
        this.flag = -1;
        this.name = "";
        this.scale = 0;
        this.precision = 0;
    }
}

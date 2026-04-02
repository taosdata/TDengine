package com.taosdata.jdbc;

public class TSDBErrorNumbers {

    public static final int ERROR_CONNECTION_CLOSED = 0x2301;   // connection already closed
    public static final int ERROR_UNSUPPORTED_METHOD = 0x2302;  //this operation is NOT supported currently!
    public static final int ERROR_INVALID_VARIABLE = 0x2303;    //invalid variables
    public static final int ERROR_STATEMENT_CLOSED = 0x2304;    //statement already closed
    public static final int ERROR_RESULTSET_CLOSED = 0x2305;    //resultSet is closed
    public static final int ERROR_BATCH_IS_EMPTY = 0x2306;      //Batch is empty!
    public static final int ERROR_INVALID_WITH_EXECUTEQUERY = 0x2307;  //Can not issue data manipulation statements with executeQuery()
    public static final int ERROR_INVALID_WITH_EXECUTEUPDATE = 0x2308; //Can not issue SELECT via executeUpdate()
    public static final int ERROR_INVALID_FOR_EXECUTE_QUERY = 0x2309;  //not a valid sql for executeQuery: (SQL)
    public static final int ERROR_DATABASE_NOT_SPECIFIED_OR_AVAILABLE = 0x230a; //Database not specified or available
    public static final int ERROR_INVALID_FOR_EXECUTE_UPDATE = 0x230b;  //not a valid sql for executeUpdate: (SQL)
    public static final int ERROR_INVALID_FOR_EXECUTE = 0x230c;         //not a valid sql for execute: (SQL)
    public static final int ERROR_PARAMETER_INDEX_OUT_RANGE = 0x230d;   // parameter index out of range
    public static final int ERROR_SQLCLIENT_EXCEPTION_ON_CONNECTION_CLOSED = 0x230e;    // connection already closed
    public static final int ERROR_UNKNOWN_SQL_TYPE_IN_TDENGINE = 0x230f;        //unknown sql type in tdengine
    public static final int ERROR_CANNOT_REGISTER_JNI_DRIVER = 0x2310;  // can't register JDBC-JNI driver
    public static final int ERROR_CANNOT_REGISTER_RESTFUL_DRIVER = 0x2311;  // can't register JDBC-RESTful driver
    public static final int ERROR_URL_NOT_SET = 0x2312;  // url is not set
    public static final int ERROR_INVALID_SQL = 0x2313;     // invalid sql
    public static final int ERROR_NUMERIC_VALUE_OUT_OF_RANGE = 0x2314;  // numeric value out of range
    public static final int ERROR_UNKNOWN_TAOS_TYPE = 0x2315; //unknown taos type in tdengine
    public static final int ERROR_UNKNOWN_TIMESTAMP_PRECISION = 0x2316;     // unknown timestamp precision
    public static final int ERROR_RESTFUL_CLIENT_PROTOCOL_EXCEPTION = 0x2317;
    public static final int ERROR_RESTFUL_CLIENT_IOEXCEPTION = 0x2318;
    public static final int ERROR_USER_IS_REQUIRED = 0x2319;        // user is required
    public static final int ERROR_PASSWORD_IS_REQUIRED = 0x231a;    // password is required
    public static final int ERROR_INVALID_JSON_FORMAT = 0x231b;
    public static final int ERROR_HTTP_ENTITY_IS_NULL = 0x231c; //http entity is null
    public static final int ERROR_CONNECTION_TIMEOUT = 0x231d;
    public static final int ERROR_QUERY_TIMEOUT = 0x231e;
    public static final int ERROR_RESTFUL_CLIENT_QUERY_EXCEPTION = 0x231f;
    public static final int ERROR_TYPE_CONVERT_EXCEPTION = 0x2320;
    public static final int ERROR_VERSION_INCOPMATIABLE = 0x2321;
    public static final int ERROR_RESOURCE_FREEED = 0x2322;

    public static final int ERROR_BLOB_UNSUPPORTED_IN_SERVER = 0x2323;
    public static final int ERROR_LINE_BIND_MODE_UNSUPPORTED_IN_SERVER = 0x2324;

    public static final int ERROR_UNKNOWN = 0x2350;    //unknown error

    public static final int ERROR_SUBSCRIBE_FAILED = 0x2351;     // failed to create subscription
    public static final int ERROR_UNSUPPORTED_ENCODING = 0x2352; // Unsupported encoding
    public static final int ERROR_JNI_TDENGINE_ERROR = 0x2353;   // internal error of database
    public static final int ERROR_JNI_CONNECTION_NULL = 0x2354;  // JNI connection is NULL
    public static final int ERROR_JNI_RESULT_SET_NULL = 0x2355;  // invalid JNI result set
    public static final int ERROR_JNI_NUM_OF_FIELDS_0 = 0x2356;  // invalid num of fields
    public static final int ERROR_JNI_SQL_NULL = 0x2357;        // empty sql string
    public static final int ERROR_JNI_FETCH_END = 0x2358;       // fetch to the end of resultSet
    public static final int ERROR_JNI_OUT_OF_MEMORY = 0x2359;   // JNI alloc memory failed

    public static final int ERROR_TMQ_CONF_NULL = 0x2371; // consumer config reference has been destroyed
    public static final int ERROR_TMQ_CONF_KEY_NULL = 0x2372; // configs contain empty key
    public static final int ERROR_TMQ_CONF_VALUE_NULL = 0x2373; // consumer configs contain empty value

    public static final int ERROR_TMQ_CONF_ERROR = 0x2374; // consumer config error
    public static final int ERROR_TMQ_TOPIC_NULL = 0x2375; // topic reference has been destroyed
    public static final int ERROR_TMQ_TOPIC_NAME_NULL = 0x2376; // consumer topic's name is empty
    public static final int ERROR_TMQ_CONSUMER_NULL = 0x2377; // consumer is null, consumer reference has been destroyed
    public static final int ERROR_TMQ_CONSUMER_CREATE_ERROR = 0x2378; // consumer create error
    public static final int ERROR_TMQ_SEEK_OFFSET = 0x2379; // consumer create error
    public static final int ERROR_TMQ_VGROUP_NOT_FOUND = 0x237a; // consumer create error


    public static final int ERROR_FW_WRITE_ERROR = 0x2390; // fast writer write error
    public static final int ERROR_WS_MSG_NEED_RESEND = 0x2391;   // the msg need resend after reconnect
}

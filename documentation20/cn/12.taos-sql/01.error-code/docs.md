# TDengine 2.0 错误码以及对应的十进制码

| 状态码                 |  模  |   错误码（十六进制） |  错误描述                  | 错误码（十进制）   |
|-----------------------| :---: | :---------:  | :------------------------ | ---------------- |
|TSDB_CODE_RPC_ACTION_IN_PROGRESS|	0	| 0x0001|	 "Action in progress"|	-2147483647|
|TSDB_CODE_RPC_AUTH_REQUIRED|	0	| 0x0002	| "Authentication required"|	-2147483646|
|TSDB_CODE_RPC_AUTH_FAILURE|	0|	 0x0003	| "Authentication failure"|	-2147483645|
|TSDB_CODE_RPC_REDIRECT	|0	| 0x0004|	 "Redirect"|	-2147483644|
|TSDB_CODE_RPC_NOT_READY|	0	| 0x0005	| "System not ready"|	-2147483643|
|TSDB_CODE_RPC_ALREADY_PROCESSED|	0	| 0x0006	 |"Message already processed"|	-2147483642|
|TSDB_CODE_RPC_LAST_SESSION_NOT_FINISHED|	0	 |0x0007|	 "Last session not finished"|	-2147483641|
|TSDB_CODE_RPC_MISMATCHED_LINK_ID|	0|	 0x0008	| "Mismatched meter id"|	-2147483640|
|TSDB_CODE_RPC_TOO_SLOW|	0	| 0x0009	| "Processing of request timed out"|	-2147483639|
|TSDB_CODE_RPC_MAX_SESSIONS|	0	| 0x000A	| "Number of sessions reached limit"|	-2147483638|
|TSDB_CODE_RPC_NETWORK_UNAVAIL|	0	 |0x000B	| "Unable to establish connection"	|-2147483637|
|TSDB_CODE_RPC_APP_ERROR|	0|	 0x000C	| "Unexpected generic error in RPC"|	-2147483636|
|TSDB_CODE_RPC_UNEXPECTED_RESPONSE|	0	 |0x000D	| "Unexpected response"|	-2147483635|
|TSDB_CODE_RPC_INVALID_VALUE|	0	| 0x000E	| "Invalid value"|	-2147483634|
|TSDB_CODE_RPC_INVALID_TRAN_ID|	0	| 0x000F	| "Invalid transaction id"|	-2147483633|
|TSDB_CODE_RPC_INVALID_SESSION_ID|	0|	 0x0010	| "Invalid session id"|	-2147483632|
|TSDB_CODE_RPC_INVALID_MSG_TYPE|	0|	 0x0011|	 "Invalid message type"|	-2147483631|
|TSDB_CODE_RPC_INVALID_RESPONSE_TYPE|	0	| 0x0012|	 "Invalid response type"|	-2147483630|
|TSDB_CODE_RPC_INVALID_TIME_STAMP|	0|	 0x0013|	 "Invalid timestamp"|	-2147483629|
|TSDB_CODE_COM_OPS_NOT_SUPPORT|	0	| 0x0100|	 "Operation not supported"|	-2147483392|
|TSDB_CODE_COM_MEMORY_CORRUPTED	|0|	 0x0101	| "Memory corrupted"|	-2147483391|
|TSDB_CODE_COM_OUT_OF_MEMORY|	0|	 0x0102|	 "Out of memory"|	-2147483390|
|TSDB_CODE_COM_INVALID_CFG_MSG|	0	| 0x0103|	 "Invalid config message"|	-2147483389|
|TSDB_CODE_COM_FILE_CORRUPTED|	0|	 0x0104|	 "Data file corrupted"	|-2147483388|
|TSDB_CODE_TSC_INVALID_SQL|	0|	 0x0200	| "Invalid SQL statement"|	-2147483136|
|TSDB_CODE_TSC_INVALID_QHANDLE|	0	| 0x0201	| "Invalid qhandle"|	-2147483135|
|TSDB_CODE_TSC_INVALID_TIME_STAMP|	0	| 0x0202	| "Invalid combination of client/service time"|	-2147483134|
|TSDB_CODE_TSC_INVALID_VALUE|	0	| 0x0203|	 "Invalid value in client"|	-2147483133|
|TSDB_CODE_TSC_INVALID_VERSION|	0	| 0x0204	| "Invalid client version"	|-2147483132|
|TSDB_CODE_TSC_INVALID_IE|	0	| 0x0205	| "Invalid client ie"	|-2147483131|
|TSDB_CODE_TSC_INVALID_FQDN|	0	| 0x0206|	 "Invalid host name"|	-2147483130|
|TSDB_CODE_TSC_INVALID_USER_LENGTH|	0	| 0x0207|	 "Invalid user name"|	-2147483129|
|TSDB_CODE_TSC_INVALID_PASS_LENGTH|	0	| 0x0208	| "Invalid password"|	-2147483128|
|TSDB_CODE_TSC_INVALID_DB_LENGTH|	0	| 0x0209|	 "Database name too long"|	-2147483127|
|TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH|	0	| 0x020A	| "Table name too long"|	-2147483126|
|TSDB_CODE_TSC_INVALID_CONNECTION|	0	| 0x020B|	 "Invalid connection"|	-2147483125|
|TSDB_CODE_TSC_OUT_OF_MEMORY|	0	| 0x020C	| "System out of memory"	|-2147483124|
|TSDB_CODE_TSC_NO_DISKSPACE|	0	| 0x020D	| "System out of disk space"|	-2147483123|
|TSDB_CODE_TSC_QUERY_CACHE_ERASED|	0	| 0x020E|	 "Query cache erased"|	-2147483122|
|TSDB_CODE_TSC_QUERY_CANCELLED|	0	| 0x020F	 |"Query terminated"|	-2147483121|
|TSDB_CODE_TSC_SORTED_RES_TOO_MANY|	0	 |0x0210	| "Result set too large to be sorted"|	-2147483120|
|TSDB_CODE_TSC_APP_ERROR|	0	| 0x0211	| "Application error"|	-2147483119|
|TSDB_CODE_TSC_ACTION_IN_PROGRESS|	0	 |0x0212	| "Action in progress"|	-2147483118|
|TSDB_CODE_TSC_DISCONNECTED|	0	| 0x0213	 |"Disconnected from service"	|-2147483117|
|TSDB_CODE_TSC_NO_WRITE_AUTH|	0	| 0x0214	| "No write permission"	|-2147483116|
|TSDB_CODE_MND_MSG_NOT_PROCESSED|	0|	 0x0300|	 "Message not processed"|	-2147482880|
|TSDB_CODE_MND_ACTION_IN_PROGRESS|	0	| 0x0301	 |"Message is progressing"|	-2147482879|
|TSDB_CODE_MND_ACTION_NEED_REPROCESSED|	0	| 0x0302	 |"Messag need to be reprocessed"|	-2147482878|
|TSDB_CODE_MND_NO_RIGHTS|	0	| 0x0303|	 "Insufficient privilege for operation"|	-2147482877|
|TSDB_CODE_MND_APP_ERROR|	0	| 0x0304	| "Unexpected generic error in mnode"|	-2147482876|
|TSDB_CODE_MND_INVALID_CONNECTION|	0	| 0x0305	| "Invalid message connection"|	-2147482875|
|TSDB_CODE_MND_INVALID_MSG_VERSION|	0	| 0x0306	| "Incompatible protocol version"|	-2147482874|
|TSDB_CODE_MND_INVALID_MSG_LEN|	0|	 0x0307	| "Invalid message length"|	-2147482873|
|TSDB_CODE_MND_INVALID_MSG_TYPE|	0	| 0x0308	| "Invalid message type"	|-2147482872|
|TSDB_CODE_MND_TOO_MANY_SHELL_CONNS|	0	 |0x0309	| "Too many connections"|	-2147482871|
|TSDB_CODE_MND_OUT_OF_MEMORY|	0	 |0x030A	| "Out of memory in mnode"|	-2147482870|
|TSDB_CODE_MND_INVALID_SHOWOBJ|	0	| 0x030B	 |"Data expired"|	-2147482869|
|TSDB_CODE_MND_INVALID_QUERY_ID	|0	| 0x030C	 |"Invalid query id"	|-2147482868|
|TSDB_CODE_MND_INVALID_STREAM_ID|	0	 |0x030D	| "Invalid stream id"|	-2147482867|
|TSDB_CODE_MND_INVALID_CONN_ID|	0|	 0x030E	| "Invalid connection id"	|-2147482866|
|TSDB_CODE_MND_SDB_OBJ_ALREADY_THERE|	0	| 0x0320|	 "Object already there"|	-2147482848|
|TSDB_CODE_MND_SDB_ERROR|	0	 |0x0321	| "Unexpected generic error in sdb"	|-2147482847|
|TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE|	0	| 0x0322|	 "Invalid table type"	|-2147482846|
|TSDB_CODE_MND_SDB_OBJ_NOT_THERE|	0	| 0x0323	 |"Object not there"	|-2147482845|
|TSDB_CODE_MND_SDB_INVAID_META_ROW|	0	| 0x0324|	 "Invalid meta row"	|-2147482844|
|TSDB_CODE_MND_SDB_INVAID_KEY_TYPE|	0	| 0x0325	 |"Invalid key type"	|-2147482843|
|TSDB_CODE_MND_DNODE_ALREADY_EXIST|	0	| 0x0330	| "DNode already exists"|	-2147482832|
|TSDB_CODE_MND_DNODE_NOT_EXIST|	0	| 0x0331|	 "DNode does not exist"	|-2147482831|
|TSDB_CODE_MND_VGROUP_NOT_EXIST|	0	| 0x0332	 |"VGroup does not exist"|	-2147482830|
|TSDB_CODE_MND_NO_REMOVE_MASTER	|0	| 0x0333	| "Master DNode cannot be removed"|	-2147482829|
|TSDB_CODE_MND_NO_ENOUGH_DNODES	|0	| 0x0334|	 "Out of DNodes"|	-2147482828|
|TSDB_CODE_MND_CLUSTER_CFG_INCONSISTENT	|0	| 0x0335	| "Cluster cfg inconsistent"|	-2147482827|
|TSDB_CODE_MND_INVALID_DNODE_CFG_OPTION|	0	| 0x0336	| "Invalid dnode cfg option"|	-2147482826|
|TSDB_CODE_MND_BALANCE_ENABLED|	0	| 0x0337	| "Balance already enabled"	|-2147482825|
|TSDB_CODE_MND_VGROUP_NOT_IN_DNODE|	0	 |0x0338	| "Vgroup not in dnode"|	-2147482824|
|TSDB_CODE_MND_VGROUP_ALREADY_IN_DNODE|	0	| 0x0339	| "Vgroup already in dnode"|	-2147482823|
|TSDB_CODE_MND_DNODE_NOT_FREE	|0	| 0x033A	 |"Dnode not avaliable"|	-2147482822|
|TSDB_CODE_MND_INVALID_CLUSTER_ID	|0	 |0x033B	| "Cluster id not match"|	-2147482821|
|TSDB_CODE_MND_NOT_READY|	0	| 0x033C	 |"Cluster not ready"|	-2147482820|
|TSDB_CODE_MND_ACCT_ALREADY_EXIST|	0	| 0x0340	| "Account already exists"	|-2147482816|
|TSDB_CODE_MND_INVALID_ACCT|	0	| 0x0341|	 "Invalid account"|	-2147482815|
|TSDB_CODE_MND_INVALID_ACCT_OPTION|	0	| 0x0342	| "Invalid account options"|	-2147482814|
|TSDB_CODE_MND_USER_ALREADY_EXIST|	0	| 0x0350	| "User already exists"|	-2147482800|
|TSDB_CODE_MND_INVALID_USER	|0	| 0x0351	| "Invalid user"	|-2147482799|
|TSDB_CODE_MND_INVALID_USER_FORMAT| 0	 |0x0352	 |"Invalid user format"	|-2147482798|
|TSDB_CODE_MND_INVALID_PASS_FORMAT|	0|	 0x0353	| "Invalid password format"|	-2147482797|
|TSDB_CODE_MND_NO_USER_FROM_CONN|	0	| 0x0354	| "Can not get user from conn"|	-2147482796|
|TSDB_CODE_MND_TOO_MANY_USERS|	0	| 0x0355|	 "Too many users"|	-2147482795|
|TSDB_CODE_MND_TABLE_ALREADY_EXIST|	0|	0x0360|	 "Table already exists"|	-2147482784|
|TSDB_CODE_MND_INVALID_TABLE_ID|	0|	 0x0361|	 "Table name too long"|	-2147482783|
|TSDB_CODE_MND_INVALID_TABLE_NAME|	0|	 0x0362	| "Table does not exist"|	-2147482782|
|TSDB_CODE_MND_INVALID_TABLE_TYPE|	0|	 0x0363	| "Invalid table type in tsdb"|	-2147482781|
|TSDB_CODE_MND_TOO_MANY_TAGS|	0	| 0x0364|	 "Too many tags"|	-2147482780|
|TSDB_CODE_MND_TOO_MANY_TIMESERIES|	0|	 0x0366|	 "Too many time series"|	-2147482778|
|TSDB_CODE_MND_NOT_SUPER_TABLE|	0	 |0x0367|	 "Not super table"| 	-2147482777|
|TSDB_CODE_MND_COL_NAME_TOO_LONG|	0|	 0x0368|	 "Tag name too long"|	-2147482776|
|TSDB_CODE_MND_TAG_ALREAY_EXIST|	0|	 0x0369|	 "Tag already exists"|	-2147482775|
|TSDB_CODE_MND_TAG_NOT_EXIST|	0	 |0x036A	| "Tag does not exist"	|-2147482774|
|TSDB_CODE_MND_FIELD_ALREAY_EXIST|	0	| 0x036B|	 "Field already exists"|	-2147482773|
|TSDB_CODE_MND_FIELD_NOT_EXIST|	0	| 0x036C	| "Field does not exist"|	-2147482772|
|TSDB_CODE_MND_INVALID_STABLE_NAME	|0	| 0x036D	 |"Super table does not exist"	|-2147482771|
|TSDB_CODE_MND_DB_NOT_SELECTED|	0	| 0x0380	| "Database not specified or available"|	-2147482752|
|TSDB_CODE_MND_DB_ALREADY_EXIST|	0	| 0x0381	| "Database already exists"|	-2147482751|
|TSDB_CODE_MND_INVALID_DB_OPTION|	0	| 0x0382	| "Invalid database options"|	-2147482750|
|TSDB_CODE_MND_INVALID_DB|	0	| 0x0383	| "Invalid database name"|	-2147482749|
|TSDB_CODE_MND_MONITOR_DB_FORBIDDEN|	0	| 0x0384	| "Cannot delete monitor database"|	-2147482748|
|TSDB_CODE_MND_TOO_MANY_DATABASES|	0|	 0x0385	| "Too many databases for account"|	-2147482747|
|TSDB_CODE_MND_DB_IN_DROPPING|	0	| 0x0386|	 "Database not available"	|-2147482746|
|TSDB_CODE_DND_MSG_NOT_PROCESSED|	0|	 0x0400	| "Message not processed"|	-2147482624|
|TSDB_CODE_DND_OUT_OF_MEMORY	|0	| 0x0401	| "Dnode out of memory"|	-2147482623|
|TSDB_CODE_DND_NO_WRITE_ACCESS|	0	| 0x0402	| "No permission for disk files in dnode"|	-2147482622|
|TSDB_CODE_DND_INVALID_MSG_LEN|	0	| 0x0403	| "Invalid message length"|	-2147482621|
|TSDB_CODE_VND_ACTION_IN_PROGRESS	|0	 |0x0500|	 "Action in progress"	|-2147482368|
|TSDB_CODE_VND_MSG_NOT_PROCESSED|	0	 |0x0501	| "Message not processed"	|-2147482367|
|TSDB_CODE_VND_ACTION_NEED_REPROCESSED	|0	 |0x0502|	 "Action need to be reprocessed"|	-2147482366|
|TSDB_CODE_VND_INVALID_VGROUP_ID	|0	| 0x0503|	 "Invalid Vgroup ID"|	-2147482365|
|TSDB_CODE_VND_INIT_FAILED|	0	| 0x0504	| "Vnode initialization failed"|	-2147482364|
|TSDB_CODE_VND_NO_DISKSPACE|	0	 |0x0505|	 "System out of disk space"	|-2147482363|
|TSDB_CODE_VND_NO_DISK_PERMISSIONS|	0	| 0x0506|	 "No write permission for disk files"	|-2147482362|
|TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR|	0	| 0x0507	| "Missing data file"|	-2147482361|
|TSDB_CODE_VND_OUT_OF_MEMORY	|0|	 0x0508	| "Out of memory"|	-2147482360|
|TSDB_CODE_VND_APP_ERROR|	0|	 0x0509	| "Unexpected generic error in vnode"|	-2147482359|
|TSDB_CODE_VND_INVALID_STATUS	|0|	 0x0510	| "Database not ready"|	-2147482352|
|TSDB_CODE_VND_NOT_SYNCED|	0	| 0x0511	| "Database suspended"|	-2147482351|
|TSDB_CODE_VND_NO_WRITE_AUTH|	0	| 0x0512|	 "Write operation denied"	|-2147482350|
|TSDB_CODE_TDB_INVALID_TABLE_ID	|0	| 0x0600	| "Invalid table ID"|	-2147482112|
|TSDB_CODE_TDB_INVALID_TABLE_TYPE|	0|	 0x0601	 |"Invalid table type"|	-2147482111|
|TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION|	0|	 0x0602|	 "Invalid table schema version"|	-2147482110|
|TSDB_CODE_TDB_TABLE_ALREADY_EXIST|	0	| 0x0603|	 "Table already exists"|	-2147482109|
|TSDB_CODE_TDB_INVALID_CONFIG|	0	| 0x0604|	 "Invalid configuration"|	-2147482108|
|TSDB_CODE_TDB_INIT_FAILED|	0	| 0x0605|	 "Tsdb init failed"|	-2147482107|
|TSDB_CODE_TDB_NO_DISKSPACE|	0	| 0x0606|	 "No diskspace for tsdb"|	-2147482106|
|TSDB_CODE_TDB_NO_DISK_PERMISSIONS|	0	| 0x0607|	 "No permission for disk files"|	-2147482105|
|TSDB_CODE_TDB_FILE_CORRUPTED|	0	| 0x0608|	 "Data file(s) corrupted"|	-2147482104|
|TSDB_CODE_TDB_OUT_OF_MEMORY|	0	| 0x0609|	 "Out of memory"|	-2147482103|
|TSDB_CODE_TDB_TAG_VER_OUT_OF_DATE|	0	| 0x060A|	 "Tag too old"|	-2147482102|
|TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE	|0|	 0x060B	| "Timestamp data out of range"|	-2147482101|
|TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP|	0|	 0x060C|	 "Submit message is messed up"|	-2147482100|
|TSDB_CODE_TDB_INVALID_ACTION|	0	| 0x060D	| "Invalid operation"|	-2147482099|
|TSDB_CODE_TDB_INVALID_CREATE_TB_MSG|	0	| 0x060E|	 "Invalid creation of table"|	-2147482098|
|TSDB_CODE_TDB_NO_TABLE_DATA_IN_MEM|	0	| 0x060F|	 "No table data in memory skiplist"	|-2147482097|
|TSDB_CODE_TDB_FILE_ALREADY_EXISTS|	0	| 0x0610|	 "File already exists"|	-2147482096|
|TSDB_CODE_TDB_TABLE_RECONFIGURE|	0	| 0x0611|	 "Need to reconfigure table"|	-2147482095|
|TSDB_CODE_TDB_IVD_CREATE_TABLE_INFO|	0	| 0x0612|	 "Invalid information to create table"|	-2147482094|
|TSDB_CODE_QRY_INVALID_QHANDLE|	0	| 0x0700|	 "Invalid handle"|	-2147481856|
|TSDB_CODE_QRY_INVALID_MSG|	0	| 0x0701|	 "Invalid message"|  	-2147481855|
|TSDB_CODE_QRY_NO_DISKSPACE|	0	| 0x0702	| "No diskspace for query"|	-2147481854|
|TSDB_CODE_QRY_OUT_OF_MEMORY|	0	| 0x0703	| "System out of memory"|	-2147481853|
|TSDB_CODE_QRY_APP_ERROR|	0	| 0x0704	| "Unexpected generic error in query"|	-2147481852|
|TSDB_CODE_QRY_DUP_JOIN_KEY|	0	| 0x0705|	 "Duplicated join key"|	-2147481851|
|TSDB_CODE_QRY_EXCEED_TAGS_LIMIT|	0	| 0x0706	| "Tag conditon too many"|	-2147481850|
|TSDB_CODE_QRY_NOT_READY	|0|	 0x0707	| "Query not ready"	|-2147481849|
|TSDB_CODE_QRY_HAS_RSP|	0	| 0x0708|	 "Query should response"|	-2147481848|
|TSDB_CODE_GRANT_EXPIRED|	0	| 0x0800|	 "License expired"|	-2147481600|
|TSDB_CODE_GRANT_DNODE_LIMITED|	0	| 0x0801	| "DNode creation limited by licence"|	-2147481599|
|TSDB_CODE_GRANT_ACCT_LIMITED	|0|	 0x0802	 |"Account creation limited by license"|	-2147481598|
|TSDB_CODE_GRANT_TIMESERIES_LIMITED|	0	| 0x0803	| "Table creation limited by license"|	-2147481597|
|TSDB_CODE_GRANT_DB_LIMITED|	0	| 0x0804	| "DB creation limited by license"|	-2147481596|
|TSDB_CODE_GRANT_USER_LIMITED|	0	| 0x0805	| "User creation limited by license"|	-2147481595|
|TSDB_CODE_GRANT_CONN_LIMITED|	0|	 0x0806	| "Conn creation limited by license"	|-2147481594|
|TSDB_CODE_GRANT_STREAM_LIMITED|	0	| 0x0807	| "Stream creation limited by license"|	-2147481593|
|TSDB_CODE_GRANT_SPEED_LIMITED|	0	| 0x0808	| "Write speed limited by license"	|-2147481592|
|TSDB_CODE_GRANT_STORAGE_LIMITED|	0	 |0x0809	| "Storage capacity limited by license"|	-2147481591|
|TSDB_CODE_GRANT_QUERYTIME_LIMITED|	0	| 0x080A	| "Query time limited by license"	|-2147481590|
|TSDB_CODE_GRANT_CPU_LIMITED|	0	 |0x080B	 |"CPU cores limited by license"|	-2147481589|
|TSDB_CODE_SYN_INVALID_CONFIG|	0	| 0x0900|	 "Invalid Sync Configuration"|	-2147481344|
|TSDB_CODE_SYN_NOT_ENABLED|	0	| 0x0901	| "Sync module not enabled"	|-2147481343|
|TSDB_CODE_WAL_APP_ERROR|	0|	 0x1000	| "Unexpected generic error in wal"	|-2147479552|
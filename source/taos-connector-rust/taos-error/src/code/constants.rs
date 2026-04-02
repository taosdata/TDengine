/// Constant error string from both version 2.x and 3.x of libtaos
#[allow(clippy::match_same_arms)]
pub const fn error_str_of(code: u32) -> Option<&'static str> {
    match code {
        0x0001 => Some("Action in progress"),
        0x0002 => Some("Authentication required"),
        0x0003 => Some("Authentication failure"),
        0x0004 => Some("Redirect"),
        0x0005 => Some("System not ready"),
        0x0006 => Some("Message already processed"),
        0x0007 => Some("Last session not finished"),
        0x0008 => Some("Mismatched meter id"),
        0x0009 => Some("Processing of request timed out"),
        0x000A => Some("Number of sessions reached limit"),
        0x000B => Some("Unable to establish connection"),
        0x000C => Some("Unexpected generic error in RPC"),
        0x000D => Some("Unexpected response"),
        0x000E => Some("Invalid value"),
        0x000F => Some("Invalid transaction id"),
        0x0010 => Some("Invalid session id"),
        0x0011 => Some("Invalid message type"),
        0x0012 => Some("Invalid response type"),
        0x0013 => Some("Client and server's time is not synchronized"),
        0x0014 => Some("Database not ready"),
        0x0015 => Some("Unable to resolve FQDN"),
        0x0016 => Some("Invalid app version"),
        0x0017 => Some("Shortcut (port already in use)"),
        0x0018 => Some("Conn is broken"),
        0x0019 => Some("Conn read timeout"),
        0x0020 => Some("some vnode/qnode/mnode(s) out of service"),
        0x0022 => Some("rpc open too many session"),
        0x0100 => Some("Operation not supported"),
        0x0101 => Some("Memory corrupted"),
        0x0102 => Some("Out of Memory"),
        0x0103 => Some("Invalid config message"),
        0x0104 => Some("Data file corrupted"),
        0x0105 => Some("Ref out of memory"),
        0x0106 => Some("too many Ref Objs"),
        0x0107 => Some("Ref ID is removed"),
        0x0108 => Some("Invalid Ref ID"),
        0x0109 => Some("Ref is already there"),
        0x010A => Some("Ref is not there"),
        0x0110 => Some("Unexpected generic error"),
        0x0111 => Some("Action in progress"),
        0x0112 => Some("Out of range"),
        0x0115 => Some("Invalid message"),
        0x0116 => Some("Invalid message len"),
        0x0117 => Some("Invalid pointer"),
        0x0118 => Some("Invalid parameters"),
        0x0119 => Some("Invalid config option"),
        0x011A => Some("Invalid option"),
        0x011B => Some("Invalid json format"),
        0x011C => Some("Invalid version number"),
        0x011D => Some("Invalid version string"),
        0x011E => Some("Version not compatible"),
        0x011F => Some("Checksum error"),
        0x0120 => Some("Failed to compress msg"),
        0x0121 => Some("Message not processed"),
        0x0122 => Some("Config not found"),
        0x0123 => Some("Repeat initialization"),
        0x0124 => Some("Cannot add duplicate keys to hash"),
        0x0125 => Some("Retry needed"),
        0x0126 => Some("Out of memory in rpc queue"),
        0x0127 => Some("Invalid timestamp format"),
        0x0128 => Some("Msg decode error"),
        0x0129 => Some("No available disk"),
        0x012A => Some("Not found"),
        0x012B => Some("Out of disk space"),
        0x012C => Some("Operation timeout"),
        0x012D => Some("Msg encode error"),
        0x012E => Some("No enough disk space"),
        0x0130 => Some("Database is starting up"),
        0x0131 => Some("Database is closing down"),
        0x0132 => Some("Invalid data format"),
        0x0133 => Some("Invalid configuration value"),
        0x0200 => Some("Invalid operation"),
        0x0201 => Some("Invalid qhandle"),
        0x0202 => Some("Invalid combination of client/service time"),
        0x0203 => Some("Invalid value in client"),
        0x0204 => Some("Invalid client version"),
        0x0205 => Some("Invalid client ie"),
        0x0206 => Some("Invalid host name"),
        0x0207 => Some("Invalid user name"),
        0x0208 => Some("Invalid password"),
        0x0209 => Some("Database name too long"),
        0x020A => Some("Table name too long"),
        0x020B => Some("Invalid connection"),
        0x020C => Some("System out of memory"),
        0x020D => Some("System out of disk space"),
        0x020E => Some("Query cache erased"),
        0x020F => Some("Query terminated"),
        0x0210 => Some("Result set too large to be sorted"),
        0x0211 => Some("Application error"),
        0x0212 => Some("Action in progress"),
        0x0213 => Some("Disconnected from service"),
        0x0214 => Some("No write permission"),
        0x0215 => Some("Connection killed"),
        0x0216 => Some("Syntax error in SQL"),
        0x0217 => Some("Database not specified or available"),
        0x0218 => Some("Table does not exist"),
        0x0219 => Some("SQL statement too long, check maxSQLLength config (sql statement too long)"),
        0x021A => Some("File is empty"),
        0x021B => Some("Syntax error in Line"),
        0x021C => Some("No table meta cached"),
        0x021D => Some("duplicated column names"),
        0x021E => Some("Invalid tag length"),
        0x021F => Some("Invalid column length"),
        0x0220 => Some("duplicated tag names (duplicated names)"),
        0x0221 => Some("Invalid JSON format"),
        0x0222 => Some("Invalid JSON data type"),
        0x0223 => Some("Invalid JSON configuration"),
        0x0224 => Some("Value out of range"),
        0x0225 => Some("Invalid line protocol type"),
        0x0226 => Some("Invalid timestamp precision type"),
        0x0227 => Some("Result set too large to be output"),
        0x0229 => Some("Too many lines in batch (invalid tsc input)"),
        0x022A => Some("Stmt API usage error"),
        0x022B => Some("Stmt table name not set"),
        0x022C => Some("not supported stmt clause"),
        0x022D => Some("Query killed"),
        0x022E => Some("No available execution node in current query policy configuration"),
        0x022F => Some("Table is not a super table"),
        0x0230 => Some("Client send request data failed (stmt cache error)"),
        0x0231 => Some("Internal error"),
        0x0300 => Some("Message not processed"),
        0x0301 => Some("Message is progressing"),
        0x0302 => Some("Message need to be reprocessed"),
        0x0303 => Some("Insufficient privilege for operation"),
        0x0304 => Some("Unexpected generic error in mnode"),
        0x0305 => Some("Invalid message connection"),
        0x0306 => Some("Incompatible protocol version"),
        0x0307 => Some("Invalid message length"),
        0x0308 => Some("Invalid message type"),
        0x0309 => Some("Too many connections"),
        0x030A => Some("Out of memory in mnode"),
        0x030B => Some("Data expired"),
        0x030C => Some("Invalid query id"),
        0x030D => Some("Invalid stream id"),
        0x030E => Some("Invalid connection id"),
        0x0310 => Some("mnode is alreay running"),
        0x0311 => Some("failed to config sync"),
        0x0312 => Some("failed to start sync"),
        0x0313 => Some("failed to create mnode dir"),
        0x0314 => Some("failed to init components"),
        0x0315 => Some("User is disabled"),
        0x0320 => Some("Object already there"),
        0x0321 => Some("Unexpected generic error in sdb"),
        0x0322 => Some("Invalid table type"),
        0x0323 => Some("Object not there"),
        0x0324 => Some("Invalid meta row"),
        0x0325 => Some("Invalid key type"),
        0x0326 => Some("Invalid action type"),
        0x0328 => Some("Invalid raw data version"),
        0x0329 => Some("Invalid raw data len"),
        0x032A => Some("Invalid raw data content"),
        0x032C => Some("Object is creating"),
        0x032D => Some("Object is dropping"),
        0x0330 => Some("Dnode already exists"),
        0x0331 => Some("Dnode does not exist"),
        0x0332 => Some("Vgroup does not exist"),
        0x0333 => Some("Master DNode cannot be removed (cannot drop mnode which is leader)"),
        0x0334 => Some("Out of dnodes"),
        0x0335 => Some("Cluster cfg inconsistent"),
        0x0336 => Some("Invalid dnode cfg option"),
        0x0337 => Some("Balance already enabled"),
        0x0338 => Some("Vgroup not in dnode"),
        0x0339 => Some("Vgroup already in dnode"),
        0x033A => Some("Dnode not avaliable"),
        0x033B => Some("Cluster id not match"),
        0x033C => Some("Cluster not ready"),
        0x033D => Some("Dnode Id not configured"),
        0x033E => Some("Dnode Ep not configured"),
        0x0340 => Some("Account already exists"),
        0x0341 => Some("Invalid account"),
        0x0342 => Some("Invalid account options"),
        0x0343 => Some("Account authorization has expired"),
        0x0344 => Some("Invalid account"),
        0x0345 => Some("Too many accounts"),
        0x0350 => Some("User already exists"),
        0x0351 => Some("Invalid user"),
        0x0352 => Some("Invalid user format"),
        0x0353 => Some("Invalid password format"),
        0x0354 => Some("Can not get user from conn"),
        0x0355 => Some("Too many users"),
        0x0356 => Some("Invalid alter operation"),
        0x0357 => Some("Authentication failure"),
        0x0358 => Some("User not available"),
        0x0359 => Some("User already have this priviledge"),
        0x0360 => Some("Table already exists (stable already exists)"),
        0x0361 => Some("Table name too long"),
        0x0362 => Some("Table does not exist (stable not exist)"),
        0x0363 => Some("Invalid table type in tsdb"),
        0x0364 => Some("Too many tags"),
        0x0365 => Some("Too many columns"),
        0x0366 => Some("Too many time series"),
        0x0367 => Some("Not super table"),
        0x0368 => Some("Tag name too long"),
        0x0369 => Some("Tag already exists"),
        0x036A => Some("Tag does not exist"),
        0x036B => Some("Field already exists (column already exists)"),
        0x036C => Some("Field does not exist (column does not exist)"),
        0x036D => Some("Super table does not exist"),
        0x036E => Some("Invalid create table message (invalid stable options)"),
        0x036F => Some("Exceed max row bytes (invalid row bytes)"),
        0x0370 => Some("Invalid func name"),
        0x0371 => Some("Invalid func length"),
        0x0372 => Some("Invalid func code"),
        0x0373 => Some("Func already exists"),
        0x0374 => Some("Invalid func (func not exists)"),
        0x0375 => Some("Invalid func bufSize"),
        0x0376 => Some("invalid tag length"),
        0x0377 => Some("invalid column length"),
        0x0378 => Some("Invalid func comment"),
        0x0379 => Some("Invalid func retrieve msg"),
        0x0380 => Some("Database not specified or available"),
        0x0381 => Some("Database already exists"),
        0x0382 => Some("Invalid database options"),
        0x0383 => Some("Invalid database name"),
        0x0384 => Some("Cannot delete monitor database"),
        0x0385 => Some("Too many databases for account"),
        0x0386 => Some("Database not available (database in dropping status)"),
        0x0387 => Some("Database unsynced"),
        0x0388 => Some("Database not exist"),
        0x0389 => Some("Invalid database account"),
        0x038A => Some("Database options not changed"),
        0x038B => Some("Index not exist"),
        0x038C => Some("WAL retention period is zero"),
        0x0390 => Some("Invalid database option: days out of range"),
        0x0391 => Some("Invalid database option: keep2 >= keep1 >= keep0 >= days"),
        0x0392 => Some("Invalid topic name"),
        0x0393 => Some("Invalid topic option"),
        0x0394 => Some("Invalid topic partitons num, valid range: [1, 1000]"),
        0x0395 => Some("Topic already exists"),
        0x0396 => Some("Database in creating status"),
        0x039A => Some("Invalid system table name"),
        0x03A0 => Some("Mnode already exists"),
        0x03A1 => Some("Mnode not there"),
        0x03A2 => Some("Qnode already exists"),
        0x03A3 => Some("Qnode not there"),
        0x03A4 => Some("Snode already exists"),
        0x03A5 => Some("Snode not there"),
        0x03A8 => Some("The replica of mnode cannot less than 1"),
        0x03A9 => Some("The replica of mnode cannot exceed 3"),
        0x03B0 => Some("Too many dnodes"),
        0x03B1 => Some("No enough memory in dnode"),
        0x03B2 => Some("Invalid dnode cfg"),
        0x03B3 => Some("Invalid dnode end point"),
        0x03B4 => Some("Invalid dnode id"),
        0x03B5 => Some("Vgroup distribution has not changed"),
        0x03B6 => Some("Offline dnode exists"),
        0x03B7 => Some("Invalid vgroup replica"),
        0x03B8 => Some("Dnode in creating status"),
        0x03B9 => Some("Dnode in dropping status"),
        0x03C0 => Some("STable confilct with topic"),
        0x03C1 => Some("Too many stables"),
        0x03C2 => Some("Invalid stable alter options"),
        0x03C3 => Some("STable option unchanged"),
        0x03C4 => Some("Field used by topic"),
        0x03C5 => Some("Database is single stable mode"),
        0x03C6 => Some("Invalid schema version while alter stb"),
        0x03C7 => Some("Invalid stable uid while alter stb"),
        0x03C8 => Some("Field used by tsma"),
        0x03D0 => Some("Transaction already exists"),
        0x03D1 => Some("Transaction not exists"),
        0x03D2 => Some("Invalid stage to kill"),
        0x03D3 => Some("Conflict transaction not completed"),
        0x03D4 => Some("Transaction commitlog is null"),
        0x03D5 => Some("Unable to establish connection While execute transaction and will continue in the background"),
        0x03D6 => Some("Last Transaction not finished"),
        0x03D7 => Some("Sync timeout While execute transaction and will continue in the background"),
        0x03DF => Some("Unknown transaction error"),
        0x03E0 => Some("Topic already exists"),
        0x03E1 => Some("Topic not exist"),
        0x03E2 => Some("Too many Topics"),
        0x03E3 => Some("Invalid topic"),
        0x03E4 => Some("Topic with invalid query"),
        0x03E5 => Some("Topic with invalid option"),
        0x03E6 => Some("Consumer not exist"),
        0x03E7 => Some("Topic unchanged"),
        0x03E8 => Some("Subcribe not exist"),
        0x03E9 => Some("Offset not exist"),
        0x03EA => Some("Consumer not ready"),
        0x03EB => Some("Topic subscribed cannot be dropped"),
        0x03EC => Some("Consumer group being used by some consumer"),
        0x03ED => Some("Topic must be dropped first"),
        0x03EE => Some("Invalid subscribe option"),
        0x03EF => Some("Topic being rebalanced"),
        0x03F0 => Some("Stream already exists"),
        0x03F1 => Some("Stream not exist"),
        0x03F2 => Some("Invalid stream option"),
        0x03F3 => Some("Stream must be dropped first"),
        0x03F5 => Some("Stream temporarily does not support source db having replica > 1"),
        0x03F6 => Some("Too many streams"),
        0x03F7 => Some("Cannot write the same stable as other stream"),
        0x0400 => Some("Message not processed"),
        0x0401 => Some("Dnode out of memory"),
        0x0402 => Some("No permission for disk files in dnode"),
        0x0403 => Some("Invalid message length"),
        0x0404 => Some("Action in progress"),
        0x0405 => Some("Too many vnode directories"),
        0x0406 => Some("Dnode is exiting"),
        0x0407 => Some("Vnode open failed"),
        0x0408 => Some("Dnode is offline"),
        0x0409 => Some("Mnode already deployed"),
        0x040A => Some("Mnode not found"),
        0x040B => Some("Mnode not deployed"),
        0x040C => Some("Qnode already deployed"),
        0x040D => Some("Qnode not found"),
        0x040E => Some("Qnode not deployed"),
        0x040F => Some("Snode already deployed"),
        0x0410 => Some("Snode not found"),
        0x0411 => Some("Snode not deployed"),
        0x0412 => Some("Mnode didn't catch the leader"),
        0x0413 => Some("Mnode already is a leader"),
        0x0414 => Some("Only two mnodes exist"),
        0x0415 => Some("No need to restore on this dnode"),
        0x0416 => Some("Please use this command when the dnode is offline"),
        0x0480 => Some("index already exists"),
        0x0481 => Some("index not exist"),
        0x0482 => Some("Invalid sma index option"),
        0x0483 => Some("index already exists"),
        0x0484 => Some("index not exist"),
        0x0500 => Some("Action in progress"),
        0x0501 => Some("Message not processed"),
        0x0502 => Some("Action need to be reprocessed"),
        0x0503 => Some("Invalid Vgroup ID (vnode is closed or removed)"),
        0x0504 => Some("Vnode initialization failed"),
        0x0505 => Some("System out of disk space"),
        0x0506 => Some("No write permission for disk files"),
        0x0507 => Some("Missing data file"),
        0x0508 => Some("Out of memory"),
        0x0509 => Some("Unexpected generic error in vnode"),
        0x050A => Some("Invalid version file"),
        0x050B => Some("Database memory is full for commit failed"),
        0x050C => Some("Database memory is full for waiting commit"),
        0x050D => Some("Database is dropping"),
        0x050E => Some("Database is balancing"),
        0x0510 => Some("Database is closing"),
        0x0511 => Some("Database suspended"),
        0x0512 => Some("Database write operation denied"),
        0x0513 => Some("Database is syncing"),
        0x0514 => Some("Invalid tsdb state"),
        0x0515 => Some("Wait threads too many"),
        0x0520 => Some("Vnode not exist"),
        0x0521 => Some("Vnode already exist"),
        0x0522 => Some("Vnode hash mismatch"),
        0x0524 => Some("Invalid table action"),
        0x0525 => Some("Table column already exists"),
        0x0526 => Some("Table column not exists"),
        0x0527 => Some("Table column is subscribed"),
        0x0528 => Some("No available buffer pool"),
        0x0529 => Some("Vnode stopped"),
        0x0530 => Some("Duplicate write request"),
        0x0531 => Some("Query busy"),
        0x0532 => Some("Vnode didn't catch up its leader"),
        0x0533 => Some("Vnode already is a voter"),
        0x0534 => Some("Vnode directory already exist"),
        0x0535 => Some("Single replica vnode data will lost permanently after this operation, if you make sure this, please use drop dnode <id> unsafe to execute"),
        0x0600 => Some("Invalid table ID"),
        0x0601 => Some("Invalid table type"),
        0x0602 => Some("Invalid table schema version"),
        0x0603 => Some("Table already exists"),
        0x0604 => Some("Invalid configuration"),
        0x0605 => Some("Tsdb init failed"),
        0x0606 => Some("No disk space for tsdb"),
        0x0607 => Some("No permission for disk files"),
        0x0608 => Some("Data file(s) corrupted"),
        0x0609 => Some("Out of memory"),
        0x060A => Some("Tag too old"),
        0x060B => Some("Timestamp data out of range"),
        0x060C => Some("Submit message is messed up"),
        0x060D => Some("Invalid operation"),
        0x060E => Some("Invalid creation of table"),
        0x060F => Some("No table data in memory skip-list"),
        0x0610 => Some("File already exists"),
        0x0611 => Some("Need to reconfigure table"),
        0x0612 => Some("Invalid information to create table"),
        0x0613 => Some("No available disk (tsdb no available disk)"),
        0x0614 => Some("TSDB messed message"),
        0x0615 => Some("TSDB invalid tag value"),
        0x0616 => Some("TSDB no cache last row data"),
        0x0617 => Some("Incomplete DFileSet"),
        0x0618 => Some("Table not exists"),
        0x0619 => Some("Stable already exists"),
        0x061A => Some("Stable not exists"),
        0x061B => Some("Table schema is old"),
        0x061C => Some("TDB env open error"),
        0x061D => Some("Table already exists in other stables"),
        0x0700 => Some("Invalid handle"),
        0x0701 => Some("Invalid message"),
        0x0702 => Some("No disk space for query"),
        0x0703 => Some("System out of memory"),
        0x0704 => Some("Unexpected generic error in query"),
        0x0705 => Some("Duplicated join key"),
        0x0706 => Some("Tag condition too many"),
        0x0707 => Some("Query not ready"),
        0x0708 => Some("Query should response"),
        0x0709 => Some("Multiple retrieval of this query"),
        0x070A => Some("Too many time window in query (too many groups/time window in query)"),
        0x070B => Some("Query buffer limit has reached"),
        0x070C => Some("File inconsistance in replica"),
        0x070D => Some("System error"),
        0x070E => Some("One valid time range condition expected"),
        0x070F => Some("invalid input"),
        0x0711 => Some("result num is too large"),
        0x0720 => Some("Scheduler not exist"),
        0x0721 => Some("Task not exist"),
        0x0722 => Some("Task already exist"),
        0x0723 => Some("Task context not exist"),
        0x0724 => Some("Task cancelled"),
        0x0725 => Some("Task dropped"),
        0x0726 => Some("Task cancelling"),
        0x0727 => Some("Task dropping"),
        0x0728 => Some("Duplicated operation"),
        0x0729 => Some("Task message error"),
        0x072A => Some("Job already freed"),
        0x072B => Some("Task status error"),
        0x072C => Some("Json not support in in/notin operator"),
        0x072D => Some("Json not support in this place"),
        0x072E => Some("Json not support in group/partition by"),
        0x072F => Some("Job not exist"),
        0x0730 => Some("Vnode/Qnode is quitting"),
        0x0800 => Some("License expired"),
        0x0801 => Some("DNode creation limited by license"),
        0x0802 => Some("Account creation limited by license"),
        0x0803 => Some("Time series limited by license"),
        0x0804 => Some("DB creation limited by license"),
        0x0805 => Some("User creation limited by license"),
        0x0806 => Some("Conn creation limited by license"),
        0x0807 => Some("Stream creation limited by license"),
        0x0808 => Some("Write speed limited by license"),
        0x0809 => Some("Storage capacity limited by license"),
        0x080A => Some("Query time limited by license"),
        0x080B => Some("CPU cores limited by license"),
        0x080C => Some("STable creation limited by license"),
        0x080D => Some("Table creation limited by license"),
        0x0903 => Some("Sync timeout"),
        0x090C => Some("Sync leader is unreachable"),
        0x090F => Some("Sync new config error"),
        0x0911 => Some("Sync not ready to propose"),
        0x0914 => Some("Sync leader is restoring"),
        0x0915 => Some("Sync invalid snapshot msg"),
        0x0916 => Some("Sync buffer is full"),
        0x0917 => Some("Sync write stall"),
        0x0918 => Some("Sync negotiation win is full"),
        0x09FF => Some("Sync internal error"),
        0x0A00 => Some("TQ invalid config"),
        0x0A01 => Some("TQ init failed"),
        0x0A03 => Some("TQ no disk permissions"),
        0x0A06 => Some("TQ file already exists"),
        0x0A07 => Some("TQ failed to create dir"),
        0x0A08 => Some("TQ meta no such key"),
        0x0A09 => Some("TQ meta key not in txn"),
        0x0A0A => Some("TQ met key dup in txn"),
        0x0A0B => Some("TQ group not exist"),
        0x0A0C => Some("TQ table schema not found"),
        0x0A0D => Some("TQ no committed offset"),
        0x1001 => Some("WAL file is corrupted"),
        0x1003 => Some("WAL invalid version"),
        0x1005 => Some("WAL log not exist"),
        0x1006 => Some("WAL checksum mismatch"),
        0x1007 => Some("WAL log incomplete"),
        0x2201 => Some("tfs invalid mount config"),
        0x2202 => Some("tfs too many mount"),
        0x2203 => Some("tfs duplicate primary mount"),
        0x2204 => Some("tfs no primary mount"),
        0x2205 => Some("tfs no mount at tier"),
        0x2206 => Some("tfs file already exists"),
        0x2207 => Some("tfs invalid level"),
        0x2208 => Some("tfs no valid disk"),
        0x2400 => Some("catalog internal error"),
        0x2401 => Some("invalid catalog input parameters"),
        0x2402 => Some("catalog is not ready"),
        0x2403 => Some("catalog system error"),
        0x2404 => Some("Database is dropped"),
        0x2405 => Some("catalog is out of service"),
        0x2406 => Some("table meta and vgroup mismatch"),
        0x2407 => Some("catalog exit"),
        0x2501 => Some("scheduler status error"),
        0x2502 => Some("scheduler internal error"),
        0x2504 => Some("Task timeout"),
        0x2505 => Some("Job is dropping"),
        0x2550 => Some("Invalid msg order"),
        0x2600 => Some("syntax error near"),
        0x2601 => Some("Incomplete SQL statement"),
        0x2602 => Some("Invalid column name"),
        0x2603 => Some("Table does not exist"),
        0x2604 => Some("Column ambiguously defined"),
        0x2605 => Some("Invalid value type"),
        0x2608 => Some("There mustn't be aggregation"),
        0x2609 => Some("ORDER BY item must be the number of a SELECT-list expression"),
        0x260A => Some("Not a GROUP BY expression"),
        0x260B => Some("Not SELECTed expression"),
        0x260C => Some("Not a single-group group function"),
        0x260D => Some("Tags number not matched"),
        0x260E => Some("Invalid tag name"),
        0x2610 => Some("Name or password too long"),
        0x2611 => Some("Password can not be empty"),
        0x2612 => Some("Port should be an integer that is less than 65535 and greater than 0"),
        0x2613 => Some("Endpoint should be in the format of 'fqdn:port'"),
        0x2614 => Some("This statement is no longer supported"),
        0x2615 => Some("Interval too small"),
        0x2616 => Some("Database not specified"),
        0x2617 => Some("Invalid identifier name"),
        0x2618 => Some("Corresponding super table not in this db"),
        0x2619 => Some("Invalid database option"),
        0x261A => Some("Invalid table option"),
        0x2624 => Some("GROUP BY and WINDOW-clause can't be used together"),
        0x2627 => Some("Aggregate functions do not support nesting"),
        0x2628 => Some("Only support STATE_WINDOW on integer/bool/varchar column"),
        0x2629 => Some("Not support STATE_WINDOW on tag column"),
        0x262A => Some("STATE_WINDOW not support for super table query"),
        0x262B => Some("SESSION gap should be fixed time window, and greater than 0"),
        0x262C => Some("Only support SESSION on primary timestamp column"),
        0x262D => Some("Interval offset cannot be negative"),
        0x262E => Some("Cannot use 'year' as offset when interval is 'month'"),
        0x262F => Some("Interval offset should be shorter than interval"),
        0x2630 => Some("Does not support sliding when interval is natural month/year"),
        0x2631 => Some("sliding value no larger than the interval value"),
        0x2632 => Some("sliding value can not less than 1%% of interval value"),
        0x2633 => Some("Only one tag if there is a json tag"),
        0x2634 => Some("Query block has incorrect number of result columns"),
        0x2635 => Some("Incorrect TIMESTAMP value"),
        0x2637 => Some("soffset/offset can not be less than 0"),
        0x2638 => Some("slimit/soffset only available for PARTITION/GROUP BY query"),
        0x2639 => Some("Invalid topic query"),
        0x263A => Some("Cannot drop super table in batch"),
        0x263B => Some("Start(end) time of query range required or time range too large"),
        0x263C => Some("Duplicated column names"),
        0x263D => Some("Tags length exceeds max length"),
        0x263E => Some("Row length exceeds max length"),
        0x263F => Some("Illegal number of columns"),
        0x2640 => Some("Too many columns"),
        0x2641 => Some("First column must be timestamp"),
        0x2642 => Some("Invalid binary/nchar column/tag length"),
        0x2643 => Some("Invalid number of tag columns"),
        0x2644 => Some("Permission denied"),
        0x2645 => Some("Invalid stream query"),
        0x2646 => Some("Invalid _c0 or _rowts expression"),
        0x2647 => Some("Invalid timeline function"),
        0x2648 => Some("Invalid password"),
        0x2649 => Some("Invalid alter table statement"),
        0x264A => Some("Primary timestamp column cannot be dropped"),
        0x264B => Some("Only binary/nchar column length could be modified, and the length can only be increased, not decreased"),
        0x264C => Some("Invalid tbname pseudo column"),
        0x264D => Some("Invalid function name"),
        0x264E => Some("Comment too long"),
        0x264F => Some("Some functions are allowed only in the SELECT list of a query. And, cannot be mixed with other non scalar functions or columns."),
        0x2650 => Some("Window query not supported, since the result of subquery not include valid timestamp column"),
        0x2651 => Some("No columns can be dropped"),
        0x2652 => Some("Only tag can be json type"),
        0x2653 => Some("Value too long for column/tag"),
        0x2655 => Some("The DELETE statement must have a definite time window range"),
        0x2656 => Some("The REDISTRIBUTE VGROUP statement only support 1 to 3 dnodes"),
        0x2657 => Some("Fill now allowed"),
        0x2658 => Some("Invalid windows pc"),
        0x2659 => Some("Window not allowed"),
        0x265A => Some("Stream not allowed"),
        0x265B => Some("Group by not allowd"),
        0x265D => Some("Invalid interp clause"),
        0x265E => Some("Not valid function ion window"),
        0x265F => Some("Only support single table"),
        0x2660 => Some("Invalid sma index"),
        0x2661 => Some("Invalid SELECTed expression"),
        0x2662 => Some("Fail to get table info"),
        0x2663 => Some("Not unique table/alias"),
        0x26FF => Some("Parser internal error"),
        0x2700 => Some("Planner internal error"),
        0x2701 => Some("Expect ts equal"),
        0x2702 => Some("Cross join not support"),
        0x2800 => Some("Function internal error"),
        0x2801 => Some("Invalid function para number"),
        0x2802 => Some("Invalid function para type"),
        0x2803 => Some("Invalid function para value"),
        0x2804 => Some("Not buildin function"),
        0x2805 => Some("Duplicate timestamps not allowed in function"),
        0x2901 => Some("udf is stopping"),
        0x2902 => Some("udf pipe read error"),
        0x2903 => Some("udf pipe connect error"),
        0x2904 => Some("udf pipe not exist"),
        0x2905 => Some("udf load failure"),
        0x2906 => Some("udf invalid function input"),
        0x2907 => Some("udf invalid bufsize"),
        0x2908 => Some("udf invalid output type"),
        0x2909 => Some("udf program language not supported"),
        0x290A => Some("udf function execution failure"),
        0x3000 => Some("Invalid line protocol type"),
        0x3001 => Some("Invalid timestamp precision type"),
        0x3002 => Some("Invalid data format"),
        0x3003 => Some("Invalid schemaless db config"),
        0x3004 => Some("Not the same type like before"),
        0x3005 => Some("Internal error"),
        0x3100 => Some("Tsma init failed"),
        0x3101 => Some("Tsma already exists"),
        0x3102 => Some("Invalid tsma env"),
        0x3103 => Some("Invalid tsma state"),
        0x3104 => Some("Invalid tsma pointer"),
        0x3105 => Some("Invalid tsma parameters"),
        0x3150 => Some("Invalid rsma env"),
        0x3151 => Some("Invalid rsma state"),
        0x3152 => Some("Rsma qtaskinfo creation error"),
        0x3153 => Some("Rsma invalid schema"),
        0x3154 => Some("Rsma stream state open"),
        0x3155 => Some("Rsma stream state commit"),
        0x3156 => Some("Rsma fs ref error"),
        0x3157 => Some("Rsma fs sync error"),
        0x3158 => Some("Rsma fs update error"),
        0x3200 => Some("Index is rebuilding"),
        0x3201 => Some("Index file is invalid"),
        0x4000 => Some("Invalid message"),
        0x4001 => Some("Consumer mismatch"),
        0x4002 => Some("Consumer closed"),
        0x4003 => Some("Consumer error, to see log"),
        0x4100 => Some("Stream task not exist"),
        0x4101 => Some("Out of memory in stream queue"),
        0x5100 => Some("Invalid TDLite open flags"),
        0x5101 => Some("Invalid TDLite open directory"),
        0x6000 => Some("Queue out of memory"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_str_of_known_codes() {
        assert_eq!(error_str_of(0x0001), Some("Action in progress"));
        assert_eq!(error_str_of(0x0002), Some("Authentication required"));
        assert_eq!(error_str_of(0x0003), Some("Authentication failure"));
        assert_eq!(error_str_of(0x0004), Some("Redirect"));
        assert_eq!(error_str_of(0x0005), Some("System not ready"));
        assert_eq!(error_str_of(0x0006), Some("Message already processed"));
        assert_eq!(error_str_of(0x0007), Some("Last session not finished"));
        assert_eq!(error_str_of(0x0008), Some("Mismatched meter id"));
        assert_eq!(
            error_str_of(0x0009),
            Some("Processing of request timed out")
        );
        assert_eq!(
            error_str_of(0x000A),
            Some("Number of sessions reached limit")
        );
        assert_eq!(error_str_of(0x000B), Some("Unable to establish connection"));
        assert_eq!(
            error_str_of(0x000C),
            Some("Unexpected generic error in RPC")
        );
        assert_eq!(error_str_of(0x000D), Some("Unexpected response"));
        assert_eq!(error_str_of(0x000E), Some("Invalid value"));
        assert_eq!(error_str_of(0x000F), Some("Invalid transaction id"));
        assert_eq!(error_str_of(0x0010), Some("Invalid session id"));
        assert_eq!(error_str_of(0x0011), Some("Invalid message type"));
        assert_eq!(error_str_of(0x0012), Some("Invalid response type"));
        assert_eq!(
            error_str_of(0x0013),
            Some("Client and server's time is not synchronized")
        );
        assert_eq!(error_str_of(0x0014), Some("Database not ready"));
        assert_eq!(error_str_of(0x0015), Some("Unable to resolve FQDN"));
        assert_eq!(error_str_of(0x0016), Some("Invalid app version"));
        assert_eq!(error_str_of(0x0017), Some("Shortcut (port already in use)"));
        assert_eq!(error_str_of(0x0018), Some("Conn is broken"));
        assert_eq!(error_str_of(0x0019), Some("Conn read timeout"));
        assert_eq!(
            error_str_of(0x0020),
            Some("some vnode/qnode/mnode(s) out of service")
        );
        assert_eq!(error_str_of(0x0022), Some("rpc open too many session"));
        assert_eq!(error_str_of(0x0100), Some("Operation not supported"));
        assert_eq!(error_str_of(0x0101), Some("Memory corrupted"));
        assert_eq!(error_str_of(0x0102), Some("Out of Memory"));
        assert_eq!(error_str_of(0x0103), Some("Invalid config message"));
        assert_eq!(error_str_of(0x0104), Some("Data file corrupted"));
        assert_eq!(error_str_of(0x0105), Some("Ref out of memory"));
        assert_eq!(error_str_of(0x0106), Some("too many Ref Objs"));
        assert_eq!(error_str_of(0x0107), Some("Ref ID is removed"));
        assert_eq!(error_str_of(0x0108), Some("Invalid Ref ID"));
        assert_eq!(error_str_of(0x0109), Some("Ref is already there"));
        assert_eq!(error_str_of(0x010A), Some("Ref is not there"));
        assert_eq!(error_str_of(0x0110), Some("Unexpected generic error"));
        assert_eq!(error_str_of(0x0111), Some("Action in progress"));
        assert_eq!(error_str_of(0x0112), Some("Out of range"));
        assert_eq!(error_str_of(0x0115), Some("Invalid message"));
        assert_eq!(error_str_of(0x0116), Some("Invalid message len"));
        assert_eq!(error_str_of(0x0117), Some("Invalid pointer"));
        assert_eq!(error_str_of(0x0118), Some("Invalid parameters"));
        assert_eq!(error_str_of(0x0119), Some("Invalid config option"));
        assert_eq!(error_str_of(0x011A), Some("Invalid option"));
        assert_eq!(error_str_of(0x011B), Some("Invalid json format"));
        assert_eq!(error_str_of(0x011C), Some("Invalid version number"));
        assert_eq!(error_str_of(0x011D), Some("Invalid version string"));
        assert_eq!(error_str_of(0x011E), Some("Version not compatible"));
        assert_eq!(error_str_of(0x011F), Some("Checksum error"));
        assert_eq!(error_str_of(0x0120), Some("Failed to compress msg"));
        assert_eq!(error_str_of(0x0121), Some("Message not processed"));
        assert_eq!(error_str_of(0x0122), Some("Config not found"));
        assert_eq!(error_str_of(0x0123), Some("Repeat initialization"));
        assert_eq!(
            error_str_of(0x0124),
            Some("Cannot add duplicate keys to hash")
        );
        assert_eq!(error_str_of(0x0125), Some("Retry needed"));
        assert_eq!(error_str_of(0x0126), Some("Out of memory in rpc queue"));
        assert_eq!(error_str_of(0x0127), Some("Invalid timestamp format"));
        assert_eq!(error_str_of(0x0128), Some("Msg decode error"));
        assert_eq!(error_str_of(0x0129), Some("No available disk"));
        assert_eq!(error_str_of(0x012A), Some("Not found"));
        assert_eq!(error_str_of(0x012B), Some("Out of disk space"));
        assert_eq!(error_str_of(0x012C), Some("Operation timeout"));
        assert_eq!(error_str_of(0x012D), Some("Msg encode error"));
        assert_eq!(error_str_of(0x012E), Some("No enough disk space"));
        assert_eq!(error_str_of(0x0130), Some("Database is starting up"));
        assert_eq!(error_str_of(0x0131), Some("Database is closing down"));
        assert_eq!(error_str_of(0x0132), Some("Invalid data format"));
        assert_eq!(error_str_of(0x0133), Some("Invalid configuration value"));
        assert_eq!(error_str_of(0x0200), Some("Invalid operation"));
        assert_eq!(error_str_of(0x0201), Some("Invalid qhandle"));
        assert_eq!(
            error_str_of(0x0202),
            Some("Invalid combination of client/service time")
        );
        assert_eq!(error_str_of(0x0203), Some("Invalid value in client"));
        assert_eq!(error_str_of(0x0204), Some("Invalid client version"));
        assert_eq!(error_str_of(0x0205), Some("Invalid client ie"));
        assert_eq!(error_str_of(0x0206), Some("Invalid host name"));
        assert_eq!(error_str_of(0x0207), Some("Invalid user name"));
        assert_eq!(error_str_of(0x0208), Some("Invalid password"));
        assert_eq!(error_str_of(0x0209), Some("Database name too long"));
        assert_eq!(error_str_of(0x020A), Some("Table name too long"));
        assert_eq!(error_str_of(0x020B), Some("Invalid connection"));
        assert_eq!(error_str_of(0x020C), Some("System out of memory"));
        assert_eq!(error_str_of(0x020D), Some("System out of disk space"));
        assert_eq!(error_str_of(0x020E), Some("Query cache erased"));
        assert_eq!(error_str_of(0x020F), Some("Query terminated"));
        assert_eq!(
            error_str_of(0x0210),
            Some("Result set too large to be sorted")
        );
        assert_eq!(error_str_of(0x0211), Some("Application error"));
        assert_eq!(error_str_of(0x0212), Some("Action in progress"));
        assert_eq!(error_str_of(0x0213), Some("Disconnected from service"));
        assert_eq!(error_str_of(0x0214), Some("No write permission"));
        assert_eq!(error_str_of(0x0215), Some("Connection killed"));
        assert_eq!(error_str_of(0x0216), Some("Syntax error in SQL"));
        assert_eq!(
            error_str_of(0x0217),
            Some("Database not specified or available")
        );
        assert_eq!(error_str_of(0x0218), Some("Table does not exist"));
        assert_eq!(
            error_str_of(0x0219),
            Some("SQL statement too long, check maxSQLLength config (sql statement too long)")
        );
        assert_eq!(error_str_of(0x021A), Some("File is empty"));
        assert_eq!(error_str_of(0x021B), Some("Syntax error in Line"));
        assert_eq!(error_str_of(0x021C), Some("No table meta cached"));
        assert_eq!(error_str_of(0x021D), Some("duplicated column names"));
        assert_eq!(error_str_of(0x021E), Some("Invalid tag length"));
        assert_eq!(error_str_of(0x021F), Some("Invalid column length"));
        assert_eq!(
            error_str_of(0x0220),
            Some("duplicated tag names (duplicated names)")
        );
        assert_eq!(error_str_of(0x0221), Some("Invalid JSON format"));
        assert_eq!(error_str_of(0x0222), Some("Invalid JSON data type"));
        assert_eq!(error_str_of(0x0223), Some("Invalid JSON configuration"));
        assert_eq!(error_str_of(0x0224), Some("Value out of range"));
        assert_eq!(error_str_of(0x0225), Some("Invalid line protocol type"));
        assert_eq!(
            error_str_of(0x0226),
            Some("Invalid timestamp precision type")
        );
        assert_eq!(
            error_str_of(0x0227),
            Some("Result set too large to be output")
        );
        assert_eq!(
            error_str_of(0x0229),
            Some("Too many lines in batch (invalid tsc input)")
        );
        assert_eq!(error_str_of(0x022A), Some("Stmt API usage error"));
        assert_eq!(error_str_of(0x022B), Some("Stmt table name not set"));
        assert_eq!(error_str_of(0x022C), Some("not supported stmt clause"));
        assert_eq!(error_str_of(0x022D), Some("Query killed"));
        assert_eq!(
            error_str_of(0x022E),
            Some("No available execution node in current query policy configuration")
        );
        assert_eq!(error_str_of(0x022F), Some("Table is not a super table"));
        assert_eq!(
            error_str_of(0x0230),
            Some("Client send request data failed (stmt cache error)")
        );
        assert_eq!(error_str_of(0x0231), Some("Internal error"));
        assert_eq!(error_str_of(0x0300), Some("Message not processed"));
        assert_eq!(error_str_of(0x0301), Some("Message is progressing"));
        assert_eq!(error_str_of(0x0302), Some("Message need to be reprocessed"));
        assert_eq!(
            error_str_of(0x0303),
            Some("Insufficient privilege for operation")
        );
        assert_eq!(
            error_str_of(0x0304),
            Some("Unexpected generic error in mnode")
        );
        assert_eq!(error_str_of(0x0305), Some("Invalid message connection"));
        assert_eq!(error_str_of(0x0306), Some("Incompatible protocol version"));
        assert_eq!(error_str_of(0x0307), Some("Invalid message length"));
        assert_eq!(error_str_of(0x0308), Some("Invalid message type"));
        assert_eq!(error_str_of(0x0309), Some("Too many connections"));
        assert_eq!(error_str_of(0x030A), Some("Out of memory in mnode"));
        assert_eq!(error_str_of(0x030B), Some("Data expired"));
        assert_eq!(error_str_of(0x030C), Some("Invalid query id"));
        assert_eq!(error_str_of(0x030D), Some("Invalid stream id"));
        assert_eq!(error_str_of(0x030E), Some("Invalid connection id"));
        assert_eq!(error_str_of(0x0310), Some("mnode is alreay running"));
        assert_eq!(error_str_of(0x0311), Some("failed to config sync"));
        assert_eq!(error_str_of(0x0312), Some("failed to start sync"));
        assert_eq!(error_str_of(0x0313), Some("failed to create mnode dir"));
        assert_eq!(error_str_of(0x0314), Some("failed to init components"));
        assert_eq!(error_str_of(0x0315), Some("User is disabled"));
        assert_eq!(error_str_of(0x0320), Some("Object already there"));
        assert_eq!(
            error_str_of(0x0321),
            Some("Unexpected generic error in sdb")
        );
        assert_eq!(error_str_of(0x0322), Some("Invalid table type"));
        assert_eq!(error_str_of(0x0323), Some("Object not there"));
        assert_eq!(error_str_of(0x0324), Some("Invalid meta row"));
        assert_eq!(error_str_of(0x0325), Some("Invalid key type"));
        assert_eq!(error_str_of(0x0326), Some("Invalid action type"));
        assert_eq!(error_str_of(0x0328), Some("Invalid raw data version"));
        assert_eq!(error_str_of(0x0329), Some("Invalid raw data len"));
        assert_eq!(error_str_of(0x032A), Some("Invalid raw data content"));
        assert_eq!(error_str_of(0x032C), Some("Object is creating"));
        assert_eq!(error_str_of(0x032D), Some("Object is dropping"));
        assert_eq!(error_str_of(0x0330), Some("Dnode already exists"));
        assert_eq!(error_str_of(0x0331), Some("Dnode does not exist"));
        assert_eq!(error_str_of(0x0332), Some("Vgroup does not exist"));
        assert_eq!(
            error_str_of(0x0333),
            Some("Master DNode cannot be removed (cannot drop mnode which is leader)")
        );
        assert_eq!(error_str_of(0x0334), Some("Out of dnodes"));
        assert_eq!(error_str_of(0x0335), Some("Cluster cfg inconsistent"));
        assert_eq!(error_str_of(0x0336), Some("Invalid dnode cfg option"));
        assert_eq!(error_str_of(0x0337), Some("Balance already enabled"));
        assert_eq!(error_str_of(0x0338), Some("Vgroup not in dnode"));
        assert_eq!(error_str_of(0x0339), Some("Vgroup already in dnode"));
        assert_eq!(error_str_of(0x033A), Some("Dnode not avaliable"));
        assert_eq!(error_str_of(0x033B), Some("Cluster id not match"));
        assert_eq!(error_str_of(0x033C), Some("Cluster not ready"));
        assert_eq!(error_str_of(0x033D), Some("Dnode Id not configured"));
        assert_eq!(error_str_of(0x033E), Some("Dnode Ep not configured"));
        assert_eq!(error_str_of(0x0340), Some("Account already exists"));
        assert_eq!(error_str_of(0x0341), Some("Invalid account"));
        assert_eq!(error_str_of(0x0342), Some("Invalid account options"));
        assert_eq!(
            error_str_of(0x0343),
            Some("Account authorization has expired")
        );
        assert_eq!(error_str_of(0x0344), Some("Invalid account"));
        assert_eq!(error_str_of(0x0345), Some("Too many accounts"));
        assert_eq!(error_str_of(0x0350), Some("User already exists"));
        assert_eq!(error_str_of(0x0351), Some("Invalid user"));
        assert_eq!(error_str_of(0x0352), Some("Invalid user format"));
        assert_eq!(error_str_of(0x0353), Some("Invalid password format"));
        assert_eq!(error_str_of(0x0354), Some("Can not get user from conn"));
        assert_eq!(error_str_of(0x0355), Some("Too many users"));
        assert_eq!(error_str_of(0x0356), Some("Invalid alter operation"));
        assert_eq!(error_str_of(0x0357), Some("Authentication failure"));
        assert_eq!(error_str_of(0x0358), Some("User not available"));
        assert_eq!(
            error_str_of(0x0359),
            Some("User already have this priviledge")
        );
        assert_eq!(
            error_str_of(0x0360),
            Some("Table already exists (stable already exists)")
        );
        assert_eq!(error_str_of(0x0361), Some("Table name too long"));
        assert_eq!(
            error_str_of(0x0362),
            Some("Table does not exist (stable not exist)")
        );
        assert_eq!(error_str_of(0x0363), Some("Invalid table type in tsdb"));
        assert_eq!(error_str_of(0x0364), Some("Too many tags"));
        assert_eq!(error_str_of(0x0365), Some("Too many columns"));
        assert_eq!(error_str_of(0x0366), Some("Too many time series"));
        assert_eq!(error_str_of(0x0367), Some("Not super table"));
        assert_eq!(error_str_of(0x0368), Some("Tag name too long"));
        assert_eq!(error_str_of(0x0369), Some("Tag already exists"));
        assert_eq!(error_str_of(0x036A), Some("Tag does not exist"));
        assert_eq!(
            error_str_of(0x036B),
            Some("Field already exists (column already exists)")
        );
        assert_eq!(
            error_str_of(0x036C),
            Some("Field does not exist (column does not exist)")
        );
        assert_eq!(error_str_of(0x036D), Some("Super table does not exist"));
        assert_eq!(
            error_str_of(0x036E),
            Some("Invalid create table message (invalid stable options)")
        );
        assert_eq!(
            error_str_of(0x036F),
            Some("Exceed max row bytes (invalid row bytes)")
        );
        assert_eq!(error_str_of(0x0370), Some("Invalid func name"));
        assert_eq!(error_str_of(0x0371), Some("Invalid func length"));
        assert_eq!(error_str_of(0x0372), Some("Invalid func code"));
        assert_eq!(error_str_of(0x0373), Some("Func already exists"));
        assert_eq!(error_str_of(0x0374), Some("Invalid func (func not exists)"));
        assert_eq!(error_str_of(0x0375), Some("Invalid func bufSize"));
        assert_eq!(error_str_of(0x0376), Some("invalid tag length"));
        assert_eq!(error_str_of(0x0377), Some("invalid column length"));
        assert_eq!(error_str_of(0x0378), Some("Invalid func comment"));
        assert_eq!(error_str_of(0x0379), Some("Invalid func retrieve msg"));
        assert_eq!(
            error_str_of(0x0380),
            Some("Database not specified or available")
        );
        assert_eq!(error_str_of(0x0381), Some("Database already exists"));
        assert_eq!(error_str_of(0x0382), Some("Invalid database options"));
        assert_eq!(error_str_of(0x0383), Some("Invalid database name"));
        assert_eq!(error_str_of(0x0384), Some("Cannot delete monitor database"));
        assert_eq!(error_str_of(0x0385), Some("Too many databases for account"));
        assert_eq!(
            error_str_of(0x0386),
            Some("Database not available (database in dropping status)")
        );
        assert_eq!(error_str_of(0x0387), Some("Database unsynced"));
        assert_eq!(error_str_of(0x0388), Some("Database not exist"));
        assert_eq!(error_str_of(0x0389), Some("Invalid database account"));
        assert_eq!(error_str_of(0x038A), Some("Database options not changed"));
        assert_eq!(error_str_of(0x038B), Some("Index not exist"));
        assert_eq!(error_str_of(0x038C), Some("WAL retention period is zero"));
        assert_eq!(
            error_str_of(0x0390),
            Some("Invalid database option: days out of range")
        );
        assert_eq!(
            error_str_of(0x0391),
            Some("Invalid database option: keep2 >= keep1 >= keep0 >= days")
        );
        assert_eq!(error_str_of(0x0392), Some("Invalid topic name"));
        assert_eq!(error_str_of(0x0393), Some("Invalid topic option"));
        assert_eq!(
            error_str_of(0x0394),
            Some("Invalid topic partitons num, valid range: [1, 1000]")
        );
        assert_eq!(error_str_of(0x0395), Some("Topic already exists"));
        assert_eq!(error_str_of(0x0396), Some("Database in creating status"));
        assert_eq!(error_str_of(0x039A), Some("Invalid system table name"));
        assert_eq!(error_str_of(0x03A0), Some("Mnode already exists"));
        assert_eq!(error_str_of(0x03A1), Some("Mnode not there"));
        assert_eq!(error_str_of(0x03A2), Some("Qnode already exists"));
        assert_eq!(error_str_of(0x03A3), Some("Qnode not there"));
        assert_eq!(error_str_of(0x03A4), Some("Snode already exists"));
        assert_eq!(error_str_of(0x03A5), Some("Snode not there"));
        assert_eq!(
            error_str_of(0x03A8),
            Some("The replica of mnode cannot less than 1")
        );
        assert_eq!(
            error_str_of(0x03A9),
            Some("The replica of mnode cannot exceed 3")
        );
        assert_eq!(error_str_of(0x03B0), Some("Too many dnodes"));
        assert_eq!(error_str_of(0x03B1), Some("No enough memory in dnode"));
        assert_eq!(error_str_of(0x03B2), Some("Invalid dnode cfg"));
        assert_eq!(error_str_of(0x03B3), Some("Invalid dnode end point"));
        assert_eq!(error_str_of(0x03B4), Some("Invalid dnode id"));
        assert_eq!(
            error_str_of(0x03B5),
            Some("Vgroup distribution has not changed")
        );
        assert_eq!(error_str_of(0x03B6), Some("Offline dnode exists"));
        assert_eq!(error_str_of(0x03B7), Some("Invalid vgroup replica"));
        assert_eq!(error_str_of(0x03B8), Some("Dnode in creating status"));
        assert_eq!(error_str_of(0x03B9), Some("Dnode in dropping status"));
        assert_eq!(error_str_of(0x03C0), Some("STable confilct with topic"));
        assert_eq!(error_str_of(0x03C1), Some("Too many stables"));
        assert_eq!(error_str_of(0x03C2), Some("Invalid stable alter options"));
        assert_eq!(error_str_of(0x03C3), Some("STable option unchanged"));
        assert_eq!(error_str_of(0x03C4), Some("Field used by topic"));
        assert_eq!(error_str_of(0x03C5), Some("Database is single stable mode"));
        assert_eq!(
            error_str_of(0x03C6),
            Some("Invalid schema version while alter stb")
        );
        assert_eq!(
            error_str_of(0x03C7),
            Some("Invalid stable uid while alter stb")
        );
        assert_eq!(error_str_of(0x03C8), Some("Field used by tsma"));
        assert_eq!(error_str_of(0x03D0), Some("Transaction already exists"));
        assert_eq!(error_str_of(0x03D1), Some("Transaction not exists"));
        assert_eq!(error_str_of(0x03D2), Some("Invalid stage to kill"));
        assert_eq!(
            error_str_of(0x03D3),
            Some("Conflict transaction not completed")
        );
        assert_eq!(error_str_of(0x03D4), Some("Transaction commitlog is null"));
        assert_eq!(error_str_of(0x03D5), Some("Unable to establish connection While execute transaction and will continue in the background"));
        assert_eq!(error_str_of(0x03D6), Some("Last Transaction not finished"));
        assert_eq!(
            error_str_of(0x03D7),
            Some("Sync timeout While execute transaction and will continue in the background")
        );
        assert_eq!(error_str_of(0x03DF), Some("Unknown transaction error"));
        assert_eq!(error_str_of(0x03E0), Some("Topic already exists"));
        assert_eq!(error_str_of(0x03E1), Some("Topic not exist"));
        assert_eq!(error_str_of(0x03E2), Some("Too many Topics"));
        assert_eq!(error_str_of(0x03E3), Some("Invalid topic"));
        assert_eq!(error_str_of(0x03E4), Some("Topic with invalid query"));
        assert_eq!(error_str_of(0x03E5), Some("Topic with invalid option"));
        assert_eq!(error_str_of(0x03E6), Some("Consumer not exist"));
        assert_eq!(error_str_of(0x03E7), Some("Topic unchanged"));
        assert_eq!(error_str_of(0x03E8), Some("Subcribe not exist"));
        assert_eq!(error_str_of(0x03E9), Some("Offset not exist"));
        assert_eq!(error_str_of(0x03EA), Some("Consumer not ready"));
        assert_eq!(
            error_str_of(0x03EB),
            Some("Topic subscribed cannot be dropped")
        );
        assert_eq!(
            error_str_of(0x03EC),
            Some("Consumer group being used by some consumer")
        );
        assert_eq!(error_str_of(0x03ED), Some("Topic must be dropped first"));
        assert_eq!(error_str_of(0x03EE), Some("Invalid subscribe option"));
        assert_eq!(error_str_of(0x03EF), Some("Topic being rebalanced"));
        assert_eq!(error_str_of(0x03F0), Some("Stream already exists"));
        assert_eq!(error_str_of(0x03F1), Some("Stream not exist"));
        assert_eq!(error_str_of(0x03F2), Some("Invalid stream option"));
        assert_eq!(error_str_of(0x03F3), Some("Stream must be dropped first"));
        assert_eq!(
            error_str_of(0x03F5),
            Some("Stream temporarily does not support source db having replica > 1")
        );
        assert_eq!(error_str_of(0x03F6), Some("Too many streams"));
        assert_eq!(
            error_str_of(0x03F7),
            Some("Cannot write the same stable as other stream")
        );
        assert_eq!(error_str_of(0x0400), Some("Message not processed"));
        assert_eq!(error_str_of(0x0401), Some("Dnode out of memory"));
        assert_eq!(
            error_str_of(0x0402),
            Some("No permission for disk files in dnode")
        );
        assert_eq!(error_str_of(0x0403), Some("Invalid message length"));
        assert_eq!(error_str_of(0x0404), Some("Action in progress"));
        assert_eq!(error_str_of(0x0405), Some("Too many vnode directories"));
        assert_eq!(error_str_of(0x0406), Some("Dnode is exiting"));
        assert_eq!(error_str_of(0x0407), Some("Vnode open failed"));
        assert_eq!(error_str_of(0x0408), Some("Dnode is offline"));
        assert_eq!(error_str_of(0x0409), Some("Mnode already deployed"));
        assert_eq!(error_str_of(0x040A), Some("Mnode not found"));
        assert_eq!(error_str_of(0x040B), Some("Mnode not deployed"));
        assert_eq!(error_str_of(0x040C), Some("Qnode already deployed"));
        assert_eq!(error_str_of(0x040D), Some("Qnode not found"));
        assert_eq!(error_str_of(0x040E), Some("Qnode not deployed"));
        assert_eq!(error_str_of(0x040F), Some("Snode already deployed"));
        assert_eq!(error_str_of(0x0410), Some("Snode not found"));
        assert_eq!(error_str_of(0x0411), Some("Snode not deployed"));
        assert_eq!(error_str_of(0x0412), Some("Mnode didn't catch the leader"));
        assert_eq!(error_str_of(0x0413), Some("Mnode already is a leader"));
        assert_eq!(error_str_of(0x0414), Some("Only two mnodes exist"));
        assert_eq!(
            error_str_of(0x0415),
            Some("No need to restore on this dnode")
        );
        assert_eq!(
            error_str_of(0x0416),
            Some("Please use this command when the dnode is offline")
        );
        assert_eq!(error_str_of(0x0480), Some("index already exists"));
        assert_eq!(error_str_of(0x0481), Some("index not exist"));
        assert_eq!(error_str_of(0x0482), Some("Invalid sma index option"));
        assert_eq!(error_str_of(0x0483), Some("index already exists"));
        assert_eq!(error_str_of(0x0484), Some("index not exist"));
        assert_eq!(error_str_of(0x0500), Some("Action in progress"));
        assert_eq!(error_str_of(0x0501), Some("Message not processed"));
        assert_eq!(error_str_of(0x0502), Some("Action need to be reprocessed"));
        assert_eq!(
            error_str_of(0x0503),
            Some("Invalid Vgroup ID (vnode is closed or removed)")
        );
        assert_eq!(error_str_of(0x0504), Some("Vnode initialization failed"));
        assert_eq!(error_str_of(0x0505), Some("System out of disk space"));
        assert_eq!(
            error_str_of(0x0506),
            Some("No write permission for disk files")
        );
        assert_eq!(error_str_of(0x0507), Some("Missing data file"));
        assert_eq!(error_str_of(0x0508), Some("Out of memory"));
        assert_eq!(
            error_str_of(0x0509),
            Some("Unexpected generic error in vnode")
        );
        assert_eq!(error_str_of(0x050A), Some("Invalid version file"));
        assert_eq!(
            error_str_of(0x050B),
            Some("Database memory is full for commit failed")
        );
        assert_eq!(
            error_str_of(0x050C),
            Some("Database memory is full for waiting commit")
        );
        assert_eq!(error_str_of(0x050D), Some("Database is dropping"));
        assert_eq!(error_str_of(0x050E), Some("Database is balancing"));
        assert_eq!(error_str_of(0x0510), Some("Database is closing"));
        assert_eq!(error_str_of(0x0511), Some("Database suspended"));
        assert_eq!(
            error_str_of(0x0512),
            Some("Database write operation denied")
        );
        assert_eq!(error_str_of(0x0513), Some("Database is syncing"));
        assert_eq!(error_str_of(0x0514), Some("Invalid tsdb state"));
        assert_eq!(error_str_of(0x0515), Some("Wait threads too many"));
        assert_eq!(error_str_of(0x0520), Some("Vnode not exist"));
        assert_eq!(error_str_of(0x0521), Some("Vnode already exist"));
        assert_eq!(error_str_of(0x0522), Some("Vnode hash mismatch"));
        assert_eq!(error_str_of(0x0524), Some("Invalid table action"));
        assert_eq!(error_str_of(0x0525), Some("Table column already exists"));
        assert_eq!(error_str_of(0x0526), Some("Table column not exists"));
        assert_eq!(error_str_of(0x0527), Some("Table column is subscribed"));
        assert_eq!(error_str_of(0x0528), Some("No available buffer pool"));
        assert_eq!(error_str_of(0x0529), Some("Vnode stopped"));
        assert_eq!(error_str_of(0x0530), Some("Duplicate write request"));
        assert_eq!(error_str_of(0x0531), Some("Query busy"));
        assert_eq!(
            error_str_of(0x0532),
            Some("Vnode didn't catch up its leader")
        );
        assert_eq!(error_str_of(0x0533), Some("Vnode already is a voter"));
        assert_eq!(error_str_of(0x0534), Some("Vnode directory already exist"));
        assert_eq!(error_str_of(0x0535), Some("Single replica vnode data will lost permanently after this operation, if you make sure this, please use drop dnode <id> unsafe to execute"));
        assert_eq!(error_str_of(0x0600), Some("Invalid table ID"));
        assert_eq!(error_str_of(0x0601), Some("Invalid table type"));
        assert_eq!(error_str_of(0x0602), Some("Invalid table schema version"));
        assert_eq!(error_str_of(0x0603), Some("Table already exists"));
        assert_eq!(error_str_of(0x0604), Some("Invalid configuration"));
        assert_eq!(error_str_of(0x0605), Some("Tsdb init failed"));
        assert_eq!(error_str_of(0x0606), Some("No disk space for tsdb"));
        assert_eq!(error_str_of(0x0607), Some("No permission for disk files"));
        assert_eq!(error_str_of(0x0608), Some("Data file(s) corrupted"));
        assert_eq!(error_str_of(0x0609), Some("Out of memory"));
        assert_eq!(error_str_of(0x060A), Some("Tag too old"));
        assert_eq!(error_str_of(0x060B), Some("Timestamp data out of range"));
        assert_eq!(error_str_of(0x060C), Some("Submit message is messed up"));
        assert_eq!(error_str_of(0x060D), Some("Invalid operation"));
        assert_eq!(error_str_of(0x060E), Some("Invalid creation of table"));
        assert_eq!(
            error_str_of(0x060F),
            Some("No table data in memory skip-list")
        );
        assert_eq!(error_str_of(0x0610), Some("File already exists"));
        assert_eq!(error_str_of(0x0611), Some("Need to reconfigure table"));
        assert_eq!(
            error_str_of(0x0612),
            Some("Invalid information to create table")
        );
        assert_eq!(
            error_str_of(0x0613),
            Some("No available disk (tsdb no available disk)")
        );
        assert_eq!(error_str_of(0x0614), Some("TSDB messed message"));
        assert_eq!(error_str_of(0x0615), Some("TSDB invalid tag value"));
        assert_eq!(error_str_of(0x0616), Some("TSDB no cache last row data"));
        assert_eq!(error_str_of(0x0617), Some("Incomplete DFileSet"));
        assert_eq!(error_str_of(0x0618), Some("Table not exists"));
        assert_eq!(error_str_of(0x0619), Some("Stable already exists"));
        assert_eq!(error_str_of(0x061A), Some("Stable not exists"));
        assert_eq!(error_str_of(0x061B), Some("Table schema is old"));
        assert_eq!(error_str_of(0x061C), Some("TDB env open error"));
        assert_eq!(
            error_str_of(0x061D),
            Some("Table already exists in other stables")
        );
        assert_eq!(error_str_of(0x0700), Some("Invalid handle"));
        assert_eq!(error_str_of(0x0701), Some("Invalid message"));
        assert_eq!(error_str_of(0x0702), Some("No disk space for query"));
        assert_eq!(error_str_of(0x0703), Some("System out of memory"));
        assert_eq!(
            error_str_of(0x0704),
            Some("Unexpected generic error in query")
        );
        assert_eq!(error_str_of(0x0705), Some("Duplicated join key"));
        assert_eq!(error_str_of(0x0706), Some("Tag condition too many"));
        assert_eq!(error_str_of(0x0707), Some("Query not ready"));
        assert_eq!(error_str_of(0x0708), Some("Query should response"));
        assert_eq!(
            error_str_of(0x0709),
            Some("Multiple retrieval of this query")
        );
        assert_eq!(
            error_str_of(0x070A),
            Some("Too many time window in query (too many groups/time window in query)")
        );
        assert_eq!(error_str_of(0x070B), Some("Query buffer limit has reached"));
        assert_eq!(error_str_of(0x070C), Some("File inconsistance in replica"));
        assert_eq!(error_str_of(0x070D), Some("System error"));
        assert_eq!(
            error_str_of(0x070E),
            Some("One valid time range condition expected")
        );
        assert_eq!(error_str_of(0x070F), Some("invalid input"));
        assert_eq!(error_str_of(0x0711), Some("result num is too large"));
        assert_eq!(error_str_of(0x0720), Some("Scheduler not exist"));
        assert_eq!(error_str_of(0x0721), Some("Task not exist"));
        assert_eq!(error_str_of(0x0722), Some("Task already exist"));
        assert_eq!(error_str_of(0x0723), Some("Task context not exist"));
        assert_eq!(error_str_of(0x0724), Some("Task cancelled"));
        assert_eq!(error_str_of(0x0725), Some("Task dropped"));
        assert_eq!(error_str_of(0x0726), Some("Task cancelling"));
        assert_eq!(error_str_of(0x0727), Some("Task dropping"));
        assert_eq!(error_str_of(0x0728), Some("Duplicated operation"));
        assert_eq!(error_str_of(0x0729), Some("Task message error"));
        assert_eq!(error_str_of(0x072A), Some("Job already freed"));
        assert_eq!(error_str_of(0x072B), Some("Task status error"));
        assert_eq!(
            error_str_of(0x072C),
            Some("Json not support in in/notin operator")
        );
        assert_eq!(error_str_of(0x072D), Some("Json not support in this place"));
        assert_eq!(
            error_str_of(0x072E),
            Some("Json not support in group/partition by")
        );
        assert_eq!(error_str_of(0x072F), Some("Job not exist"));
        assert_eq!(error_str_of(0x0730), Some("Vnode/Qnode is quitting"));
        assert_eq!(error_str_of(0x0800), Some("License expired"));
        assert_eq!(
            error_str_of(0x0801),
            Some("DNode creation limited by license")
        );
        assert_eq!(
            error_str_of(0x0802),
            Some("Account creation limited by license")
        );
        assert_eq!(error_str_of(0x0803), Some("Time series limited by license"));
        assert_eq!(error_str_of(0x0804), Some("DB creation limited by license"));
        assert_eq!(
            error_str_of(0x0805),
            Some("User creation limited by license")
        );
        assert_eq!(
            error_str_of(0x0806),
            Some("Conn creation limited by license")
        );
        assert_eq!(
            error_str_of(0x0807),
            Some("Stream creation limited by license")
        );
        assert_eq!(error_str_of(0x0808), Some("Write speed limited by license"));
        assert_eq!(
            error_str_of(0x0809),
            Some("Storage capacity limited by license")
        );
        assert_eq!(error_str_of(0x080A), Some("Query time limited by license"));
        assert_eq!(error_str_of(0x080B), Some("CPU cores limited by license"));
        assert_eq!(
            error_str_of(0x080C),
            Some("STable creation limited by license")
        );
        assert_eq!(
            error_str_of(0x080D),
            Some("Table creation limited by license")
        );
        assert_eq!(error_str_of(0x0903), Some("Sync timeout"));
        assert_eq!(error_str_of(0x090C), Some("Sync leader is unreachable"));
        assert_eq!(error_str_of(0x090F), Some("Sync new config error"));
        assert_eq!(error_str_of(0x0911), Some("Sync not ready to propose"));
        assert_eq!(error_str_of(0x0914), Some("Sync leader is restoring"));
        assert_eq!(error_str_of(0x0915), Some("Sync invalid snapshot msg"));
        assert_eq!(error_str_of(0x0916), Some("Sync buffer is full"));
        assert_eq!(error_str_of(0x0917), Some("Sync write stall"));
        assert_eq!(error_str_of(0x0918), Some("Sync negotiation win is full"));
        assert_eq!(error_str_of(0x09FF), Some("Sync internal error"));
        assert_eq!(error_str_of(0x0A00), Some("TQ invalid config"));
        assert_eq!(error_str_of(0x0A01), Some("TQ init failed"));
        assert_eq!(error_str_of(0x0A03), Some("TQ no disk permissions"));
        assert_eq!(error_str_of(0x0A06), Some("TQ file already exists"));
        assert_eq!(error_str_of(0x0A07), Some("TQ failed to create dir"));
        assert_eq!(error_str_of(0x0A08), Some("TQ meta no such key"));
        assert_eq!(error_str_of(0x0A09), Some("TQ meta key not in txn"));
        assert_eq!(error_str_of(0x0A0A), Some("TQ met key dup in txn"));
        assert_eq!(error_str_of(0x0A0B), Some("TQ group not exist"));
        assert_eq!(error_str_of(0x0A0C), Some("TQ table schema not found"));
        assert_eq!(error_str_of(0x0A0D), Some("TQ no committed offset"));
        assert_eq!(error_str_of(0x1001), Some("WAL file is corrupted"));
        assert_eq!(error_str_of(0x1003), Some("WAL invalid version"));
        assert_eq!(error_str_of(0x1005), Some("WAL log not exist"));
        assert_eq!(error_str_of(0x1006), Some("WAL checksum mismatch"));
        assert_eq!(error_str_of(0x1007), Some("WAL log incomplete"));
        assert_eq!(error_str_of(0x2201), Some("tfs invalid mount config"));
        assert_eq!(error_str_of(0x2202), Some("tfs too many mount"));
        assert_eq!(error_str_of(0x2203), Some("tfs duplicate primary mount"));
        assert_eq!(error_str_of(0x2204), Some("tfs no primary mount"));
        assert_eq!(error_str_of(0x2205), Some("tfs no mount at tier"));
        assert_eq!(error_str_of(0x2206), Some("tfs file already exists"));
        assert_eq!(error_str_of(0x2207), Some("tfs invalid level"));
        assert_eq!(error_str_of(0x2208), Some("tfs no valid disk"));
        assert_eq!(error_str_of(0x2400), Some("catalog internal error"));
        assert_eq!(
            error_str_of(0x2401),
            Some("invalid catalog input parameters")
        );
        assert_eq!(error_str_of(0x2402), Some("catalog is not ready"));
        assert_eq!(error_str_of(0x2403), Some("catalog system error"));
        assert_eq!(error_str_of(0x2404), Some("Database is dropped"));
        assert_eq!(error_str_of(0x2405), Some("catalog is out of service"));
        assert_eq!(error_str_of(0x2406), Some("table meta and vgroup mismatch"));
        assert_eq!(error_str_of(0x2407), Some("catalog exit"));
        assert_eq!(error_str_of(0x2501), Some("scheduler status error"));
        assert_eq!(error_str_of(0x2502), Some("scheduler internal error"));
        assert_eq!(error_str_of(0x2504), Some("Task timeout"));
        assert_eq!(error_str_of(0x2505), Some("Job is dropping"));
        assert_eq!(error_str_of(0x2550), Some("Invalid msg order"));
        assert_eq!(error_str_of(0x2600), Some("syntax error near"));
        assert_eq!(error_str_of(0x2601), Some("Incomplete SQL statement"));
        assert_eq!(error_str_of(0x2602), Some("Invalid column name"));
        assert_eq!(error_str_of(0x2603), Some("Table does not exist"));
        assert_eq!(error_str_of(0x2604), Some("Column ambiguously defined"));
        assert_eq!(error_str_of(0x2605), Some("Invalid value type"));
        assert_eq!(error_str_of(0x2608), Some("There mustn't be aggregation"));
        assert_eq!(
            error_str_of(0x2609),
            Some("ORDER BY item must be the number of a SELECT-list expression")
        );
        assert_eq!(error_str_of(0x260A), Some("Not a GROUP BY expression"));
        assert_eq!(error_str_of(0x260B), Some("Not SELECTed expression"));
        assert_eq!(
            error_str_of(0x260C),
            Some("Not a single-group group function")
        );
        assert_eq!(error_str_of(0x260D), Some("Tags number not matched"));
        assert_eq!(error_str_of(0x260E), Some("Invalid tag name"));
        assert_eq!(error_str_of(0x2610), Some("Name or password too long"));
        assert_eq!(error_str_of(0x2611), Some("Password can not be empty"));
        assert_eq!(
            error_str_of(0x2612),
            Some("Port should be an integer that is less than 65535 and greater than 0")
        );
        assert_eq!(
            error_str_of(0x2613),
            Some("Endpoint should be in the format of 'fqdn:port'")
        );
        assert_eq!(
            error_str_of(0x2614),
            Some("This statement is no longer supported")
        );
        assert_eq!(error_str_of(0x2615), Some("Interval too small"));
        assert_eq!(error_str_of(0x2616), Some("Database not specified"));
        assert_eq!(error_str_of(0x2617), Some("Invalid identifier name"));
        assert_eq!(
            error_str_of(0x2618),
            Some("Corresponding super table not in this db")
        );
        assert_eq!(error_str_of(0x2619), Some("Invalid database option"));
        assert_eq!(error_str_of(0x261A), Some("Invalid table option"));
        assert_eq!(
            error_str_of(0x2624),
            Some("GROUP BY and WINDOW-clause can't be used together")
        );
        assert_eq!(
            error_str_of(0x2627),
            Some("Aggregate functions do not support nesting")
        );
        assert_eq!(
            error_str_of(0x2628),
            Some("Only support STATE_WINDOW on integer/bool/varchar column")
        );
        assert_eq!(
            error_str_of(0x2629),
            Some("Not support STATE_WINDOW on tag column")
        );
        assert_eq!(
            error_str_of(0x262A),
            Some("STATE_WINDOW not support for super table query")
        );
        assert_eq!(
            error_str_of(0x262B),
            Some("SESSION gap should be fixed time window, and greater than 0")
        );
        assert_eq!(
            error_str_of(0x262C),
            Some("Only support SESSION on primary timestamp column")
        );
        assert_eq!(
            error_str_of(0x262D),
            Some("Interval offset cannot be negative")
        );
        assert_eq!(
            error_str_of(0x262E),
            Some("Cannot use 'year' as offset when interval is 'month'")
        );
        assert_eq!(
            error_str_of(0x262F),
            Some("Interval offset should be shorter than interval")
        );
        assert_eq!(
            error_str_of(0x2630),
            Some("Does not support sliding when interval is natural month/year")
        );
        assert_eq!(
            error_str_of(0x2631),
            Some("sliding value no larger than the interval value")
        );
        assert_eq!(
            error_str_of(0x2632),
            Some("sliding value can not less than 1%% of interval value")
        );
        assert_eq!(
            error_str_of(0x2633),
            Some("Only one tag if there is a json tag")
        );
        assert_eq!(
            error_str_of(0x2634),
            Some("Query block has incorrect number of result columns")
        );
        assert_eq!(error_str_of(0x2635), Some("Incorrect TIMESTAMP value"));
        assert_eq!(
            error_str_of(0x2637),
            Some("soffset/offset can not be less than 0")
        );
        assert_eq!(
            error_str_of(0x2638),
            Some("slimit/soffset only available for PARTITION/GROUP BY query")
        );
        assert_eq!(error_str_of(0x2639), Some("Invalid topic query"));
        assert_eq!(
            error_str_of(0x263A),
            Some("Cannot drop super table in batch")
        );
        assert_eq!(
            error_str_of(0x263B),
            Some("Start(end) time of query range required or time range too large")
        );
        assert_eq!(error_str_of(0x263C), Some("Duplicated column names"));
        assert_eq!(error_str_of(0x263D), Some("Tags length exceeds max length"));
        assert_eq!(error_str_of(0x263E), Some("Row length exceeds max length"));
        assert_eq!(error_str_of(0x263F), Some("Illegal number of columns"));
        assert_eq!(error_str_of(0x2640), Some("Too many columns"));
        assert_eq!(error_str_of(0x2641), Some("First column must be timestamp"));
        assert_eq!(
            error_str_of(0x2642),
            Some("Invalid binary/nchar column/tag length")
        );
        assert_eq!(error_str_of(0x2643), Some("Invalid number of tag columns"));
        assert_eq!(error_str_of(0x2644), Some("Permission denied"));
        assert_eq!(error_str_of(0x2645), Some("Invalid stream query"));
        assert_eq!(
            error_str_of(0x2646),
            Some("Invalid _c0 or _rowts expression")
        );
        assert_eq!(error_str_of(0x2647), Some("Invalid timeline function"));
        assert_eq!(error_str_of(0x2648), Some("Invalid password"));
        assert_eq!(error_str_of(0x2649), Some("Invalid alter table statement"));
        assert_eq!(
            error_str_of(0x264A),
            Some("Primary timestamp column cannot be dropped")
        );
        assert_eq!(error_str_of(0x264B), Some("Only binary/nchar column length could be modified, and the length can only be increased, not decreased"));
        assert_eq!(error_str_of(0x264C), Some("Invalid tbname pseudo column"));
        assert_eq!(error_str_of(0x264D), Some("Invalid function name"));
        assert_eq!(error_str_of(0x264E), Some("Comment too long"));
        assert_eq!(error_str_of(0x264F), Some("Some functions are allowed only in the SELECT list of a query. And, cannot be mixed with other non scalar functions or columns."));
        assert_eq!(error_str_of(0x2650), Some("Window query not supported, since the result of subquery not include valid timestamp column"));
        assert_eq!(error_str_of(0x2651), Some("No columns can be dropped"));
        assert_eq!(error_str_of(0x2652), Some("Only tag can be json type"));
        assert_eq!(error_str_of(0x2653), Some("Value too long for column/tag"));
        assert_eq!(
            error_str_of(0x2655),
            Some("The DELETE statement must have a definite time window range")
        );
        assert_eq!(
            error_str_of(0x2656),
            Some("The REDISTRIBUTE VGROUP statement only support 1 to 3 dnodes")
        );
        assert_eq!(error_str_of(0x2657), Some("Fill now allowed"));
        assert_eq!(error_str_of(0x2658), Some("Invalid windows pc"));
        assert_eq!(error_str_of(0x2659), Some("Window not allowed"));
        assert_eq!(error_str_of(0x265A), Some("Stream not allowed"));
        assert_eq!(error_str_of(0x265B), Some("Group by not allowd"));
        assert_eq!(error_str_of(0x265D), Some("Invalid interp clause"));
        assert_eq!(error_str_of(0x265E), Some("Not valid function ion window"));
        assert_eq!(error_str_of(0x265F), Some("Only support single table"));
        assert_eq!(error_str_of(0x2660), Some("Invalid sma index"));
        assert_eq!(error_str_of(0x2661), Some("Invalid SELECTed expression"));
        assert_eq!(error_str_of(0x2662), Some("Fail to get table info"));
        assert_eq!(error_str_of(0x2663), Some("Not unique table/alias"));
        assert_eq!(error_str_of(0x26FF), Some("Parser internal error"));
        assert_eq!(error_str_of(0x2700), Some("Planner internal error"));
        assert_eq!(error_str_of(0x2701), Some("Expect ts equal"));
        assert_eq!(error_str_of(0x2702), Some("Cross join not support"));
        assert_eq!(error_str_of(0x2800), Some("Function internal error"));
        assert_eq!(error_str_of(0x2801), Some("Invalid function para number"));
        assert_eq!(error_str_of(0x2802), Some("Invalid function para type"));
        assert_eq!(error_str_of(0x2803), Some("Invalid function para value"));
        assert_eq!(error_str_of(0x2804), Some("Not buildin function"));
        assert_eq!(
            error_str_of(0x2805),
            Some("Duplicate timestamps not allowed in function")
        );
        assert_eq!(error_str_of(0x2901), Some("udf is stopping"));
        assert_eq!(error_str_of(0x2902), Some("udf pipe read error"));
        assert_eq!(error_str_of(0x2903), Some("udf pipe connect error"));
        assert_eq!(error_str_of(0x2904), Some("udf pipe not exist"));
        assert_eq!(error_str_of(0x2905), Some("udf load failure"));
        assert_eq!(error_str_of(0x2906), Some("udf invalid function input"));
        assert_eq!(error_str_of(0x2907), Some("udf invalid bufsize"));
        assert_eq!(error_str_of(0x2908), Some("udf invalid output type"));
        assert_eq!(
            error_str_of(0x2909),
            Some("udf program language not supported")
        );
        assert_eq!(error_str_of(0x290A), Some("udf function execution failure"));
        assert_eq!(error_str_of(0x3000), Some("Invalid line protocol type"));
        assert_eq!(
            error_str_of(0x3001),
            Some("Invalid timestamp precision type")
        );
        assert_eq!(error_str_of(0x3002), Some("Invalid data format"));
        assert_eq!(error_str_of(0x3003), Some("Invalid schemaless db config"));
        assert_eq!(error_str_of(0x3004), Some("Not the same type like before"));
        assert_eq!(error_str_of(0x3005), Some("Internal error"));
        assert_eq!(error_str_of(0x3100), Some("Tsma init failed"));
        assert_eq!(error_str_of(0x3101), Some("Tsma already exists"));
        assert_eq!(error_str_of(0x3102), Some("Invalid tsma env"));
        assert_eq!(error_str_of(0x3103), Some("Invalid tsma state"));
        assert_eq!(error_str_of(0x3104), Some("Invalid tsma pointer"));
        assert_eq!(error_str_of(0x3105), Some("Invalid tsma parameters"));
        assert_eq!(error_str_of(0x3150), Some("Invalid rsma env"));
        assert_eq!(error_str_of(0x3151), Some("Invalid rsma state"));
        assert_eq!(error_str_of(0x3152), Some("Rsma qtaskinfo creation error"));
        assert_eq!(error_str_of(0x3153), Some("Rsma invalid schema"));
        assert_eq!(error_str_of(0x3154), Some("Rsma stream state open"));
        assert_eq!(error_str_of(0x3155), Some("Rsma stream state commit"));
        assert_eq!(error_str_of(0x3156), Some("Rsma fs ref error"));
        assert_eq!(error_str_of(0x3157), Some("Rsma fs sync error"));
        assert_eq!(error_str_of(0x3158), Some("Rsma fs update error"));
        assert_eq!(error_str_of(0x3200), Some("Index is rebuilding"));
        assert_eq!(error_str_of(0x3201), Some("Index file is invalid"));
        assert_eq!(error_str_of(0x4000), Some("Invalid message"));
        assert_eq!(error_str_of(0x4001), Some("Consumer mismatch"));
        assert_eq!(error_str_of(0x4002), Some("Consumer closed"));
        assert_eq!(error_str_of(0x4003), Some("Consumer error, to see log"));
        assert_eq!(error_str_of(0x4100), Some("Stream task not exist"));
        assert_eq!(error_str_of(0x4101), Some("Out of memory in stream queue"));
        assert_eq!(error_str_of(0x5100), Some("Invalid TDLite open flags"));
        assert_eq!(error_str_of(0x5101), Some("Invalid TDLite open directory"));
        assert_eq!(error_str_of(0x6000), Some("Queue out of memory"));
    }

    #[test]
    fn test_error_str_of_unknown_code() {
        assert_eq!(error_str_of(0xFFFF), None);
    }
}

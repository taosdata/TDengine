/***************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/
package com.taosdata.jdbc;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * TDengine error code and error message enumeration.
 *
 */
public enum TSDBError {

    TSDB_CODE_SUCCESS(0, "success"),
    TSDB_CODE_ACTION_IN_PROGRESS(1, "in progress"),
    TSDB_CODE_LAST_SESSION_NOT_FINISHED(5, "last session not finished"),
    TSDB_CODE_INVALID_SESSION_ID(6, "invalid session ID"),
    TSDB_CODE_INVALID_TRAN_ID(7, "invalid tran ID"),
    TSDB_CODE_INVALID_MSG_TYPE(8, "invalid msg type"),
    TSDB_CODE_ALREADY_PROCESSED(9, "alredy processed"),
    TSDB_CODE_AUTH_FAILURE(10, "authentication failure"),
    TSDB_CODE_WRONG_MSG_SIZE(11, "wrong msg size"),
    TSDB_CODE_UNEXPECTED_RESPONSE(12, "unexpected response"),
    TSDB_CODE_INVALID_RESPONSE_TYPE(13, "invalid response type"),
    TSDB_CODE_NO_RESOURCE(14, "no resource"),
    TSDB_CODE_INVALID_TIME_STAMP(15, "invalid time stamp"),
    TSDB_CODE_MISMATCHED_METER_ID(16, "mismatched meter ID"),
    TSDB_CODE_ACTION_TRANS_NOT_FINISHED(17, "transcation not finished"),
    TSDB_CODE_ACTION_NOT_ONLINE(18, "not online"),
    TSDB_CODE_ACTION_SEND_FAILD(19, "send failed"),
    TSDB_CODE_NOT_ACTIVE_SESSION(20, "not active session"),
    TSDB_CODE_INSERT_FAILED(21, "insert failed"),
    TSDB_CODE_APP_ERROR(22, "App error"),
    TSDB_CODE_INVALID_IE(23, "invalid IE"),
    TSDB_CODE_INVALID_VALUE(24, "invalid value"),
    TSDB_CODE_REDIRECT(25, "service not available"),
    TSDB_CODE_ALREADY_THERE(26, "already there"),
    TSDB_CODE_INVALID_METER_ID(27, "invalid meter ID"),
    TSDB_CODE_INVALID_SQL(28, "invalid SQL"), // this message often comes with additional info which will vary based on the specific error situation
    TSDB_CODE_NETWORK_UNAVAIL(29, "failed to connect to server"),
    TSDB_CODE_INVALID_MSG_LEN(30, "invalid msg len"),
    TSDB_CODE_INVALID_DB(31, "invalid DB"),
    TSDB_CODE_INVALID_TABLE(32, "invalid table"),
    TSDB_CODE_DB_ALREADY_EXIST(33, "DB already there"),
    TSDB_CODE_TABLE_ALREADY_EXIST(34, "table already there"),
    TSDB_CODE_INVALID_USER(35, "invalid user name"),
    TSDB_CODE_INVALID_ACCT(36, "invalid acct name"),
    TSDB_CODE_INVALID_PASS(37, "invalid password"),
    TSDB_CODE_DB_NOT_SELECTED(38, "DB not selected"),
    TSDB_CODE_MEMORY_CORRUPTED(39, "memory corrupted"),
    TSDB_CODE_USER_ALREADY_EXIST(40, "user name exists"),
    TSDB_CODE_NO_RIGHTS(41, "not authorized"),
    TSDB_CODE_DISCONNECTED(42, "login disconnected), login again"),
    TSDB_CODE_NO_MASTER(43, "mgmt master node not available"),
    TSDB_CODE_NOT_CONFIGURED(44, "not configured"),
    TSDB_CODE_INVALID_OPTION(45, "invalid option"),
    TSDB_CODE_NODE_OFFLINE(46, "node offline"),
    TSDB_CODE_SYNC_REQUIRED(47, "sync required"),
    TSDB_CODE_NO_ENOUGH_PNODES(48, "more dnodes are needed"),
    TSDB_CODE_UNSYNCED(49, "node in unsynced state"),
    TSDB_CODE_TOO_SLOW(50, "too slow"),
    TSDB_CODE_OTHERS(51, "others"),
    TSDB_CODE_NO_REMOVE_MASTER(52, "can't remove dnode which is master"),
    TSDB_CODE_WRONG_SCHEMA(53, "wrong schema"),
    TSDB_CODE_NO_RESULT(54, "no results"),
    TSDB_CODE_TOO_MANY_USERS(55, "num of users execeed maxUsers"),
    TSDB_CODE_TOO_MANY_DATABSES(56, "num of databases execeed maxDbs"),
    TSDB_CODE_TOO_MANY_TABLES(57, "num of tables execeed maxTables"),
    TSDB_CODE_TOO_MANY_DNODES(58, "num of dnodes execeed maxDnodes"),
    TSDB_CODE_TOO_MANY_ACCTS(59, "num of accounts execeed maxAccts"),
    TSDB_CODE_ACCT_ALREADY_EXIST(60, "accout name exists"),
    TSDB_CODE_DNODE_ALREADY_EXIST(61, "dnode ip exists"),
    TSDB_CODE_SDB_ERROR(62, "sdb error"),
    TSDB_CODE_METRICMETA_EXPIRED(63, "metric meta expired"), // local cached metric-meta expired causes error in metric query
    TSDB_CODE_NOT_READY(64, "not ready"), // peer is not ready to process data
    TSDB_CODE_MAX_SESSIONS(65, "too many sessions on server"), // too many sessions
    TSDB_CODE_MAX_CONNECTIONS(66, "too many sessions from app"), // too many connections
    TSDB_CODE_SESSION_ALREADY_EXIST(67, "session to dest is already there"),
    TSDB_CODE_NO_QSUMMARY(68, "query list not there), please show again"),
    TSDB_CODE_SERV_OUT_OF_MEMORY(69, "server out of memory"),
    TSDB_CODE_INVALID_QHANDLE(70, "invalid query handle"),
    TSDB_CODE_RELATED_TABLES_EXIST(71, "tables related to metric exist"),
    TSDB_CODE_MONITOR_DB_FORBEIDDEN(72, "can't drop monitor database or tables"),
    TSDB_CODE_VG_COMMITLOG_INIT_FAILED(73, "commit log init failed"),
    TSDB_CODE_VG_INIT_FAILED(74, "vgroup init failed"),
    TSDB_CODE_DATA_ALREADY_IMPORTED(75, "data is already imported"),
    TSDB_CODE_OPS_NOT_SUPPORT(76, "not supported operation"),
    TSDB_CODE_INVALID_QUERY_ID(77, "invalid query id string"),
    TSDB_CODE_INVALID_STREAM_ID(78, "invalid stream id string"),
    TSDB_CODE_INVALID_CONNECTION(79, "invalid connection string"),
    TSDB_CODE_ACTION_NOT_BALANCED(80, "dnode not balanced"),
    TSDB_CODE_CLI_OUT_OF_MEMORY(81, "client out of memory"),
    TSDB_CODE_DATA_OVERFLOW(82, "data value overflow"),
    TSDB_CODE_QUERY_CANCELLED(83, "query cancelled"),
    TSDB_CODE_GRANT_POINT_LIMITED(84, "grant points limited"),
    TSDB_CODE_GRANT_EXPIRED(85, "grant expired"),
    TSDB_CODE_CLI_NO_DISKSPACE(86, "client no disk space"),
    TSDB_CODE_FILE_CORRUPTED(87, "DB file corrupted"),
    TSDB_CODE_INVALID_CLIENT_VERSION(88, "version of client and server not match");

    private long errCode;
    private String errMessage;
    private static Map<Integer, String> errorCodeMap = new HashMap<>(86);
    static {
        errorCodeMap.put(0, "success");
        errorCodeMap.put(1, "in progress");
        errorCodeMap.put(5, "last session not finished");
        errorCodeMap.put(6, "invalid session ID");
        errorCodeMap.put(7, "invalid tran ID");
        errorCodeMap.put(8, "invalid msg type");
        errorCodeMap.put(9, "alredy processed");
        errorCodeMap.put(10, "authentication failure");
        errorCodeMap.put(11, "wrong msg size");
        errorCodeMap.put(12, "unexpected response");
        errorCodeMap.put(13, "invalid response type");
        errorCodeMap.put(14, "no resource");
        errorCodeMap.put(15, "invalid time stamp");
        errorCodeMap.put(16, "mismatched meter ID");
        errorCodeMap.put(17, "transcation not finished");
        errorCodeMap.put(18, "not online");
        errorCodeMap.put(19, "send failed");
        errorCodeMap.put(20, "not active session");
        errorCodeMap.put(21, "insert failed");
        errorCodeMap.put(22, "App error");
        errorCodeMap.put(23, "invalid IE");
        errorCodeMap.put(24, "invalid value");
        errorCodeMap.put(25, "service not available");
        errorCodeMap.put(26, "already there");
        errorCodeMap.put(27, "invalid meter ID");
        errorCodeMap.put(28, "invalid SQL"); // this message often comes with additional info which will vary based on the specific error situation
        errorCodeMap.put(29, "failed to connect to server");
        errorCodeMap.put(30, "invalid msg len");
        errorCodeMap.put(31, "invalid DB");
        errorCodeMap.put(32, "invalid table");
        errorCodeMap.put(33, "DB already there");
        errorCodeMap.put(34, "table already there");
        errorCodeMap.put(35, "invalid user name");
        errorCodeMap.put(36, "invalid acct name");
        errorCodeMap.put(37, "invalid password");
        errorCodeMap.put(38, "DB not selected");
        errorCodeMap.put(39, "memory corrupted");
        errorCodeMap.put(40, "user name exists");
        errorCodeMap.put(41, "not authorized");
        errorCodeMap.put(42, "login disconnected); login again");
        errorCodeMap.put(43, "mgmt master node not available");
        errorCodeMap.put(44, "not configured");
        errorCodeMap.put(45, "invalid option");
        errorCodeMap.put(46, "node offline");
        errorCodeMap.put(47, "sync required");
        errorCodeMap.put(48, "more dnodes are needed");
        errorCodeMap.put(49, "node in unsynced state");
        errorCodeMap.put(50, "too slow");
        errorCodeMap.put(51, "others");
        errorCodeMap.put(52, "can't remove dnode which is master");
        errorCodeMap.put(53, "wrong schema");
        errorCodeMap.put(54, "no results");
        errorCodeMap.put(55, "num of users execeed maxUsers");
        errorCodeMap.put(56, "num of databases execeed maxDbs");
        errorCodeMap.put(57, "num of tables execeed maxTables");
        errorCodeMap.put(58, "num of dnodes execeed maxDnodes");
        errorCodeMap.put(59, "num of accounts execeed maxAccts");
        errorCodeMap.put(60, "accout name exists");
        errorCodeMap.put(61, "dnode ip exists");
        errorCodeMap.put(62, "sdb error");
        errorCodeMap.put(63, "metric meta expired"); // local cached metric-meta expired causes error in metric query
        errorCodeMap.put(64, "not ready"); // peer is not ready to process data
        errorCodeMap.put(65, "too many sessions on server"); // too many sessions
        errorCodeMap.put(66, "too many sessions from app"); // too many connections
        errorCodeMap.put(67, "session to dest is already there");
        errorCodeMap.put(68, "query list not there); please show again");
        errorCodeMap.put(69, "server out of memory");
        errorCodeMap.put(70, "invalid query handle");
        errorCodeMap.put(71, "tables related to metric exist");
        errorCodeMap.put(72, "can't drop monitor database or tables");
        errorCodeMap.put(73, "commit log init failed");
        errorCodeMap.put(74, "vgroup init failed");
        errorCodeMap.put(75, "data is already imported");
        errorCodeMap.put(76, "not supported operation");
        errorCodeMap.put(77, "invalid query id string");
        errorCodeMap.put(78, "invalid stream id string");
        errorCodeMap.put(79, "invalid connection string");
        errorCodeMap.put(80, "dnode not balanced");
        errorCodeMap.put(81, "client out of memory");
        errorCodeMap.put(82, "data value overflow");
        errorCodeMap.put(83, "query cancelled");
        errorCodeMap.put(84, "grant points limited");
        errorCodeMap.put(85, "grant expired");
        errorCodeMap.put(86, "client no disk space");
        errorCodeMap.put(87, "DB file corrupted");
        errorCodeMap.put(88, "version of client and server not match");
    }

    TSDBError(long code, String message) {
        this.errCode = code;
        this.errMessage = message;
    }

    public long getErrCode() {
        return this.errCode;
    }

    public String getErrMessage() {
        return this.errMessage;
    }

    public static String getErrMessageByCode(long errCode) {
        return errorCodeMap.get(errCode);
    }

}

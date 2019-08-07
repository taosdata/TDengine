/*
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
 */

char *taosMsg[] = {"null",
                   "registration",
                   "registration-rsp",
                   "submit",
                   "submit-rsp",
                   "nw-change",
                   "nw-change-rsp",
                   "deliver",
                   "deliver-rsp",

                   "create",
                   "create-rsp",
                   "remove",
                   "remove-rsp",
                   "vpeers",
                   "vpeers-rsp",
                   "free-vnode",
                   "free-vnode-rsp",
                   "vpeer-cfg",
                   "vpeer-cfg-rsp",
                   "meter-cfg",
                   "meter-cfg-rsp",

                   "vpeer-fwd",
                   "vpeer-fwd-rsp",
                   "sync",
                   "sync-rsp",

                   "insert",
                   "insert-rsp",
                   "query",
                   "query-rsp",
                   "retrieve",
                   "retrieve-rsp",

                   "connect",
                   "connect-rsp",
                   "create-acct",
                   "create-acct-rsp",
                   "create-user",
                   "create-user-rsp",
                   "drop-acct",
                   "drop-acct-rsp",
                   "drop-user",
                   "drop-user-rsp",
                   "alter-user",
                   "alter-user-rsp",
                   "create-mnode",
                   "create-mnode-rsp",
                   "drop-mnode",
                   "drop-mnode-rsp",
                   "create-dnode",
                   "create-dnode-rsp",
                   "drop-dnode",
                   "drop-dnode-rsp",
                   "create-db",
                   "create-db-rsp",
                   "drop-db",
                   "drop-db-rsp",
                   "use-db",
                   "use-db-rsp",
                   "create-table",
                   "create-table-rsp",
                   "drop-table",
                   "drop-table-rsp",
                   "meter-info",
                   "meter-info-rsp",
                   "metric-meta",
                   "metric-meta-rsp",
                   "show",
                   "show-rsp",

                   "forward",
                   "forward-rsp",

                   "cfg-dnode",
                   "cfg-dnode-rsp",
                   "cfg-mnode",
                   "cfg-mnode-rsp",

                   "kill-query",
                   "kill-query-rsp",
                   "kill-stream",
                   "kill-stream-rsp",
                   "kill-connection",
                   "kill-connectoin-rsp",  // 78
                   "alter-stream",
                   "alter-stream-rsp",
                   "alter-table",
                   "alter-table-rsp",

                   "",
                   "",
                   "",
                   "",
                   "",
                   "",
                   "",
                   "",

                   "heart-beat",           // 91
                   "heart-beat-rsp",
                   "status",
                   "status-rsp",
                   "grant",
                   "grant-rsp",
                   "alter-acct",
                   "alter-acct-rsp",
                   "invalid"};

char *tsError[] = {"success",
                   "in progress",
                   "",
                   "",
                   "",

                   "last session not finished",  // 5
                   "invalid session ID",
                   "invalid tran ID",
                   "invalid msg type",
                   "alredy processed",
                   "authentication failure",     // 10
                   "wrong msg size",
                   "unexpected response",
                   "invalid response type",
                   "no resource",
                   "server-client date time unsynced",         // 15
                   "mismatched meter ID",
                   "transcation not finished",
                   "not online",
                   "send failed",
                   "not active session",     // 20
                   "insert failed",
                   "App error",
                   "invalid IE",
                   "invalid value",
                   "service not available",  // 25
                   "already there",
                   "invalid meter ID",
                   "invalid SQL",
                   "failed to connect to server",
                   "invalid msg len",        // 30
                   "invalid DB",
                   "invalid table",
                   "DB already there",
                   "table already there",
                   "invalid user name",      // 35
                   "invalid acct name",
                   "invalid password",
                   "DB not selected",
                   "memory corrupted",
                   "user name exists",       // 40
                   "not authorized",
                   "login disconnected, login again",
                   "mgmt master node not available",
                   "not configured",
                   "invalid option",         // 45
                   "node offline",
                   "sync required",
                   "more dnodes are needed",
                   "node in unsynced state",
                   "too slow",               // 50
                   "others",
                   "can't remove dnode which is master",
                   "wrong schema",
                   "no results",
                   "num of users execeed maxUsers",
                   "num of databases execeed maxDbs",
                   "num of tables execeed maxTables",
                   "num of dnodes execeed maxDnodes",
                   "num of accounts execeed maxAccts",
                   "accout name exists",     // 60
                   "dnode ip exists",
                   "sdb error",
                   "metric meta expired",
                   "not ready",
                   "too many sessions on server",  // 65
                   "too many sessions from app",
                   "session to dest is already there",
                   "query list not there, please show again",
                   "server out of memory",
                   "invalid query handle",         // 70
                   "tables related to metric exist",
                   "can't drop monitor database or tables",
                   "commit log init failed",
                   "vgroup init failed",
                   "data is already imported",     // 75
                   "not supported operation",
                   "invalid query id string",
                   "invalid stream id string",
                   "invalid connection string",
                   "dnode not balanced",        // 80
                   "client out of memory",
                   "data value overflow",
                   "query cancelled",
                   "grant timeseries limited",  // 84
                   "grant expired",             // 85
                   "client no disk space",
                   "DB file corrupted",
                   "version of client and server not match",
                   "invalid account parameter",
                   "no enough available time series",
                   "storage credit is used up",
                   "query credit is used up",    // 92
                   "grant database limited",
                   "grant user limited",
                   "grant connection limited",
                   "grant stream limited",
                   "grant writing speed limited",
                   "grant storage limited",
                   "grant query time limited",   // 99
                   "grant account limited",
                   "grant dnode limited",
                   "grant cpu core limited",     // 102
                   "session not ready",
                   "batch size too big",
                   "timestamp out of range",
                   "invalid query message",
                   "timestamp disordered in cache block",
                   "timestamp disordered in file block",
                   "invalid commit log"
};

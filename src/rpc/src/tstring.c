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

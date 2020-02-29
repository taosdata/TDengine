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

char *taosMsg[] = {
    "null",
    "registration",
    "registration-rsp",
    "submit",
    "submit-rsp",
    "query",
    "query-rsp",
    "retrieve",
    "retrieve-rsp",
    "create-table",
    "create-table-rsp",        //10

    "remove-table",
    "remove-table-rsp",
    "create-vnode",
    "create-vnode-rsp",
    "free-vnode",
    "free-vnode-rsp",
    "cfg-dnode",
    "cfg-dnode-rsp",
    "alter-stream",
    "alter-stream-rsp",        //20

    "sync",
    "sync-rsp",
    "forward",
    "forward-rsp",
    "drop-stable",
    "drop-stable-rsp",
    "",
    "",
    "",
    "",                        //30

    "connect",
    "connect-rsp",
    "create-acct",
    "create-acct-rsp",
    "alter-acct",
    "alter-acct-rsp",
    "drop-acct",
    "drop-acct-rsp",
    "create-user",
    "create-user-rsp",         //40

    "alter-user",
    "alter-user-rsp",
    "drop-user",
    "drop-user-rsp",
    "create-mnode",
    "create-mnode-rsp",
    "drop-mnode",
    "drop-mnode-rsp",
    "create-dnode",
    "create-dnode-rsp",        //50

    "drop-dnode",
    "drop-dnode-rsp",
    "alter-dnode",
    "alter-dnode-rsp",
    "create-db",
    "create-db-rsp",
    "drop-db",
    "drop-db-rsp",
    "use-db",
    "use-db-rsp",              //60

    "alter-db",
    "alter-db-rsp",
    "create-table",
    "create-table-rsp",
    "drop-table",
    "drop-table-rsp",
    "alter-table",
    "alter-table-rsp",
    "cfg-vnode",
    "cfg-vnode-rsp",           //70

    "cfg-table",
    "cfg-table-rsp",
    "table-meta",
    "table-meta-rsp",
    "super-table-meta",
    "super-stable-meta-rsp",
    "multi-table-meta",
    "multi-table-meta-rsp",
    "alter-stream",
    "alter-stream-rsp",        //80

    "show",
    "show-rsp",
    "cfg-mnode",
    "cfg-mnode-rsp",
    "kill-query",
    "kill-query-rsp",
    "kill-stream",
    "kill-stream-rsp",
    "kill-connection",
    "kill-connectoin-rsp",     //90

    "heart-beat",
    "heart-beat-rsp",
    "status",
    "status-rsp",
    "grant",
    "grant-rsp",
    "max"
};
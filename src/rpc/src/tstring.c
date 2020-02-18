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

    "create-normal-table",
    "create-normal-table-rsp",
    "create-stream-table",
    "create-stream-table-rsp",
    "create-super-table",
    "create-super-table-rsp",
    "remove-table",
    "remove-table-rsp",
    "remove-normal-table",
    "remove-normal-table-rsp",  //20

    "remove-stream-table",
    "remove-stream-table-rsp",
    "remove-super-table",
    "remove-super-table-rsp",
    "alter-table",
    "alter-table-rsp",
    "alter-normal-table",
    "alter-normal-table-rsp",
    "alter-stream-table",
    "alter-stream-table-rsp",  //30

    "alter-super-table",
    "alter-super-table-rsp",
    "vpeers",
    "vpeers-rsp",
    "free-vnode",
    "free-vnode-rsp",
    "cfg-dnode",
    "cfg-dnode-rsp",
    "alter-stream",
    "alter-stream-rsp",        //40

    "sync",
    "sync-rsp",
    "forward",
    "forward-rsp",
    "",
    "",
    "",
    "",
    "",
    "",                        //50

    "connect",
    "connect-rsp",
    "create-acct",
    "create-acct-rsp",
    "alter-acct",
    "alter-acct-rsp",
    "drop-acct",
    "drop-acct-rsp",
    "create-user",
    "create-user-rsp",         //60

    "alter-user",
    "alter-user-rsp",
    "drop-user",
    "drop-user-rsp",
    "create-mnode",
    "create-mnode-rsp",
    "drop-mnode",
    "drop-mnode-rsp",
    "create-dnode",
    "create-dnode-rsp",        //70

    "drop-dnode",
    "drop-dnode-rsp",
    "alter-dnode",
    "alter-dnode-rsp",
    "create-db",
    "create-db-rsp",
    "drop-db",
    "drop-db-rsp",
    "use-db",
    "use-db-rsp",              //80

    "alter-db",
    "alter-db-rsp",
    "create-table",
    "create-table-rsp",
    "drop-table",
    "drop-table-rsp",
    "alter-table",
    "alter-table-rsp",
    "cfg-vnode",
    "cfg-vnode-rsp",           //90

    "cfg-table",
    "cfg-table-rsp",
    "table-meta",
    "table-meta-rsp",
    "super-table-meta",
    "super-stable-meta-rsp",
    "multi-table-meta",
    "multi-table-meta-rsp",
    "alter-stream",
    "alter-stream-rsp",        //100

    "show",
    "show-rsp",
    "cfg-mnode",
    "cfg-mnode-rsp",
    "kill-query",
    "kill-query-rsp",
    "kill-stream",
    "kill-stream-rsp",
    "kill-connection",
    "kill-connectoin-rsp",     //110

    "heart-beat",
    "heart-beat-rsp",
    "status",
    "status-rsp",
    "grant",
    "grant-rsp",
    "max"
};
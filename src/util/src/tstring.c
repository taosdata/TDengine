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

    "drop-table",
    "drop-table-rsp",
    "alter-table",
    "alter-table-rsp",
    "create-vnode",
    "create-vnode-rsp",
    "drop-vnode",
    "drop-vnode-rsp",
    "alter-vnode",
    "alter-vnode-rsp",        //20

    "drop-stable",
    "drop-stable-rsp",
    "alter-stream",
    "alter-stream-rsp",
    "config-dnode",
    "config-dnode-rsp",
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
    "create-dnode",
    "create-dnode-rsp",
    "drop-dnode",
    "drop-dnode-rsp",
    "create-db",
    "create-db-rsp",          //50

    "drop-db",
    "drop-db-rsp",
    "use-db",
    "use-db-rsp",
    "alter-db",
    "alter-db-rsp",
    "create-table",
    "create-table-rsp",
    "drop-table",
    "drop-table-rsp",        //60

    "alter-table",
    "alter-table-rsp",
    "table-meta",
    "table-meta-rsp",
    "super-table-meta",
    "super-stable-meta-rsp",
    "multi-table-meta",
    "multi-table-meta-rsp",
    "alter-stream",
    "alter-stream-rsp",     //70

    "show",
    "show-rsp",
    "kill-query",
    "kill-query-rsp",
    "kill-stream",
    "kill-stream-rsp",
    "kill-connection",
    "kill-connectoin-rsp",
    "heart-beat",
    "heart-beat-rsp",      //80

    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",                    //90

    "config-table",
    "config-table-rsp",
    "config-vnode",
    "config-vnode-rsp",
    "status",
    "status-rsp",
    "grant",
    "grant-rsp",
    "",
    "",                   //100

    "sdb-sync",
    "sdb-sync-rsp",
    "sdb-forward",
    "sdb-forward-rsp",
    "max"
};


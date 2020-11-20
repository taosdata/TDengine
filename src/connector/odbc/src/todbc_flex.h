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

#ifndef _TODBC_FLEX_H_
#define _TODBC_FLEX_H_

typedef struct conn_val_s              conn_val_t;
struct conn_val_s {
    char                *key;
    char                *dsn;
    char                *uid;
    char                *pwd;
    char                *db;
    char                *server;
    char                *svr_enc;
    char                *cli_enc;
};


void conn_val_reset(conn_val_t *val);
int todbc_parse_conn_string(const char *conn, conn_val_t *val);

#endif // _TODBC_FLEX_H_


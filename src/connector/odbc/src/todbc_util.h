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

#ifndef _TODBC_UTIL_H_
#define _TODBC_UTIL_H_

#include "os.h"

#include <sql.h>
#include <sqltypes.h>

const char* sql_sql_type(int type);
const char* sql_c_type(int type);

int is_valid_sql_c_type(int type);
int is_valid_sql_sql_type(int type);

int utf8_chars(const char *src);

#endif // _TODBC_UTIL_H_


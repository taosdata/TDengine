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

#ifndef TDENGINE_TSQL_H
#define TDENGINE_TSQL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taos.h"
#include "taosmsg.h"
#include "ttokendef.h"
#include "ttypes.h"
#include "tvariant.h"
#include "qsqlparser.h"

enum {
  TSQL_NODE_TYPE_EXPR = 0x1,
  TSQL_NODE_TYPE_ID = 0x2,
  TSQL_NODE_TYPE_VALUE = 0x4,
};

#define NON_ARITHMEIC_EXPR 0
#define NORMAL_ARITHMETIC 1
#define AGG_ARIGHTMEIC    2

int32_t tSQLParse(SSqlInfo *pSQLInfo, const char *pSql);

#ifdef __cplusplus
}
#endif

#endif

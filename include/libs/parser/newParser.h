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

#ifndef _TD_NEW_PARSER_H_
#define _TD_NEW_PARSER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "parser.h"

typedef enum EStmtType {
  STMT_TYPE_CMD = 1,
  STMT_TYPE_QUERY
} EStmtType;

typedef struct SQuery {
  EStmtType stmtType;
  SNode* pRoot;
  int32_t numOfResCols;
  SSchema* pResSchema;
} SQuery;

int32_t parser(SParseContext* pParseCxt, SQuery* pQuery);

#ifdef __cplusplus
}
#endif

#endif /*_TD_NEW_PARSER_H_*/

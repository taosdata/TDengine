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

#ifndef _TD_PARSER_INT_H_
#define _TD_PARSER_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "parToken.h"
#include "parUtil.h"
#include "parser.h"

int32_t parseInsertSql(SParseContext* pContext, SQuery** pQuery);
int32_t parse(SParseContext* pParseCxt, SQuery** pQuery);
int32_t collectMetaKey(SParseContext* pParseCxt, SQuery* pQuery);
int32_t authenticate(SParseContext* pParseCxt, SQuery* pQuery);
int32_t translate(SParseContext* pParseCxt, SQuery* pQuery);
int32_t extractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema);
int32_t calculateConstant(SParseContext* pParseCxt, SQuery* pQuery);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_INT_H_*/

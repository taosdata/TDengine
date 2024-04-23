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
#include "parTranslater.h"
#include "parser.h"

#define QUERY_SMA_OPTIMIZE_DISABLE 0
#define QUERY_SMA_OPTIMIZE_ENABLE  1

int32_t parseInsertSql(SParseContext* pCxt, SQuery** pQuery, SCatalogReq* pCatalogReq, const SMetaData* pMetaData);
int32_t parse(SParseContext* pParseCxt, SQuery** pQuery);
int32_t collectMetaKey(SParseContext* pParseCxt, SQuery* pQuery, SParseMetaCache* pMetaCache);
int32_t authenticate(SParseContext* pParseCxt, SQuery* pQuery, SParseMetaCache* pMetaCache);
int32_t translate(SParseContext* pParseCxt, SQuery* pQuery, SParseMetaCache* pMetaCache);
int32_t extractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema);
int32_t calculateConstant(SParseContext* pParseCxt, SQuery* pQuery);
int32_t translatePostCreateStream(SParseContext* pParseCxt, SQuery* pQuery, SSDataBlock* pBlock);
int32_t translatePostCreateSmaIndex(SParseContext* pParseCxt, SQuery* pQuery, SSDataBlock* pBlock);
int32_t buildQueryAfterParse(SQuery** pQuery, SNode* pRootNode, int16_t placeholderNo, SArray** pPlaceholderValues);
int32_t translateTable(STranslateContext* pCxt, SNode** pTable);
int32_t getMetaDataFromHash(const char* pKey, int32_t len, SHashObj* pHash, void** pOutput);
void    tfreeSParseQueryRes(void* p);

#ifdef TD_ENTERPRISE
int32_t translateView(STranslateContext* pCxt, SNode** pTable, SName* pName);
int32_t getViewMetaFromMetaCache(STranslateContext* pCxt, SName* pName, SViewMeta** ppViewMeta);
#endif
#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_INT_H_*/

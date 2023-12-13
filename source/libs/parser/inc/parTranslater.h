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

#ifndef _TD_PARSER_TRANS_H_
#define _TD_PARSER_TRANS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "parToken.h"
#include "parUtil.h"
#include "parser.h"
#include "cmdnodes.h"

typedef struct STranslateContext {
  SParseContext*   pParseCxt;
  int32_t          errCode;
  SMsgBuf          msgBuf;
  SArray*          pNsLevel;  // element is SArray*, the element of this subarray is STableNode*
  int32_t          currLevel;
  int32_t          levelNo;
  ESqlClause       currClause;
  SNode*           pCurrStmt;
  SCmdMsgInfo*     pCmdMsg;
  SHashObj*        pDbs;
  SHashObj*        pTables;
  SHashObj*        pTargetTables;
  SExplainOptions* pExplainOpt;
  SParseMetaCache* pMetaCache;
  bool             createStream;
  bool             stableQuery;
  bool             showRewrite;
  SNode*           pPrevRoot;
  SNode*           pPostRoot;
} STranslateContext;

bool biRewriteToTbnameFunc(STranslateContext* pCxt, SNode** ppNode);
int32_t biRewriteSelectStar(STranslateContext* pCxt, SSelectStmt* pSelect);
int32_t biCheckCreateTableTbnameCol(STranslateContext* pCxt, SCreateTableStmt* pStmt);
int32_t findTable(STranslateContext* pCxt, const char* pTableAlias, STableNode** pOutput);
int32_t getTargetMetaImpl(SParseContext* pParCxt, SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta, bool couldBeView);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_TRANS_H_*/
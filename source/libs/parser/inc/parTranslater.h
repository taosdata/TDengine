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

#include "cmdnodes.h"
#include "parToken.h"
#include "parUtil.h"
#include "parser.h"

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
  bool             stableQuery;
  bool             showRewrite;
  bool             withOpt;
  SNode*           pPrevRoot;
  SNode*           pPostRoot;
  bool             dual;  // whether select stmt without from stmt, true for without.
  bool             skipCheck;
  bool             refTable;
  int64_t          placeHolderBitmap;
  bool             createStreamCalc;
  bool             createStreamTrigger;
  bool             createStreamOutTable;
  bool             extLeftEq; // used for external window, true means include left border
  bool             extRightEq; // used for external window, true means include right border
  SNode*           createStreamTriggerTbl;
  SNodeList*       createStreamTriggerPartitionList;
} STranslateContext;

int32_t biRewriteToTbnameFunc(STranslateContext* pCxt, SNode** ppNode, bool* pRet);
int32_t biRewriteSelectStar(STranslateContext* pCxt, SSelectStmt* pSelect);
int32_t biCheckCreateTableTbnameCol(STranslateContext* pCxt, SNodeList* pTags, SNodeList* pCols);
int32_t findTable(STranslateContext* pCxt, const char* pTableAlias, STableNode** pOutput);
int32_t getTargetMetaImpl(SParseContext* pParCxt, SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta,
                          bool couldBeView);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_TRANS_H_*/

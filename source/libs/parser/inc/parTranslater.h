/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

typedef struct SCorrelationSemanticContext SCorrelationSemanticContext;

typedef struct STranslateContext {
  SParseContext*           pParseCxt;
  int32_t                  errCode;
  ESubQueryType            expSubQueryType;
  SMsgBuf                  msgBuf;
  SArray*                  pNsLevel;     // element is SArray*, the element of this subarray is STableNode*
  SNodeList*               pSubQueries;  // sub queries NOT from FROM clause, SNodeList<SNode*>
  int32_t                  currLevel;
  int32_t                  levelNo;
  ESqlClause               currClause;
  SNode*                   pCurrStmt;
  SCmdMsgInfo*             pCmdMsg;
  SHashObj*                pDbs;
  SHashObj*                pTables;
  SHashObj*                pTargetTables;
  SExplainOptions*         pExplainOpt;
  SParseMetaCache*         pMetaCache;
  int64_t                  statementTimeNs;  // Shared by time functions in this translation.
  bool                     isExprSubQ;
  bool                     isCorrelatedSubQ;
  bool                     hasNonLocalSubQ;
  bool                     hasLocalSubQ;
  bool                     statementTimeSet;
  bool                     hasVolatileFunc;  // query/statement contains a volatile function (e.g. SLEEP)
  bool                     stableQuery;
  bool                     showRewrite;
  bool                     withOpt;
  SNode*                   pPrevRoot;
  SNode*                   pPostRoot;
  bool                     dual;  // whether select stmt without from stmt, true for without.
  bool                     skipCheck;
  bool                     refTable;
  bool                     isCurrOpIn;
  bool                     inJoinOnCond;
  ENodeType                origStmtType;
  timezone_t               streamTimezone;     // borrowed from the current CREATE STREAM statement
  const SStreamTimeConfig* pStreamTimeConfig;  // borrowed from the current CREATE STREAM statement
  SParseStreamInfo         streamInfo;
  // When a correlated EXISTS/NOT EXISTS is allowed because both outer and inner
  // FROM tables are from the same external source, this flag is set so that
  // rewriteExprSubQuery can pre-render the EXISTS SQL for pushdown rather than
  // creating a separate inner subquery plan.
  bool sameExtSourceCorrelatedExists;
  SCorrelationSemanticContext* pCorrelation;  // Shared scope and binding registry.
  SArray* pCorrelationScopeLevels;  // Current scopeId at each query level; owned by this context.
  int32_t currCorrelationScopeId;   // Scope currently being translated.
  int32_t rootCorrelationScopeId;   // Root scope created for this translation context.
  int32_t subqueryCorrelationScopeId;  // Scope returned by the latest expression subquery.
  int64_t nextCorrelationScopeExprId;  // exprId assigned to the next scope that is entered.
  int64_t subqueryCorrelationExprId;   // exprId returned by the latest expression subquery.
  bool ownsCorrelationContext;  // Whether this context must destroy pCorrelation.
  bool subqueryIsCorrelated;     // Whether the latest expression subquery captured an outer binding.
  // Set once vtbRefSetSrcTableErrMsg has written an enriched "virtual table ... query
  // failed" message: an outer hop hitting the same failure must not overwrite the
  // inner (more precise) message.
  bool vtbRefErrMsgSet;
  int32_t virtualTableDepth;  // Active nested virtual-table translations.
} STranslateContext;

int32_t normalizeCreateStreamSql(STranslateContext* pCxt, const SCreateStreamStmt* pStmt, char** ppSql);
int32_t biRewriteToTbnameFunc(STranslateContext* pCxt, SNode** ppNode, bool* pRet);
int32_t biRewriteSelectStar(STranslateContext* pCxt, SSelectStmt* pSelect);
int32_t biCheckCreateTableTbnameCol(STranslateContext* pCxt, SNodeList* pTags, SNodeList* pCols);
int32_t findTable(STranslateContext* pCxt, const char* pTableAlias, STableNode** pOutput);
int32_t getTargetMetaImpl(SParseContext* pParCxt, SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta,
                          bool couldBeView);
// On success, *pExtInfo is an independent heap copy owned by the caller.
int32_t getExternalSourceInfo(STranslateContext* pCxt, const char* sourceName, SExtSourceInfo** pExtInfo);

#ifdef TD_ENTERPRISE
int32_t translateCheckPrivCols(STranslateContext* pCxt, SSelectStmt* pSelect);
int32_t translateProcessMaskColFunc(STranslateContext* pCxt, SSelectStmt* pSelect);
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_TRANS_H_*/

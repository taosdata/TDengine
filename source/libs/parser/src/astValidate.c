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

#include "ttime.h"
#include "parserInt.h"
#include "parserUtil.h"

static int32_t evaluateImpl(tSqlExpr* pExpr, int32_t tsPrecision) {
  int32_t code = 0;
  if (pExpr->type == SQL_NODE_EXPR) {
    code = evaluateImpl(pExpr->pLeft, tsPrecision);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = evaluateImpl(pExpr->pRight, tsPrecision);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pExpr->pLeft->type == SQL_NODE_VALUE && pExpr->pRight->type == SQL_NODE_VALUE) {
      tSqlExpr* pLeft  = pExpr->pLeft;
      tSqlExpr* pRight = pExpr->pRight;
      if ((pLeft->tokenId == TK_TIMESTAMP && (pRight->tokenId == TK_INTEGER || pRight->tokenId == TK_FLOAT)) ||
          ((pRight->tokenId == TK_TIMESTAMP && (pLeft->tokenId == TK_INTEGER || pLeft->tokenId == TK_FLOAT)))) {
        return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
      } else if (pLeft->tokenId == TK_TIMESTAMP && pRight->tokenId == TK_TIMESTAMP) {
        tSqlExprEvaluate(pExpr);
      } else {
        tSqlExprEvaluate(pExpr);
      }
    } else {
      // Other types of expressions are not evaluated, they will be handled during the validation of the abstract syntax tree.
    }
  } else if (pExpr->type == SQL_NODE_VALUE) {
    if (pExpr->tokenId == TK_NOW) {
      pExpr->value.i64   = taosGetTimestamp(tsPrecision);
      pExpr->value.nType = TSDB_DATA_TYPE_BIGINT;
      pExpr->tokenId     = TK_TIMESTAMP;
    } else if (pExpr->tokenId == TK_VARIABLE) {
      char    unit = 0;
      SToken* pToken = &pExpr->exprToken;
      int32_t ret = parseAbsoluteDuration(pToken->z, pToken->n, &pExpr->value.i64, &unit, tsPrecision);
      if (ret != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
      }

      pExpr->value.nType = TSDB_DATA_TYPE_BIGINT;
      pExpr->tokenId = TK_TIMESTAMP;
    }  else if (pExpr->tokenId == TK_NULL) {
      pExpr->value.nType = TSDB_DATA_TYPE_NULL;
    } else if (pExpr->tokenId == TK_INTEGER || pExpr->tokenId == TK_STRING || pExpr->tokenId == TK_FLOAT || pExpr->tokenId == TK_BOOL) {
      SToken* pToken = &pExpr->exprToken;

      int32_t tokenType = pToken->type;
      toTSDBType(tokenType);
      taosVariantCreate(&pExpr->value, pToken->z, pToken->n, tokenType);
    }

    return  TSDB_CODE_SUCCESS;
    // other types of data are handled in the parent level.
  }

  return  TSDB_CODE_SUCCESS;
}

int32_t validateSqlNode(SSqlNode* pSqlNode, SQueryStmtInfo* pQueryInfo, char* msg, int32_t msgBufLen) {
  assert(pSqlNode != NULL && (pSqlNode->from == NULL || taosArrayGetSize(pSqlNode->from->list) > 0));

  const char* msg1 = "point interpolation query needs timestamp";
  const char* msg2 = "too many tables in from clause";
  const char* msg3 = "start(end) time of query range required or time range too large";
  const char* msg4 = "interval query not supported, since the result of sub query not include valid timestamp column";
  const char* msg5 = "only tag query not compatible with normal column filter";
  const char* msg6 = "not support stddev/percentile/interp in the outer query yet";
  const char* msg7 = "derivative/twa/irate requires timestamp column exists in subquery";
  const char* msg8 = "condition missing for join query";
  const char* msg9 = "not support 3 level select";

  int32_t  code = TSDB_CODE_SUCCESS;
  STableMetaInfo  *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (pTableMetaInfo == NULL) {
    pTableMetaInfo = tscAddEmptyMetaInfo(pQueryInfo);
  }

  /*
   * handle the sql expression without from subclause
   * select server_status();
   * select server_version();
   * select client_version();
   * select database();
   */
  if (pSqlNode->from == NULL) {
    assert(pSqlNode->fillType == NULL && pSqlNode->pGroupby == NULL && pSqlNode->pWhere == NULL &&
           pSqlNode->pSortOrder == NULL);
    return doLocalQueryProcess(pCmd, pQueryInfo, pSqlNode);
  }

  if (pSqlNode->from->type == SQL_NODE_FROM_SUBQUERY) {
    clearAllTableMetaInfo(pQueryInfo, false, pSql->self);
    pQueryInfo->numOfTables = 0;

    // parse the subquery in the first place
    int32_t numOfSub = (int32_t)taosArrayGetSize(pSqlNode->from->list);
    for (int32_t i = 0; i < numOfSub; ++i) {
      // check if there is 3 level select
      SRelElementPair* subInfo = taosArrayGet(pSqlNode->from->list, i);
      SSqlNode* p = taosArrayGetP(subInfo->pSubquery, 0);
      if (p->from->type == SQL_NODE_FROM_SUBQUERY) {
        return parserSetInvalidOperatorMsg(msg, msgBufLen, msg9);
      }

      code = doValidateSubquery(pSqlNode, i, pSql, pQueryInfo, tscGetErrorMsgPayload(pCmd));
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    int32_t timeWindowQuery =
        (TPARSER_HAS_TOKEN(pSqlNode->interval.interval) || TPARSER_HAS_TOKEN(pSqlNode->sessionVal.gap));
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TABLE_QUERY);

    // parse the group by clause in the first place
    if (validateGroupbyNode(pQueryInfo, pSqlNode->pGroupby, pCmd) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }


    if (validateSelectNodeList(pCmd, pQueryInfo, pSqlNode->pSelNodeList, false, timeWindowQuery, true) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // todo NOT support yet
    for (int32_t i = 0; i < tscNumOfExprs(pQueryInfo); ++i) {
      SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
      int32_t    f = pExpr->base.functionId;
      if (f == TSDB_FUNC_STDDEV || f == TSDB_FUNC_PERCT || f == TSDB_FUNC_INTERP) {
        return parserSetInvalidOperatorMsg(msg, msgBufLen, msg6);
      }

      if ((timeWindowQuery || pQueryInfo->stateWindow) && f == TSDB_FUNC_LAST) {
        pExpr->base.numOfParams = 1;
        pExpr->base.param[0].i64 = TSDB_ORDER_ASC;
        pExpr->base.param[0].nType = TSDB_DATA_TYPE_INT;
      }
    }

    STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, 0)->pTableMeta;
    SSchema*    pSchema = tscGetTableColumnSchema(pTableMeta, 0);

    if (pSchema->type != TSDB_DATA_TYPE_TIMESTAMP) {
      int32_t numOfExprs = (int32_t)tscNumOfExprs(pQueryInfo);

      for (int32_t i = 0; i < numOfExprs; ++i) {
        SExprInfo* pExpr = tscExprGet(pQueryInfo, i);

        int32_t f = pExpr->base.functionId;
        if (f == TSDB_FUNC_DERIVATIVE || f == TSDB_FUNC_TWA || f == TSDB_FUNC_IRATE) {
          return parserSetInvalidOperatorMsg(msg, msgBufLen, msg7);
        }
      }
    }

    // validate the query filter condition info
    if (pSqlNode->pWhere != NULL) {
      if (validateWhereNode(pQueryInfo, &pSqlNode->pWhere, pSql) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else {
      if (pQueryInfo->numOfTables > 1) {
        return parserSetInvalidOperatorMsg(msg, msgBufLen, msg8);
      }
    }

    // validate the interval info
    if (validateIntervalNode(pSql, pQueryInfo, pSqlNode) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    } else {
      if (validateSessionNode(pCmd, pQueryInfo, pSqlNode) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      // parse the window_state
      if (validateStateWindowNode(pCmd, pQueryInfo, pSqlNode, false) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (isTimeWindowQuery(pQueryInfo)) {
        // check if the first column of the nest query result is timestamp column
        SColumn* pCol = taosArrayGetP(pQueryInfo->colList, 0);
        if (pCol->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
          return parserSetInvalidOperatorMsg(msg, msgBufLen, msg4);
        }

        if (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      }
    }

    // disable group result mixed up if interval/session window query exists.
    if (isTimeWindowQuery(pQueryInfo)) {
      size_t num = taosArrayGetSize(pQueryInfo->pUpstream);
      for(int32_t i = 0; i < num; ++i) {
        SQueryInfo* pUp = taosArrayGetP(pQueryInfo->pUpstream, i);
        pUp->multigroupResult = false;
      }
    }

    // parse the having clause in the first place
    int32_t joinQuery = (pSqlNode->from != NULL && taosArrayGetSize(pSqlNode->from->list) > 1);
    if (validateHavingClause(pQueryInfo, pSqlNode->pHaving, pCmd, pSqlNode->pSelNodeList, joinQuery, timeWindowQuery) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if ((code = validateLimitNode(pCmd, pQueryInfo, pSqlNode, pSql)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    // set order by info
    if (validateOrderbyNode(pCmd, pQueryInfo, pSqlNode, tscGetTableSchema(pTableMeta)) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if ((code = doFunctionsCompatibleCheck(pCmd, pQueryInfo, tscGetErrorMsgPayload(pCmd))) != TSDB_CODE_SUCCESS) {
      return code;
    }

//    updateFunctionInterBuf(pQueryInfo, false);
    updateLastScanOrderIfNeeded(pQueryInfo);

    if ((code = validateFillNode(pCmd, pQueryInfo, pSqlNode)) != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else {
    pQueryInfo->command = TSDB_SQL_SELECT;

    size_t numOfTables = taosArrayGetSize(pSqlNode->from->list);
    if (numOfTables > TSDB_MAX_JOIN_TABLE_NUM) {
      return parserSetInvalidOperatorMsg(msg, msgBufLen, msg2);
    }

    // set all query tables, which are maybe more than one.
    code = doLoadAllTableMeta(pSql, pQueryInfo, pSqlNode, (int32_t) numOfTables);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    bool isSTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);

    int32_t type = isSTable? TSDB_QUERY_TYPE_STABLE_QUERY:TSDB_QUERY_TYPE_TABLE_QUERY;
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, type);

    // parse the group by clause in the first place
    if (validateGroupbyNode(pQueryInfo, pSqlNode->pGroupby, pCmd) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
    pQueryInfo->onlyHasTagCond = true;
    // set where info
    if (pSqlNode->pWhere != NULL) {
      if (validateWhereNode(pQueryInfo, &pSqlNode->pWhere, pSql) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      pSqlNode->pWhere = NULL;
    } else {
      if (taosArrayGetSize(pSqlNode->from->list) > 1) { // Cross join not allowed yet
        return parserSetInvalidOperatorMsg(msg, msgBufLen, "cross join not supported yet");
      }
    }

    int32_t joinQuery = (pSqlNode->from != NULL && taosArrayGetSize(pSqlNode->from->list) > 1);
    int32_t timeWindowQuery =
        (TPARSER_HAS_TOKEN(pSqlNode->interval.interval) || TPARSER_HAS_TOKEN(pSqlNode->sessionVal.gap));

    if (validateSelectNodeList(pCmd, pQueryInfo, pSqlNode->pSelNodeList, joinQuery, timeWindowQuery, false) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (isSTable && tscQueryTags(pQueryInfo) && pQueryInfo->distinct && !pQueryInfo->onlyHasTagCond) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // parse the window_state
    if (validateStateWindowNode(pCmd, pQueryInfo, pSqlNode, isSTable) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // set order by info
    if (validateOrderbyNode(pCmd, pQueryInfo, pSqlNode, tscGetTableSchema(pTableMetaInfo->pTableMeta)) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // set interval value
    if (validateIntervalNode(pSql, pQueryInfo, pSqlNode) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (tscQueryTags(pQueryInfo)) {
      SExprInfo* pExpr1 = tscExprGet(pQueryInfo, 0);

      if (pExpr1->base.functionId != TSDB_FUNC_TID_TAG) {
        if ((pQueryInfo->colCond && taosArrayGetSize(pQueryInfo->colCond) > 0) || IS_TSWINDOW_SPECIFIED(pQueryInfo->window))  {
          return parserSetInvalidOperatorMsg(msg, msgBufLen, msg5);
        }
      }
    }

    // parse the having clause in the first place
    if (validateHavingClause(pQueryInfo, pSqlNode->pHaving, pCmd, pSqlNode->pSelNodeList, joinQuery, timeWindowQuery) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    /*
     * transfer sql functions that need secondary merge into another format
     * in dealing with super table queries such as: count/first/last
     */
    if (validateSessionNode(pCmd, pQueryInfo, pSqlNode) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (isTimeWindowQuery(pQueryInfo) && (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (isSTable) {
      tscTansformFuncForSTableQuery(pQueryInfo);
      if (hasUnsupportFunctionsForSTableQuery(pCmd, pQueryInfo)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    }

    // no result due to invalid query time range
    if (pQueryInfo->window.skey > pQueryInfo->window.ekey) {
      pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      return TSDB_CODE_SUCCESS;
    }

    if (!hasTimestampForPointInterpQuery(pQueryInfo)) {
      return parserSetInvalidOperatorMsg(msg, msgBufLen, msg1);
    }

    // in case of join query, time range is required.
    if (QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
      uint64_t timeRange = (uint64_t)pQueryInfo->window.ekey - pQueryInfo->window.skey;
      if (timeRange == 0 && pQueryInfo->window.skey == 0) {
        return parserSetInvalidOperatorMsg(msg, msgBufLen, msg3);
      }
    }

    if ((code = validateLimitNode(pCmd, pQueryInfo, pSqlNode, pSql)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    if ((code = doFunctionsCompatibleCheck(pCmd, pQueryInfo,tscGetErrorMsgPayload(pCmd))) != TSDB_CODE_SUCCESS) {
      return code;
    }

    updateLastScanOrderIfNeeded(pQueryInfo);
    tscFieldInfoUpdateOffset(pQueryInfo);
//    updateFunctionInterBuf(pQueryInfo, isSTable);

    if ((code = validateFillNode(pCmd, pQueryInfo, pSqlNode)) != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  { // set the query info
    pQueryInfo->projectionQuery = tscIsProjectionQuery(pQueryInfo);
    pQueryInfo->hasFilter = tscHasColumnFilter(pQueryInfo);
    pQueryInfo->simpleAgg = isSimpleAggregateRv(pQueryInfo);
    pQueryInfo->onlyTagQuery = onlyTagPrjFunction(pQueryInfo);
    pQueryInfo->groupbyColumn = tscGroupbyColumn(pQueryInfo);
    pQueryInfo->arithmeticOnAgg   = tsIsArithmeticQueryOnAggResult(pQueryInfo);
    pQueryInfo->orderProjectQuery = tscOrderedProjectionQueryOnSTable(pQueryInfo, 0);

    SExprInfo** p = NULL;
    int32_t numOfExpr = 0;
    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    code = createProjectionExpr(pQueryInfo, pTableMetaInfo, &p, &numOfExpr);
    if (pQueryInfo->exprList1 == NULL) {
      pQueryInfo->exprList1 = taosArrayInit(4, POINTER_BYTES);
    }

    taosArrayAddBatch(pQueryInfo->exprList1, (void*) p, numOfExpr);
    tfree(p);
  }

  return TSDB_CODE_SUCCESS;  // Does not build query message here
}

int32_t evaluateSqlNode(SSqlNode* pNode, int32_t tsPrecision, char* msg, int32_t msgBufLen) {
  assert(pNode != NULL && msg != NULL && msgBufLen > 0);
  if (pNode->pWhere == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = evaluateImpl(pNode->pWhere, tsPrecision);
  if (code != TSDB_CODE_SUCCESS) {
    strncpy(msg, "invalid time expression in sql", msgBufLen);
    return code;
  }

  return code;
}

int32_t qParserValidateSqlNode(struct SCatalog* pCatalog, SSqlInfo* pInfo, SQueryStmtInfo* pQueryInfo, int64_t id, char* msgBuf, int32_t msgBufLen) {
  //1. if it is a query, get the meta info and continue.
  assert(pCatalog != NULL && pInfo != NULL);
  int32_t code = 0;
#if 0
  switch (pInfo->type) {
    case TSDB_SQL_DROP_TABLE:
    case TSDB_SQL_DROP_USER:
    case TSDB_SQL_DROP_ACCT:
    case TSDB_SQL_DROP_DNODE:
    case TSDB_SQL_DROP_DB: {
      const char* msg1 = "param name too long";
      const char* msg2 = "invalid name";

      SToken* pzName = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if ((pInfo->type != TSDB_SQL_DROP_DNODE) && (parserValidateIdToken(pzName) != TSDB_CODE_SUCCESS)) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      if (pInfo->type == TSDB_SQL_DROP_DB) {
        assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
        code = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pzName);
        if (code != TSDB_CODE_SUCCESS) {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
        }

      } else if (pInfo->type == TSDB_SQL_DROP_TABLE) {
        assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);

        code = tscSetTableFullName(&pTableMetaInfo->name, pzName, pSql);
        if(code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else if (pInfo->type == TSDB_SQL_DROP_DNODE) {
        if (pzName->type == TK_STRING) {
          pzName->n = strdequote(pzName->z);
        }
        strncpy(pCmd->payload, pzName->z, pzName->n);
      } else {  // drop user/account
        if (pzName->n >= TSDB_USER_LEN) {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg3);
        }

        strncpy(pCmd->payload, pzName->z, pzName->n);
      }

      break;
    }

    case TSDB_SQL_USE_DB: {
      const char* msg = "invalid db name";
      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pToken);
      if (ret != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }

      break;
    }

    case TSDB_SQL_RESET_CACHE: {
      return TSDB_CODE_SUCCESS;
    }

    case TSDB_SQL_SHOW: {
      if (setShowInfo(pSql, pInfo) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      break;
    }

    case TSDB_SQL_CREATE_FUNCTION:
    case TSDB_SQL_DROP_FUNCTION:  {
      code = handleUserDefinedFunc(pSql, pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      break;
    }

    case TSDB_SQL_ALTER_DB:
    case TSDB_SQL_CREATE_DB: {
      const char* msg1 = "invalid db name";
      const char* msg2 = "name too long";

      SCreateDbInfo* pCreateDB = &(pInfo->pMiscInfo->dbOpt);
      if (pCreateDB->dbname.n >= TSDB_DB_NAME_LEN) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      char buf[TSDB_DB_NAME_LEN] = {0};
      SToken token = taosTokenDup(&pCreateDB->dbname, buf, tListLen(buf));

      if (tscValidateName(&token) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), &token);
      if (ret != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      if (parseCreateDBOptions(pCmd, pCreateDB) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      break;
    }

    case TSDB_SQL_CREATE_DNODE: {
      const char* msg = "invalid host name (ip address)";

      if (taosArrayGetSize(pInfo->pMiscInfo->a) > 1) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }

      SToken* id = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (id->type == TK_STRING) {
        id->n = strdequote(id->z);
      }
      break;
    }

    case TSDB_SQL_CREATE_ACCT:
    case TSDB_SQL_ALTER_ACCT: {
      const char* msg1 = "invalid state option, available options[no, r, w, all]";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "name too long";

      SToken* pName = &pInfo->pMiscInfo->user.user;
      SToken* pPwd = &pInfo->pMiscInfo->user.passwd;

      if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (pName->n >= TSDB_USER_LEN) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      SCreateAcctInfo* pAcctOpt = &pInfo->pMiscInfo->acctOpt;
      if (pAcctOpt->stat.n > 0) {
        if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
        } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
        } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
        } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
        } else {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
        }
      }

      break;
    }

    case TSDB_SQL_DESCRIBE_TABLE: {
      const char* msg1 = "invalid table name";

      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }
      // additional msg has been attached already
      code = tscSetTableFullName(&pTableMetaInfo->name, pToken, pSql);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      return tscGetTableMeta(pSql, pTableMetaInfo);
    }
    case TSDB_SQL_SHOW_CREATE_STABLE:
    case TSDB_SQL_SHOW_CREATE_TABLE: {
      const char* msg1 = "invalid table name";

      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }

      code = tscSetTableFullName(&pTableMetaInfo->name, pToken, pSql);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      return tscGetTableMeta(pSql, pTableMetaInfo);
    }
    case TSDB_SQL_SHOW_CREATE_DATABASE: {
      const char* msg1 = "invalid database name";

      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }

      if (pToken->n > TSDB_DB_NAME_LEN) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }
      return tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pToken);
    }
    case TSDB_SQL_CFG_DNODE: {
      const char* msg2 = "invalid configure options or values, such as resetlog / debugFlag 135 / balance 'vnode:2-dnode:2' / monitor 1 ";
      const char* msg3 = "invalid dnode ep";

      /* validate the ip address */
      SMiscInfo* pMiscInfo = pInfo->pMiscInfo;

      /* validate the parameter names and options */
      if (validateDNodeConfig(pMiscInfo) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      char* pMsg = pCmd->payload;

      SCfgDnodeMsg* pCfg = (SCfgDnodeMsg*)pMsg;

      SToken* t0 = taosArrayGet(pMiscInfo->a, 0);
      SToken* t1 = taosArrayGet(pMiscInfo->a, 1);

      t0->n = strdequote(t0->z);
      strncpy(pCfg->ep, t0->z, t0->n);

      if (validateEp(pCfg->ep) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg3);
      }

      strncpy(pCfg->config, t1->z, t1->n);

      if (taosArrayGetSize(pMiscInfo->a) == 3) {
        SToken* t2 = taosArrayGet(pMiscInfo->a, 2);

        pCfg->config[t1->n] = ' ';  // add sep
        strncpy(&pCfg->config[t1->n + 1], t2->z, t2->n);
      }

      break;
    }

    case TSDB_SQL_CREATE_USER:
    case TSDB_SQL_ALTER_USER: {
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "name too long";
      const char* msg5 = "invalid user rights";
      const char* msg7 = "not support options";

      pCmd->command = pInfo->type;

      SUserInfo* pUser = &pInfo->pMiscInfo->user;
      SToken* pName = &pUser->user;
      SToken* pPwd = &pUser->passwd;

      if (pName->n >= TSDB_USER_LEN) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      if (pCmd->command == TSDB_SQL_CREATE_USER) {
        if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      } else {
        if (pUser->type == TSDB_ALTER_USER_PASSWD) {
          if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
            return TSDB_CODE_TSC_INVALID_OPERATION;
          }
        } else if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
          assert(pPwd->type == TSDB_DATA_TYPE_NULL);

          SToken* pPrivilege = &pUser->privilege;

          if (strncasecmp(pPrivilege->z, "super", 5) == 0 && pPrivilege->n == 5) {
            pCmd->count = 1;
          } else if (strncasecmp(pPrivilege->z, "read", 4) == 0 && pPrivilege->n == 4) {
            pCmd->count = 2;
          } else if (strncasecmp(pPrivilege->z, "write", 5) == 0 && pPrivilege->n == 5) {
            pCmd->count = 3;
          } else {
            return setInvalidOperatorMsg(msgBuf, msgBufLen, msg5);
          }
        } else {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg7);
        }
      }

      break;
    }

    case TSDB_SQL_CFG_LOCAL: {
      SMiscInfo  *pMiscInfo = pInfo->pMiscInfo;
      const char *msg = "invalid configure options or values";

      // validate the parameter names and options
      if (validateLocalConfig(pMiscInfo) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }

      int32_t numOfToken = (int32_t) taosArrayGetSize(pMiscInfo->a);
      assert(numOfToken >= 1 && numOfToken <= 2);

      SToken* t = taosArrayGet(pMiscInfo->a, 0);
      strncpy(pCmd->payload, t->z, t->n);
      if (numOfToken == 2) {
        SToken* t1 = taosArrayGet(pMiscInfo->a, 1);
        pCmd->payload[t->n] = ' ';  // add sep
        strncpy(&pCmd->payload[t->n + 1], t1->z, t1->n);
      }
      return TSDB_CODE_SUCCESS;
    }

    case TSDB_SQL_CREATE_TABLE: {
      SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;

      if (pCreateTable->type == TSQL_CREATE_TABLE || pCreateTable->type == TSQL_CREATE_STABLE) {
        if ((code = doCheckForCreateTable(pSql, 0, pInfo)) != TSDB_CODE_SUCCESS) {
          return code;
        }

      } else if (pCreateTable->type == TSQL_CREATE_TABLE_FROM_STABLE) {
        assert(pCmd->numOfCols == 0);
        if ((code = doCheckForCreateFromStable(pSql, pInfo)) != TSDB_CODE_SUCCESS) {
          return code;
        }

      } else if (pCreateTable->type == TSQL_CREATE_STREAM) {
        if ((code = doCheckForStream(pSql, pInfo)) != TSDB_CODE_SUCCESS) {
          return code;
        }
      }

      break;
    }

    case TSDB_SQL_SELECT: {
      const char * msg1 = "no nested query supported in union clause";
      code = loadAllTableMeta(pSql, pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      pQueryInfo = tscGetQueryInfo(pCmd);

      size_t size = taosArrayGetSize(pInfo->list);
      for (int32_t i = 0; i < size; ++i) {
        SSqlNode* pSqlNode = taosArrayGetP(pInfo->list, i);

        tscTrace("0x%"PRIx64" start to parse the %dth subclause, total:%"PRIzu, pSql->self, i, size);

        if (size > 1 && pSqlNode->from && pSqlNode->from->type == SQL_NODE_FROM_SUBQUERY) {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
        }

//        normalizeSqlNode(pSqlNode); // normalize the column name in each function
        if ((code = validateSqlNode(pSql, pSqlNode, pQueryInfo)) != TSDB_CODE_SUCCESS) {
          return code;
        }

        tscPrintSelNodeList(pSql, i);

        if ((i + 1) < size && pQueryInfo->sibling == NULL) {
          if ((code = tscAddQueryInfo(pCmd)) != TSDB_CODE_SUCCESS) {
            return code;
          }

          SArray *pUdfInfo = NULL;
          if (pQueryInfo->pUdfInfo) {
            pUdfInfo = taosArrayDup(pQueryInfo->pUdfInfo);
          }

          pQueryInfo = pCmd->active;
          pQueryInfo->pUdfInfo = pUdfInfo;
          pQueryInfo->udfCopy = true;
        }
      }

      if ((code = normalizeVarDataTypeLength(pCmd)) != TSDB_CODE_SUCCESS) {
        return code;
      }

      // set the command/global limit parameters from the first subclause to the sqlcmd object
      pCmd->active = pCmd->pQueryInfo;
      pCmd->command = pCmd->pQueryInfo->command;

      STableMetaInfo* pTableMetaInfo1 = tscGetMetaInfo(pCmd->active, 0);
      if (pTableMetaInfo1->pTableMeta != NULL) {
        pSql->res.precision = tscGetTableInfo(pTableMetaInfo1->pTableMeta).precision;
      }

      return TSDB_CODE_SUCCESS;  // do not build query message here
    }

    case TSDB_SQL_ALTER_TABLE: {
      if ((code = setAlterTableInfo(pSql, pInfo)) != TSDB_CODE_SUCCESS) {
        return code;
      }

      break;
    }

    case TSDB_SQL_KILL_QUERY:
    case TSDB_SQL_KILL_STREAM:
    case TSDB_SQL_KILL_CONNECTION: {
      if ((code = setKillInfo(pSql, pInfo, pInfo->type)) != TSDB_CODE_SUCCESS) {
        return code;
      }
      break;
    }

    case TSDB_SQL_SYNC_DB_REPLICA: {
      const char* msg1 = "invalid db name";
      SToken* pzName = taosArrayGet(pInfo->pMiscInfo->a, 0);

      assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
      code = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pzName);
      if (code != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }
      break;
    }
    case TSDB_SQL_COMPACT_VNODE:{
      const char* msg = "invalid compact";
      if (setCompactVnodeInfo(pSql, pInfo) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }
      break;
    }
    default:
      return setInvalidOperatorMsg(msgBuf, msgBufLen, "not support sql expression");
  }
#endif

  SMetaReq req = {0};
  SMetaData data = {0};

  // TODO: check if the qnode info has been cached already
  req.qNodeEpset = true;
  code = qParserExtractRequestedMetaInfo(pInfo, &req, msgBuf, msgBufLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // load the meta data from catalog
  code = catalogGetMetaData(pCatalog, &req, &data);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // evaluate the sqlnode
  STableMeta* pTableMeta = (STableMeta*) taosArrayGetP(data.pTableMeta, 0);
  assert(pTableMeta != NULL);

  size_t len = taosArrayGetSize(pInfo->list);
  for(int32_t i = 0; i < len; ++i) {
    SSqlNode* p = taosArrayGetP(pInfo->list, i);
    code = evaluateSqlNode(p, pTableMeta->tableInfo.precision, msgBuf, msgBufLen);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // convert the sqlnode into queryinfo


  return code;
}

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
  qParserExtractRequestedMetaInfo(pInfo->list, &req);

  return 0;
}

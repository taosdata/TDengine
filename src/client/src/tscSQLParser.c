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

#define _BSD_SOURCE
#define _XOPEN_SOURCE 500
#define _DEFAULT_SOURCE

#include "os.h"
#include "qAst.h"
#include "taos.h"
#include "taosmsg.h"
#include "tcompare.h"
#include "tname.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tstoken.h"
#include "tstrbuild.h"
#include "ttime.h"
#include "ttokendef.h"

#define DEFAULT_PRIMARY_TIMESTAMP_COL_NAME "_c0"

// -1 is tbname column index, so here use the -2 as the initial value
#define COLUMN_INDEX_INITIAL_VAL (-2)
#define COLUMN_INDEX_INITIALIZER \
  { COLUMN_INDEX_INITIAL_VAL, COLUMN_INDEX_INITIAL_VAL }
#define COLUMN_INDEX_VALIDE(index) (((index).tableIndex >= 0) && ((index).columnIndex >= TSDB_TBNAME_COLUMN_INDEX))
#define TBNAME_LIST_SEP ","

typedef struct SColumnList {  // todo refactor
  int32_t      num;
  SColumnIndex ids[TSDB_MAX_COLUMNS];
} SColumnList;

static SSqlExpr* doAddProjectCol(SQueryInfo* pQueryInfo, int32_t outputIndex, int32_t colIndex, int32_t tableIndex);

static int32_t setShowInfo(SSqlObj* pSql, SSqlInfo* pInfo);
static char*   getAccountId(SSqlObj* pSql);

static bool has(tFieldList* pFieldList, int32_t startIdx, const char* name);
static void getCurrentDBName(SSqlObj* pSql, SSQLToken* pDBToken);
static bool hasSpecifyDB(SSQLToken* pTableName);
static bool validateTableColumnInfo(tFieldList* pFieldList, SSqlCmd* pCmd);
static bool validateTagParams(tFieldList* pTagsList, tFieldList* pFieldList, SSqlCmd* pCmd);

static int32_t setObjFullName(char* fullName, const char* account, SSQLToken* pDB, SSQLToken* tableName, int32_t* len);

static void getColumnName(tSQLExprItem* pItem, char* resultFieldName, int32_t nameLength);
static void getRevisedName(char* resultFieldName, int32_t functionId, int32_t maxLen, char* columnName);

static int32_t addExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t colIndex, tSQLExprItem* pItem, bool finalResult);
static int32_t insertResultField(SQueryInfo* pQueryInfo, int32_t outputIndex, SColumnList* pIdList, int16_t bytes,
                                 int8_t type, char* fieldName, SSqlExpr* pSqlExpr);
static int32_t changeFunctionID(int32_t optr, int16_t* functionId);
static int32_t parseSelectClause(SSqlCmd* pCmd, int32_t clauseIndex, tSQLExprList* pSelection, bool isSTable);

static bool validateIpAddress(const char* ip, size_t size);
static bool hasUnsupportFunctionsForSTableQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static bool functionCompatibleCheck(SQueryInfo* pQueryInfo);
static void setColumnOffsetValueInResultset(SQueryInfo* pQueryInfo);

static int32_t parseGroupbyClause(SQueryInfo* pQueryInfo, tVariantList* pList, SSqlCmd* pCmd);

static int32_t parseIntervalClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySql);
static int32_t parseSlidingClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySql);

static int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSQLExprItem* pItem);

static int32_t parseWhereClause(SQueryInfo* pQueryInfo, tSQLExpr** pExpr, SSqlObj* pSql);
static int32_t parseFillClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySQL);
static int32_t parseOrderbyClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySql, SSchema* pSchema);

static int32_t tsRewriteFieldNameIfNecessary(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static int32_t setAlterTableInfo(SSqlObj* pSql, struct SSqlInfo* pInfo);
static int32_t validateSqlFunctionInStreamSql(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static int32_t buildArithmeticExprString(tSQLExpr* pExpr, char** exprString);
static int32_t validateFunctionsInIntervalOrGroupbyQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static int32_t validateArithmeticSQLExpr(SSqlCmd* pCmd, tSQLExpr* pExpr, SQueryInfo* pQueryInfo, SColumnList* pList, int32_t* type);
static int32_t validateEp(char* ep);
static int32_t validateDNodeConfig(tDCLSQL* pOptions);
static int32_t validateLocalConfig(tDCLSQL* pOptions);
static int32_t validateColumnName(char* name);
static int32_t setKillInfo(SSqlObj* pSql, struct SSqlInfo* pInfo, int32_t killType);

static bool validateOneTags(SSqlCmd* pCmd, TAOS_FIELD* pTagField);
static bool hasTimestampForPointInterpQuery(SQueryInfo* pQueryInfo);

static int32_t parseLimitClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t index, SQuerySQL* pQuerySql, SSqlObj* pSql);
static int32_t parseCreateDBOptions(SSqlCmd* pCmd, SCreateDBInfo* pCreateDbSql);
static int32_t getColumnIndexByName(SSqlCmd* pCmd, const SSQLToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex);
static int32_t getTableIndexByName(SSQLToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex);
static int32_t optrToString(tSQLExpr* pExpr, char** exprString);

static int32_t getTableIndexImpl(SSQLToken* pTableToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex);
static int32_t doFunctionsCompatibleCheck(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static int32_t doLocalQueryProcess(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySql);
static int32_t tscCheckCreateDbParams(SSqlCmd* pCmd, SCMCreateDbMsg* pCreate);

static SColumnList getColumnList(int32_t num, int16_t tableIndex, int32_t columnIndex);

static int32_t doCheckForCreateTable(SSqlObj* pSql, int32_t subClauseIndex, SSqlInfo* pInfo);
static int32_t doCheckForCreateFromStable(SSqlObj* pSql, SSqlInfo* pInfo);
static int32_t doCheckForStream(SSqlObj* pSql, SSqlInfo* pInfo);
static int32_t doCheckForQuery(SSqlObj* pSql, SQuerySQL* pQuerySql, int32_t index);
static int32_t exprTreeFromSqlExpr(SSqlCmd* pCmd, tExprNode **pExpr, const tSQLExpr* pSqlExpr, SArray* pExprInfo, SQueryInfo* pQueryInfo, SArray* pCols);

/*
 * Used during parsing query sql. Since the query sql usually small in length, error position
 * is not needed in the final error message.
 */
static int32_t invalidSqlErrMsg(char* dstBuffer, const char* errMsg) {
  return tscInvalidSQLErrMsg(dstBuffer, errMsg, NULL);
}

static int setColumnFilterInfoForTimestamp(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tVariant* pVar) {
  int64_t     time = 0;
  const char* msg = "invalid timestamp";

  strdequote(pVar->pz);
  char*           seg = strnchr(pVar->pz, '-', pVar->nLen, false);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  if (seg != NULL) {
    if (taosParseTime(pVar->pz, &time, pVar->nLen, tinfo.precision, tsDaylight) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
    }
  } else {
    if (tVariantDump(pVar, (char*)&time, TSDB_DATA_TYPE_BIGINT, true)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
    }
  }

  tVariantDestroy(pVar);
  tVariantCreateFromBinary(pVar, (char*)&time, 0, TSDB_DATA_TYPE_BIGINT);

  return TSDB_CODE_SUCCESS;
}

static int32_t handlePassword(SSqlCmd* pCmd, SSQLToken* pPwd) {
  const char* msg1 = "password can not be empty";
  const char* msg2 = "name or password too long";
  const char* msg3 = "password needs single quote marks enclosed";

  if (pPwd->type != TK_STRING) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  strdequote(pPwd->z);
  pPwd->n = strtrim(pPwd->z);  // trim space before and after passwords

  if (pPwd->n <= 0) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pPwd->n >= TSDB_PASSWORD_LEN) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscToSQLCmd(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  if (pInfo == NULL || pSql == NULL || pSql->signature != pSql) {
    return TSDB_CODE_TSC_APP_ERROR;
  }

  SSqlCmd*    pCmd = &(pSql->cmd);
  SQueryInfo* pQueryInfo = NULL;

  if (!pInfo->valid) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), pInfo->pzErrMsg);
  }

  int32_t code = tscGetQueryInfoDetailSafely(pCmd, pCmd->clauseIndex, &pQueryInfo);

  STableMetaInfo* pTableMetaInfo = NULL;
  if (pQueryInfo->numOfTables == 0) {
    pTableMetaInfo = tscAddEmptyMetaInfo(pQueryInfo);
  } else {
    pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];
  }

  pCmd->command = pInfo->type;

  switch (pInfo->type) {
    case TSDB_SQL_DROP_TABLE:
    case TSDB_SQL_DROP_USER:
    case TSDB_SQL_DROP_ACCT:
    case TSDB_SQL_DROP_DNODE:
    case TSDB_SQL_DROP_DB: {
      const char* msg2 = "invalid name";
      const char* msg3 = "param name too long";

      SSQLToken* pzName = &pInfo->pDCLInfo->a[0];
      if ((pInfo->type != TSDB_SQL_DROP_DNODE) && (tscValidateName(pzName) != TSDB_CODE_SUCCESS)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      if (pInfo->type == TSDB_SQL_DROP_DB) {
        assert(pInfo->pDCLInfo->nTokens == 1);

        code = setObjFullName(pTableMetaInfo->name, getAccountId(pSql), pzName, NULL, NULL);
        if (code != TSDB_CODE_SUCCESS) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
        }

      } else if (pInfo->type == TSDB_SQL_DROP_TABLE) {
        assert(pInfo->pDCLInfo->nTokens == 1);

        if (tscSetTableFullName(pTableMetaInfo, pzName, pSql) != TSDB_CODE_SUCCESS) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }
      } else if (pInfo->type == TSDB_SQL_DROP_DNODE) {
        pzName->n = strdequote(pzName->z);
        strncpy(pTableMetaInfo->name, pzName->z, pzName->n);
      } else {  // drop user
        if (pzName->n >= TSDB_USER_LEN) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        strncpy(pTableMetaInfo->name, pzName->z, pzName->n);
      }

      break;
    }

    case TSDB_SQL_USE_DB: {
      const char* msg = "invalid db name";
      SSQLToken*  pToken = &pInfo->pDCLInfo->a[0];

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }

      int32_t ret = setObjFullName(pTableMetaInfo->name, getAccountId(pSql), pToken, NULL, NULL);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }

      break;
    }

    case TSDB_SQL_RESET_CACHE: {
      return TSDB_CODE_SUCCESS;
    }

    case TSDB_SQL_SHOW: {
      if (setShowInfo(pSql, pInfo) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      break;
    }

    case TSDB_SQL_ALTER_DB:
    case TSDB_SQL_CREATE_DB: {
      const char* msg1 = "invalid db name";
      const char* msg2 = "name too long";

      SCreateDBInfo* pCreateDB = &(pInfo->pDCLInfo->dbOpt);
      if (tscValidateName(&pCreateDB->dbname) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      int32_t ret = setObjFullName(pTableMetaInfo->name, getAccountId(pSql), &(pCreateDB->dbname), NULL, NULL);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      if (parseCreateDBOptions(pCmd, pCreateDB) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      break;
    }

    case TSDB_SQL_CREATE_DNODE: {  // todo hostname
      const char* msg = "invalid host name (ip address)";

      if (pInfo->pDCLInfo->nTokens > 1) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }

      SSQLToken* pIpAddr = &pInfo->pDCLInfo->a[0];
      pIpAddr->n = strdequote(pIpAddr->z);
      break;
    }

    case TSDB_SQL_CREATE_ACCT:
    case TSDB_SQL_ALTER_ACCT: {
      const char* msg1 = "invalid state option, available options[no, r, w, all]";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "name too long";

      SSQLToken* pName = &pInfo->pDCLInfo->user.user;
      SSQLToken* pPwd = &pInfo->pDCLInfo->user.passwd;

      if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      if (pName->n >= TSDB_USER_LEN) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SCreateAcctSQL* pAcctOpt = &pInfo->pDCLInfo->acctOpt;
      if (pAcctOpt->stat.n > 0) {
        if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
        } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
        } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
        } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
        } else {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
        }
      }

      break;
    }

    case TSDB_SQL_DESCRIBE_TABLE: {
      SSQLToken*  pToken = &pInfo->pDCLInfo->a[0];
      const char* msg2 = "table name is too long";
      const char* msg1 = "invalid table name";

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (!tscValidateTableNameLength(pToken->n)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      if (tscSetTableFullName(pTableMetaInfo, pToken, pSql) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      return tscGetTableMeta(pSql, pTableMetaInfo);
    }

    case TSDB_SQL_CFG_DNODE: {
      const char* msg2 = "invalid configure options or values, such as resetlog / debugFlag 135 / balance 'vnode:2-dnode:2' / monitor 1 ";
      const char* msg3 = "invalid dnode ep";

      /* validate the ip address */
      tDCLSQL* pDCL = pInfo->pDCLInfo;

      /* validate the parameter names and options */
      if (validateDNodeConfig(pDCL) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      char* pMsg = pCmd->payload;

      SCMCfgDnodeMsg* pCfg = (SCMCfgDnodeMsg*)pMsg;
      pDCL->a[0].n = strdequote(pDCL->a[0].z);
      
      strncpy(pCfg->ep, pDCL->a[0].z, pDCL->a[0].n);

      if (validateEp(pCfg->ep) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      strncpy(pCfg->config, pDCL->a[1].z, pDCL->a[1].n);

      if (pDCL->nTokens == 3) {
        pCfg->config[pDCL->a[1].n] = ' ';  // add sep
        strncpy(&pCfg->config[pDCL->a[1].n + 1], pDCL->a[2].z, pDCL->a[2].n);
      }

      break;
    }

    case TSDB_SQL_CREATE_USER:
    case TSDB_SQL_ALTER_USER: {
      const char* msg5 = "invalid user rights";
      const char* msg7 = "not support options";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "name too long";

      pCmd->command = pInfo->type;
      // tDCLSQL* pDCL = pInfo->pDCLInfo;

      SUserInfo* pUser = &pInfo->pDCLInfo->user;
      SSQLToken* pName = &pUser->user;
      SSQLToken* pPwd = &pUser->passwd;

      if (pName->n >= TSDB_USER_LEN) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      if (pCmd->command == TSDB_SQL_CREATE_USER) {
        if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }
      } else {
        if (pUser->type == TSDB_ALTER_USER_PASSWD) {
          if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
            return TSDB_CODE_TSC_INVALID_SQL;
          }
        } else if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
          assert(pPwd->type == TSDB_DATA_TYPE_NULL);

          SSQLToken* pPrivilege = &pUser->privilege;

          if (strncasecmp(pPrivilege->z, "super", 5) == 0 && pPrivilege->n == 5) {
            pCmd->count = 1;
          } else if (strncasecmp(pPrivilege->z, "read", 4) == 0 && pPrivilege->n == 4) {
            pCmd->count = 2;
          } else if (strncasecmp(pPrivilege->z, "write", 5) == 0 && pPrivilege->n == 5) {
            pCmd->count = 3;
          } else {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
          }
        } else {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
        }
      }

      break;
    }

    case TSDB_SQL_CFG_LOCAL: {
      tDCLSQL*    pDCL = pInfo->pDCLInfo;
      const char* msg = "invalid configure options or values";

      // validate the parameter names and options
      if (validateLocalConfig(pDCL) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }

      strncpy(pCmd->payload, pDCL->a[0].z, pDCL->a[0].n);
      if (pDCL->nTokens == 2) {
        pCmd->payload[pDCL->a[0].n] = ' ';  // add sep
        strncpy(&pCmd->payload[pDCL->a[0].n + 1], pDCL->a[1].z, pDCL->a[1].n);
      }

      break;
    }

    case TSDB_SQL_CREATE_TABLE: {
      SCreateTableSQL* pCreateTable = pInfo->pCreateTableInfo;

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
      const char* msg1 = "columns in select clause not identical";

      for (int32_t i = pCmd->numOfClause; i < pInfo->subclauseInfo.numOfClause; ++i) {
        SQueryInfo* pqi = NULL;
        if ((code = tscGetQueryInfoDetailSafely(pCmd, i, &pqi)) != TSDB_CODE_SUCCESS) {
          return code;
        }
      }

      assert(pCmd->numOfClause == pInfo->subclauseInfo.numOfClause);
      for (int32_t i = pCmd->clauseIndex; i < pInfo->subclauseInfo.numOfClause; ++i) {
        SQuerySQL* pQuerySql = pInfo->subclauseInfo.pClause[i];
        tscTrace("%p start to parse %dth subclause, total:%d", pSql, i, pInfo->subclauseInfo.numOfClause);
        if ((code = doCheckForQuery(pSql, pQuerySql, i)) != TSDB_CODE_SUCCESS) {
          return code;
        }

        tscPrintSelectClause(pSql, i);
        pCmd->clauseIndex += 1;
      }

      // restore the clause index
      pCmd->clauseIndex = 0;
      // set the command/global limit parameters from the first subclause to the sqlcmd object
      SQueryInfo* pQueryInfo1 = tscGetQueryInfoDetail(pCmd, 0);
      pCmd->command = pQueryInfo1->command;

      // if there is only one element, the limit of clause is the limit of global result.
      for (int32_t i = 1; i < pCmd->numOfClause; ++i) {
        SQueryInfo* pQueryInfo2 = tscGetQueryInfoDetail(pCmd, i);

        int32_t ret = tscFieldInfoCompare(&pQueryInfo1->fieldsInfo, &pQueryInfo2->fieldsInfo);
        if (ret != 0) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
        }
      }

      pCmd->parseFinished = 1;
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

    default:
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), "not support sql expression");
  }

  pSql->cmd.parseFinished = 1;
  return tscBuildMsg[pCmd->command](pSql, pInfo);
}

/*
 * if the top/bottom exists, only tags columns, tbname column, and primary timestamp column
 * are available.
 */
static bool isTopBottomQuery(SQueryInfo* pQueryInfo) {
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  for (int32_t i = 0; i < size; ++i) {
    int32_t functionId = tscSqlExprGet(pQueryInfo, i)->functionId;

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      return true;
    }
  }

  return false;
}

int32_t parseIntervalClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySql) {
  const char* msg1 = "invalid query expression";
  const char* msg2 = "interval cannot be less than 10 ms";

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  if (pQuerySql->interval.type == 0 || pQuerySql->interval.n == 0) {
    return TSDB_CODE_SUCCESS;
  }

  // interval is not null
  SSQLToken* t = &pQuerySql->interval;
  if (getTimestampInUsFromStr(t->z, t->n, &pQueryInfo->intervalTime) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  // if the unit of time window value is millisecond, change the value from microsecond
  if (tinfo.precision == TSDB_TIME_PRECISION_MILLI) {
    pQueryInfo->intervalTime = pQueryInfo->intervalTime / 1000;
  }

  /* parser has filter the illegal type, no need to check here */
  pQueryInfo->slidingTimeUnit = pQuerySql->interval.z[pQuerySql->interval.n - 1];

  // interval cannot be less than 10 milliseconds
  if (pQueryInfo->intervalTime < tsMinIntervalTime) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  // for top/bottom + interval query, we do not add additional timestamp column in the front
  if (isTopBottomQuery(pQueryInfo)) {
    if (parseSlidingClause(pCmd, pQueryInfo, pQuerySql) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    return TSDB_CODE_SUCCESS;
  }

  /*
   * check invalid SQL:
   * select count(tbname)/count(tag1)/count(tag2) from super_table_name interval(1d);
   */
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_COUNT && TSDB_COL_IS_TAG(pExpr->colInfo.flag)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  /*
   * check invalid SQL:
   * select tbname, tags_fields from super_table_name interval(1s)
   */
  if (tscQueryTags(pQueryInfo) && pQueryInfo->intervalTime > 0) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  // need to add timestamp column in result set, if interval is existed
  uint64_t uid = tscSqlExprGet(pQueryInfo, 0)->uid;

  int32_t tableIndex = COLUMN_INDEX_INITIAL_VAL;
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    if (pTableMetaInfo->pTableMeta->uid == uid) {
      tableIndex = i;
      break;
    }
  }

  if (tableIndex == COLUMN_INDEX_INITIAL_VAL) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  SSchema s = {.bytes = TSDB_KEYSIZE, .type = TSDB_DATA_TYPE_TIMESTAMP, .colId = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tstrncpy(s.name, aAggs[TSDB_FUNC_TS].aName, sizeof(s.name));

  SColumnIndex index = {tableIndex, PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscAddSpecialColumnForSelect(pQueryInfo, 0, TSDB_FUNC_TS, &index, &s, TSDB_COL_NORMAL);

  if (parseSlidingClause(pCmd, pQueryInfo, pQuerySql) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t parseSlidingClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySql) {
  const char* msg0 = "sliding value too small";
  const char* msg1 = "sliding value no larger than the interval value";
  const char* msg2 = "sliding value can not less than 1% of interval value";

  const static int32_t INTERVAL_SLIDING_FACTOR = 100;

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);

  SSQLToken* pSliding = &pQuerySql->sliding;
  if (pSliding->n != 0) {
    getTimestampInUsFromStr(pSliding->z, pSliding->n, &pQueryInfo->slidingTime);
    if (tinfo.precision == TSDB_TIME_PRECISION_MILLI) {
      pQueryInfo->slidingTime /= 1000;
    }

    if (pQueryInfo->slidingTime < tsMinSlidingTime) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }

    if (pQueryInfo->slidingTime > pQueryInfo->intervalTime) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  } else {
    pQueryInfo->slidingTime = pQueryInfo->intervalTime;
  }

  if ((pQueryInfo->intervalTime != 0) && (pQueryInfo->intervalTime/pQueryInfo->slidingTime > INTERVAL_SLIDING_FACTOR)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscSetTableFullName(STableMetaInfo* pTableMetaInfo, SSQLToken* pzTableName, SSqlObj* pSql) {
  const char* msg = "name too long";

  SSqlCmd* pCmd = &pSql->cmd;
  int32_t  code = TSDB_CODE_SUCCESS;

  // backup the old name in pTableMetaInfo
  size_t size = strlen(pTableMetaInfo->name);
  char*  oldName = NULL;
  if (size > 0) {
    oldName = strdup(pTableMetaInfo->name);
  }

  if (hasSpecifyDB(pzTableName)) {
    // db has been specified in sql string so we ignore current db path
    code = setObjFullName(pTableMetaInfo->name, getAccountId(pSql), NULL, pzTableName, NULL);
  } else {  // get current DB name first, then set it into path
    SSQLToken t = {0};
    getCurrentDBName(pSql, &t);

    code = setObjFullName(pTableMetaInfo->name, NULL, &t, pzTableName, NULL);
  }

  if (code != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (code != TSDB_CODE_SUCCESS) {
    free(oldName);
    return code;
  }

  /*
   * the old name exists and is not equalled to the new name. Release the metermeta/metricmeta
   * that are corresponding to the old name for the new table name.
   */
  if (size > 0) {
    if (strncasecmp(oldName, pTableMetaInfo->name, tListLen(pTableMetaInfo->name)) != 0) {
      tscClearTableMetaInfo(pTableMetaInfo, false);
    }
  } else {
    assert(pTableMetaInfo->pTableMeta == NULL);
  }

  tfree(oldName);
  return TSDB_CODE_SUCCESS;
}

static bool validateTableColumnInfo(tFieldList* pFieldList, SSqlCmd* pCmd) {
  assert(pFieldList != NULL);

  const char* msg = "illegal number of columns";
  const char* msg1 = "first column must be timestamp";
  const char* msg2 = "row length exceeds max length";
  const char* msg3 = "duplicated column names";
  const char* msg4 = "invalid data types";
  const char* msg5 = "invalid binary/nchar column length";
  const char* msg6 = "invalid column name";

  // number of fields no less than 2
  if (pFieldList->nField <= 1 || pFieldList->nField > TSDB_MAX_COLUMNS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
    return false;
  }

  // first column must be timestamp
  if (pFieldList->p[0].type != TSDB_DATA_TYPE_TIMESTAMP) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < pFieldList->nField; ++i) {
    if (pFieldList->p[i].bytes == 0) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      return false;
    }
    nLen += pFieldList->p[i].bytes;
  }

  // max row length must be less than TSDB_MAX_BYTES_PER_ROW
  if (nLen > TSDB_MAX_BYTES_PER_ROW) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < pFieldList->nField; ++i) {
    TAOS_FIELD* pField = &pFieldList->p[i];
    if (pField->type < TSDB_DATA_TYPE_BOOL || pField->type > TSDB_DATA_TYPE_NCHAR) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
      return false;
    }

    if ((pField->type == TSDB_DATA_TYPE_BINARY && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_BINARY_LEN)) ||
        (pField->type == TSDB_DATA_TYPE_NCHAR && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_NCHAR_LEN))) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      return false;
    }

    if (validateColumnName(pField->name) != TSDB_CODE_SUCCESS) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      return false;
    }

    if (has(pFieldList, i + 1, pFieldList->p[i].name) == true) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return false;
    }
  }

  return true;
}

static bool validateTagParams(tFieldList* pTagsList, tFieldList* pFieldList, SSqlCmd* pCmd) {
  assert(pTagsList != NULL);

  const char* msg1 = "invalid number of tag columns";
  const char* msg2 = "tag length too long";
  const char* msg3 = "duplicated column names";
  const char* msg4 = "timestamp not allowed in tags";
  const char* msg5 = "invalid data type in tags";
  const char* msg6 = "invalid tag name";
  const char* msg7 = "invalid binary/nchar tag length";

  // number of fields at least 1
  if (pTagsList->nField < 1 || pTagsList->nField > TSDB_MAX_TAGS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < pTagsList->nField; ++i) {
    if (pTagsList->p[i].bytes == 0) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
      return false;
    }
    nLen += pTagsList->p[i].bytes;
  }

  // max tag row length must be less than TSDB_MAX_TAGS_LEN
  if (nLen > TSDB_MAX_TAGS_LEN) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < pTagsList->nField; ++i) {
    if (has(pFieldList, 0, pTagsList->p[i].name) == true) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return false;
    }
  }

  /* timestamp in tag is not allowed */
  for (int32_t i = 0; i < pTagsList->nField; ++i) {
    if (pTagsList->p[i].type == TSDB_DATA_TYPE_TIMESTAMP) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
      return false;
    }

    if (pTagsList->p[i].type < TSDB_DATA_TYPE_BOOL || pTagsList->p[i].type > TSDB_DATA_TYPE_NCHAR) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      return false;
    }

    if ((pTagsList->p[i].type == TSDB_DATA_TYPE_BINARY && pTagsList->p[i].bytes <= 0) ||
        (pTagsList->p[i].type == TSDB_DATA_TYPE_NCHAR && pTagsList->p[i].bytes <= 0)) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
      return false;
    }

    if (validateColumnName(pTagsList->p[i].name) != TSDB_CODE_SUCCESS) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      return false;
    }

    if (has(pTagsList, i + 1, pTagsList->p[i].name) == true) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return false;
    }
  }

  return true;
}

/*
 * tags name /column name is truncated in sql.y
 */
bool validateOneTags(SSqlCmd* pCmd, TAOS_FIELD* pTagField) {
  const char* msg1 = "timestamp not allowed in tags";
  const char* msg2 = "duplicated column names";
  const char* msg3 = "tag length too long";
  const char* msg4 = "invalid tag name";
  const char* msg5 = "invalid binary/nchar tag length";
  const char* msg6 = "invalid data type in tags";

  assert(pCmd->numOfClause == 1);

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

  int32_t numOfTags = tscGetNumOfTags(pTableMeta);
  int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
  
  // no more than 6 tags
  if (numOfTags == TSDB_MAX_TAGS) {
    char msg[128] = {0};
    sprintf(msg, "tags no more than %d", TSDB_MAX_TAGS);

    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
    return false;
  }

  // no timestamp allowable
  if (pTagField->type == TSDB_DATA_TYPE_TIMESTAMP) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  if ((pTagField->type < TSDB_DATA_TYPE_BOOL) || (pTagField->type > TSDB_DATA_TYPE_NCHAR)) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
    return false;
  }

  SSchema* pTagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
  int32_t  nLen = 0;

  for (int32_t i = 0; i < numOfTags; ++i) {
    nLen += pTagSchema[i].bytes;
  }

  // length less than TSDB_MAX_TASG_LEN
  if (nLen + pTagField->bytes > TSDB_MAX_TAGS_LEN) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    return false;
  }

  // tags name can not be a keyword
  if (validateColumnName(pTagField->name) != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
    return false;
  }

  // binary(val), val can not be equalled to or less than 0
  if ((pTagField->type == TSDB_DATA_TYPE_BINARY || pTagField->type == TSDB_DATA_TYPE_NCHAR) && pTagField->bytes <= 0) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
    return false;
  }

  // field name must be unique
  SSchema* pSchema = tscGetTableSchema(pTableMeta);

  for (int32_t i = 0; i < numOfTags + numOfCols; ++i) {
    if (strncasecmp(pTagField->name, pSchema[i].name, sizeof(pTagField->name) - 1) == 0) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      return false;
    }
  }

  return true;
}

bool validateOneColumn(SSqlCmd* pCmd, TAOS_FIELD* pColField) {
  const char* msg1 = "too many columns";
  const char* msg2 = "duplicated column names";
  const char* msg3 = "column length too long";
  const char* msg4 = "invalid data types";
  const char* msg5 = "invalid column name";
  const char* msg6 = "invalid column length";

  assert(pCmd->numOfClause == 1);
  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;
  
  int32_t numOfTags = tscGetNumOfTags(pTableMeta);
  int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
  
  // no more max columns
  if (numOfCols >= TSDB_MAX_COLUMNS || numOfTags + numOfCols >= TSDB_MAX_COLUMNS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  if (pColField->type < TSDB_DATA_TYPE_BOOL || pColField->type > TSDB_DATA_TYPE_NCHAR) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
    return false;
  }

  if (validateColumnName(pColField->name) != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
    return false;
  }

  SSchema* pSchema = tscGetTableSchema(pTableMeta);
  int32_t  nLen = 0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    nLen += pSchema[i].bytes;
  }

  if (pColField->bytes <= 0) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
    return false;
  }

  // length less than TSDB_MAX_BYTES_PER_ROW
  if (nLen + pColField->bytes > TSDB_MAX_BYTES_PER_ROW) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < numOfTags + numOfCols; ++i) {
    if (strncasecmp(pColField->name, pSchema[i].name, sizeof(pColField->name) - 1) == 0) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      return false;
    }
  }

  return true;
}

/* is contained in pFieldList or not */
static bool has(tFieldList* pFieldList, int32_t startIdx, const char* name) {
  for (int32_t j = startIdx; j < pFieldList->nField; ++j) {
    TAOS_FIELD* field = pFieldList->p + j;
    if (strncasecmp(name, field->name, sizeof(field->name) - 1) == 0) return true;
  }

  return false;
}

static char* getAccountId(SSqlObj* pSql) { return pSql->pTscObj->acctId; }

static void getCurrentDBName(SSqlObj* pSql, SSQLToken* pDBToken) {
  pDBToken->z = pSql->pTscObj->db;
  pDBToken->n = strlen(pSql->pTscObj->db);
}

/* length limitation, strstr cannot be applied */
static bool hasSpecifyDB(SSQLToken* pTableName) {
  for (int32_t i = 0; i < pTableName->n; ++i) {
    if (pTableName->z[i] == TS_PATH_DELIMITER[0]) {
      return true;
    }
  }

  return false;
}

int32_t setObjFullName(char* fullName, const char* account, SSQLToken* pDB, SSQLToken* tableName, int32_t* xlen) {
  int32_t totalLen = 0;

  if (account != NULL) {
    int32_t len = strlen(account);
    strcpy(fullName, account);
    fullName[len] = TS_PATH_DELIMITER[0];
    totalLen += (len + 1);
  }

  /* db name is not specified, the tableName dose not include db name */
  if (pDB != NULL) {
    if (pDB->n >= TSDB_ACCT_LEN + TSDB_DB_NAME_LEN) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    memcpy(&fullName[totalLen], pDB->z, pDB->n);
    totalLen += pDB->n;
  }

  if (tableName != NULL) {
    if (pDB != NULL) {
      fullName[totalLen] = TS_PATH_DELIMITER[0];
      totalLen += 1;

      /* here we only check the table name length limitation */
      if (!tscValidateTableNameLength(tableName->n)) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    } else {  // pDB == NULL, the db prefix name is specified in tableName
      /* the length limitation includes tablename + dbname + sep */
      if (tableName->n >= TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    }

    memcpy(&fullName[totalLen], tableName->z, tableName->n);
    totalLen += tableName->n;
  }

  if (xlen != NULL) {
    *xlen = totalLen;
  }

  if (totalLen < TSDB_TABLE_ID_LEN) {
    fullName[totalLen] = 0;
  }

  return (totalLen < TSDB_TABLE_ID_LEN) ? TSDB_CODE_SUCCESS : TSDB_CODE_TSC_INVALID_SQL;
}

static void extractColumnNameFromString(tSQLExprItem* pItem) {
  if (pItem->pNode->nSQLOptr == TK_STRING) {
    pItem->pNode->val.nLen = strdequote(pItem->pNode->val.pz);
    pItem->pNode->nSQLOptr = TK_ID;

    SSQLToken* pIdToken = &pItem->pNode->colInfo;
    pIdToken->type = TK_ID;
    pIdToken->z = pItem->pNode->val.pz;
    pIdToken->n = pItem->pNode->val.nLen;
  }
}

int32_t parseSelectClause(SSqlCmd* pCmd, int32_t clauseIndex, tSQLExprList* pSelection, bool isSTable) {
  assert(pSelection != NULL && pCmd != NULL);

  const char* msg1 = "invalid column name, or illegal column type";
  const char* msg2 = "functions can not be mixed up";
  const char* msg3 = "not support query expression";
  const char* msg4 = "columns from different table mixed up in arithmetic expression";
  const char* msg5 = "invalid function name";

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);
  
  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }
  
  for (int32_t i = 0; i < pSelection->nExpr; ++i) {
    int32_t outputIndex = tscSqlExprNumOfExprs(pQueryInfo);
    tSQLExprItem* pItem = &pSelection->a[i];

    // project on all fields
    if (pItem->pNode->nSQLOptr == TK_ALL || pItem->pNode->nSQLOptr == TK_ID || pItem->pNode->nSQLOptr == TK_STRING) {
      // it is actually a function, but the function name is invalid
      if (pItem->pNode->nSQLOptr == TK_ID && (pItem->pNode->colInfo.z == NULL && pItem->pNode->colInfo.n == 0)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      // if the name of column is quoted, remove it and set the right information for later process
      extractColumnNameFromString(pItem);
      TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_PROJECTION_QUERY);

      // select table_name1.field_name1, table_name2.field_name2  from table_name1, table_name2
      if (addProjectionExprAndResultField(pCmd, pQueryInfo, pItem) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    } else if (pItem->pNode->nSQLOptr >= TK_COUNT && pItem->pNode->nSQLOptr <= TK_TBID) {
      // sql function in selection clause, append sql function info in pSqlCmd structure sequentially
      if (addExprAndResultField(pCmd, pQueryInfo, outputIndex, pItem, true) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

    } else if (pItem->pNode->nSQLOptr >= TK_PLUS && pItem->pNode->nSQLOptr <= TK_REM) {
      // arithmetic function in select clause
      SColumnList columnList = {0};
      int32_t     arithmeticType = NON_ARITHMEIC_EXPR;

      if (validateArithmeticSQLExpr(pCmd, pItem->pNode, pQueryInfo, &columnList, &arithmeticType) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }
      
      int32_t tableIndex = columnList.ids[0].tableIndex;
      char  arithmeticExprStr[1024] = {0};
      char* p = arithmeticExprStr;
      
      if (arithmeticType == NORMAL_ARITHMETIC) {
        pQueryInfo->type |= TSDB_QUERY_TYPE_PROJECTION_QUERY;
  
        // all columns in arithmetic expression must belong to the same table
        for (int32_t f = 1; f < columnList.num; ++f) {
          if (columnList.ids[f].tableIndex != tableIndex) {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
          }
        }
  
        if (buildArithmeticExprString(pItem->pNode, &p) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }
  
        // expr string is set as the parameter of function
        SColumnIndex index = {.tableIndex = tableIndex};
        SSqlExpr*    pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_ARITHM, &index, TSDB_DATA_TYPE_DOUBLE,
                                              sizeof(double), sizeof(double), false);
        
        /* todo alias name should use the original sql string */
        char* name = (pItem->aliasName != NULL)? pItem->aliasName:arithmeticExprStr;
        tstrncpy(pExpr->aliasName, name, sizeof(pExpr->aliasName));
        
        tExprNode* pNode = NULL;
        SArray* colList = taosArrayInit(10, sizeof(SColIndex));
        
        int32_t ret = exprTreeFromSqlExpr(pCmd, &pNode, pItem->pNode, pQueryInfo->exprList, pQueryInfo, colList);
        if (ret != TSDB_CODE_SUCCESS) {
          tExprTreeDestroy(&pNode, NULL);
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), "invalid arithmetic expression in select clause");
        }
        
        SBufferWriter bw = tbufInitWriter(NULL, false);

        TRY(0) {
          exprTreeToBinary(&bw, pNode);
        } CATCH(code) {
          tbufCloseWriter(&bw);
          UNUSED(code);
          // TODO: other error handling
        } END_TRY
        
        size_t len = tbufTell(&bw);
        char* c = tbufGetData(&bw, true);
        
        // set the serialized binary string as the parameter of arithmetic expression
        addExprParams(pExpr, c, TSDB_DATA_TYPE_BINARY, len, index.tableIndex);
        
        insertResultField(pQueryInfo, i, &columnList, sizeof(double), TSDB_DATA_TYPE_DOUBLE, pExpr->aliasName, pExpr);
        
        taosArrayDestroy(colList);
        tExprTreeDestroy(&pNode, NULL);
      } else {
        columnList.num = 0;
        columnList.ids[0] = (SColumnIndex) {0, 0};
        
        insertResultField(pQueryInfo, i, &columnList, sizeof(double), TSDB_DATA_TYPE_DOUBLE, "dummy_column", NULL);
        
        int32_t slot = tscNumOfFields(pQueryInfo) - 1;
        SFieldSupInfo* pInfo = tscFieldInfoGetSupp(&pQueryInfo->fieldsInfo, slot);
        
        if (pInfo->pSqlExpr == NULL) {
          SExprInfo* pArithExprInfo = calloc(1, sizeof(SExprInfo));
          
          // arithmetic expression always return result in the format of double float
          pArithExprInfo->bytes = sizeof(double);
          pArithExprInfo->interBytes = sizeof(double);
          pArithExprInfo->type = TSDB_DATA_TYPE_DOUBLE;

          int32_t ret = exprTreeFromSqlExpr(pCmd, &pArithExprInfo->pExpr, pItem->pNode, pQueryInfo->exprList, pQueryInfo, NULL);
          if (ret != TSDB_CODE_SUCCESS) {
            tExprTreeDestroy(&pArithExprInfo->pExpr, NULL);
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), "invalid expression in select clause");
          }
  
          pInfo->pArithExprInfo = pArithExprInfo;
        }
      }
    } else {
      /*
       * not support such expression
       * e.g., select 12+5 from table_name
       */
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    if (pQueryInfo->fieldsInfo.numOfOutput > TSDB_MAX_COLUMNS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  if (!functionCompatibleCheck(pQueryInfo)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  /*
   * transfer sql functions that need secondary merge into another format
   * in dealing with metric queries such as: count/first/last
   */
  if (isSTable) {
    tscTansformSQLFuncForSTableQuery(pQueryInfo);

    if (hasUnsupportFunctionsForSTableQuery(pCmd, pQueryInfo)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t insertResultField(SQueryInfo* pQueryInfo, int32_t outputIndex, SColumnList* pIdList, int16_t bytes,
    int8_t type, char* fieldName, SSqlExpr* pSqlExpr) {
  
  for (int32_t i = 0; i < pIdList->num; ++i) {
    int32_t tableId = pIdList->ids[i].tableIndex;
    STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[tableId];
    
    int32_t numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
    if (pIdList->ids[i].columnIndex >= numOfCols) {
      continue;
    }
    
    tscColumnListInsert(pQueryInfo->colList, &(pIdList->ids[i]));
  }
  
  TAOS_FIELD f = tscCreateField(type, fieldName, bytes);
  SFieldSupInfo* pInfo = tscFieldInfoInsert(&pQueryInfo->fieldsInfo, outputIndex, &f);
  pInfo->pSqlExpr = pSqlExpr;
  
  return TSDB_CODE_SUCCESS;
}

SSqlExpr* doAddProjectCol(SQueryInfo* pQueryInfo, int32_t outputIndex, int32_t colIndex, int32_t tableIndex) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;
  int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
  
  SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, colIndex);

  int16_t functionId = (int16_t)((colIndex >= numOfCols) ? TSDB_FUNC_TAGPRJ : TSDB_FUNC_PRJ);
  SColumnIndex index = {.tableIndex = tableIndex,};
  
  if (functionId == TSDB_FUNC_TAGPRJ) {
    index.columnIndex = colIndex - tscGetNumOfColumns(pTableMeta);
  
    tscColumnListInsert(pTableMetaInfo->tagColList, &index);
  } else {
    index.columnIndex = colIndex;
  }
  
  return tscSqlExprAppend(pQueryInfo, functionId, &index, pSchema->type, pSchema->bytes,
      pSchema->bytes, functionId == TSDB_FUNC_TAGPRJ);
}

static void addProjectQueryCol(SQueryInfo* pQueryInfo, int32_t startPos, SColumnIndex* pIndex, tSQLExprItem* pItem) {
  SSqlExpr* pExpr = doAddProjectCol(pQueryInfo, startPos, pIndex->columnIndex, pIndex->tableIndex);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pIndex->tableIndex);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;
  
  SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, pIndex->columnIndex);

  char* colName = (pItem->aliasName == NULL) ? pSchema->name : pItem->aliasName;
  tstrncpy(pExpr->aliasName, colName, sizeof(pExpr->aliasName));
  
  SColumnList ids = {0};
  ids.num = 1;
  ids.ids[0] = *pIndex;

  if (pIndex->columnIndex >= tscGetNumOfColumns(pTableMeta) || pIndex->columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    ids.num = 0;
  }

  insertResultField(pQueryInfo, startPos, &ids, pExpr->resBytes, pExpr->resType, pExpr->aliasName, pExpr);
}

void tscAddSpecialColumnForSelect(SQueryInfo* pQueryInfo, int32_t outputColIndex, int16_t functionId,
                                  SColumnIndex* pIndex, SSchema* pColSchema, int16_t flag) {
  SSqlExpr* pExpr = tscSqlExprInsert(pQueryInfo, outputColIndex, functionId, pIndex, pColSchema->type,
                                     pColSchema->bytes, pColSchema->bytes, flag);
  tstrncpy(pExpr->aliasName, pColSchema->name, sizeof(pExpr->aliasName));

  SColumnList ids = getColumnList(1, pIndex->tableIndex, pIndex->columnIndex);
  if (TSDB_COL_IS_TAG(flag)) {
    ids.num = 0;
  }

  insertResultField(pQueryInfo, outputColIndex, &ids, pColSchema->bytes, pColSchema->type, pColSchema->name, pExpr);

  pExpr->colInfo.flag = flag;
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pIndex->tableIndex);
  
  if (TSDB_COL_IS_TAG(flag)) {
    tscColumnListInsert(pTableMetaInfo->tagColList, pIndex);
  }
}

static int32_t doAddProjectionExprAndResultFields(SQueryInfo* pQueryInfo, SColumnIndex* pIndex, int32_t startPos) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pIndex->tableIndex);

  int32_t     numOfTotalColumns = 0;
  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
  SSchema*    pSchema = tscGetTableSchema(pTableMeta);

  STableComInfo tinfo = tscGetTableInfo(pTableMeta);
  
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    numOfTotalColumns = tinfo.numOfColumns + tinfo.numOfTags;
  } else {
    numOfTotalColumns = tinfo.numOfColumns;
  }

  for (int32_t j = 0; j < numOfTotalColumns; ++j) {
    SSqlExpr* pExpr = doAddProjectCol(pQueryInfo, startPos + j, j, pIndex->tableIndex);
    tstrncpy(pExpr->aliasName, pSchema[j].name, sizeof(pExpr->aliasName));

    pIndex->columnIndex = j;
    SColumnList ids = {0};
    ids.ids[0] = *pIndex;
    ids.num = 1;

    insertResultField(pQueryInfo, startPos + j, &ids, pSchema[j].bytes, pSchema[j].type, pSchema[j].name, pExpr);
  }

  return numOfTotalColumns;
}

static void tscInsertPrimaryTSSourceColumn(SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  SColumnIndex tsCol = {.tableIndex = pIndex->tableIndex, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscColumnListInsert(pQueryInfo->colList, &tsCol);
}

int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSQLExprItem* pItem) {
  const char* msg0 = "invalid column name";
  const char* msg1 = "tag for normal table query is not allowed";
  
  int32_t startPos = tscSqlExprNumOfExprs(pQueryInfo);

  if (pItem->pNode->nSQLOptr == TK_ALL) {  // project on all fields
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getTableIndexByName(&pItem->pNode->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }

    // all meters columns are required
    if (index.tableIndex == COLUMN_INDEX_INITIAL_VAL) {  // all table columns are required.
      for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
        index.tableIndex = i;
        int32_t inc = doAddProjectionExprAndResultFields(pQueryInfo, &index, startPos);
        startPos += inc;
      }
    } else {
      doAddProjectionExprAndResultFields(pQueryInfo, &index, startPos);
    }
  } else if (pItem->pNode->nSQLOptr == TK_ID) {  // simple column projection query
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;

    if (getColumnIndexByName(pCmd, &pItem->pNode->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }

    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      SSchema colSchema = tGetTableNameColumnSchema();
      tscAddSpecialColumnForSelect(pQueryInfo, startPos, TSDB_FUNC_TAGPRJ, &index, &colSchema, TSDB_COL_TAG);
    } else {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
      STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

      if (index.columnIndex >= tscGetNumOfColumns(pTableMeta) && UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      addProjectQueryCol(pQueryInfo, startPos, &index, pItem);
    }

    tscInsertPrimaryTSSourceColumn(pQueryInfo, &index);
  } else {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setExprInfoForFunctions(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSchema* pSchema, int32_t functionID, char* aliasName,
                                       int32_t resColIdx, SColumnIndex* pColIndex) {
  int16_t type = 0;
  int16_t bytes = 0;

  char        columnName[TSDB_COL_NAME_LEN] = {0};
  const char* msg1 = "not support column types";

  if (functionID == TSDB_FUNC_SPREAD) {
    if (pSchema[pColIndex->columnIndex].type == TSDB_DATA_TYPE_BINARY ||
        pSchema[pColIndex->columnIndex].type == TSDB_DATA_TYPE_NCHAR ||
        pSchema[pColIndex->columnIndex].type == TSDB_DATA_TYPE_BOOL) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      return -1;
    } else {
      type = TSDB_DATA_TYPE_DOUBLE;
      bytes = tDataTypeDesc[type].nSize;
    }
  } else {
    type = pSchema[pColIndex->columnIndex].type;
    bytes = pSchema[pColIndex->columnIndex].bytes;
  }

  if (aliasName != NULL) {
    tstrncpy(columnName, aliasName, sizeof(columnName));
  } else {
    getRevisedName(columnName, functionID, sizeof(columnName) - 1, pSchema[pColIndex->columnIndex].name);
  }
  
  SSqlExpr* pExpr = tscSqlExprAppend(pQueryInfo, functionID, pColIndex, type, bytes, bytes, false);
  tstrncpy(pExpr->aliasName, columnName, sizeof(pExpr->aliasName));

  // set reverse order scan data blocks for last query
  if (functionID == TSDB_FUNC_LAST) {
    pExpr->numOfParams = 1;
    pExpr->param[0].i64Key = TSDB_ORDER_DESC;
    pExpr->param[0].nType = TSDB_DATA_TYPE_INT;
  }
  
  // for all queries, the timestamp column needs to be loaded
  SColumnIndex index = {.tableIndex = pColIndex->tableIndex, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscColumnListInsert(pQueryInfo->colList, &index);

  SColumnList ids = getColumnList(1, pColIndex->tableIndex, pColIndex->columnIndex);
  insertResultField(pQueryInfo, resColIdx, &ids, bytes, type, columnName, pExpr);

  return TSDB_CODE_SUCCESS;
}

int32_t addExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t colIndex, tSQLExprItem* pItem, bool finalResult) {
  STableMetaInfo* pTableMetaInfo = NULL;
  int32_t optr = pItem->pNode->nSQLOptr;

  const char* msg1 = "not support column types";
  const char* msg2 = "invalid parameters";
  const char* msg3 = "illegal column name";
  const char* msg4 = "invalid table name";
  const char* msg5 = "parameter is out of range [0, 100]";
  const char* msg6 = "function applied to tags not allowed";
  const char* msg7 = "normal table can not apply this function";
  const char* msg8 = "multi-columns selection does not support alias column name";
  const char* msg9 = "invalid function";

  switch (optr) {
    case TK_COUNT: {
        /* more than one parameter for count() function */
      if (pItem->pNode->pParam != NULL && pItem->pNode->pParam->nExpr != 1) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      int16_t functionID = 0;
      if (changeFunctionID(optr, &functionID) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      SSqlExpr* pExpr = NULL;
      SColumnIndex index = COLUMN_INDEX_INITIALIZER;

      if (pItem->pNode->pParam != NULL) {
        SSQLToken* pToken = &pItem->pNode->pParam->a[0].pNode->colInfo;
        if (pToken->z == NULL || pToken->n == 0) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        tSQLExprItem* pParamElem = &pItem->pNode->pParam->a[0];
        if (pParamElem->pNode->nSQLOptr == TK_ALL) {
          // select table.*
          // check if the table name is valid or not
          SSQLToken tmpToken = pParamElem->pNode->colInfo;

          if (getTableIndexByName(&tmpToken, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
          }

          index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
          int32_t size = tDataTypeDesc[TSDB_DATA_TYPE_BIGINT].nSize;
          pExpr = tscSqlExprAppend(pQueryInfo, functionID, &index, TSDB_DATA_TYPE_BIGINT, size, size, false);
        } else {
          // count the number of meters created according to the super table
          if (getColumnIndexByName(pCmd, pToken, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }

          pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);

          // count tag is equalled to count(tbname)
          bool isTag = false;
          if (index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta) || index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
            index.columnIndex = TSDB_TBNAME_COLUMN_INDEX;
            isTag = true;
          }

          int32_t size = tDataTypeDesc[TSDB_DATA_TYPE_BIGINT].nSize;
          pExpr = tscSqlExprAppend(pQueryInfo, functionID, &index, TSDB_DATA_TYPE_BIGINT, size, size, isTag);
        }
      } else {  // count(*) is equalled to count(primary_timestamp_key)
        index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
        int32_t size = tDataTypeDesc[TSDB_DATA_TYPE_BIGINT].nSize;
        pExpr = tscSqlExprAppend(pQueryInfo, functionID, &index, TSDB_DATA_TYPE_BIGINT, size, size, false);
      }

      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);

      memset(pExpr->aliasName, 0, tListLen(pExpr->aliasName));
      getColumnName(pItem, pExpr->aliasName, sizeof(pExpr->aliasName) - 1);
      
      SColumnList ids = getColumnList(1, index.tableIndex, index.columnIndex);
      if (finalResult) {
        int32_t numOfOutput = tscNumOfFields(pQueryInfo);
        insertResultField(pQueryInfo, numOfOutput, &ids, sizeof(int64_t), TSDB_DATA_TYPE_BIGINT, pExpr->aliasName, pExpr);
      } else {
        for (int32_t i = 0; i < ids.num; ++i) {
          tscColumnListInsert(pQueryInfo->colList, &(ids.ids[i]));
        }
      }

      // the time stamp may be always needed
      if (index.tableIndex < tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
        tscInsertPrimaryTSSourceColumn(pQueryInfo, &index);
      }

      return TSDB_CODE_SUCCESS;
    }
    case TK_SUM:
    case TK_AVG:
    case TK_RATE:
    case TK_IRATE:
    case TK_SUM_RATE:
    case TK_SUM_IRATE:
    case TK_AVG_RATE:
    case TK_AVG_IRATE:
    case TK_TWA:
    case TK_MIN:
    case TK_MAX:
    case TK_DIFF:
    case TK_STDDEV:
    case TK_LEASTSQUARES: {
      // 1. valid the number of parameters
      if (pItem->pNode->pParam == NULL || (optr != TK_LEASTSQUARES && pItem->pNode->pParam->nExpr != 1) ||
          (optr == TK_LEASTSQUARES && pItem->pNode->pParam->nExpr != 3)) {
        /* no parameters or more than one parameter for function */
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tSQLExprItem* pParamElem = &(pItem->pNode->pParam->a[0]);
      if (pParamElem->pNode->nSQLOptr != TK_ALL && pParamElem->pNode->nSQLOptr != TK_ID) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if ((getColumnIndexByName(pCmd, &pParamElem->pNode->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) ||
          index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      // 2. check if sql function can be applied on this column data type
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
      SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);
      int16_t  colType = pSchema->type;

      if (colType <= TSDB_DATA_TYPE_BOOL || colType >= TSDB_DATA_TYPE_BINARY) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      int16_t resultType = 0;
      int16_t resultSize = 0;
      int32_t intermediateResSize = 0;

      int16_t functionID = 0;
      if (changeFunctionID(optr, &functionID) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      if (getResultDataInfo(pSchema->type, pSchema->bytes, functionID, 0, &resultType, &resultSize,
                            &intermediateResSize, 0, false) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      // set the first column ts for diff query
      if (optr == TK_DIFF) {
        colIndex += 1;
        SColumnIndex indexTS = {.tableIndex = index.tableIndex, .columnIndex = 0};
        SSqlExpr* pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &indexTS, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE,
                         TSDB_KEYSIZE, false);

        SColumnList ids = getColumnList(1, 0, 0);
        insertResultField(pQueryInfo, 0, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, aAggs[TSDB_FUNC_TS_DUMMY].aName, pExpr);
      }

      // functions can not be applied to tags
      if (index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      SSqlExpr* pExpr = tscSqlExprAppend(pQueryInfo, functionID, &index, resultType, resultSize, resultSize, false);

      if (optr == TK_LEASTSQUARES) {
        /* set the leastsquares parameters */
        char val[8] = {0};
        if (tVariantDump(&pParamElem[1].pNode->val, val, TSDB_DATA_TYPE_DOUBLE, true) < 0) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }

        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, DOUBLE_BYTES, 0);

        memset(val, 0, tListLen(val));
        if (tVariantDump(&pParamElem[2].pNode->val, val, TSDB_DATA_TYPE_DOUBLE, true) < 0) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }

        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double), 0);
      }

      SColumnList ids = {0};
      ids.num = 1;
      ids.ids[0] = index;
  
      memset(pExpr->aliasName, 0, tListLen(pExpr->aliasName));
      getColumnName(pItem, pExpr->aliasName, sizeof(pExpr->aliasName) - 1);
  
      if (finalResult) {
        int32_t numOfOutput = tscNumOfFields(pQueryInfo);
        insertResultField(pQueryInfo, numOfOutput, &ids, pExpr->resBytes, pExpr->resType, pExpr->aliasName, pExpr);
      } else {
        for (int32_t i = 0; i < ids.num; ++i) {
          tscColumnListInsert(pQueryInfo->colList, &(ids.ids[i]));
        }
      }

      tscInsertPrimaryTSSourceColumn(pQueryInfo, &index);
      return TSDB_CODE_SUCCESS;
    }
    case TK_FIRST:
    case TK_LAST:
    case TK_SPREAD:
    case TK_LAST_ROW:
    case TK_INTERP: {
      bool requireAllFields = (pItem->pNode->pParam == NULL);

      int16_t functionID = 0;
      if (changeFunctionID(optr, &functionID) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg9);
      }

      if (!requireAllFields) {
        if (pItem->pNode->pParam->nExpr < 1) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        if (pItem->pNode->pParam->nExpr > 1 && (pItem->aliasName != NULL && strlen(pItem->aliasName) > 0)) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
        }

        /* in first/last function, multiple columns can be add to resultset */
        for (int32_t i = 0; i < pItem->pNode->pParam->nExpr; ++i) {
          tSQLExprItem* pParamElem = &(pItem->pNode->pParam->a[i]);
          if (pParamElem->pNode->nSQLOptr != TK_ALL && pParamElem->pNode->nSQLOptr != TK_ID) {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }

          SColumnIndex index = COLUMN_INDEX_INITIALIZER;

          if (pParamElem->pNode->nSQLOptr == TK_ALL) {
            // select table.*
            SSQLToken tmpToken = pParamElem->pNode->colInfo;

            if (getTableIndexByName(&tmpToken, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
              return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
            }

            pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
            SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

            for (int32_t j = 0; j < tscGetNumOfColumns(pTableMetaInfo->pTableMeta); ++j) {
              index.columnIndex = j;
              if (setExprInfoForFunctions(pCmd, pQueryInfo, pSchema, functionID, pItem->aliasName, colIndex++, &index) != 0) {
                return TSDB_CODE_TSC_INVALID_SQL;
              }
            }

          } else {
            if (getColumnIndexByName(pCmd, &pParamElem->pNode->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
              return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
            }

            pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
            SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

            // functions can not be applied to tags
            if ((index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) || (index.columnIndex < 0)) {
              return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
            }

            if (setExprInfoForFunctions(pCmd, pQueryInfo, pSchema, functionID, pItem->aliasName, colIndex + i, &index) != 0) {
              return TSDB_CODE_TSC_INVALID_SQL;
            }

            if (optr == TK_LAST) {  // todo refactor
              SSqlGroupbyExpr* pGroupBy = &pQueryInfo->groupbyExpr;
              if (pGroupBy->numOfGroupCols > 0) {
                for(int32_t k = 0; k < pGroupBy->numOfGroupCols; ++k) {
                  SColIndex* pIndex = taosArrayGet(pGroupBy->columnInfo, k);
                  if (!TSDB_COL_IS_TAG(pIndex->flag) && pIndex->colIndex < tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) { // group by normal columns
                    SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, colIndex + i);
                    pExpr->numOfParams = 1;
                    pExpr->param->i64Key = TSDB_ORDER_ASC;

                    break;
                  }
                }
              }
            }
          }
        }
        
        return TSDB_CODE_SUCCESS;
      } else {  // select * from xxx
        int32_t numOfFields = 0;

        // multicolumn selection does not support alias name
        if (pItem->aliasName != NULL && strlen(pItem->aliasName) > 0) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
        }

        for (int32_t j = 0; j < pQueryInfo->numOfTables; ++j) {
          pTableMetaInfo = tscGetMetaInfo(pQueryInfo, j);
          SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

          for (int32_t i = 0; i < tscGetNumOfColumns(pTableMetaInfo->pTableMeta); ++i) {
            SColumnIndex index = {.tableIndex = j, .columnIndex = i};
            if (setExprInfoForFunctions(pCmd, pQueryInfo, pSchema, functionID, pItem->aliasName, colIndex + i + j, &index) !=
                0) {
              return TSDB_CODE_TSC_INVALID_SQL;
            }
          }

          numOfFields += tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
        }

        
        return TSDB_CODE_SUCCESS;
      }
    }
    case TK_TOP:
    case TK_BOTTOM:
    case TK_PERCENTILE:
    case TK_APERCENTILE: {
      // 1. valid the number of parameters
      if (pItem->pNode->pParam == NULL || pItem->pNode->pParam->nExpr != 2) {
        /* no parameters or more than one parameter for function */
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tSQLExprItem* pParamElem = &(pItem->pNode->pParam->a[0]);
      if (pParamElem->pNode->nSQLOptr != TK_ID) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
      
      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByName(pCmd, &pParamElem->pNode->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
      SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

      // functions can not be applied to tags
      if (index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      // 2. valid the column type
      int16_t colType = pSchema[index.columnIndex].type;
      if (colType == TSDB_DATA_TYPE_BOOL || colType >= TSDB_DATA_TYPE_BINARY) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      // 3. valid the parameters
      if (pParamElem[1].pNode->nSQLOptr == TK_ID) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tVariant* pVariant = &pParamElem[1].pNode->val;

      int8_t  resultType = pSchema[index.columnIndex].type;
      int16_t resultSize = pSchema[index.columnIndex].bytes;

      char    val[8] = {0};
      SSqlExpr* pExpr = NULL;
      
      if (optr == TK_PERCENTILE || optr == TK_APERCENTILE) {
        tVariantDump(pVariant, val, TSDB_DATA_TYPE_DOUBLE, true);

        double dp = GET_DOUBLE_VAL(val);
        if (dp < 0 || dp > TOP_BOTTOM_QUERY_LIMIT) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
        }

        resultSize = sizeof(double);
        resultType = TSDB_DATA_TYPE_DOUBLE;

        /*
         * sql function transformation
         * for dp = 0, it is actually min,
         * for dp = 100, it is max,
         */
        int16_t functionId = 0;
        if (changeFunctionID(optr, &functionId) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }

        pExpr = tscSqlExprAppend(pQueryInfo, functionId, &index, resultType, resultSize, resultSize, false);
        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double), 0);
      } else {
        tVariantDump(pVariant, val, TSDB_DATA_TYPE_BIGINT, true);

        int64_t nTop = GET_INT32_VAL(val);
        if (nTop <= 0 || nTop > 100) {  // todo use macro
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
        }

        int16_t functionId = 0;
        if (changeFunctionID(optr, &functionId) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }

        // todo REFACTOR
        // set the first column ts for top/bottom query
        SColumnIndex index1 = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
        pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS, &index1, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE,
            TSDB_KEYSIZE, false);
        tstrncpy(pExpr->aliasName, aAggs[TSDB_FUNC_TS].aName, sizeof(pExpr->aliasName));

        const int32_t TS_COLUMN_INDEX = PRIMARYKEY_TIMESTAMP_COL_INDEX;
        SColumnList   ids = getColumnList(1, 0, TS_COLUMN_INDEX);
        insertResultField(pQueryInfo, TS_COLUMN_INDEX, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP,
                          aAggs[TSDB_FUNC_TS].aName, pExpr);

        colIndex += 1;  // the first column is ts

        pExpr = tscSqlExprAppend(pQueryInfo, functionId, &index, resultType, resultSize, resultSize, false);
        addExprParams(pExpr, val, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0);
      }
  
      memset(pExpr->aliasName, 0, tListLen(pExpr->aliasName));
      getColumnName(pItem, pExpr->aliasName, sizeof(pExpr->aliasName) - 1);
  
      SColumnList ids = getColumnList(1, 0, index.columnIndex);
      if (finalResult) {
        insertResultField(pQueryInfo, colIndex, &ids, resultSize, resultType, pExpr->aliasName, pExpr);
      } else {
        for (int32_t i = 0; i < ids.num; ++i) {
          tscColumnListInsert(pQueryInfo->colList, &(ids.ids[i]));
        }
      }

      return TSDB_CODE_SUCCESS;
    };
    
    case TK_TBID: {
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
      if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
      }
    
      // no parameters or more than one parameter for function
      if (pItem->pNode->pParam == NULL || pItem->pNode->pParam->nExpr != 1) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
      
      tSQLExpr* pParam = pItem->pNode->pParam->a[0].pNode;
    
      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByName(pCmd, &pParam->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }
    
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
      SSchema* pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
  
      // functions can not be applied to normal columns
      int32_t numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
      if (index.columnIndex < numOfCols && index.columnIndex != TSDB_TBNAME_COLUMN_INDEX) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }
    
      if (index.columnIndex > 0) {
        index.columnIndex -= numOfCols;
      }
      
      // 2. valid the column type
      int16_t colType = 0;
      if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        colType = TSDB_DATA_TYPE_BINARY;
      } else {
        colType = pSchema[index.columnIndex].type;
      }
      
      if (colType == TSDB_DATA_TYPE_BOOL) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      tscColumnListInsert(pTableMetaInfo->tagColList, &index);
      SSchema* pTagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
      
      SSchema s = {0};
      if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        s = tGetTableNameColumnSchema();
      } else {
        s = pTagSchema[index.columnIndex];
      }
      
      int16_t bytes = 0;
      int16_t type  = 0;
      int32_t inter = 0;

      int32_t ret = getResultDataInfo(s.type, s.bytes, TSDB_FUNC_TID_TAG, 0, &type, &bytes, &inter, 0, 0);
      assert(ret == TSDB_CODE_SUCCESS);
      
      s.type  = type;
      s.bytes = bytes;

      TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);
      tscAddSpecialColumnForSelect(pQueryInfo, 0, TSDB_FUNC_TID_TAG, &index, &s, TSDB_COL_TAG);
      
      return TSDB_CODE_SUCCESS;
    }
    
    default:
      return TSDB_CODE_TSC_INVALID_SQL;
  }
  
}

// todo refactor
static SColumnList getColumnList(int32_t num, int16_t tableIndex, int32_t columnIndex) {
  assert(num == 1 && columnIndex >= -1 && tableIndex >= 0);

  SColumnList columnList = {0};
  columnList.num = num;

  int32_t index = num - 1;
  columnList.ids[index].tableIndex = tableIndex;
  columnList.ids[index].columnIndex = columnIndex;

  return columnList;
}

void getColumnName(tSQLExprItem* pItem, char* resultFieldName, int32_t nameLength) {
  if (pItem->aliasName != NULL) {
    strncpy(resultFieldName, pItem->aliasName, nameLength);
  } else {
    int32_t len = (pItem->pNode->operand.n < nameLength) ? pItem->pNode->operand.n : nameLength;
    strncpy(resultFieldName, pItem->pNode->operand.z, len);
  }
}

void getRevisedName(char* resultFieldName, int32_t functionId, int32_t maxLen, char* columnName) {
  snprintf(resultFieldName, maxLen, "%s(%s)", aAggs[functionId].aName, columnName);
}

static bool isTablenameToken(SSQLToken* token) {
  SSQLToken tmpToken = *token;
  SSQLToken tableToken = {0};

  extractTableNameFromToken(&tmpToken, &tableToken);

  return (strncasecmp(TSQL_TBNAME_L, tmpToken.z, tmpToken.n) == 0 && tmpToken.n == strlen(TSQL_TBNAME_L));
}

static int16_t doGetColumnIndex(SQueryInfo* pQueryInfo, int32_t index, SSQLToken* pToken) {
  STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, index)->pTableMeta;

  int32_t  numOfCols = tscGetNumOfColumns(pTableMeta) + tscGetNumOfTags(pTableMeta);
  SSchema* pSchema = tscGetTableSchema(pTableMeta);

  int16_t columnIndex = COLUMN_INDEX_INITIAL_VAL;

  for (int16_t i = 0; i < numOfCols; ++i) {
    if (pToken->n != strlen(pSchema[i].name)) {
      continue;
    }

    if (strncasecmp(pSchema[i].name, pToken->z, pToken->n) == 0) {
      columnIndex = i;
      break;
    }
  }

  return columnIndex;
}

int32_t doGetColumnIndexByName(SSqlCmd* pCmd, SSQLToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  const char* msg0 = "ambiguous column name";
  const char* msg1 = "invalid column name";

  if (isTablenameToken(pToken)) {
    pIndex->columnIndex = TSDB_TBNAME_COLUMN_INDEX;
  } else if (strncasecmp(pToken->z, DEFAULT_PRIMARY_TIMESTAMP_COL_NAME, pToken->n) == 0) {
    pIndex->columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX;
  } else {
    // not specify the table name, try to locate the table index by column name
    if (pIndex->tableIndex == COLUMN_INDEX_INITIAL_VAL) {
      for (int16_t i = 0; i < pQueryInfo->numOfTables; ++i) {
        int16_t colIndex = doGetColumnIndex(pQueryInfo, i, pToken);

        if (colIndex != COLUMN_INDEX_INITIAL_VAL) {
          if (pIndex->columnIndex != COLUMN_INDEX_INITIAL_VAL) {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
          } else {
            pIndex->tableIndex = i;
            pIndex->columnIndex = colIndex;
          }
        }
      }
    } else {  // table index is valid, get the column index
      int16_t colIndex = doGetColumnIndex(pQueryInfo, pIndex->tableIndex, pToken);
      if (colIndex != COLUMN_INDEX_INITIAL_VAL) {
        pIndex->columnIndex = colIndex;
      }
    }

    if (pIndex->columnIndex == COLUMN_INDEX_INITIAL_VAL) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  if (COLUMN_INDEX_VALIDE(*pIndex)) {
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_TSC_INVALID_SQL;
  }
}

int32_t getTableIndexImpl(SSQLToken* pTableToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  if (pTableToken->n == 0) {  // only one table and no table name prefix in column name
    if (pQueryInfo->numOfTables == 1) {
      pIndex->tableIndex = 0;
    }

    return TSDB_CODE_SUCCESS;
  }

  pIndex->tableIndex = COLUMN_INDEX_INITIAL_VAL;
  char tableName[TSDB_TABLE_ID_LEN] = {0};

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    extractTableName(pTableMetaInfo->name, tableName);

    if (strncasecmp(tableName, pTableToken->z, pTableToken->n) == 0 && strlen(tableName) == pTableToken->n) {
      pIndex->tableIndex = i;
      break;
    }
  }

  if (pIndex->tableIndex < 0) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getTableIndexByName(SSQLToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  SSQLToken tableToken = {0};
  extractTableNameFromToken(pToken, &tableToken);

  if (getTableIndexImpl(&tableToken, pQueryInfo, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getColumnIndexByName(SSqlCmd* pCmd, const SSQLToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  if (pQueryInfo->pTableMetaInfo == NULL || pQueryInfo->numOfTables == 0) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  SSQLToken tmpToken = *pToken;

  if (getTableIndexByName(&tmpToken, pQueryInfo, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return doGetColumnIndexByName(pCmd, &tmpToken, pQueryInfo, pIndex);
}

int32_t changeFunctionID(int32_t optr, int16_t* functionId) {
  switch (optr) {
    case TK_COUNT:
      *functionId = TSDB_FUNC_COUNT;
      break;
    case TK_SUM:
      *functionId = TSDB_FUNC_SUM;
      break;
    case TK_AVG:
      *functionId = TSDB_FUNC_AVG;
      break;
    case TK_RATE:
      *functionId = TSDB_FUNC_RATE;
      break;
    case TK_IRATE:
      *functionId = TSDB_FUNC_IRATE;
      break;
    case TK_SUM_RATE:
      *functionId = TSDB_FUNC_SUM_RATE;
      break;
    case TK_SUM_IRATE:
      *functionId = TSDB_FUNC_SUM_IRATE;
      break;
    case TK_AVG_RATE:
      *functionId = TSDB_FUNC_AVG_RATE;
      break;
    case TK_AVG_IRATE:
      *functionId = TSDB_FUNC_AVG_IRATE;
      break;
    case TK_MIN:
      *functionId = TSDB_FUNC_MIN;
      break;
    case TK_MAX:
      *functionId = TSDB_FUNC_MAX;
      break;
    case TK_STDDEV:
      *functionId = TSDB_FUNC_STDDEV;
      break;
    case TK_PERCENTILE:
      *functionId = TSDB_FUNC_PERCT;
      break;
    case TK_APERCENTILE:
      *functionId = TSDB_FUNC_APERCT;
      break;
    case TK_FIRST:
      *functionId = TSDB_FUNC_FIRST;
      break;
    case TK_LAST:
      *functionId = TSDB_FUNC_LAST;
      break;
    case TK_LEASTSQUARES:
      *functionId = TSDB_FUNC_LEASTSQR;
      break;
    case TK_TOP:
      *functionId = TSDB_FUNC_TOP;
      break;
    case TK_BOTTOM:
      *functionId = TSDB_FUNC_BOTTOM;
      break;
    case TK_DIFF:
      *functionId = TSDB_FUNC_DIFF;
      break;
    case TK_SPREAD:
      *functionId = TSDB_FUNC_SPREAD;
      break;
    case TK_TWA:
      *functionId = TSDB_FUNC_TWA;
      break;
    case TK_INTERP:
      *functionId = TSDB_FUNC_INTERP;
      break;
    case TK_LAST_ROW:
      *functionId = TSDB_FUNC_LAST_ROW;
      break;
    default:
      return -1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setShowInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  SSqlCmd*        pCmd = &pSql->cmd;
  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  assert(pCmd->numOfClause == 1);

  pCmd->command = TSDB_SQL_SHOW;

  const char* msg1 = "invalid name";
  const char* msg2 = "pattern filter string too long";
  const char* msg3 = "database name too long";
  const char* msg4 = "invalid ip address";
  const char* msg5 = "database name is empty";
  const char* msg6 = "pattern string is empty";

  /*
   * database prefix in pInfo->pDCLInfo->a[0]
   * wildcard in like clause in pInfo->pDCLInfo->a[1]
   */
  SShowInfo* pShowInfo = &pInfo->pDCLInfo->showOpt;
  int16_t    showType = pShowInfo->showType;
  if (showType == TSDB_MGMT_TABLE_TABLE || showType == TSDB_MGMT_TABLE_METRIC || showType == TSDB_MGMT_TABLE_VGROUP) {
    // db prefix in tagCond, show table conds in payload
    SSQLToken* pDbPrefixToken = &pShowInfo->prefix;
    if (pDbPrefixToken->type != 0) {

      if (pDbPrefixToken->n >= TSDB_DB_NAME_LEN) {  // db name is too long
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (pDbPrefixToken->n <= 0) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      if (tscValidateName(pDbPrefixToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      int32_t ret = setObjFullName(pTableMetaInfo->name, getAccountId(pSql), pDbPrefixToken, NULL, NULL);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }
    }

    // show table/stable like 'xxxx', set the like pattern for show tables
    SSQLToken* pPattern = &pShowInfo->pattern;
    if (pPattern->type != 0) {
      pPattern->n = strdequote(pPattern->z);

      if (pPattern->n <= 0) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      if (!tscValidateTableNameLength(pCmd->payloadLen)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
    }
  } else if (showType == TSDB_MGMT_TABLE_VNODES) {
    if (pShowInfo->prefix.type == 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), "No specified ip of dnode");
    }

    // show vnodes may be ip addr of dnode in payload
    SSQLToken* pDnodeIp = &pShowInfo->prefix;
    if (pDnodeIp->n >= TSDB_IPv4ADDR_LEN) {  // ip addr is too long
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    if (!validateIpAddress(pDnodeIp->z, pDnodeIp->n)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setKillInfo(SSqlObj* pSql, struct SSqlInfo* pInfo, int32_t killType) {
  const char* msg1 = "invalid connection ID";
  const char* msg2 = "invalid query ID";
  const char* msg3 = "invalid stream ID";

  SSqlCmd* pCmd = &pSql->cmd;
  pCmd->command = pInfo->type;
  
  SSQLToken* idStr = &(pInfo->pDCLInfo->ip);
  if (idStr->n > TSDB_KILL_MSG_LEN) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  strncpy(pCmd->payload, idStr->z, idStr->n);

  const char delim = ':';
  char* connIdStr = strtok(idStr->z, &delim);
  char* queryIdStr = strtok(NULL, &delim);

  int32_t connId = (int32_t)strtol(connIdStr, NULL, 10);
  if (connId <= 0) {
    memset(pCmd->payload, 0, strlen(pCmd->payload));
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (killType == TSDB_SQL_KILL_CONNECTION) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t queryId = (int32_t)strtol(queryIdStr, NULL, 10);
  if (queryId <= 0) {
    memset(pCmd->payload, 0, strlen(pCmd->payload));
    if (killType == TSDB_SQL_KILL_QUERY) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    } else {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

bool validateIpAddress(const char* ip, size_t size) {
  char tmp[128] = {0};  // buffer to build null-terminated string
  assert(size < 128);

  strncpy(tmp, ip, size);

  in_addr_t epAddr = inet_addr(tmp);

  return epAddr != INADDR_NONE;
}

int32_t tscTansformSQLFuncForSTableQuery(SQueryInfo* pQueryInfo) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  if (pTableMetaInfo->pTableMeta == NULL || !UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  assert(tscGetNumOfTags(pTableMetaInfo->pTableMeta) >= 0);

  int16_t bytes = 0;
  int16_t type = 0;
  int32_t interBytes = 0;
  
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t k = 0; k < size; ++k) {
    SSqlExpr*   pExpr = tscSqlExprGet(pQueryInfo, k);
    int16_t functionId = aAggs[pExpr->functionId].stableFuncId;

    int32_t colIndex = pExpr->colInfo.colIndex;
    SSchema* pSrcSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, colIndex);
    
    if ((functionId >= TSDB_FUNC_SUM && functionId <= TSDB_FUNC_TWA) ||
        (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_LAST_DST) ||
        (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_AVG_IRATE)) {
      if (getResultDataInfo(pSrcSchema->type, pSrcSchema->bytes, functionId, pExpr->param[0].i64Key, &type, &bytes,
                            &interBytes, 0, true) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      tscSqlExprUpdate(pQueryInfo, k, functionId, pExpr->colInfo.colIndex, TSDB_DATA_TYPE_BINARY, bytes);
      // todo refactor
      pExpr->interBytes = interBytes;
    }
  }

  tscFieldInfoUpdateOffsetForInterResult(pQueryInfo);
  return TSDB_CODE_SUCCESS;
}

/* transfer the field-info back to original input format */
void tscRestoreSQLFuncForSTableQuery(SQueryInfo* pQueryInfo) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    return;
  }
  
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr*   pExpr = tscSqlExprGet(pQueryInfo, i);
    SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, pExpr->colInfo.colIndex);
    
    // the final result size and type in the same as query on single table.
    // so here, set the flag to be false;
    int32_t inter = 0;
    
    int32_t functionId = pExpr->functionId;
    if (functionId >= TSDB_FUNC_TS && functionId <= TSDB_FUNC_DIFF) {
      continue;
    }
    
    if (functionId == TSDB_FUNC_FIRST_DST) {
      functionId = TSDB_FUNC_FIRST;
    } else if (functionId == TSDB_FUNC_LAST_DST) {
      functionId = TSDB_FUNC_LAST;
    }
    
    getResultDataInfo(pSchema->type, pSchema->bytes, functionId, 0, &pExpr->resType, &pExpr->resBytes,
                      &inter, 0, false);
  }
}

bool hasUnsupportFunctionsForSTableQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg1 = "TWA not allowed to apply to super table directly";
  const char* msg2 = "TWA only support group by tbname for super table query";
  const char* msg3 = "function not support for super table query";

  // filter sql function not supported by metric query yet.
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    int32_t functionId = tscSqlExprGet(pQueryInfo, i)->functionId;
    if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_STABLE) == 0) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return true;
    }
  }

  if (tscIsTWAQuery(pQueryInfo)) {
    if (pQueryInfo->groupbyExpr.numOfGroupCols == 0) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      return true;
    }

    if (pQueryInfo->groupbyExpr.numOfGroupCols != 1) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      return true;
    } else {
      SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, 0);
      if (pColIndex->colIndex != TSDB_TBNAME_COLUMN_INDEX) {
        invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
        return true;
      }
    }
  }

  return false;
}

static bool functionCompatibleCheck(SQueryInfo* pQueryInfo) {
  int32_t startIdx = 0;
  
  SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, startIdx);
  int32_t functionID = pExpr->functionId;

  // ts function can be simultaneously used with any other functions.
  if (functionID == TSDB_FUNC_TS || functionID == TSDB_FUNC_TS_DUMMY) {
    startIdx++;
  }

  int32_t factor = functionCompatList[tscSqlExprGet(pQueryInfo, startIdx)->functionId];

  // diff function cannot be executed with other function
  // arithmetic function can be executed with other arithmetic functions
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  for (int32_t i = startIdx + 1; i < size; ++i) {
    SSqlExpr* pExpr1 = tscSqlExprGet(pQueryInfo, i);

    int16_t functionId = pExpr1->functionId;
    if (functionId == TSDB_FUNC_TAGPRJ || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (functionId == TSDB_FUNC_PRJ && pExpr1->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      continue;
    }

    if (functionCompatList[functionId] != factor) {
      return false;
    }
  }

  return true;
}

int32_t parseGroupbyClause(SQueryInfo* pQueryInfo, tVariantList* pList, SSqlCmd* pCmd) {
  const char* msg1 = "too many columns in group by clause";
  const char* msg2 = "invalid column name in group by clause";
//  const char* msg3 = "group by columns must belong to one table";
  const char* msg7 = "not support group by expression";
  const char* msg8 = "not allowed column type for group by";
  const char* msg9 = "tags not allowed for table query";

  // todo : handle two tables situation
  STableMetaInfo* pTableMetaInfo = NULL;

  if (pList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }
  
  pQueryInfo->groupbyExpr.numOfGroupCols = pList->nExpr;
  if (pList->nExpr > TSDB_MAX_TAGS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  STableMeta* pTableMeta = NULL;
  SSchema*    pSchema = NULL;
  SSchema     s = tscGetTbnameColumnSchema();

  int32_t tableIndex = COLUMN_INDEX_INITIAL_VAL;
  
  for (int32_t i = 0; i < pList->nExpr; ++i) {
    tVariant* pVar = &pList->a[i].pVar;
    SSQLToken token = {pVar->nLen, pVar->nType, pVar->pz};

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(pCmd, &token, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    tableIndex = index.tableIndex;

    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
    pTableMeta = pTableMetaInfo->pTableMeta;
  
    int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      pSchema = &s;
    } else {
      pSchema = tscGetTableColumnSchema(pTableMeta, index.columnIndex);
    }

    bool groupTag = false;
    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX || index.columnIndex >= numOfCols) {
      groupTag = true;
    }
  
    SSqlGroupbyExpr* pGroupExpr = &pQueryInfo->groupbyExpr;
    if (pGroupExpr->columnInfo == NULL) {
      pGroupExpr->columnInfo = taosArrayInit(4, sizeof(SColIndex));
    }
    
    if (groupTag) {
      if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg9);
      }

      int32_t relIndex = index.columnIndex;
      if (index.columnIndex != TSDB_TBNAME_COLUMN_INDEX) {
        relIndex -= numOfCols;
      }

      SColIndex colIndex = { .colIndex = relIndex, .flag = TSDB_COL_TAG, .colId = pSchema->colId, };
      taosArrayPush(pGroupExpr->columnInfo, &colIndex);
      
      index.columnIndex = relIndex;
      tscColumnListInsert(pTableMetaInfo->tagColList, &index);
    } else {
      // check if the column type is valid, here only support the bool/tinyint/smallint/bigint group by
      if (pSchema->type > TSDB_DATA_TYPE_BINARY) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
      }

      tscColumnListInsert(pQueryInfo->colList, &index);
      
      SColIndex colIndex = { .colIndex = index.columnIndex, .flag = TSDB_COL_NORMAL, .colId = pSchema->colId };
      taosArrayPush(pGroupExpr->columnInfo, &colIndex);
      pQueryInfo->groupbyExpr.orderType = TSDB_ORDER_ASC;

      if (i == 0 && pList->nExpr > 1) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
      }
    }
  }

  pQueryInfo->groupbyExpr.tableIndex = tableIndex;
  return TSDB_CODE_SUCCESS;
}

void setColumnOffsetValueInResultset(SQueryInfo* pQueryInfo) {
  if (QUERY_IS_STABLE_QUERY(pQueryInfo->type)) {
    tscFieldInfoUpdateOffsetForInterResult(pQueryInfo);
  } else {
    tscFieldInfoUpdateOffset(pQueryInfo);
  }
}

static SColumnFilterInfo* addColumnFilterInfo(SColumn* pColumn) {
  if (pColumn == NULL) {
    return NULL;
  }

  int32_t size = pColumn->numOfFilters + 1;
  char*   tmp = (char*)realloc((void*)(pColumn->filterInfo), sizeof(SColumnFilterInfo) * (size));
  if (tmp != NULL) {
    pColumn->filterInfo = (SColumnFilterInfo*)tmp;
  }

  pColumn->numOfFilters++;

  SColumnFilterInfo* pColFilterInfo = &pColumn->filterInfo[pColumn->numOfFilters - 1];
  memset(pColFilterInfo, 0, sizeof(SColumnFilterInfo));

  return pColFilterInfo;
}

static int32_t doExtractColumnFilterInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SColumnFilterInfo* pColumnFilter,
                                         SColumnIndex* columnIndex, tSQLExpr* pExpr) {
  const char* msg = "not supported filter condition";

  tSQLExpr*       pRight = pExpr->pRight;
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, columnIndex->tableIndex);

  SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, columnIndex->columnIndex);

  int16_t colType = pSchema->type;
  if (colType >= TSDB_DATA_TYPE_TINYINT && colType <= TSDB_DATA_TYPE_BIGINT) {
    colType = TSDB_DATA_TYPE_BIGINT;
  } else if (colType == TSDB_DATA_TYPE_FLOAT || colType == TSDB_DATA_TYPE_DOUBLE) {
    colType = TSDB_DATA_TYPE_DOUBLE;
  } else if ((colType == TSDB_DATA_TYPE_TIMESTAMP) && (TSDB_DATA_TYPE_BINARY == pRight->val.nType)) {
    int retVal = setColumnFilterInfoForTimestamp(pCmd, pQueryInfo, &pRight->val);
    if (TSDB_CODE_SUCCESS != retVal) {
      return retVal;
    }
  }

  if (pExpr->nSQLOptr == TK_LE || pExpr->nSQLOptr == TK_LT) {
    tVariantDump(&pRight->val, (char*)&pColumnFilter->upperBndd, colType, false);
  } else {  // TK_GT,TK_GE,TK_EQ,TK_NE are based on the pColumn->lowerBndd
    if (colType == TSDB_DATA_TYPE_BINARY) {
      pColumnFilter->pz = (int64_t)calloc(1, pRight->val.nLen + TSDB_NCHAR_SIZE);
      pColumnFilter->len = pRight->val.nLen;

      tVariantDump(&pRight->val, (char*)pColumnFilter->pz, colType, false);
    } else if (colType == TSDB_DATA_TYPE_NCHAR) {
      // pRight->val.nLen + 1 is larger than the actual nchar string length
      pColumnFilter->pz = (int64_t)calloc(1, (pRight->val.nLen + 1) * TSDB_NCHAR_SIZE);

      tVariantDump(&pRight->val, (char*)pColumnFilter->pz, colType, false);

      size_t len = twcslen((wchar_t*)pColumnFilter->pz);
      pColumnFilter->len = len * TSDB_NCHAR_SIZE;
    } else {
      tVariantDump(&pRight->val, (char*)&pColumnFilter->lowerBndd, colType, false);
    }
  }

  switch (pExpr->nSQLOptr) {
    case TK_LE:
      pColumnFilter->upperRelOptr = TSDB_RELATION_LESS_EQUAL;
      break;
    case TK_LT:
      pColumnFilter->upperRelOptr = TSDB_RELATION_LESS;
      break;
    case TK_GT:
      pColumnFilter->lowerRelOptr = TSDB_RELATION_GREATER;
      break;
    case TK_GE:
      pColumnFilter->lowerRelOptr = TSDB_RELATION_GREATER_EQUAL;
      break;
    case TK_EQ:
      pColumnFilter->lowerRelOptr = TSDB_RELATION_EQUAL;
      break;
    case TK_NE:
      pColumnFilter->lowerRelOptr = TSDB_RELATION_NOT_EQUAL;
      break;
    case TK_LIKE:
      pColumnFilter->lowerRelOptr = TSDB_RELATION_LIKE;
      break;
    default:
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SCondExpr {
  tSQLExpr* pTagCond;
  tSQLExpr* pTimewindow;

  tSQLExpr* pColumnCond;

  tSQLExpr* pTableCond;
  int16_t   relType;  // relation between table name in expression and other tag
                      // filter condition expression, TK_AND or TK_OR
  int16_t tableCondIndex;

  tSQLExpr* pJoinExpr;  // join condition
  bool      tsJoin;
} SCondExpr;

static int32_t getTimeRange(STimeWindow* win, tSQLExpr* pRight, int32_t optr, int16_t timePrecision);

static int32_t tSQLExprNodeToString(tSQLExpr* pExpr, char** str) {
  if (pExpr->nSQLOptr == TK_ID) {  // column name
    strncpy(*str, pExpr->colInfo.z, pExpr->colInfo.n);
    *str += pExpr->colInfo.n;

  } else if (pExpr->nSQLOptr >= TK_BOOL && pExpr->nSQLOptr <= TK_STRING) {  // value
    *str += tVariantToString(&pExpr->val, *str);

  } else if (pExpr->nSQLOptr >= TK_COUNT && pExpr->nSQLOptr <= TK_AVG_IRATE) {
    /*
     * arithmetic expression of aggregation, such as count(ts) + count(ts) *2
     */
    strncpy(*str, pExpr->operand.z, pExpr->operand.n);
    *str += pExpr->operand.n;
  } else {  // not supported operation
    assert(false);
  }

  return TSDB_CODE_SUCCESS;
}

static bool isExprLeafNode(tSQLExpr* pExpr) {
  return (pExpr->pRight == NULL && pExpr->pLeft == NULL) &&
         (pExpr->nSQLOptr == TK_ID || (pExpr->nSQLOptr >= TK_BOOL && pExpr->nSQLOptr <= TK_NCHAR) ||
          pExpr->nSQLOptr == TK_SET);
}

static bool isExprDirectParentOfLeaftNode(tSQLExpr* pExpr) {
  return (pExpr->pLeft != NULL && pExpr->pRight != NULL) &&
         (isExprLeafNode(pExpr->pLeft) && isExprLeafNode(pExpr->pRight));
}

static int32_t tSQLExprLeafToString(tSQLExpr* pExpr, bool addParentheses, char** output) {
  if (!isExprDirectParentOfLeaftNode(pExpr)) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;

  if (addParentheses) {
    *(*output) = '(';
    *output += 1;
  }

  tSQLExprNodeToString(pLeft, output);
  if (optrToString(pExpr, output) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  tSQLExprNodeToString(pRight, output);

  if (addParentheses) {
    *(*output) = ')';
    *output += 1;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t optrToString(tSQLExpr* pExpr, char** exprString) {
  const char* le = "<=";
  const char* ge = ">=";
  const char* ne = "<>";
  const char* likeOptr = "LIKE";

  switch (pExpr->nSQLOptr) {
    case TK_LE: {
      *(int16_t*)(*exprString) = *(int16_t*)le;
      *exprString += 1;
      break;
    }
    case TK_GE: {
      *(int16_t*)(*exprString) = *(int16_t*)ge;
      *exprString += 1;
      break;
    }
    case TK_NE: {
      *(int16_t*)(*exprString) = *(int16_t*)ne;
      *exprString += 1;
      break;
    }

    case TK_LT:
      *(*exprString) = '<';
      break;
    case TK_GT:
      *(*exprString) = '>';
      break;
    case TK_EQ:
      *(*exprString) = '=';
      break;
    case TK_PLUS:
      *(*exprString) = '+';
      break;
    case TK_MINUS:
      *(*exprString) = '-';
      break;
    case TK_STAR:
      *(*exprString) = '*';
      break;
    case TK_DIVIDE:
      *(*exprString) = '/';
      break;
    case TK_REM:
      *(*exprString) = '%';
      break;
    case TK_LIKE: {
      int32_t len = sprintf(*exprString, " %s ", likeOptr);
      *exprString += (len - 1);
      break;
    }
    default:
      return TSDB_CODE_TSC_INVALID_SQL;
  }

  *exprString += 1;

  return TSDB_CODE_SUCCESS;
}

static int32_t tablenameListToString(tSQLExpr* pExpr, SStringBuilder* sb) {
  tSQLExprList* pList = pExpr->pParam;
  if (pList->nExpr <= 0) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (pList->nExpr > 0) {
    taosStringBuilderAppendStringLen(sb, QUERY_COND_REL_PREFIX_IN, QUERY_COND_REL_PREFIX_IN_LEN);
  }

  for (int32_t i = 0; i < pList->nExpr; ++i) {
    tSQLExpr* pSub = pList->a[i].pNode;
    taosStringBuilderAppendStringLen(sb, pSub->val.pz, pSub->val.nLen);

    if (i < pList->nExpr - 1) {
      taosStringBuilderAppendString(sb, TBNAME_LIST_SEP);
    }

    if (pSub->val.nLen <= 0 || !tscValidateTableNameLength(pSub->val.nLen)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tablenameCondToString(tSQLExpr* pExpr, SStringBuilder* sb) {
  taosStringBuilderAppendStringLen(sb, QUERY_COND_REL_PREFIX_LIKE, QUERY_COND_REL_PREFIX_LIKE_LEN);
  taosStringBuilderAppendString(sb, pExpr->val.pz);

  return TSDB_CODE_SUCCESS;
}

enum {
  TSQL_EXPR_TS = 0,
  TSQL_EXPR_TAG = 1,
  TSQL_EXPR_COLUMN = 2,
  TSQL_EXPR_TBNAME = 3,
};

static int32_t extractColumnFilterInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SColumnIndex* pIndex, tSQLExpr* pExpr, int32_t sqlOptr) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pIndex->tableIndex);

  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
  SSchema*    pSchema = tscGetTableColumnSchema(pTableMeta, pIndex->columnIndex);

  const char* msg1 = "non binary column not support like operator";
  const char* msg2 = "binary column not support this operator";  
  const char* msg3 = "bool column not support this operator";

  SColumn* pColumn = tscColumnListInsert(pQueryInfo->colList, pIndex);
  SColumnFilterInfo* pColFilter = NULL;

  /*
   * in case of TK_AND filter condition, we first find the corresponding column and build the query condition together
   * the already existed condition.
   */
  if (sqlOptr == TK_AND) {
    // this is a new filter condition on this column
    if (pColumn->numOfFilters == 0) {
      pColFilter = addColumnFilterInfo(pColumn);
    } else {  // update the existed column filter information, find the filter info here
      pColFilter = &pColumn->filterInfo[0];
    }
  } else if (sqlOptr == TK_OR) {
    // TODO fixme: failed to invalid the filter expression: "col1 = 1 OR col2 = 2"
    pColFilter = addColumnFilterInfo(pColumn);
  } else {  // error;
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  pColFilter->filterstr =
      ((pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) ? 1 : 0);

  if (pColFilter->filterstr) {
    if (pExpr->nSQLOptr != TK_EQ && pExpr->nSQLOptr != TK_NE && pExpr->nSQLOptr != TK_LIKE) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  } else {
    if (pExpr->nSQLOptr == TK_LIKE) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
    
    if (pSchema->type == TSDB_DATA_TYPE_BOOL) {
      if (pExpr->nSQLOptr != TK_EQ && pExpr->nSQLOptr != TK_NE) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }
    }
  }

  pColumn->colIndex = *pIndex;
  return doExtractColumnFilterInfo(pCmd, pQueryInfo, pColFilter, pIndex, pExpr);
}

static void relToString(tSQLExpr* pExpr, char** str) {
  assert(pExpr->nSQLOptr == TK_AND || pExpr->nSQLOptr == TK_OR);

  const char* or = "OR";
  const char*and = "AND";

  //    if (pQueryInfo->tagCond.relType == TSQL_STABLE_QTYPE_COND) {
  if (pExpr->nSQLOptr == TK_AND) {
    strcpy(*str, and);
    *str += strlen(and);
  } else {
    strcpy(*str, or);
    *str += strlen(or);
  }
}

UNUSED_FUNC
static int32_t getTagCondString(tSQLExpr* pExpr, char** str) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isExprDirectParentOfLeaftNode(pExpr)) {
    *(*str) = '(';
    *str += 1;

    int32_t ret = getTagCondString(pExpr->pLeft, str);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    relToString(pExpr, str);

    ret = getTagCondString(pExpr->pRight, str);

    *(*str) = ')';
    *str += 1;

    return ret;
  }

  return tSQLExprLeafToString(pExpr, true, str);
}

static int32_t getTablenameCond(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSQLExpr* pTableCond, SStringBuilder* sb) {
  const char* msg0 = "invalid table name list";

  if (pTableCond == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSQLExpr* pLeft = pTableCond->pLeft;
  tSQLExpr* pRight = pTableCond->pRight;

  if (!isTablenameToken(&pLeft->colInfo)) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  int32_t ret = TSDB_CODE_SUCCESS;

  if (pTableCond->nSQLOptr == TK_IN) {
    ret = tablenameListToString(pRight, sb);
  } else if (pTableCond->nSQLOptr == TK_LIKE) {
    ret = tablenameCondToString(pRight, sb);
  }

  if (ret != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }

  return ret;
}

static int32_t getColumnQueryCondInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSQLExpr* pExpr, int32_t relOptr) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isExprDirectParentOfLeaftNode(pExpr)) {  // internal node
    int32_t ret = getColumnQueryCondInfo(pCmd, pQueryInfo, pExpr->pLeft, pExpr->nSQLOptr);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    return getColumnQueryCondInfo(pCmd, pQueryInfo, pExpr->pRight, pExpr->nSQLOptr);
  } else {  // handle leaf node
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(pCmd, &pExpr->pLeft->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    return extractColumnFilterInfo(pCmd, pQueryInfo, &index, pExpr, relOptr);
  }
}

static int32_t getJoinCondInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSQLExpr* pExpr) {
  const char* msg1 = "invalid join query condition";
  const char* msg2 = "join on binary/nchar not supported";
  const char* msg3 = "type of join columns must be identical";
  const char* msg4 = "invalid column name in join condition";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isExprDirectParentOfLeaftNode(pExpr)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  STagCond*  pTagCond = &pQueryInfo->tagCond;
  SJoinNode* pLeft = &pTagCond->joinInfo.left;
  SJoinNode* pRight = &pTagCond->joinInfo.right;

  SColumnIndex index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(pCmd, &pExpr->pLeft->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
  SSchema* pTagSchema1 = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);

  pLeft->uid = pTableMetaInfo->pTableMeta->uid;
  pLeft->tagColId = pTagSchema1->colId;
  strcpy(pLeft->tableId, pTableMetaInfo->name);

  index = (SColumnIndex)COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(pCmd, &pExpr->pRight->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
  SSchema* pTagSchema2 = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);

  pRight->uid = pTableMetaInfo->pTableMeta->uid;
  pRight->tagColId = pTagSchema2->colId;
  strcpy(pRight->tableId, pTableMetaInfo->name);

  if (pTagSchema1->type != pTagSchema2->type) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  if (pTagSchema1->type == TSDB_DATA_TYPE_BINARY || pTagSchema1->type == TSDB_DATA_TYPE_NCHAR) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  pTagCond->joinInfo.hasJoin = true;
  return TSDB_CODE_SUCCESS;
}

// todo error handle / such as and /or mixed with +/-/*/
int32_t buildArithmeticExprString(tSQLExpr* pExpr, char** exprString) {
  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;

  *(*exprString)++ = '(';

  if (pLeft->nSQLOptr >= TK_PLUS && pLeft->nSQLOptr <= TK_REM) {
    buildArithmeticExprString(pLeft, exprString);
  } else {
    int32_t ret = tSQLExprNodeToString(pLeft, exprString);
    if (ret != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  optrToString(pExpr, exprString);

  if (pRight->nSQLOptr >= TK_PLUS && pRight->nSQLOptr <= TK_REM) {
    buildArithmeticExprString(pRight, exprString);
  } else {
    int32_t ret = tSQLExprNodeToString(pRight, exprString);
    if (ret != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  *(*exprString)++ = ')';

  return TSDB_CODE_SUCCESS;
}

static int32_t validateSQLExpr(SSqlCmd* pCmd, tSQLExpr* pExpr, SQueryInfo* pQueryInfo, SColumnList* pList, int32_t* type) {
  if (pExpr->nSQLOptr == TK_ID) {
    if (*type == NON_ARITHMEIC_EXPR) {
      *type = NORMAL_ARITHMETIC;
    } else if (*type == AGG_ARIGHTMEIC) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(pCmd, &pExpr->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    // if column is timestamp, bool, binary, nchar, not support arithmetic, so return invalid sql
    STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, index.tableIndex)->pTableMeta;
    SSchema*    pSchema = tscGetTableSchema(pTableMeta) + index.columnIndex;
    
    if ((pSchema->type == TSDB_DATA_TYPE_TIMESTAMP) || (pSchema->type == TSDB_DATA_TYPE_BOOL) ||
        (pSchema->type == TSDB_DATA_TYPE_BINARY) || (pSchema->type == TSDB_DATA_TYPE_NCHAR)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    pList->ids[pList->num++] = index;
  } else if (pExpr->nSQLOptr == TK_FLOAT && (isnan(pExpr->val.dKey) || isinf(pExpr->val.dKey))) {
    return TSDB_CODE_TSC_INVALID_SQL;
  } else if (pExpr->nSQLOptr >= TK_COUNT && pExpr->nSQLOptr <= TK_AVG_IRATE) {
    if (*type == NON_ARITHMEIC_EXPR) {
      *type = AGG_ARIGHTMEIC;
    } else if (*type == NORMAL_ARITHMETIC) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    int32_t outputIndex = tscSqlExprNumOfExprs(pQueryInfo);
  
    tSQLExprItem item = {.pNode = pExpr, .aliasName = NULL};
  
    // sql function in selection clause, append sql function info in pSqlCmd structure sequentially
    if (addExprAndResultField(pCmd, pQueryInfo, outputIndex, &item, false) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateArithmeticSQLExpr(SSqlCmd* pCmd, tSQLExpr* pExpr, SQueryInfo* pQueryInfo, SColumnList* pList, int32_t* type) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSQLExpr* pLeft = pExpr->pLeft;
  if (pLeft->nSQLOptr >= TK_PLUS && pLeft->nSQLOptr <= TK_REM) {
    int32_t ret = validateArithmeticSQLExpr(pCmd, pLeft, pQueryInfo, pList, type);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateSQLExpr(pCmd, pLeft, pQueryInfo, pList, type);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  tSQLExpr* pRight = pExpr->pRight;
  if (pRight->nSQLOptr >= TK_PLUS && pRight->nSQLOptr <= TK_REM) {
    int32_t ret = validateArithmeticSQLExpr(pCmd, pRight, pQueryInfo, pList, type);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateSQLExpr(pCmd, pRight, pQueryInfo, pList, type);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool isValidExpr(tSQLExpr* pLeft, tSQLExpr* pRight, int32_t optr) {
  if (pLeft == NULL || (pRight == NULL && optr != TK_IN)) {
    return false;
  }

  /*
   * filter illegal expression in where clause:
   * 1. count(*) > 12
   * 2. sum(columnA) > sum(columnB)
   * 3. 4 < 5,  'ABC'>'abc'
   *
   * However, columnA < 4+12 is valid
   */
  if (pLeft->nSQLOptr >= TK_COUNT && pLeft->nSQLOptr <= TK_AVG_IRATE) {
    return false;
  }

  if (pRight == NULL) {
    return true;
  }

  if (pRight->nSQLOptr >= TK_COUNT && pRight->nSQLOptr <= TK_AVG_IRATE) {
    return false;
  }
  
  if (pLeft->nSQLOptr >= TK_BOOL
    && pLeft->nSQLOptr <= TK_BINARY
    && pRight->nSQLOptr >= TK_BOOL
    && pRight->nSQLOptr <= TK_BINARY) {
    return false;
  }

  return true;
}

static void exchangeExpr(tSQLExpr* pExpr) {
  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;

  if (pRight->nSQLOptr == TK_ID && (pLeft->nSQLOptr == TK_INTEGER || pLeft->nSQLOptr == TK_FLOAT ||
                                    pLeft->nSQLOptr == TK_STRING || pLeft->nSQLOptr == TK_BOOL)) {
    /*
     * exchange value of the left handside and the value of the right-handside
     * to make sure that the value of filter expression always locates in
     * right-handside and
     * the column-id is at the left handside.
     */
    uint32_t optr = 0;
    switch (pExpr->nSQLOptr) {
      case TK_LE:
        optr = TK_GE;
        break;
      case TK_LT:
        optr = TK_GT;
        break;
      case TK_GT:
        optr = TK_LT;
        break;
      case TK_GE:
        optr = TK_LE;
        break;
      default:
        optr = pExpr->nSQLOptr;
    }

    pExpr->nSQLOptr = optr;
    SWAP(pExpr->pLeft, pExpr->pRight, void*);
  }
}

static bool validateJoinExprNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSQLExpr* pExpr, SColumnIndex* pLeftIndex) {
  const char* msg1 = "illegal column name";
  const char* msg2 = "= is expected in join expression";
  const char* msg3 = "join column must have same type";
  const char* msg4 = "self join is not allowed";
  const char* msg5 = "join table must be the same type(table to table, super table to super table)";

  tSQLExpr* pRight = pExpr->pRight;

  if (pRight->nSQLOptr != TK_ID) {
    return true;
  }

  if (pExpr->nSQLOptr != TK_EQ) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    return false;
  }

  SColumnIndex rightIndex = COLUMN_INDEX_INITIALIZER;

  if (getColumnIndexByName(pCmd, &pRight->colInfo, pQueryInfo, &rightIndex) != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  // todo extract function
  STableMetaInfo* pLeftMeterMeta = tscGetMetaInfo(pQueryInfo, pLeftIndex->tableIndex);
  SSchema*        pLeftSchema = tscGetTableSchema(pLeftMeterMeta->pTableMeta);
  int16_t         leftType = pLeftSchema[pLeftIndex->columnIndex].type;

  STableMetaInfo* pRightMeterMeta = tscGetMetaInfo(pQueryInfo, rightIndex.tableIndex);
  SSchema*        pRightSchema = tscGetTableSchema(pRightMeterMeta->pTableMeta);
  int16_t         rightType = pRightSchema[rightIndex.columnIndex].type;

  if (leftType != rightType) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    return false;
  } else if (pLeftIndex->tableIndex == rightIndex.tableIndex) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
    return false;
  }

  // table to table/ super table to super table are allowed
  if (UTIL_TABLE_IS_SUPER_TABLE(pLeftMeterMeta) != UTIL_TABLE_IS_SUPER_TABLE(pRightMeterMeta)) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
    return false;
  }

  return true;
}

static bool validTableNameOptr(tSQLExpr* pExpr) {
  const char nameFilterOptr[] = {TK_IN, TK_LIKE};

  for (int32_t i = 0; i < tListLen(nameFilterOptr); ++i) {
    if (pExpr->nSQLOptr == nameFilterOptr[i]) {
      return true;
    }
  }

  return false;
}

static int32_t setExprToCond(tSQLExpr** parent, tSQLExpr* pExpr, const char* msg, int32_t parentOptr, char* msgBuf) {
  if (*parent != NULL) {
    if (parentOptr == TK_OR && msg != NULL) {
      return invalidSqlErrMsg(msgBuf, msg);
    }

    *parent = tSQLExprCreate((*parent), pExpr, parentOptr);
  } else {
    *parent = pExpr;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t handleExprInQueryCond(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSQLExpr** pExpr, SCondExpr* pCondExpr,
                                     int32_t* type, int32_t parentOptr) {
  const char* msg1 = "table query cannot use tags filter";
  const char* msg2 = "illegal column name";
  const char* msg3 = "only one query time range allowed";
  const char* msg4 = "only one join condition allowed";
  const char* msg5 = "not support ordinary column join";
  const char* msg6 = "only one query condition on tbname allowed";
  const char* msg7 = "only in/like allowed in filter table name";
  const char* msg8 = "wildcard string should be less than 20 characters";
  
  tSQLExpr* pLeft = (*pExpr)->pLeft;
  tSQLExpr* pRight = (*pExpr)->pRight;

  int32_t ret = TSDB_CODE_SUCCESS;

  SColumnIndex index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(pCmd, &pLeft->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  assert(isExprDirectParentOfLeaftNode(*pExpr));

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

  if (index.columnIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {  // query on time range
    if (!validateJoinExprNode(pCmd, pQueryInfo, *pExpr, &index)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    // set join query condition
    if (pRight->nSQLOptr == TK_ID) {  // no need to keep the timestamp join condition
      TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_QUERY);
      pCondExpr->tsJoin = true;

      /*
       * to release expression, e.g., m1.ts = m2.ts,
       * since this expression is used to set the join query type
       */
      tSQLExprDestroy(*pExpr);
    } else {
      ret = setExprToCond(&pCondExpr->pTimewindow, *pExpr, msg3, parentOptr, pQueryInfo->msg);
    }

    *pExpr = NULL;  // remove this expression
    *type = TSQL_EXPR_TS;
  } else if (index.columnIndex >= tscGetNumOfColumns(pTableMeta) || index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    // query on tags, check for tag query condition
    if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    // check for like expression
    if ((*pExpr)->nSQLOptr == TK_LIKE) {
      if (pRight->val.nLen > TSDB_PATTERN_STRING_MAX_LEN) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
      }

      SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

      if ((!isTablenameToken(&pLeft->colInfo)) && pSchema[index.columnIndex].type != TSDB_DATA_TYPE_BINARY &&
          pSchema[index.columnIndex].type != TSDB_DATA_TYPE_NCHAR) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
    }

    // in case of in operator, keep it in a seperate attribute
    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      if (!validTableNameOptr(*pExpr)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
      }
  
      if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (pCondExpr->pTableCond == NULL) {
        pCondExpr->pTableCond = *pExpr;
        pCondExpr->relType = parentOptr;
        pCondExpr->tableCondIndex = index.tableIndex;
      } else {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      *type = TSQL_EXPR_TBNAME;
      *pExpr = NULL;
    } else {
      if (pRight->nSQLOptr == TK_ID) {  // join on tag columns for stable query
        if (!validateJoinExprNode(pCmd, pQueryInfo, *pExpr, &index)) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }

        if (pCondExpr->pJoinExpr != NULL) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
        }

        pQueryInfo->type |= TSDB_QUERY_TYPE_JOIN_QUERY;
        ret = setExprToCond(&pCondExpr->pJoinExpr, *pExpr, NULL, parentOptr, pQueryInfo->msg);
        *pExpr = NULL;
      } else {
        // do nothing
        //                ret = setExprToCond(pCmd, &pCondExpr->pTagCond,
        //                *pExpr, NULL, parentOptr);
      }

      *type = TSQL_EXPR_TAG;
    }

  } else {  // query on other columns
    *type = TSQL_EXPR_COLUMN;

    if (pRight->nSQLOptr == TK_ID) {  // other column cannot be served as the join column
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
    }

    ret = setExprToCond(&pCondExpr->pColumnCond, *pExpr, NULL, parentOptr, pQueryInfo->msg);
    *pExpr = NULL;  // remove it from expr tree
  }

  return ret;
}

int32_t getQueryCondExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSQLExpr** pExpr, SCondExpr* pCondExpr,
                        int32_t* type, int32_t parentOptr) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* msg1 = "query condition between different columns must use 'AND'";

  tSQLExpr* pLeft = (*pExpr)->pLeft;
  tSQLExpr* pRight = (*pExpr)->pRight;

  if (!isValidExpr(pLeft, pRight, (*pExpr)->nSQLOptr)) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  int32_t leftType = -1;
  int32_t rightType = -1;

  if (!isExprDirectParentOfLeaftNode(*pExpr)) {
    int32_t ret = getQueryCondExpr(pCmd, pQueryInfo, &(*pExpr)->pLeft, pCondExpr, &leftType, (*pExpr)->nSQLOptr);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    ret = getQueryCondExpr(pCmd, pQueryInfo, &(*pExpr)->pRight, pCondExpr, &rightType, (*pExpr)->nSQLOptr);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    /*
     *  if left child and right child do not belong to the same group, the sub
     *  expression is not valid for parent node, it must be TK_AND operator.
     */
    if (leftType != rightType) {
      if ((*pExpr)->nSQLOptr == TK_OR && (leftType + rightType != TSQL_EXPR_TBNAME + TSQL_EXPR_TAG)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }
    }

    *type = rightType;
    return TSDB_CODE_SUCCESS;
  }

  exchangeExpr(*pExpr);

  return handleExprInQueryCond(pCmd, pQueryInfo, pExpr, pCondExpr, type, parentOptr);
}

static void doCompactQueryExpr(tSQLExpr** pExpr) {
  if (*pExpr == NULL || isExprDirectParentOfLeaftNode(*pExpr)) {
    return;
  }

  if ((*pExpr)->pLeft) {
    doCompactQueryExpr(&(*pExpr)->pLeft);
  }

  if ((*pExpr)->pRight) {
    doCompactQueryExpr(&(*pExpr)->pRight);
  }

  if ((*pExpr)->pLeft == NULL && (*pExpr)->pRight == NULL &&
      ((*pExpr)->nSQLOptr == TK_OR || (*pExpr)->nSQLOptr == TK_AND)) {
    tSQLExprNodeDestroy(*pExpr);
    *pExpr = NULL;

  } else if ((*pExpr)->pLeft == NULL && (*pExpr)->pRight != NULL) {
    tSQLExpr* tmpPtr = (*pExpr)->pRight;
    tSQLExprNodeDestroy(*pExpr);

    (*pExpr) = tmpPtr;
  } else if ((*pExpr)->pRight == NULL && (*pExpr)->pLeft != NULL) {
    tSQLExpr* tmpPtr = (*pExpr)->pLeft;
    tSQLExprNodeDestroy(*pExpr);

    (*pExpr) = tmpPtr;
  }
}

static void doExtractExprForSTable(SSqlCmd* pCmd, tSQLExpr** pExpr, SQueryInfo* pQueryInfo, tSQLExpr** pOut, int32_t tableIndex) {
  if (isExprDirectParentOfLeaftNode(*pExpr)) {
    tSQLExpr* pLeft = (*pExpr)->pLeft;

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(pCmd, &pLeft->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return;
    }

    if (index.tableIndex != tableIndex) {
      return;
    }

    SSQLToken t = {0};
    extractTableNameFromToken(&pLeft->colInfo, &t);

    *pOut = *pExpr;
    (*pExpr) = NULL;

  } else {
    *pOut = tSQLExprCreate(NULL, NULL, (*pExpr)->nSQLOptr);

    doExtractExprForSTable(pCmd, &(*pExpr)->pLeft, pQueryInfo, &((*pOut)->pLeft), tableIndex);
    doExtractExprForSTable(pCmd, &(*pExpr)->pRight, pQueryInfo, &((*pOut)->pRight), tableIndex);
  }
}

static tSQLExpr* extractExprForSTable(SSqlCmd* pCmd, tSQLExpr** pExpr, SQueryInfo* pQueryInfo, int32_t tableIndex) {
  tSQLExpr* pResExpr = NULL;

  if (*pExpr != NULL) {
    doExtractExprForSTable(pCmd, pExpr, pQueryInfo, &pResExpr, tableIndex);
    doCompactQueryExpr(&pResExpr);
  }

  return pResExpr;
}

int tableNameCompar(const void* lhs, const void* rhs) {
  char* left = *(char**)lhs;
  char* right = *(char**)rhs;

  int32_t ret = strcmp(left, right);

  if (ret == 0) {
    return 0;
  }

  return ret > 0 ? 1 : -1;
}

static int32_t setTableCondForSTableQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, const char* account,
                                          tSQLExpr* pExpr, int16_t tableCondIndex, SStringBuilder* sb) {
  const char* msg = "table name too long";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableCondIndex);

  STagCond* pTagCond = &pQueryInfo->tagCond;
  pTagCond->tbnameCond.uid = pTableMetaInfo->pTableMeta->uid;

  assert(pExpr->nSQLOptr == TK_LIKE || pExpr->nSQLOptr == TK_IN);

  if (pExpr->nSQLOptr == TK_LIKE) {
    char* str = taosStringBuilderGetResult(sb, NULL);
    pQueryInfo->tagCond.tbnameCond.cond = strdup(str);
    return TSDB_CODE_SUCCESS;
  }

  SStringBuilder sb1; memset(&sb1, 0, sizeof(sb1));
  taosStringBuilderAppendStringLen(&sb1, QUERY_COND_REL_PREFIX_IN, QUERY_COND_REL_PREFIX_IN_LEN);

  char db[TSDB_TABLE_ID_LEN] = {0};

  // remove the duplicated input table names
  int32_t num = 0;
  char*   tableNameString = taosStringBuilderGetResult(sb, NULL);

  char** segments = strsplit(tableNameString + QUERY_COND_REL_PREFIX_IN_LEN, TBNAME_LIST_SEP, &num);
  qsort(segments, num, POINTER_BYTES, tableNameCompar);

  int32_t j = 1;
  for (int32_t i = 1; i < num; ++i) {
    if (strcmp(segments[i], segments[i - 1]) != 0) {
      segments[j++] = segments[i];
    }
  }
  num = j;

  char* name = extractDBName(pTableMetaInfo->name, db);
  SSQLToken dbToken = {.type = TK_STRING, .z = name, .n = strlen(name)};
  
  for (int32_t i = 0; i < num; ++i) {
    if (i >= 1) {
      taosStringBuilderAppendStringLen(&sb1, TBNAME_LIST_SEP, 1);
    }

    char      idBuf[TSDB_TABLE_ID_LEN] = {0};
    int32_t   xlen = strlen(segments[i]);
    SSQLToken t = {.z = segments[i], .n = xlen, .type = TK_STRING};

    int32_t ret = setObjFullName(idBuf, account, &dbToken, &t, &xlen);
    if (ret != TSDB_CODE_SUCCESS) {
      taosStringBuilderDestroy(&sb1);
      tfree(segments);

      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      return ret;
    }

    taosStringBuilderAppendString(&sb1, idBuf);
  }

  char* str = taosStringBuilderGetResult(&sb1, NULL);
  pQueryInfo->tagCond.tbnameCond.cond = strdup(str);

  taosStringBuilderDestroy(&sb1);
  tfree(segments);
  return TSDB_CODE_SUCCESS;
}

static bool validateFilterExpr(SQueryInfo* pQueryInfo) {
  SArray* pColList = pQueryInfo->colList;
  
  size_t num = taosArrayGetSize(pColList);
  
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(pColList, i);

    for (int32_t j = 0; j < pCol->numOfFilters; ++j) {
      SColumnFilterInfo* pColFilter = &pCol->filterInfo[j];
      int32_t            lowerOptr = pColFilter->lowerRelOptr;
      int32_t            upperOptr = pColFilter->upperRelOptr;

      if ((lowerOptr == TSDB_RELATION_GREATER_EQUAL || lowerOptr == TSDB_RELATION_GREATER) &&
          (upperOptr == TSDB_RELATION_LESS_EQUAL || upperOptr == TSDB_RELATION_LESS)) {
        continue;
      }

      // there must be at least two range, not support yet.
      if (lowerOptr * upperOptr != TSDB_RELATION_INVALID) {
        return false;
      }
    }
  }

  return true;
}

static int32_t getTimeRangeFromExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSQLExpr* pExpr) {
  const char* msg0 = "invalid timestamp";
  const char* msg1 = "only one time stamp window allowed";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isExprDirectParentOfLeaftNode(pExpr)) {
    if (pExpr->nSQLOptr == TK_OR) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    getTimeRangeFromExpr(pCmd, pQueryInfo, pExpr->pLeft);

    return getTimeRangeFromExpr(pCmd, pQueryInfo, pExpr->pRight);
  } else {
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(pCmd, &pExpr->pLeft->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
    STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
    
    tSQLExpr* pRight = pExpr->pRight;

    STimeWindow win = {.skey = INT64_MIN, .ekey = INT64_MAX};
    if (getTimeRange(&win, pRight, pExpr->nSQLOptr, tinfo.precision) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }

    // update the timestamp query range
    if (pQueryInfo->window.skey < win.skey) {
      pQueryInfo->window.skey = win.skey;
    }

    if (pQueryInfo->window.ekey > win.ekey) {
      pQueryInfo->window.ekey = win.ekey;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateJoinExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SCondExpr* pCondExpr) {
  const char* msg1 = "super table join requires tags column";
  const char* msg2 = "timestamp join condition missing";
  const char* msg3 = "condition missing for join query";

  if (!QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
    if (pQueryInfo->numOfTables == 1) {
      return TSDB_CODE_SUCCESS;
    } else {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {  // for stable join, tag columns
                                                   // must be present for join
    if (pCondExpr->pJoinExpr == NULL) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  if (!pCondExpr->tsJoin) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

static void cleanQueryExpr(SCondExpr* pCondExpr) {
  if (pCondExpr->pTableCond) {
    tSQLExprDestroy(pCondExpr->pTableCond);
  }

  if (pCondExpr->pTagCond) {
    tSQLExprDestroy(pCondExpr->pTagCond);
  }

  if (pCondExpr->pColumnCond) {
    tSQLExprDestroy(pCondExpr->pColumnCond);
  }

  if (pCondExpr->pTimewindow) {
    tSQLExprDestroy(pCondExpr->pTimewindow);
  }

  if (pCondExpr->pJoinExpr) {
    tSQLExprDestroy(pCondExpr->pJoinExpr);
  }
}

static void doAddJoinTagsColumnsIntoTagList(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SCondExpr* pCondExpr) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (QUERY_IS_JOIN_QUERY(pQueryInfo->type) && UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    SColumnIndex index = {0};

    if (getColumnIndexByName(pCmd, &pCondExpr->pJoinExpr->pLeft->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      tscError("%p: invalid column name (left)", pQueryInfo);
    }
    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
  
    index.columnIndex = index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
    tscColumnListInsert(pTableMetaInfo->tagColList, &index);
  
    if (getColumnIndexByName(pCmd, &pCondExpr->pJoinExpr->pRight->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      tscError("%p: invalid column name (right)", pQueryInfo);
    }
    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
  
    index.columnIndex = index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
    tscColumnListInsert(pTableMetaInfo->tagColList, &index);
  }
}

static int32_t getTagQueryCondExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SCondExpr* pCondExpr, tSQLExpr** pExpr) {
  int32_t ret = TSDB_CODE_SUCCESS;

  if (pCondExpr->pTagCond == NULL) {
    return ret;
  }
  
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    tSQLExpr* p1 = extractExprForSTable(pCmd, pExpr, pQueryInfo, i);
    if (p1 == NULL) {  // no query condition on this table
      continue;
    }

    tExprNode* p = NULL;
  
    SArray* colList = taosArrayInit(10, sizeof(SColIndex));
    ret = exprTreeFromSqlExpr(pCmd, &p, p1, NULL, pQueryInfo, colList);
    SBufferWriter bw = tbufInitWriter(NULL, false);

    TRY(0) {
      exprTreeToBinary(&bw, p);
    } CATCH(code) {
      tbufCloseWriter(&bw);
      UNUSED(code);
      // TODO: more error handling
    } END_TRY
    
    // add to source column list
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    int64_t uid = pTableMetaInfo->pTableMeta->uid;
    int32_t numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
    
    size_t num = taosArrayGetSize(colList);
    for(int32_t j = 0; j < num; ++j) {
      SColIndex* pIndex = taosArrayGet(colList, j);
      SColumnIndex index = {.tableIndex = i, .columnIndex = pIndex->colIndex - numOfCols};
      tscColumnListInsert(pTableMetaInfo->tagColList, &index);
    }
    
    tsSetSTableQueryCond(&pQueryInfo->tagCond, uid, &bw);
    doCompactQueryExpr(pExpr);
    
    tSQLExprDestroy(p1);
    tExprTreeDestroy(&p, NULL);
    
    taosArrayDestroy(colList);
  }

  pCondExpr->pTagCond = NULL;
  return ret;
}
int32_t parseWhereClause(SQueryInfo* pQueryInfo, tSQLExpr** pExpr, SSqlObj* pSql) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* msg1 = "invalid expression";
  const char* msg2 = "invalid filter expression";

  int32_t ret = TSDB_CODE_SUCCESS;
  pQueryInfo->window = TSWINDOW_INITIALIZER;

  // tags query condition may be larger than 512bytes, therefore, we need to prepare enough large space
  SStringBuilder sb; memset(&sb, 0, sizeof(sb));
  SCondExpr      condExpr = {0};

  if ((*pExpr)->pLeft == NULL || (*pExpr)->pRight == NULL) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(&pSql->cmd), msg1);
  }

  int32_t type = 0;
  if ((ret = getQueryCondExpr(&pSql->cmd, pQueryInfo, pExpr, &condExpr, &type, (*pExpr)->nSQLOptr)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  doCompactQueryExpr(pExpr);

  // after expression compact, the expression tree is only include tag query condition
  condExpr.pTagCond = (*pExpr);

  // 1. check if it is a join query
  if ((ret = validateJoinExpr(&pSql->cmd, pQueryInfo, &condExpr)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 2. get the query time range
  if ((ret = getTimeRangeFromExpr(&pSql->cmd, pQueryInfo, condExpr.pTimewindow)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 3. get the tag query condition
  if ((ret = getTagQueryCondExpr(&pSql->cmd, pQueryInfo, &condExpr, pExpr)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 4. get the table name query condition
  if ((ret = getTablenameCond(&pSql->cmd, pQueryInfo, condExpr.pTableCond, &sb)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 5. other column query condition
  if ((ret = getColumnQueryCondInfo(&pSql->cmd, pQueryInfo, condExpr.pColumnCond, TK_AND)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 6. join condition
  if ((ret = getJoinCondInfo(&pSql->cmd, pQueryInfo, condExpr.pJoinExpr)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 7. query condition for table name
  pQueryInfo->tagCond.relType = (condExpr.relType == TK_AND) ? TSDB_RELATION_AND : TSDB_RELATION_OR;

  ret = setTableCondForSTableQuery(&pSql->cmd, pQueryInfo, getAccountId(pSql), condExpr.pTableCond, condExpr.tableCondIndex, &sb);
  taosStringBuilderDestroy(&sb);

  if (!validateFilterExpr(pQueryInfo)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(&pSql->cmd), msg2);
  }

  doAddJoinTagsColumnsIntoTagList(&pSql->cmd, pQueryInfo, &condExpr);

  cleanQueryExpr(&condExpr);
  return ret;
}

int32_t getTimeRange(STimeWindow* win, tSQLExpr* pRight, int32_t optr, int16_t timePrecision) {
  // this is join condition, do nothing
  if (pRight->nSQLOptr == TK_ID) {
    return TSDB_CODE_SUCCESS;
  }

  /*
   * filter primary ts filter expression like:
   * where ts in ('2015-12-12 4:8:12')
   */
  if (pRight->nSQLOptr == TK_SET || optr == TK_IN) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  int64_t val = 0;
  bool    parsed = false;
  if (pRight->val.nType == TSDB_DATA_TYPE_BINARY) {
    pRight->val.nLen = strdequote(pRight->val.pz);

    char* seg = strnchr(pRight->val.pz, '-', pRight->val.nLen, false);
    if (seg != NULL) {
      if (taosParseTime(pRight->val.pz, &val, pRight->val.nLen, TSDB_TIME_PRECISION_MICRO, tsDaylight) == TSDB_CODE_SUCCESS) {
        parsed = true;
      } else {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    } else {
      SSQLToken token = {.z = pRight->val.pz, .n = pRight->val.nLen, .type = TK_ID};
      int32_t   len = tSQLGetToken(pRight->val.pz, &token.type);

      if ((token.type != TK_INTEGER && token.type != TK_FLOAT) || len != pRight->val.nLen) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    }
  } else if (pRight->nSQLOptr == TK_INTEGER && timePrecision == TSDB_TIME_PRECISION_MILLI) {
    /*
     * if the pRight->nSQLOptr == TK_INTEGER/TK_FLOAT, the value is adaptive, we
     * need the time precision in metermeta to transfer the value in MICROSECOND
     *
     * Additional check to avoid data overflow
     */
    if (pRight->val.i64Key <= INT64_MAX / 1000) {
      pRight->val.i64Key *= 1000;
    }
  } else if (pRight->nSQLOptr == TK_FLOAT && timePrecision == TSDB_TIME_PRECISION_MILLI) {
    pRight->val.dKey *= 1000;
  }

  if (!parsed) {
    /*
     * failed to parse timestamp in regular formation, try next
     * it may be a epoch time in string format
     */
    tVariantDump(&pRight->val, (char*)&val, TSDB_DATA_TYPE_BIGINT, true);

    /*
     * transfer it into MICROSECOND format if it is a string, since for
     * TK_INTEGER/TK_FLOAT the value has been transferred
     *
     * additional check to avoid data overflow
     */
    if (pRight->nSQLOptr == TK_STRING && timePrecision == TSDB_TIME_PRECISION_MILLI) {
      if (val <= INT64_MAX / 1000) {
        val *= 1000;
      }
    }
  }

  int32_t delta = 1;
  /* for millisecond, delta is 1ms=1000us */
  if (timePrecision == TSDB_TIME_PRECISION_MILLI) {
    delta *= 1000;
  }

  if (optr == TK_LE) {
    win->ekey = val;
  } else if (optr == TK_LT) {
    win->ekey = val - delta;
  } else if (optr == TK_GT) {
    win->skey = val + delta;
  } else if (optr == TK_GE) {
    win->skey = val;
  } else if (optr == TK_EQ) {
    win->ekey = win->skey = val;
  }
  return TSDB_CODE_SUCCESS;
}

// todo error !!!!
int32_t tsRewriteFieldNameIfNecessary(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char rep[] = {'(', ')', '*', ',', '.', '/', '\\', '+', '-', '%', ' '};

  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    char* fieldName = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i)->name;
    for (int32_t j = 0; j < (TSDB_COL_NAME_LEN - 1) && fieldName[j] != 0; ++j) {
      for (int32_t k = 0; k < tListLen(rep); ++k) {
        if (fieldName[j] == rep[k]) {
          fieldName[j] = '_';
          break;
        }
      }
    }

    fieldName[TSDB_COL_NAME_LEN - 1] = 0;
  }

  // the column name may be identical, here check again
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    char* fieldName = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i)->name;
    for (int32_t j = i + 1; j < pQueryInfo->fieldsInfo.numOfOutput; ++j) {
      if (strncasecmp(fieldName, tscFieldInfoGetField(&pQueryInfo->fieldsInfo, j)->name, (TSDB_COL_NAME_LEN - 1)) == 0) {
        const char* msg = "duplicated column name in new table";
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t parseFillClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySQL) {
  tVariantList*     pFillToken = pQuerySQL->fillType;
  tVariantListItem* pItem = &pFillToken->a[0];

  const int32_t START_INTERPO_COL_IDX = 1;

  const char* msg = "illegal value or data overflow";
  const char* msg1 = "value is expected";
  const char* msg2 = "invalid fill option";

  if (pItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }
  
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  if (pQueryInfo->fillVal == NULL) {
    pQueryInfo->fillVal = calloc(size, sizeof(int64_t));
    if (pQueryInfo->fillVal == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  if (strncasecmp(pItem->pVar.pz, "none", 4) == 0 && pItem->pVar.nLen == 4) {
    pQueryInfo->fillType = TSDB_FILL_NONE;
  } else if (strncasecmp(pItem->pVar.pz, "null", 4) == 0 && pItem->pVar.nLen == 4) {
    pQueryInfo->fillType = TSDB_FILL_NULL;
    for (int32_t i = START_INTERPO_COL_IDX; i < size; ++i) {
      TAOS_FIELD* pFields = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
      if (pFields->type == TSDB_DATA_TYPE_BINARY || pFields->type == TSDB_DATA_TYPE_NCHAR) {
        setVardataNull((char*) &pQueryInfo->fillVal[i], pFields->type);
      } else {
        setNull((char*)&pQueryInfo->fillVal[i], pFields->type, pFields->bytes);
      };
    }
  } else if (strncasecmp(pItem->pVar.pz, "prev", 4) == 0 && pItem->pVar.nLen == 4) {
    pQueryInfo->fillType = TSDB_FILL_PREV;
  } else if (strncasecmp(pItem->pVar.pz, "linear", 6) == 0 && pItem->pVar.nLen == 6) {
    pQueryInfo->fillType = TSDB_FILL_LINEAR;
  } else if (strncasecmp(pItem->pVar.pz, "value", 5) == 0 && pItem->pVar.nLen == 5) {
    pQueryInfo->fillType = TSDB_FILL_SET_VALUE;

    if (pFillToken->nExpr == 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    int32_t startPos = 1;
    int32_t numOfFillVal = pFillToken->nExpr - 1;

    /* for point interpolation query, we do not have the timestamp column */
    if (tscIsPointInterpQuery(pQueryInfo)) {
      startPos = 0;

      if (numOfFillVal > size) {
        numOfFillVal = size;
      }
    } else {
      numOfFillVal = (pFillToken->nExpr > size) ? size : pFillToken->nExpr;
    }

    int32_t j = 1;

    for (int32_t i = startPos; i < numOfFillVal; ++i, ++j) {
      TAOS_FIELD* pFields = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);

      if (pFields->type == TSDB_DATA_TYPE_BINARY || pFields->type == TSDB_DATA_TYPE_NCHAR) {
        setVardataNull((char*) &pQueryInfo->fillVal[i], pFields->type);
        continue;
      }

      int32_t ret = tVariantDump(&pFillToken->a[j].pVar, (char*)&pQueryInfo->fillVal[i], pFields->type, true);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }
    }
    
    if ((pFillToken->nExpr < size) || ((pFillToken->nExpr - 1 < size) && (tscIsPointInterpQuery(pQueryInfo)))) {
      tVariantListItem* lastItem = &pFillToken->a[pFillToken->nExpr - 1];

      for (int32_t i = numOfFillVal; i < size; ++i) {
        TAOS_FIELD* pFields = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);

        if (pFields->type == TSDB_DATA_TYPE_BINARY || pFields->type == TSDB_DATA_TYPE_NCHAR) {
          setVardataNull((char*) &pQueryInfo->fillVal[i], pFields->type);
        } else {
          tVariantDump(&lastItem->pVar, (char*)&pQueryInfo->fillVal[i], pFields->type, true);
        }
      }
    }
  } else {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

static void setDefaultOrderInfo(SQueryInfo* pQueryInfo) {
  /* set default timestamp order information for all queries */
  pQueryInfo->order.order = TSDB_ORDER_ASC;
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  if (isTopBottomQuery(pQueryInfo)) {
    pQueryInfo->order.order = TSDB_ORDER_ASC;
    pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
  } else {
    pQueryInfo->order.orderColId = -1;
  }

  /* for super table query, set default ascending order for group output */
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    pQueryInfo->groupbyExpr.orderType = TSDB_ORDER_ASC;
  }
}

int32_t parseOrderbyClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySql, SSchema* pSchema) {
  const char* msg0 = "only support order by primary timestamp";
  const char* msg1 = "invalid column name";
  const char* msg2 = "only support order by primary timestamp and queried column";
  const char* msg3 = "only support order by primary timestamp and first tag in groupby clause";

  setDefaultOrderInfo(pQueryInfo);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  if (pQuerySql->pSortOrder == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tVariantList* pSortorder = pQuerySql->pSortOrder;

  /*
   * for table query, there is only one or none order option is allowed, which is the
   * ts or values(top/bottom) order is supported.
   *
   * for super table query, the order option must be less than 3.
   */
  if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
    if (pSortorder->nExpr > 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }
  } else {
    if (pSortorder->nExpr > 2) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }
  }

  // handle the first part of order by
  tVariant* pVar = &pSortorder->a[0].pVar;

  // e.g., order by 1 asc, return directly with out further check.
  if (pVar->nType >= TSDB_DATA_TYPE_TINYINT && pVar->nType <= TSDB_DATA_TYPE_BIGINT) {
    return TSDB_CODE_SUCCESS;
  }

  SSQLToken    columnName = {pVar->nLen, pVar->nType, pVar->pz};
  SColumnIndex index = {0};

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {  // super table query
    if (getColumnIndexByName(pCmd, &columnName, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    bool orderByTags = false;
    bool orderByTS = false;

    if (index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
      int32_t relTagIndex = index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
      
      // it is a tag column
      if (pQueryInfo->groupbyExpr.columnInfo == NULL) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
      SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, 0);
      if (relTagIndex == pColIndex->colIndex) {
        orderByTags = true;
      }
    } else if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      orderByTags = true;
    }

    if (PRIMARYKEY_TIMESTAMP_COL_INDEX == index.columnIndex) {
      orderByTS = true;
    }

    if (!(orderByTags || orderByTS) && !isTopBottomQuery(pQueryInfo)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    } else {
      assert(!(orderByTags && orderByTS));
    }

    if (pSortorder->nExpr == 1) {
      if (orderByTags) {
        pQueryInfo->groupbyExpr.orderIndex = index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
        pQueryInfo->groupbyExpr.orderType = pQuerySql->pSortOrder->a[0].sortOrder;
      } else if (isTopBottomQuery(pQueryInfo)) {
        /* order of top/bottom query in interval is not valid  */
        SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, 0);
        assert(pExpr->functionId == TSDB_FUNC_TS);

        pExpr = tscSqlExprGet(pQueryInfo, 1);
        if (pExpr->colInfo.colIndex != index.columnIndex && index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
        }

        pQueryInfo->order.order = pQuerySql->pSortOrder->a[0].sortOrder;
        pQueryInfo->order.orderColId = pSchema[index.columnIndex].colId;
        return TSDB_CODE_SUCCESS;
      } else {
        pQueryInfo->order.order = pSortorder->a[0].sortOrder;
        pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }
    }

    if (pSortorder->nExpr == 2) {
      if (orderByTags) {
        pQueryInfo->groupbyExpr.orderIndex = index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
        pQueryInfo->groupbyExpr.orderType = pQuerySql->pSortOrder->a[0].sortOrder;
      } else {
        pQueryInfo->order.order = pSortorder->a[0].sortOrder;
        pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }

      tVariant* pVar2 = &pSortorder->a[1].pVar;
      SSQLToken cname = {pVar2->nLen, pVar2->nType, pVar2->pz};
      if (getColumnIndexByName(pCmd, &cname, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      } else {
        pQueryInfo->order.order = pSortorder->a[1].sortOrder;
        pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }
    }

  } else {  // meter query
    if (getColumnIndexByName(pCmd, &columnName, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    if (index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX && !isTopBottomQuery(pQueryInfo)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    if (isTopBottomQuery(pQueryInfo)) {
      /* order of top/bottom query in interval is not valid  */
      SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, 0);
      assert(pExpr->functionId == TSDB_FUNC_TS);

      pExpr = tscSqlExprGet(pQueryInfo, 1);
      if (pExpr->colInfo.colIndex != index.columnIndex && index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      pQueryInfo->order.order = pQuerySql->pSortOrder->a[0].sortOrder;
      pQueryInfo->order.orderColId = pSchema[index.columnIndex].colId;
      return TSDB_CODE_SUCCESS;
    }

    pQueryInfo->order.order = pQuerySql->pSortOrder->a[0].sortOrder;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setAlterTableInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  const int32_t DEFAULT_TABLE_INDEX = 0;

  const char* msg1 = "invalid table name";
  const char* msg2 = "table name too long";
  const char* msg3 = "manipulation of tag available for super table";
  const char* msg4 = "set tag value only available for table";
  const char* msg5 = "only support add one tag";
  const char* msg6 = "column can only be modified by super table";
  
  const char* msg7 = "no tags can be dropped";
  const char* msg8 = "only support one tag";
  const char* msg9 = "tag name too long";
  
  const char* msg10 = "invalid tag name";
  const char* msg11 = "primary tag cannot be dropped";
  const char* msg12 = "update normal column not supported";
  const char* msg13 = "invalid tag value";
  const char* msg14 = "tag value too long";
  
  const char* msg15 = "no columns can be dropped";
  const char* msg16 = "only support one column";
  const char* msg17 = "invalid column name";
  const char* msg18 = "primary timestamp column cannot be dropped";
  
  SSqlCmd*        pCmd = &pSql->cmd;
  SAlterTableSQL* pAlterSQL = pInfo->pAlterInfo;
  SQueryInfo*     pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, DEFAULT_TABLE_INDEX);

  if (tscValidateName(&(pAlterSQL->name)) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (tscSetTableFullName(pTableMetaInfo, &(pAlterSQL->name), pSql) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  int32_t ret = tscGetTableMeta(pSql, pTableMetaInfo);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;

  if (pAlterSQL->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN || pAlterSQL->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN ||
      pAlterSQL->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }
  } else if ((pAlterSQL->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) && (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo))) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
  } else if ((pAlterSQL->type == TSDB_ALTER_TABLE_ADD_COLUMN || pAlterSQL->type == TSDB_ALTER_TABLE_DROP_COLUMN) &&
             UTIL_TABLE_IS_CHILD_TABLE(pTableMetaInfo)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
  }

  if (pAlterSQL->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
    tFieldList* pFieldList = pAlterSQL->pAddColumns;
    if (pFieldList->nField > 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
    }

    if (!validateOneTags(pCmd, &pFieldList->p[0])) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &pFieldList->p[0]);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
    if (tscGetNumOfTags(pTableMeta) == 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
    }

    // numOfTags == 1
    if (pAlterSQL->varList->nExpr > 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
    }

    tVariantListItem* pItem = &pAlterSQL->varList->a[0];
    if (pItem->pVar.nLen >= TSDB_COL_NAME_LEN) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg9);
    }

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    SSQLToken    name = {.z = pItem->pVar.pz, .n = pItem->pVar.nLen, .type = TK_STRING};

    if (getColumnIndexByName(pCmd, &name, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
    if (index.columnIndex < numOfCols) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg10);
    } else if (index.columnIndex == numOfCols) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg11);
    }

    char name1[128] = {0};
    strncpy(name1, pItem->pVar.pz, pItem->pVar.nLen);
  
    TAOS_FIELD f = tscCreateField(TSDB_DATA_TYPE_INT, name1, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    tVariantList* pVarList = pAlterSQL->varList;
    if (pVarList->nExpr > 2) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    tVariantListItem* pSrcItem = &pAlterSQL->varList->a[0];
    tVariantListItem* pDstItem = &pAlterSQL->varList->a[1];

    if (pSrcItem->pVar.nLen >= TSDB_COL_NAME_LEN || pDstItem->pVar.nLen >= TSDB_COL_NAME_LEN) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg9);
    }

    if (pSrcItem->pVar.nType != TSDB_DATA_TYPE_BINARY || pDstItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg10);
    }

    SColumnIndex srcIndex = COLUMN_INDEX_INITIALIZER;
    SColumnIndex destIndex = COLUMN_INDEX_INITIALIZER;

    SSQLToken srcToken = {.z = pSrcItem->pVar.pz, .n = pSrcItem->pVar.nLen, .type = TK_STRING};
    if (getColumnIndexByName(pCmd, &srcToken, pQueryInfo, &srcIndex) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    SSQLToken destToken = {.z = pDstItem->pVar.pz, .n = pDstItem->pVar.nLen, .type = TK_STRING};
    if (getColumnIndexByName(pCmd, &destToken, pQueryInfo, &destIndex) == TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    char name[TSDB_COL_NAME_LEN] = {0};
    strncpy(name, pVarList->a[0].pVar.pz, pVarList->a[0].pVar.nLen);
    TAOS_FIELD f = tscCreateField(TSDB_DATA_TYPE_INT, name, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);

    memset(name, 0, tListLen(name));
    strncpy(name, pVarList->a[1].pVar.pz, pVarList->a[1].pVar.nLen);
    f = tscCreateField(TSDB_DATA_TYPE_INT, name, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    // Note: update can only be applied to table not super table.
    // the following is used to handle tags value for table created according to super table
    pCmd->command = TSDB_SQL_UPDATE_TAGS_VAL;
    
    tVariantList* pVarList = pAlterSQL->varList;
    tVariant*     pTagName = &pVarList->a[0].pVar;
    int16_t       numOfTags = tscGetNumOfTags(pTableMeta);

    SColumnIndex columnIndex = COLUMN_INDEX_INITIALIZER;
    SSQLToken    name = {.type = TK_STRING, .z = pTagName->pz, .n = pTagName->nLen};
    if (getColumnIndexByName(pCmd, &name, pQueryInfo, &columnIndex) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    if (columnIndex.columnIndex < tscGetNumOfColumns(pTableMeta)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg12);
    }

    SSchema* pTagsSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, columnIndex.columnIndex);
    if (tVariantDump(&pVarList->a[1].pVar, pAlterSQL->tagData.data, pTagsSchema->type, true) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg13);
    }
    
    pAlterSQL->tagData.dataLen = pTagsSchema->bytes;

    // validate the length of binary
    if ((pTagsSchema->type == TSDB_DATA_TYPE_BINARY || pTagsSchema->type == TSDB_DATA_TYPE_NCHAR) &&
        (pVarList->a[1].pVar.nLen + VARSTR_HEADER_SIZE) > pTagsSchema->bytes) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg14);
    }

    int32_t schemaLen = sizeof(STColumn) * numOfTags;
    int32_t size = sizeof(SUpdateTableTagValMsg) + pTagsSchema->bytes + schemaLen + TSDB_EXTRA_PAYLOAD_SIZE;

    if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
      tscError("%p failed to malloc for alter table msg", pSql);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    SUpdateTableTagValMsg* pUpdateMsg = (SUpdateTableTagValMsg*) pCmd->payload;
    pUpdateMsg->head.vgId = htonl(pTableMeta->vgroupInfo.vgId);
    pUpdateMsg->tid       = htonl(pTableMeta->sid);
    pUpdateMsg->uid       = htobe64(pTableMeta->uid);
    pUpdateMsg->colId     = htons(pTagsSchema->colId);
    pUpdateMsg->type      = pTagsSchema->type;
    pUpdateMsg->bytes     = htons(pTagsSchema->bytes);
    pUpdateMsg->tversion  = htons(pTableMeta->tversion);
    pUpdateMsg->numOfTags = htons(numOfTags);
    pUpdateMsg->schemaLen = htonl(schemaLen);

    // the schema is located after the msg body, then followed by true tag value
    char* d = pUpdateMsg->data;
    SSchema* pTagCols = tscGetTableTagSchema(pTableMeta);
    for (int i = 0; i < numOfTags; ++i) {
      STColumn* pCol = (STColumn*) d;
      pCol->colId = htons(pTagCols[i].colId);
      pCol->bytes = htons(pTagCols[i].bytes);
      pCol->type  = pTagCols[i].type;
      pCol->offset = 0;

      d += sizeof(STColumn);
    }

    // copy the tag value to msg body
    tVariantDump(&pVarList->a[1].pVar, pUpdateMsg->data + schemaLen, pTagsSchema->type, true);
    
    int32_t len = 0;
    if (pTagsSchema->type != TSDB_DATA_TYPE_BINARY && pTagsSchema->type != TSDB_DATA_TYPE_NCHAR) {
      len = tDataTypeDesc[pTagsSchema->type].nSize;
    } else {
      len = varDataTLen(pUpdateMsg->data + schemaLen);
    }
    
    pUpdateMsg->tagValLen = htonl(len);  // length may be changed after dump data
    
    int32_t total = sizeof(SUpdateTableTagValMsg) + len + schemaLen;
    pUpdateMsg->head.contLen = htonl(total);
    
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
    tFieldList* pFieldList = pAlterSQL->pAddColumns;
    if (pFieldList->nField > 1) {
      const char* msg = "only support add one column";
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
    }

    if (!validateOneColumn(pCmd, &pFieldList->p[0])) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &pFieldList->p[0]);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    if (tscGetNumOfColumns(pTableMeta) == TSDB_MIN_COLUMNS) {  //
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg15);
    }

    if (pAlterSQL->varList->nExpr > 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg16);
    }

    tVariantListItem* pItem = &pAlterSQL->varList->a[0];

    SColumnIndex columnIndex = COLUMN_INDEX_INITIALIZER;
    SSQLToken    name = {.type = TK_STRING, .z = pItem->pVar.pz, .n = pItem->pVar.nLen};
    if (getColumnIndexByName(pCmd, &name, pQueryInfo, &columnIndex) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg17);
    }

    if (columnIndex.columnIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg18);
    }

    char name1[TSDB_COL_NAME_LEN] = {0};
    tstrncpy(name1, pItem->pVar.pz, sizeof(name1));
    TAOS_FIELD f = tscCreateField(TSDB_DATA_TYPE_INT, name1, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateSqlFunctionInStreamSql(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg0 = "sample interval can not be less than 10ms.";
  const char* msg1 = "functions not allowed in select clause";

  if (pQueryInfo->intervalTime != 0 && pQueryInfo->intervalTime < 10) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }
  
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  for (int32_t i = 0; i < size; ++i) {
    int32_t functId = tscSqlExprGet(pQueryInfo, i)->functionId;
    if (!IS_STREAM_QUERY_VALID(aAggs[functId].nStatus)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateFunctionsInIntervalOrGroupbyQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  bool        isProjectionFunction = false;
  const char* msg1 = "column projection is not compatible with interval";

  // multi-output set/ todo refactor
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  
  for (int32_t k = 0; k < size; ++k) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, k);

    // projection query on primary timestamp, the selectivity function needs to be present.
    if (pExpr->functionId == TSDB_FUNC_PRJ && pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      bool hasSelectivity = false;
      for (int32_t j = 0; j < size; ++j) {
        SSqlExpr* pEx = tscSqlExprGet(pQueryInfo, j);
        if ((aAggs[pEx->functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) == TSDB_FUNCSTATE_SELECTIVITY) {
          hasSelectivity = true;
          break;
        }
      }

      if (hasSelectivity) {
        continue;
      }
    }

    if (pExpr->functionId == TSDB_FUNC_PRJ || pExpr->functionId == TSDB_FUNC_DIFF ||
        pExpr->functionId == TSDB_FUNC_ARITHM) {
      isProjectionFunction = true;
    }
  }

  if (isProjectionFunction) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  return isProjectionFunction == true ? TSDB_CODE_TSC_INVALID_SQL : TSDB_CODE_SUCCESS;
}

typedef struct SDNodeDynConfOption {
  char*   name;  // command name
  int32_t len;   // name string length
} SDNodeDynConfOption;


int32_t validateEp(char* ep) {  
  char buf[TSDB_EP_LEN + 1] = {0};
  tstrncpy(buf, ep, TSDB_EP_LEN);

  char* pos = strchr(buf, ':');
  if (NULL == pos) {
    int32_t val = strtol(ep, NULL, 10);
    if (val <= 0 || val > 65536) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  } else {
    uint16_t port = atoi(pos + 1);
    if (0 == port) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateDNodeConfig(tDCLSQL* pOptions) {
  if (pOptions->nTokens < 2 || pOptions->nTokens > 3) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  const int tokenLogEnd = 2;
  const int tokenBalance = 2;
  const int tokenMonitor = 3;
  const int tokenDebugFlag = 4;
  const int tokenDebugFlagEnd = 20;
  const SDNodeDynConfOption cfgOptions[] = {
      {"resetLog", 8},    {"resetQueryCache", 15},  {"balance", 7},     {"monitor", 7},
      {"debugFlag", 9},   {"monitorDebugFlag", 16}, {"vDebugFlag", 10}, {"mDebugFlag", 10},
      {"cDebugFlag", 10}, {"httpDebugFlag", 13},    {"qDebugflag", 10}, {"sdbDebugFlag", 12},
      {"uDebugFlag", 10}, {"tsdbDebugFlag", 13},    {"sDebugflag", 10}, {"rpcDebugFlag", 12},
      {"dDebugFlag", 10}, {"mqttDebugFlag", 13},    {"wDebugFlag", 10}, {"tmrDebugFlag", 12},
  };

  SSQLToken* pOptionToken = &pOptions->a[1];

  if (pOptions->nTokens == 2) {
    // reset log and reset query cache does not need value
    for (int32_t i = 0; i < tokenLogEnd; ++i) {
      const SDNodeDynConfOption* pOption = &cfgOptions[i];
      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  } else if ((strncasecmp(cfgOptions[tokenBalance].name, pOptionToken->z, pOptionToken->n) == 0) &&
             (cfgOptions[tokenBalance].len == pOptionToken->n)) {
    SSQLToken* pValToken = &pOptions->a[2];
    int32_t vnodeId = 0;
    int32_t dnodeId = 0;
    strdequote(pValToken->z);
    bool parseOk = taosCheckBalanceCfgOptions(pValToken->z, &vnodeId, &dnodeId);
    if (!parseOk) {
      return TSDB_CODE_TSC_INVALID_SQL;  // options value is invalid
    }
    return TSDB_CODE_SUCCESS;
  } else if ((strncasecmp(cfgOptions[tokenMonitor].name, pOptionToken->z, pOptionToken->n) == 0) &&
             (cfgOptions[tokenMonitor].len == pOptionToken->n)) {
    SSQLToken* pValToken = &pOptions->a[2];
    int32_t    val = strtol(pValToken->z, NULL, 10);
    if (val != 0 && val != 1) {
      return TSDB_CODE_TSC_INVALID_SQL;  // options value is invalid
    }
    return TSDB_CODE_SUCCESS;
  } else {
    SSQLToken* pValToken = &pOptions->a[2];

    int32_t val = strtol(pValToken->z, NULL, 10);
    if (val < 0 || val > 256) {
      /* options value is out of valid range */
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    for (int32_t i = tokenDebugFlag; i < tokenDebugFlagEnd; ++i) {
      const SDNodeDynConfOption* pOption = &cfgOptions[i];

      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        /* options is valid */
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  return TSDB_CODE_TSC_INVALID_SQL;
}

int32_t validateLocalConfig(tDCLSQL* pOptions) {
  if (pOptions->nTokens < 1 || pOptions->nTokens > 2) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  SDNodeDynConfOption LOCAL_DYNAMIC_CFG_OPTIONS[6] = {{"resetLog", 8},    {"rpcDebugFlag", 12}, {"tmrDebugFlag", 12},
                                                      {"cDebugFlag", 10}, {"uDebugFlag", 10},   {"debugFlag", 9}};

  SSQLToken* pOptionToken = &pOptions->a[0];

  if (pOptions->nTokens == 1) {
    // reset log does not need value
    for (int32_t i = 0; i < 1; ++i) {
      SDNodeDynConfOption* pOption = &LOCAL_DYNAMIC_CFG_OPTIONS[i];
      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  } else {
    SSQLToken* pValToken = &pOptions->a[1];

    int32_t val = strtol(pValToken->z, NULL, 10);
    if (val < 131 || val > 199) {
      // options value is out of valid range
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    for (int32_t i = 1; i < tListLen(LOCAL_DYNAMIC_CFG_OPTIONS); ++i) {
      SDNodeDynConfOption* pOption = &LOCAL_DYNAMIC_CFG_OPTIONS[i];
      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        // options is valid
        return TSDB_CODE_SUCCESS;
      }
    }
  }
  return TSDB_CODE_TSC_INVALID_SQL;
}

int32_t validateColumnName(char* name) {
  bool ret = isKeyWord(name, strlen(name));
  if (ret) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  SSQLToken token = {.z = name};
  token.n = tSQLGetToken(name, &token.type);

  if (token.type != TK_STRING && token.type != TK_ID) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (token.type == TK_STRING) {
    strdequote(token.z);
    token.n = strtrim(token.z);

    int32_t k = tSQLGetToken(token.z, &token.type);
    if (k != token.n) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    return validateColumnName(token.z);
  } else {
    if (isNumber(&token)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

bool hasTimestampForPointInterpQuery(SQueryInfo* pQueryInfo) {
  if (!tscIsPointInterpQuery(pQueryInfo)) {
    return true;
  }

  return (pQueryInfo->window.skey == pQueryInfo->window.ekey) && (pQueryInfo->window.skey != 0);
}

int32_t parseLimitClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t clauseIndex, SQuerySQL* pQuerySql, SSqlObj* pSql) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  const char* msg0 = "soffset/offset can not be less than 0";
  const char* msg1 = "slimit/soffset only available for STable query";
  const char* msg2 = "functions mixed up in table query";
  const char* msg3 = "slimit/soffset can not apply to projection query";

  // handle the limit offset value, validate the limit
  pQueryInfo->limit = pQuerySql->limit;
  pQueryInfo->clauseLimit = pQueryInfo->limit.limit;
  pQueryInfo->slimit = pQuerySql->slimit;
  
  tscDebug("%p limit:%" PRId64 ", offset:%" PRId64 " slimit:%" PRId64 ", soffset:%" PRId64, pSql, pQueryInfo->limit.limit,
      pQueryInfo->limit.offset, pQueryInfo->slimit.limit, pQueryInfo->slimit.offset);
  
  if (pQueryInfo->slimit.offset < 0 || pQueryInfo->limit.offset < 0) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }

  if (pQueryInfo->limit.limit == 0) {
    tscDebug("%p limit 0, no output result", pSql);
    pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    return TSDB_CODE_SUCCESS;
  }

  // todo refactor
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    if (!tscQueryTags(pQueryInfo)) {  // local handle the super table tag query
      if (tscIsProjectionQueryOnSTable(pQueryInfo, 0)) {
        if (pQueryInfo->slimit.limit > 0 || pQueryInfo->slimit.offset > 0) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        // for projection query on super table, all queries are subqueries
        if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) &&
            !TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_QUERY)) {
          pQueryInfo->type |= TSDB_QUERY_TYPE_SUBQUERY;
        }
      }
    }

    if (pQueryInfo->slimit.limit == 0) {
      tscDebug("%p slimit 0, no output result", pSql);
      pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      return TSDB_CODE_SUCCESS;
    }

    /*
     * Get the distribution of all tables among all available virtual nodes that are qualified for the query condition
     * and created according to this super table from management node.
     * And then launching multiple async-queries against all qualified virtual nodes, during the first-stage
     * query operation.
     */
    int32_t code = tscGetSTableVgroupInfo(pSql, clauseIndex);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // No tables included. No results generated. Query results are empty.
    if (pTableMetaInfo->vgroupList->numOfVgroups == 0) {
      tscDebug("%p no table in super table, no output result", pSql);
      pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      return TSDB_CODE_SUCCESS;
    }

    // keep original limitation value in globalLimit
    pQueryInfo->clauseLimit = pQueryInfo->limit.limit;
    pQueryInfo->prjOffset = pQueryInfo->limit.offset;

    if (tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
      /*
       * the limitation/offset value should be removed during retrieve data from virtual node,
       * since the global order are done in client side, so the limitation should also
       * be done at the client side.
       */
      if (pQueryInfo->limit.limit > 0) {
        pQueryInfo->limit.limit = -1;
      }

      pQueryInfo->limit.offset = 0;
    }
  } else {
    if (pQueryInfo->slimit.limit != -1 || pQueryInfo->slimit.offset != 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  
    size_t size = taosArrayGetSize(pQueryInfo->exprList);
    
    bool hasTags = false;
    bool hasOtherFunc = false;
    // filter the query functions operating on "tbname" column that are not supported by normal columns.
    for (int32_t i = 0; i < size; ++i) {
      SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
      if (TSDB_COL_IS_TAG(pExpr->colInfo.flag)) {
        hasTags = true;
      } else {
        hasOtherFunc = true;
      }
    }
    
    if (hasTags && hasOtherFunc) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setKeepOption(SSqlCmd* pCmd, SCMCreateDbMsg* pMsg, SCreateDBInfo* pCreateDb) {
  const char* msg = "invalid number of options";

  pMsg->daysToKeep = htonl(-1);
  pMsg->daysToKeep1 = htonl(-1);
  pMsg->daysToKeep2 = htonl(-1);

  tVariantList* pKeep = pCreateDb->keep;
  if (pKeep != NULL) {
    switch (pKeep->nExpr) {
      case 1:
        pMsg->daysToKeep = htonl(pKeep->a[0].pVar.i64Key);
        break;
      case 2: {
        pMsg->daysToKeep = htonl(pKeep->a[0].pVar.i64Key);
        pMsg->daysToKeep1 = htonl(pKeep->a[1].pVar.i64Key);
        break;
      }
      case 3: {
        pMsg->daysToKeep = htonl(pKeep->a[0].pVar.i64Key);
        pMsg->daysToKeep1 = htonl(pKeep->a[1].pVar.i64Key);
        pMsg->daysToKeep2 = htonl(pKeep->a[2].pVar.i64Key);
        break;
      }
      default: { return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg); }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setTimePrecision(SSqlCmd* pCmd, SCMCreateDbMsg* pMsg, SCreateDBInfo* pCreateDbInfo) {
  const char* msg = "invalid time precision";

  pMsg->precision = TSDB_TIME_PRECISION_MILLI;  // millisecond by default

  SSQLToken* pToken = &pCreateDbInfo->precision;
  if (pToken->n > 0) {
    pToken->n = strdequote(pToken->z);

    if (strncmp(pToken->z, TSDB_TIME_PRECISION_MILLI_STR, pToken->n) == 0 &&
        strlen(TSDB_TIME_PRECISION_MILLI_STR) == pToken->n) {
      // time precision for this db: million second
      pMsg->precision = TSDB_TIME_PRECISION_MILLI;
    } else if (strncmp(pToken->z, TSDB_TIME_PRECISION_MICRO_STR, pToken->n) == 0 &&
               strlen(TSDB_TIME_PRECISION_MICRO_STR) == pToken->n) {
      pMsg->precision = TSDB_TIME_PRECISION_MICRO;
    } else {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void setCreateDBOption(SCMCreateDbMsg* pMsg, SCreateDBInfo* pCreateDb) {
  pMsg->maxTables = htonl(pCreateDb->maxTablesPerVnode);
  pMsg->cacheBlockSize = htonl(pCreateDb->cacheBlockSize);
  pMsg->totalBlocks = htonl(pCreateDb->numOfBlocks);
  pMsg->daysPerFile = htonl(pCreateDb->daysPerFile);
  pMsg->commitTime = htonl(pCreateDb->commitTime);
  pMsg->minRowsPerFileBlock = htonl(pCreateDb->minRowsPerBlock);
  pMsg->maxRowsPerFileBlock = htonl(pCreateDb->maxRowsPerBlock);
  pMsg->fsyncPeriod = htonl(pCreateDb->fsyncPeriod);
  pMsg->compression = pCreateDb->compressionLevel;
  pMsg->walLevel = (char)pCreateDb->walLevel;
  pMsg->replications = pCreateDb->replica;
  pMsg->ignoreExist = pCreateDb->ignoreExists;
}

int32_t parseCreateDBOptions(SSqlCmd* pCmd, SCreateDBInfo* pCreateDbSql) {
  SCMCreateDbMsg* pMsg = (SCMCreateDbMsg*)(pCmd->payload);
  setCreateDBOption(pMsg, pCreateDbSql);

  if (setKeepOption(pCmd, pMsg, pCreateDbSql) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (setTimePrecision(pCmd, pMsg, pCreateDbSql) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (tscCheckCreateDbParams(pCmd, pMsg) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

//void tscAddTimestampColumn(SQueryInfo* pQueryInfo, int16_t functionId, int16_t tableIndex) {
//  // the first column not timestamp column, add it
//  SSqlExpr* pExpr = NULL;
//  if (tscSqlExprNumOfExprs(pQueryInfo) > 0) {
//    pExpr = tscSqlExprGet(pQueryInfo, 0);
//  }
//
//  if (pExpr == NULL || pExpr->colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX || pExpr->functionId != functionId) {
//    SColumnIndex index = {tableIndex, PRIMARYKEY_TIMESTAMP_COL_INDEX};
//
//    pExpr = tscSqlExprInsert(pQueryInfo, 0, functionId, &index, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE, TSDB_KEYSIZE, false);
//    pExpr->colInfo.flag = TSDB_COL_NORMAL;
//
//    // NOTE: tag column does not add to source column list
//    SColumnList ids = getColumnList(1, tableIndex, PRIMARYKEY_TIMESTAMP_COL_INDEX);
//
//    insertResultField(pQueryInfo, 0, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, "ts", pExpr);
//  }
//}

void addGroupInfoForSubquery(SSqlObj* pParentObj, SSqlObj* pSql, int32_t subClauseIndex, int32_t tableIndex) {
  SQueryInfo* pParentQueryInfo = tscGetQueryInfoDetail(&pParentObj->cmd, subClauseIndex);

  if (pParentQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, subClauseIndex);
    size_t size = taosArrayGetSize(pQueryInfo->exprList);
  
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, size - 1);

    if (pExpr->functionId != TSDB_FUNC_TAG) {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
      int16_t         columnInfo = tscGetJoinTagColIdByUid(&pQueryInfo->tagCond, pTableMetaInfo->pTableMeta->uid);
      SColumnIndex    index = {.tableIndex = 0, .columnIndex = columnInfo};
      SSchema*        pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);

      int16_t type = pSchema[index.columnIndex].type;
      int16_t bytes = pSchema[index.columnIndex].bytes;
      char*   name = pSchema[index.columnIndex].name;
  
      pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TAG, &index, type, bytes, bytes, true);
      pExpr->colInfo.flag = TSDB_COL_TAG;

      // NOTE: tag column does not add to source column list
      SColumnList ids = {0};
      insertResultField(pQueryInfo, size, &ids, bytes, type, name, pExpr);

      int32_t relIndex = index.columnIndex;

      pExpr->colInfo.colIndex = relIndex;
      SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, 0);
      pColIndex->colIndex = relIndex;

      index = (SColumnIndex) {.tableIndex = tableIndex, .columnIndex = relIndex};
      tscColumnListInsert(pTableMetaInfo->tagColList, &index);
    }
  }
}

// limit the output to be 1 for each state value
static void doLimitOutputNormalColOfGroupby(SSqlExpr* pExpr) {
  int32_t outputRow = 1;
  tVariantCreateFromBinary(&pExpr->param[0], (char*)&outputRow, sizeof(int32_t), TSDB_DATA_TYPE_INT);
  pExpr->numOfParams = 1;
}

void doAddGroupColumnForSubquery(SQueryInfo* pQueryInfo, int32_t tagIndex) {
  SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, tagIndex);
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  SSchema*     pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, pColIndex->colIndex);
  SColumnIndex colIndex = {.tableIndex = 0, .columnIndex = pColIndex->colIndex};

  tscAddSpecialColumnForSelect(pQueryInfo, size, TSDB_FUNC_PRJ, &colIndex, pSchema, TSDB_COL_NORMAL);

  SFieldSupInfo* pInfo = tscFieldInfoGetSupp(&pQueryInfo->fieldsInfo, size);
  doLimitOutputNormalColOfGroupby(pInfo->pSqlExpr);
  pInfo->visible = false;
}

static void doUpdateSqlFunctionForTagPrj(SQueryInfo* pQueryInfo) {
  int32_t tagLength = 0;
  
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_TAGPRJ || pExpr->functionId == TSDB_FUNC_TAG) {
      pExpr->functionId = TSDB_FUNC_TAG_DUMMY;
      tagLength += pExpr->resBytes;
    } else if (pExpr->functionId == TSDB_FUNC_PRJ && pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      pExpr->functionId = TSDB_FUNC_TS_DUMMY;
      tagLength += pExpr->resBytes;
    }
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSchema*        pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId != TSDB_FUNC_TAG_DUMMY && pExpr->functionId != TSDB_FUNC_TS_DUMMY) {
      SSchema* pColSchema = &pSchema[pExpr->colInfo.colIndex];
      getResultDataInfo(pColSchema->type, pColSchema->bytes, pExpr->functionId, pExpr->param[0].i64Key, &pExpr->resType,
                        &pExpr->resBytes, &pExpr->interBytes, tagLength, true);
    }
  }
}

static void doUpdateSqlFunctionForColPrj(SQueryInfo* pQueryInfo) {
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_PRJ) {
      bool qualifiedCol = false;
      for (int32_t j = 0; j < pQueryInfo->groupbyExpr.numOfGroupCols; ++j) {
        SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, j);
  
        if (pExpr->colInfo.colId == pColIndex->colId) {
          qualifiedCol = true;
          doLimitOutputNormalColOfGroupby(pExpr);
          pExpr->numOfParams = 1;
          break;
        }
      }

      assert(qualifiedCol);
    }
  }
}

static bool tagColumnInGroupby(SSqlGroupbyExpr* pGroupbyExpr, int16_t columnId) {
  for (int32_t j = 0; j < pGroupbyExpr->numOfGroupCols; ++j) {
    SColIndex* pColIndex = taosArrayGet(pGroupbyExpr->columnInfo, j);
  
    if (columnId == pColIndex->colId && pColIndex->flag == TSDB_COL_TAG) {
      return true;
    }
  }

  return false;
}

static bool onlyTagPrjFunction(SQueryInfo* pQueryInfo) {
  bool hasTagPrj = false;
  bool hasColumnPrj = false;
  
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_PRJ) {
      hasColumnPrj = true;
    } else if (pExpr->functionId == TSDB_FUNC_TAGPRJ) {
      hasTagPrj = true;
    }
  }

  return (hasTagPrj) && (hasColumnPrj == false);
}

// check if all the tags prj columns belongs to the group by columns
static bool allTagPrjInGroupby(SQueryInfo* pQueryInfo) {
  bool allInGroupby = true;

  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId != TSDB_FUNC_TAGPRJ) {
      continue;
    }

    if (!tagColumnInGroupby(&pQueryInfo->groupbyExpr, pExpr->colInfo.colId)) {
      allInGroupby = false;
      break;
    }
  }

  // all selected tag columns belong to the group by columns set, always correct
  return allInGroupby;
}

static void updateTagPrjFunction(SQueryInfo* pQueryInfo) {
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_TAGPRJ) {
      pExpr->functionId = TSDB_FUNC_TAG;
    }
  }
}

/*
 * check for selectivity function + tags column function both exist.
 * 1. tagprj functions are not compatible with aggregated function when missing "group by" clause
 * 2. if selectivity function and tagprj function both exist, there should be only
 *    one selectivity function exists.
 */
static int32_t checkUpdateTagPrjFunctions(SQueryInfo* pQueryInfo, SSqlCmd* pCmd) {
  const char* msg1 = "only one selectivity function allowed in presence of tags function";
  const char* msg3 = "aggregation function should not be mixed up with projection";

  bool    tagColExists = false;
  int16_t numOfSelectivity = 0;
  int16_t numOfAggregation = 0;

  size_t numOfExprs = taosArrayGetSize(pQueryInfo->exprList);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, i);
    if (pExpr->functionId == TSDB_FUNC_TAGPRJ ||
        (pExpr->functionId == TSDB_FUNC_PRJ && pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX)) {
      tagColExists = true;
      break;
    }
  }

  for (int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, i);
  
    int16_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TAGPRJ || functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_TS ||
        functionId == TSDB_FUNC_ARITHM) {
      continue;
    }

    if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      numOfSelectivity++;
    } else {
      numOfAggregation++;
    }
  }

  if (tagColExists) {  // check if the selectivity function exists
    // When the tag projection function on tag column that is not in the group by clause, aggregation function and
    // selectivity function exist in select clause is not allowed.
    if (numOfAggregation > 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    /*
     *  if numOfSelectivity equals to 0, it is a super table projection query
     */
    if (numOfSelectivity == 1) {
      doUpdateSqlFunctionForTagPrj(pQueryInfo);
      doUpdateSqlFunctionForColPrj(pQueryInfo);
    } else if (numOfSelectivity > 1) {
      /*
       * If more than one selectivity functions exist, all the selectivity functions must be last_row.
       * Otherwise, return with error code.
       */
      for (int32_t i = 0; i < numOfExprs; ++i) {
        
        int16_t functionId = tscSqlExprGet(pQueryInfo, i)->functionId;
        if (functionId == TSDB_FUNC_TAGPRJ) {
          continue;
        }

        if (((aAggs[functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) && (functionId != TSDB_FUNC_LAST_ROW)) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
        }
      }

      doUpdateSqlFunctionForTagPrj(pQueryInfo);
      doUpdateSqlFunctionForColPrj(pQueryInfo);
    }
  } else {
    if ((pQueryInfo->type & TSDB_QUERY_TYPE_PROJECTION_QUERY) != 0) {
      if (numOfAggregation > 0 && pQueryInfo->groupbyExpr.numOfGroupCols == 0) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (numOfAggregation > 0 || numOfSelectivity > 0) {
        // clear the projection type flag
        pQueryInfo->type &= (~TSDB_QUERY_TYPE_PROJECTION_QUERY);
        doUpdateSqlFunctionForColPrj(pQueryInfo);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doAddGroupbyColumnsOnDemand(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg2 = "interval not allowed in group by normal column";

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  SSchema s = tGetTableNameColumnSchema();
  SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);
  int16_t  bytes = 0;
  int16_t  type = 0;
  char*    name = NULL;

  for (int32_t i = 0; i < pQueryInfo->groupbyExpr.numOfGroupCols; ++i) {
    SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, i);
    int16_t colIndex = pColIndex->colIndex;
    if (colIndex == TSDB_TBNAME_COLUMN_INDEX) {
      type  = s.type;
      bytes = s.bytes;
      name  = s.name;
    } else {
      if (TSDB_COL_IS_TAG(pColIndex->flag)) {
        SSchema* tagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
        
        type  = tagSchema[colIndex].type;
        bytes = tagSchema[colIndex].bytes;
        name  = tagSchema[colIndex].name;
      } else {
        type  = pSchema[colIndex].type;
        bytes = pSchema[colIndex].bytes;
        name  = pSchema[colIndex].name;
      }
    }
  
    size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
    if (TSDB_COL_IS_TAG(pColIndex->flag)) {
      SColumnIndex index = {.tableIndex = pQueryInfo->groupbyExpr.tableIndex, .columnIndex = colIndex};
      SSqlExpr* pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TAG, &index, type, bytes, bytes, true);
      
      memset(pExpr->aliasName, 0, sizeof(pExpr->aliasName));
      tstrncpy(pExpr->aliasName, name, sizeof(pExpr->aliasName));
      
      pExpr->colInfo.flag = TSDB_COL_TAG;

      // NOTE: tag column does not add to source column list
      SColumnList ids = getColumnList(1, 0, pColIndex->colIndex);
      insertResultField(pQueryInfo, size, &ids, bytes, type, name, pExpr);
    } else {
      // if this query is "group by" normal column, interval is not allowed
      if (pQueryInfo->intervalTime > 0) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      bool hasGroupColumn = false;
      for (int32_t j = 0; j < size; ++j) {
        SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, j);
        if (pExpr->colInfo.colId == pColIndex->colId) {
          break;
        }
      }

      /*
       * if the group by column does not required by user, add this column into the final result set
       * but invisible to user
       */
      if (!hasGroupColumn) {
        doAddGroupColumnForSubquery(pQueryInfo, i);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doFunctionsCompatibleCheck(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg1 = "functions/columns not allowed in group by query";
  const char* msg2 = "projection query on columns not allowed";
  const char* msg3 = "group by not allowed on projection query";
  const char* msg4 = "retrieve tags not compatible with group by or interval query";

  // only retrieve tags, group by is not supportted
  if (tscQueryTags(pQueryInfo)) {
    if (pQueryInfo->groupbyExpr.numOfGroupCols > 0 || pQueryInfo->intervalTime > 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
    } else {
      return TSDB_CODE_SUCCESS;
    }
  }

  if (pQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    // check if all the tags prj columns belongs to the group by columns
    if (onlyTagPrjFunction(pQueryInfo) && allTagPrjInGroupby(pQueryInfo)) {
      updateTagPrjFunction(pQueryInfo);
      return doAddGroupbyColumnsOnDemand(pCmd, pQueryInfo);
    }

    // check all query functions in selection clause, multi-output functions are not allowed
    size_t size = tscSqlExprNumOfExprs(pQueryInfo);
    for (int32_t i = 0; i < size; ++i) {
      SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
      int32_t   functId = pExpr->functionId;

      /*
       * group by normal columns.
       * Check if the column projection is identical to the group by column or not
       */
      if (functId == TSDB_FUNC_PRJ && pExpr->colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        bool qualified = false;
        for (int32_t j = 0; j < pQueryInfo->groupbyExpr.numOfGroupCols; ++j) {
          SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, j);
          if (pColIndex->colId == pExpr->colInfo.colId) {
            qualified = true;
            break;
          }
        }

        if (!qualified) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
        }
      }

      if (IS_MULTIOUTPUT(aAggs[functId].nStatus) && functId != TSDB_FUNC_TOP && functId != TSDB_FUNC_BOTTOM &&
          functId != TSDB_FUNC_TAGPRJ && functId != TSDB_FUNC_PRJ) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (functId == TSDB_FUNC_COUNT && pExpr->colInfo.colIndex == TSDB_TBNAME_COLUMN_INDEX) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }
    }

    if (checkUpdateTagPrjFunctions(pQueryInfo, pCmd) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    /*
     * group by tag function must be not changed the function name, otherwise, the group operation may fail to
     * divide the subset of final result.
     */
    if (doAddGroupbyColumnsOnDemand(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    // projection query on super table does not compatible with "group by" syntax
    if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    return TSDB_CODE_SUCCESS;
  } else {
    return checkUpdateTagPrjFunctions(pQueryInfo, pCmd);
  }
}

int32_t doLocalQueryProcess(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySQL* pQuerySql) {
  const char* msg1 = "only one expression allowed";
  const char* msg2 = "invalid expression in select clause";
  const char* msg3 = "invalid function";

  tSQLExprList* pExprList = pQuerySql->pSelection;
  if (pExprList->nExpr != 1) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  tSQLExpr* pExpr = pExprList->a[0].pNode;
  if (pExpr->operand.z == NULL) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  // TODO redefine the function
  SDNodeDynConfOption functionsInfo[5] = {{"database()", 10},
                                          {"server_version()", 16},
                                          {"server_status()", 15},
                                          {"client_version()", 16},
                                          {"current_user()", 14}};

  int32_t index = -1;
  for (int32_t i = 0; i < tListLen(functionsInfo); ++i) {
    if (strncasecmp(functionsInfo[i].name, pExpr->operand.z, functionsInfo[i].len) == 0 &&
        functionsInfo[i].len == pExpr->operand.n) {
      index = i;
      break;
    }
  }

  switch (index) {
    case 0:
      pQueryInfo->command = TSDB_SQL_CURRENT_DB;break;
    case 1:
      pQueryInfo->command = TSDB_SQL_SERV_VERSION;break;
      case 2:
      pQueryInfo->command = TSDB_SQL_SERV_STATUS;break;
    case 3:
      pQueryInfo->command = TSDB_SQL_CLI_VERSION;break;
    case 4:
      pQueryInfo->command = TSDB_SQL_CURRENT_USER;break;
    default: { return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3); }
  }
  
  SColumnIndex ind = {0};
  SSqlExpr* pExpr1 = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TAG_DUMMY, &ind, TSDB_DATA_TYPE_INT,
                                      tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize, false);
  
  const char* name = (pExprList->a[0].aliasName != NULL)? pExprList->a[0].aliasName:functionsInfo[index].name;
  tstrncpy(pExpr1->aliasName, name, tListLen(pExpr1->aliasName));
  
  return TSDB_CODE_SUCCESS;
}

// can only perform the parameters based on the macro definitation
int32_t tscCheckCreateDbParams(SSqlCmd* pCmd, SCMCreateDbMsg* pCreate) {
  char msg[512] = {0};

  if (pCreate->walLevel != -1 && (pCreate->walLevel < TSDB_MIN_WAL_LEVEL || pCreate->walLevel > TSDB_MAX_WAL_LEVEL)) {
    snprintf(msg, tListLen(msg), "invalid db option walLevel: %d, only 0-2 allowed", pCreate->walLevel);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (pCreate->replications != -1 &&
      (pCreate->replications < TSDB_MIN_DB_REPLICA_OPTION || pCreate->replications > TSDB_MAX_DB_REPLICA_OPTION)) {
    snprintf(msg, tListLen(msg), "invalid db option replications: %d valid range: [%d, %d]", pCreate->replications,
             TSDB_MIN_DB_REPLICA_OPTION, TSDB_MAX_DB_REPLICA_OPTION);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  int32_t val = htonl(pCreate->daysPerFile);
  if (val != -1 && (val < TSDB_MIN_DAYS_PER_FILE || val > TSDB_MAX_DAYS_PER_FILE)) {
    snprintf(msg, tListLen(msg), "invalid db option daysPerFile: %d valid range: [%d, %d]", val,
             TSDB_MIN_DAYS_PER_FILE, TSDB_MAX_DAYS_PER_FILE);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  val = htonl(pCreate->cacheBlockSize);
  if (val != -1 && (val < TSDB_MIN_CACHE_BLOCK_SIZE || val > TSDB_MAX_CACHE_BLOCK_SIZE)) {
    snprintf(msg, tListLen(msg), "invalid db option cacheBlockSize: %d valid range: [%d, %d]", val,
             TSDB_MIN_CACHE_BLOCK_SIZE, TSDB_MAX_CACHE_BLOCK_SIZE);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  val = htonl(pCreate->maxTables);
  if (val != -1 && (val < TSDB_MIN_TABLES || val > TSDB_MAX_TABLES)) {
    snprintf(msg, tListLen(msg), "invalid db option maxSessions: %d valid range: [%d, %d]", val,
             TSDB_MIN_TABLES, TSDB_MAX_TABLES);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (pCreate->precision != TSDB_TIME_PRECISION_MILLI && pCreate->precision != TSDB_TIME_PRECISION_MICRO) {
    snprintf(msg, tListLen(msg), "invalid db option timePrecision: %d valid value: [%d, %d]", pCreate->precision,
             TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  val = htonl(pCreate->commitTime);
  if (val != -1 && (val < TSDB_MIN_COMMIT_TIME || val > TSDB_MAX_COMMIT_TIME)) {
    snprintf(msg, tListLen(msg), "invalid db option commitTime: %d valid range: [%d, %d]", val,
             TSDB_MIN_COMMIT_TIME, TSDB_MAX_COMMIT_TIME);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  val = htonl(pCreate->fsyncPeriod);
  if (val != -1 && (val < TSDB_MIN_FSYNC_PERIOD || val > TSDB_MAX_FSYNC_PERIOD)) {
    snprintf(msg, tListLen(msg), "invalid db option fsyncPeriod: %d valid range: [%d, %d]", val,
             TSDB_MIN_FSYNC_PERIOD, TSDB_MAX_FSYNC_PERIOD);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (pCreate->compression != -1 &&
      (pCreate->compression < TSDB_MIN_COMP_LEVEL || pCreate->compression > TSDB_MAX_COMP_LEVEL)) {
    snprintf(msg, tListLen(msg), "invalid db option compression: %d valid range: [%d, %d]", pCreate->compression,
             TSDB_MIN_COMP_LEVEL, TSDB_MAX_COMP_LEVEL);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  return TSDB_CODE_SUCCESS;
}

// for debug purpose
void tscPrintSelectClause(SSqlObj* pSql, int32_t subClauseIndex) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, subClauseIndex);

  int32_t size = tscSqlExprNumOfExprs(pQueryInfo);
  if (size == 0) {
    return;
  }

  int32_t totalBufSize = 1024;

  char    str[1024] = {0};
  int32_t offset = 0;

  offset += sprintf(str, "num:%d [", size);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);

    char    tmpBuf[1024] = {0};
    int32_t tmpLen = 0;
    tmpLen =
        sprintf(tmpBuf, "%s(uid:%" PRId64 ", %d)", aAggs[pExpr->functionId].aName, pExpr->uid, pExpr->colInfo.colId);
    if (tmpLen + offset > totalBufSize) break;

    offset += sprintf(str + offset, "%s", tmpBuf);

    if (i < size - 1) {
      str[offset++] = ',';
    }
  }

  str[offset] = ']';
  tscDebug("%p select clause:%s", pSql, str);
}

int32_t doCheckForCreateTable(SSqlObj* pSql, int32_t subClauseIndex, SSqlInfo* pInfo) {
  const char* msg1 = "invalid table name";
  const char* msg2 = "table name too long";

  SSqlCmd*        pCmd = &pSql->cmd;
  SQueryInfo*     pQueryInfo = tscGetQueryInfoDetail(pCmd, subClauseIndex);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  SCreateTableSQL* pCreateTable = pInfo->pCreateTableInfo;

  tFieldList* pFieldList = pCreateTable->colInfo.pColumns;
  tFieldList* pTagList = pCreateTable->colInfo.pTagColumns;

  assert(pFieldList != NULL);

  // if sql specifies db, use it, otherwise use default db
  SSQLToken* pzTableName = &(pCreateTable->name);

  if (tscValidateName(pzTableName) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (tscSetTableFullName(pTableMetaInfo, pzTableName, pSql) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  if (!validateTableColumnInfo(pFieldList, pCmd) ||
      (pTagList != NULL && !validateTagParams(pTagList, pFieldList, pCmd))) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  int32_t col = 0;
  for (; col < pFieldList->nField; ++col) {
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &pFieldList->p[col]);
  }

  pCmd->numOfCols = (int16_t)pFieldList->nField;

  if (pTagList != NULL) {  // create super table[optional]
    for (int32_t i = 0; i < pTagList->nField; ++i) {
      tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &pTagList->p[i]);
    }

    pCmd->count = pTagList->nField;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doCheckForCreateFromStable(SSqlObj* pSql, SSqlInfo* pInfo) {
  const char* msg1 = "invalid table name";
  const char* msg3 = "tag value too long";
  const char* msg4 = "illegal value or data overflow";
  const char* msg5 = "tags number not matched";

  SSqlCmd* pCmd = &pSql->cmd;

  SCreateTableSQL* pCreateTable = pInfo->pCreateTableInfo;
  SQueryInfo*      pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  // two table: the first one is for current table, and the secondary is for the super table.
  if (pQueryInfo->numOfTables < 2) {
    tscAddEmptyMetaInfo(pQueryInfo);
  }

  const int32_t TABLE_INDEX = 0;
  const int32_t STABLE_INDEX = 1;

  STableMetaInfo* pStableMeterMetaInfo = tscGetMetaInfo(pQueryInfo, STABLE_INDEX);

  // super table name, create table by using dst
  SSQLToken* pToken = &(pCreateTable->usingInfo.stableName);

  if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (tscSetTableFullName(pStableMeterMetaInfo, pToken, pSql) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  // get meter meta from mnode
  tstrncpy(pCreateTable->usingInfo.tagdata.name, pStableMeterMetaInfo->name, sizeof(pCreateTable->usingInfo.tagdata.name));
  tVariantList* pList = pInfo->pCreateTableInfo->usingInfo.pTagVals;

  int32_t code = tscGetTableMeta(pSql, pStableMeterMetaInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (tscGetNumOfTags(pStableMeterMetaInfo->pTableMeta) != pList->nExpr) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
  }

  // too long tag values will return invalid sql, not be truncated automatically
  SSchema* pTagSchema = tscGetTableTagSchema(pStableMeterMetaInfo->pTableMeta);

  STagData* pTag = &pCreateTable->usingInfo.tagdata;
  SKVRowBuilder kvRowBuilder = {0};
  if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int32_t ret = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < pList->nExpr; ++i) {
    SSchema* pSchema = pTagSchema + i;
    if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
      // validate the length of binary
      if (pList->a[i].pVar.nLen + VARSTR_HEADER_SIZE > pSchema->bytes) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }
    }
    
    char tagVal[TSDB_MAX_TAGS_LEN];
    ret = tVariantDump(&(pList->a[i].pVar), tagVal, pSchema->type, true);
    if (ret != TSDB_CODE_SUCCESS) {
      tdDestroyKVRowBuilder(&kvRowBuilder);
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
    }

    tdAddColToKVRow(&kvRowBuilder, pSchema->colId, pSchema->type, tagVal);
  }

  SKVRow row = tdGetKVRowFromBuilder(&kvRowBuilder);
  tdDestroyKVRowBuilder(&kvRowBuilder);
  if (row == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  tdSortKVRowByColIdx(row);
  pTag->dataLen = kvRowLen(row);
  kvRowCpy(pTag->data, row);
  free(row);

  // table name
  if (tscValidateName(&pInfo->pCreateTableInfo->name) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  STableMetaInfo* pTableMeterMetaInfo = tscGetMetaInfo(pQueryInfo, TABLE_INDEX);
  ret = tscSetTableFullName(pTableMeterMetaInfo, &pInfo->pCreateTableInfo->name, pSql);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doCheckForStream(SSqlObj* pSql, SSqlInfo* pInfo) {
  const char* msg1 = "invalid table name";
  const char* msg2 = "table name too long";
  const char* msg3 = "fill only available for interval query";
  const char* msg4 = "fill option not supported in stream computing";
  const char* msg5 = "sql too long";  // todo ADD support
  const char* msg6 = "from missing in subclause";
  
  SSqlCmd*    pCmd = &pSql->cmd;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  assert(pQueryInfo->numOfTables == 1);

  SCreateTableSQL* pCreateTable = pInfo->pCreateTableInfo;
  STableMetaInfo*  pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  // if sql specifies db, use it, otherwise use default db
  SSQLToken* pzTableName = &(pCreateTable->name);
  SQuerySQL* pQuerySql = pCreateTable->pSelect;

  if (tscValidateName(pzTableName) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }
  
  tVariantList* pSrcMeterName = pInfo->pCreateTableInfo->pSelect->from;
  if (pSrcMeterName == NULL || pSrcMeterName->nExpr == 0) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
  }
  
  tVariant* pVar = &pSrcMeterName->a[0].pVar;
  SSQLToken srcToken = {.z = pVar->pz, .n = pVar->nLen, .type = TK_STRING};
  if (tscValidateName(&srcToken) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (tscSetTableFullName(pTableMetaInfo, &srcToken, pSql) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  int32_t code = tscGetTableMeta(pSql, pTableMetaInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  bool isSTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);
  if (parseSelectClause(&pSql->cmd, 0, pQuerySql->pSelection, isSTable) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (pQuerySql->pWhere != NULL) {  // query condition in stream computing
    if (parseWhereClause(pQueryInfo, &pQuerySql->pWhere, pSql) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  // set interval value
  if (parseIntervalClause(pCmd, pQueryInfo, pQuerySql) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  } else {
    if ((pQueryInfo->intervalTime > 0) &&
        (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  // set the created table[stream] name
  if (tscSetTableFullName(pTableMetaInfo, pzTableName, pSql) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pQuerySql->selectToken.n > TSDB_MAX_SAVED_SQL_LEN) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
  }

  if (tsRewriteFieldNameIfNecessary(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  pCmd->numOfCols = pQueryInfo->fieldsInfo.numOfOutput;

  if (validateSqlFunctionInStreamSql(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  /*
   * check if fill operation is available, the fill operation is parsed and executed during query execution,
   * not here.
   */
  if (pQuerySql->fillType != NULL) {
    if (pQueryInfo->intervalTime == 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    tVariantListItem* pItem = &pQuerySql->fillType->a[0];
    if (pItem->pVar.nType == TSDB_DATA_TYPE_BINARY) {
      if (!((strncmp(pItem->pVar.pz, "none", 4) == 0 && pItem->pVar.nLen == 4) ||
            (strncmp(pItem->pVar.pz, "null", 4) == 0 && pItem->pVar.nLen == 4))) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
      }
    }
  }

  // set the number of stream table columns
  pCmd->numOfCols = pQueryInfo->fieldsInfo.numOfOutput;
  return TSDB_CODE_SUCCESS;
}

int32_t doCheckForQuery(SSqlObj* pSql, SQuerySQL* pQuerySql, int32_t index) {
  assert(pQuerySql != NULL && (pQuerySql->from == NULL || pQuerySql->from->nExpr > 0));

  const char* msg0 = "invalid table name";
  const char* msg1 = "table name too long";
  const char* msg2 = "point interpolation query needs timestamp";
  const char* msg5 = "fill only available for interval query";
  const char* msg6 = "start(end) time of query range required or time range too large";
  const char* msg7 = "illegal number of tables in from clause";
  const char* msg8 = "too many columns in selection clause";
  const char* msg9 = "TWA query requires both the start and end time";
  const char* msg10= "too many tables in from clause";

  int32_t code = TSDB_CODE_SUCCESS;

  SSqlCmd* pCmd = &pSql->cmd;

  SQueryInfo*     pQueryInfo = tscGetQueryInfoDetail(pCmd, index);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (pTableMetaInfo == NULL) {
    pTableMetaInfo = tscAddEmptyMetaInfo(pQueryInfo);
  }

  assert(pCmd->clauseIndex == index);

  // too many result columns not support order by in query
  if (pQuerySql->pSelection->nExpr > TSDB_MAX_COLUMNS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
  }

  /*
   * handle the sql expression without from subclause
   * select current_database();
   * select server_version();
   * select client_version();
   * select server_state();
   */
  if (pQuerySql->from == NULL) {
    assert(pQuerySql->fillType == NULL && pQuerySql->pGroupby == NULL && pQuerySql->pWhere == NULL &&
           pQuerySql->pSortOrder == NULL);
    return doLocalQueryProcess(pCmd, pQueryInfo, pQuerySql);
  }

  if (pQuerySql->from->nExpr > TSDB_MAX_JOIN_TABLE_NUM) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
  }

  pQueryInfo->command = TSDB_SQL_SELECT;

  if (pQuerySql->from->nExpr > 2) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg10);
  }

  // set all query tables, which are maybe more than one.
  for (int32_t i = 0; i < pQuerySql->from->nExpr; ++i) {
    tVariant* pTableItem = &pQuerySql->from->a[i].pVar;

    if (pTableItem->nType != TSDB_DATA_TYPE_BINARY) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }

    pTableItem->nLen = strdequote(pTableItem->pz);

    SSQLToken tableName = {.z = pTableItem->pz, .n = pTableItem->nLen, .type = TK_STRING};
    if (tscValidateName(&tableName) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }

    if (pQueryInfo->numOfTables <= i) {  // more than one table
      tscAddEmptyMetaInfo(pQueryInfo);
    }

    STableMetaInfo* pMeterInfo1 = tscGetMetaInfo(pQueryInfo, i);

    SSQLToken t = {.type = TSDB_DATA_TYPE_BINARY, .n = pTableItem->nLen, .z = pTableItem->pz};
    if (tscSetTableFullName(pMeterInfo1, &t, pSql) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    code = tscGetTableMeta(pSql, pMeterInfo1);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  assert(pQueryInfo->numOfTables == pQuerySql->from->nExpr);
  bool isSTable = false;
  
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    isSTable = true;
    code = tscGetSTableVgroupInfo(pSql, index);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_STABLE_QUERY);
  } else {
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TABLE_QUERY);
  }

  // parse the group by clause in the first place
  if (parseGroupbyClause(pQueryInfo, pQuerySql->pGroupby, pCmd) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (parseSelectClause(pCmd, index, pQuerySql->pSelection, isSTable) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  // set interval value
  if (parseIntervalClause(pCmd, pQueryInfo, pQuerySql) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  } else {
    if ((pQueryInfo->intervalTime > 0) &&
        (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  // set order by info
  if (parseOrderbyClause(pCmd, pQueryInfo, pQuerySql, tscGetTableSchema(pTableMetaInfo->pTableMeta)) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  // set where info
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  if (pQuerySql->pWhere != NULL) {
    if (parseWhereClause(pQueryInfo, &pQuerySql->pWhere, pSql) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    pQuerySql->pWhere = NULL;
    if (tinfo.precision == TSDB_TIME_PRECISION_MILLI) {
      pQueryInfo->window.skey = pQueryInfo->window.skey / 1000;
      pQueryInfo->window.ekey = pQueryInfo->window.ekey / 1000;
    }
  } else {  // set the time rang
    pQueryInfo->window = TSWINDOW_INITIALIZER;
  }

  // user does not specified the query time window, twa is not allowed in such case.
  if ((pQueryInfo->window.skey == INT64_MIN || pQueryInfo->window.ekey == INT64_MAX ||
       (pQueryInfo->window.ekey == INT64_MAX / 1000 && tinfo.precision == TSDB_TIME_PRECISION_MILLI)) && tscIsTWAQuery(pQueryInfo)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg9);
  }

  // no result due to invalid query time range
  if (pQueryInfo->window.skey > pQueryInfo->window.ekey) {
    pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    return TSDB_CODE_SUCCESS;
  }

  if (!hasTimestampForPointInterpQuery(pQueryInfo)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  // in case of join query, time range is required.
  if (QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
    int64_t timeRange = labs(pQueryInfo->window.skey - pQueryInfo->window.ekey);

    if (timeRange == 0 && pQueryInfo->window.skey == 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
    }
  }

  if ((code = parseLimitClause(pCmd, pQueryInfo, index, pQuerySql, pSql)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  if ((code = doFunctionsCompatibleCheck(pCmd, pQueryInfo)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  setColumnOffsetValueInResultset(pQueryInfo);

  /*
   * fill options are set at the end position, when all columns are set properly
   * the columns may be increased due to group by operation
   */
  if (pQuerySql->fillType != NULL) {
    if (pQueryInfo->intervalTime == 0 && (!tscIsPointInterpQuery(pQueryInfo))) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
    }

    if (pQueryInfo->intervalTime > 0) {
      int64_t timeRange = labs(pQueryInfo->window.skey - pQueryInfo->window.ekey);
      // number of result is not greater than 10,000,000
      if ((timeRange == 0) || (timeRange / pQueryInfo->intervalTime) > MAX_RETRIEVE_ROWS_IN_INTERVAL_QUERY) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }
    }

    int32_t ret = parseFillClause(pCmd, pQueryInfo, pQuerySql);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  return TSDB_CODE_SUCCESS;  // Does not build query message here
}

int32_t exprTreeFromSqlExpr(SSqlCmd* pCmd, tExprNode **pExpr, const tSQLExpr* pSqlExpr, SArray* pExprInfo, SQueryInfo* pQueryInfo, SArray* pCols) {
  tExprNode* pLeft = NULL;
  tExprNode* pRight= NULL;
  
  if (pSqlExpr->pLeft != NULL) {
    int32_t ret = exprTreeFromSqlExpr(pCmd, &pLeft, pSqlExpr->pLeft, pExprInfo, pQueryInfo, pCols);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }
  
  if (pSqlExpr->pRight != NULL) {
    int32_t ret = exprTreeFromSqlExpr(pCmd, &pRight, pSqlExpr->pRight, pExprInfo, pQueryInfo, pCols);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }
  
  if (pSqlExpr->pLeft == NULL) {
    if (pSqlExpr->nSQLOptr >= TK_BOOL && pSqlExpr->nSQLOptr <= TK_STRING) {
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_VALUE;
      (*pExpr)->pVal = calloc(1, sizeof(tVariant));
      
      tVariantAssign((*pExpr)->pVal, &pSqlExpr->val);
      return TSDB_CODE_SUCCESS;
    } else if (pSqlExpr->nSQLOptr >= TK_COUNT && pSqlExpr->nSQLOptr <= TK_AVG_IRATE) {
      // arithmetic expression on the results of aggregation functions
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_COL;
      (*pExpr)->pSchema = calloc(1, sizeof(SSchema));
      strncpy((*pExpr)->pSchema->name, pSqlExpr->operand.z, pSqlExpr->operand.n);
      
      // set the input column data byte and type.
      size_t size = taosArrayGetSize(pExprInfo);
      
      for (int32_t i = 0; i < size; ++i) {
        SSqlExpr* p1 = taosArrayGetP(pExprInfo, i);
        
        if (strcmp((*pExpr)->pSchema->name, p1->aliasName) == 0) {
          (*pExpr)->pSchema->type = p1->resType;
          (*pExpr)->pSchema->bytes = p1->resBytes;
          break;
        }
      }
    } else if (pSqlExpr->nSQLOptr == TK_ID) { // column name, normal column arithmetic expression
      SColumnIndex index = {0};
      int32_t ret = getColumnIndexByName(pCmd, &pSqlExpr->colInfo, pQueryInfo, &index);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
  
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_COL;
      (*pExpr)->pSchema = calloc(1, sizeof(SSchema));
      
      STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, 0)->pTableMeta;
      SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, index.columnIndex);
      *(*pExpr)->pSchema = *pSchema;
  
      if (pCols != NULL) {  // record the involved columns
        SColIndex colIndex = {0};
        tstrncpy(colIndex.name, pSchema->name, sizeof(colIndex.name));
        colIndex.colId = pSchema->colId;
        colIndex.colIndex = index.columnIndex;
        
        taosArrayPush(pCols, &colIndex);
      }
      
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
    
  } else {
    *pExpr = (tExprNode *)calloc(1, sizeof(tExprNode));
    (*pExpr)->nodeType = TSQL_NODE_EXPR;
    
    (*pExpr)->_node.hasPK = false;
    (*pExpr)->_node.pLeft = pLeft;
    (*pExpr)->_node.pRight = pRight;
    
    SSQLToken t = {.type = pSqlExpr->nSQLOptr};
    (*pExpr)->_node.optr = getBinaryExprOptr(&t);
    
    assert((*pExpr)->_node.optr != 0);
    
    if ((*pExpr)->_node.optr == TSDB_BINARY_OP_DIVIDE) {
      if (pRight->nodeType == TSQL_NODE_VALUE) {
        if (pRight->pVal->nType == TSDB_DATA_TYPE_INT && pRight->pVal->i64Key == 0) {
          return TSDB_CODE_TSC_INVALID_SQL;
        } else if (pRight->pVal->nType == TSDB_DATA_TYPE_FLOAT && pRight->pVal->dKey == 0) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }
      }
    }

    // NOTE: binary|nchar data allows the >|< type filter
    if ((*pExpr)->_node.optr != TSDB_RELATION_EQUAL && (*pExpr)->_node.optr != TSDB_RELATION_NOT_EQUAL) {
      if (pRight->nodeType == TSQL_NODE_VALUE) {
        if (pRight->pVal->nType == TSDB_DATA_TYPE_BOOL) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }
      }
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

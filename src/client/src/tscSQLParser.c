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

#ifndef __APPLE__
#define _BSD_SOURCE
#define _XOPEN_SOURCE 500
#define _DEFAULT_SOURCE
#define _GNU_SOURCE
#endif // __APPLE__

#include "os.h"
#include "ttype.h"
#include "texpr.h"
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
#include "ttokendef.h"

#define DEFAULT_PRIMARY_TIMESTAMP_COL_NAME "_c0"

#define TSWINDOW_IS_EQUAL(t1, t2) (((t1).skey == (t2).skey) && ((t1).ekey == (t2).ekey))

// -1 is tbname column index, so here use the -3 as the initial value
#define COLUMN_INDEX_INITIAL_VAL (-3)
#define COLUMN_INDEX_INITIALIZER \
  { COLUMN_INDEX_INITIAL_VAL, COLUMN_INDEX_INITIAL_VAL }
#define COLUMN_INDEX_VALIDE(index) (((index).tableIndex >= 0) && ((index).columnIndex >= TSDB_BLOCK_DIST_COLUMN_INDEX))
#define TBNAME_LIST_SEP ","

typedef struct SColumnList {  // todo refactor
  int32_t      num;
  SColumnIndex ids[TSDB_MAX_COLUMNS];
} SColumnList;

typedef struct SConvertFunc {
  int32_t originFuncId;
  int32_t execFuncId;
} SConvertFunc;

static SSqlExpr* doAddProjectCol(SQueryInfo* pQueryInfo, int32_t colIndex, int32_t tableIndex);

static int32_t setShowInfo(SSqlObj* pSql, SSqlInfo* pInfo);
static char*   getAccountId(SSqlObj* pSql);

static bool has(SArray* pFieldList, int32_t startIdx, const char* name);
static char* cloneCurrentDBName(SSqlObj* pSql);
static bool hasSpecifyDB(SStrToken* pTableName);
static bool validateTableColumnInfo(SArray* pFieldList, SSqlCmd* pCmd);
static bool validateTagParams(SArray* pTagsList, SArray* pFieldList, SSqlCmd* pCmd);

static int32_t setObjFullName(char* fullName, const char* account, SStrToken* pDB, SStrToken* tableName, int32_t* len);

static void getColumnName(tSqlExprItem* pItem, char* resultFieldName, int32_t nameLength);

static int32_t addExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t colIndex, tSqlExprItem* pItem, bool finalResult);
static int32_t insertResultField(SQueryInfo* pQueryInfo, int32_t outputIndex, SColumnList* pIdList, int16_t bytes,
                                 int8_t type, char* fieldName, SSqlExpr* pSqlExpr);

static uint8_t convertOptr(SStrToken *pToken);

static int32_t parseSelectClause(SSqlCmd* pCmd, int32_t clauseIndex, SArray* pSelectList, bool isSTable, bool joinQuery, bool timeWindowQuery);

static bool validateIpAddress(const char* ip, size_t size);
static bool hasUnsupportFunctionsForSTableQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static bool functionCompatibleCheck(SQueryInfo* pQueryInfo, bool joinQuery, bool twQuery);

static int32_t parseGroupbyClause(SQueryInfo* pQueryInfo, SArray* pList, SSqlCmd* pCmd);

static int32_t parseIntervalClause(SSqlObj* pSql, SQueryInfo* pQueryInfo, SQuerySqlNode* pQuerySqlNode);
static int32_t parseIntervalOffset(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SStrToken* offsetToken);
static int32_t parseSlidingClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SStrToken* pSliding);

static int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExprItem* pItem);

static int32_t parseWhereClause(SQueryInfo* pQueryInfo, tSqlExpr** pExpr, SSqlObj* pSql);
static int32_t parseFillClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySqlNode* pQuerySQL);
static int32_t parseOrderbyClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySqlNode* pQuerySqlNode, SSchema* pSchema);

static int32_t tsRewriteFieldNameIfNecessary(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static int32_t setAlterTableInfo(SSqlObj* pSql, struct SSqlInfo* pInfo);
static int32_t validateSqlFunctionInStreamSql(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static int32_t validateFunctionsInIntervalOrGroupbyQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static int32_t validateArithmeticSQLExpr(SSqlCmd* pCmd, tSqlExpr* pExpr, SQueryInfo* pQueryInfo, SColumnList* pList, int32_t* type);
static int32_t validateEp(char* ep);
static int32_t validateDNodeConfig(SMiscInfo* pOptions);
static int32_t validateLocalConfig(SMiscInfo* pOptions);
static int32_t validateColumnName(char* name);
static int32_t setKillInfo(SSqlObj* pSql, struct SSqlInfo* pInfo, int32_t killType);

static bool validateOneTags(SSqlCmd* pCmd, TAOS_FIELD* pTagField);
static bool hasTimestampForPointInterpQuery(SQueryInfo* pQueryInfo);
static bool hasNormalColumnFilter(SQueryInfo* pQueryInfo);

static int32_t parseLimitClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t index, SQuerySqlNode* pQuerySqlNode, SSqlObj* pSql);
static int32_t parseCreateDBOptions(SSqlCmd* pCmd, SCreateDbInfo* pCreateDbSql);
static int32_t getColumnIndexByName(SSqlCmd* pCmd, const SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex);
static int32_t getTableIndexByName(SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex);

static int32_t getTableIndexImpl(SStrToken* pTableToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex);
static int32_t doFunctionsCompatibleCheck(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static int32_t doLocalQueryProcess(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySqlNode* pQuerySqlNode);
static int32_t tscCheckCreateDbParams(SSqlCmd* pCmd, SCreateDbMsg* pCreate);

static SColumnList getColumnList(int32_t num, int16_t tableIndex, int32_t columnIndex);

static int32_t doCheckForCreateTable(SSqlObj* pSql, int32_t subClauseIndex, SSqlInfo* pInfo);
static int32_t doCheckForCreateFromStable(SSqlObj* pSql, SSqlInfo* pInfo);
static int32_t doCheckForStream(SSqlObj* pSql, SSqlInfo* pInfo);
static int32_t doValidateSqlNode(SSqlObj* pSql, SQuerySqlNode* pQuerySqlNode, int32_t index);
static int32_t exprTreeFromSqlExpr(SSqlCmd* pCmd, tExprNode **pExpr, const tSqlExpr* pSqlExpr, SQueryInfo* pQueryInfo, SArray* pCols, int64_t *uid);
static bool    validateDebugFlag(int32_t v);

static bool    isTimeWindowQuery(SQueryInfo* pQueryInfo) {
  return pQueryInfo->interval.interval > 0 || pQueryInfo->sessionWindow.gap > 0;
}

int16_t getNewResColId(SQueryInfo* pQueryInfo) {
  return pQueryInfo->resColumnId--;
}

static uint8_t convertOptr(SStrToken *pToken) {
  switch (pToken->type) {
    case TK_LT:
      return TSDB_RELATION_LESS;
    case TK_LE:
      return TSDB_RELATION_LESS_EQUAL;
    case TK_GT:
      return TSDB_RELATION_GREATER;
    case TK_GE:
      return TSDB_RELATION_GREATER_EQUAL;
    case TK_NE:
      return TSDB_RELATION_NOT_EQUAL;
    case TK_AND:
      return TSDB_RELATION_AND;
    case TK_OR:
      return TSDB_RELATION_OR;
    case TK_EQ:
      return TSDB_RELATION_EQUAL;
    case TK_PLUS:
      return TSDB_BINARY_OP_ADD;
    case TK_MINUS:
      return TSDB_BINARY_OP_SUBTRACT;
    case TK_STAR:
      return TSDB_BINARY_OP_MULTIPLY;
    case TK_SLASH:
    case TK_DIVIDE:
      return TSDB_BINARY_OP_DIVIDE;
    case TK_REM:
      return TSDB_BINARY_OP_REMAINDER;
    case TK_LIKE:
      return TSDB_RELATION_LIKE;
    case TK_ISNULL:
      return TSDB_RELATION_ISNULL;
    case TK_NOTNULL:
      return TSDB_RELATION_NOTNULL;
    default: { return 0; }
  }
}

static bool validateDebugFlag(int32_t v) {
  const static int validFlag[] = {131, 135, 143};

  for (int i = 0; i < tListLen(validFlag); i++) {
    if (v == validFlag[i]) {
        return true;
    }
  }
  return false;
}
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

static int32_t handlePassword(SSqlCmd* pCmd, SStrToken* pPwd) {
  const char* msg1 = "password can not be empty";
  const char* msg2 = "name or password too long";
  const char* msg3 = "password needs single quote marks enclosed";

  if (pPwd->type != TK_STRING) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  strdequote(pPwd->z);
  pPwd->n = (uint32_t)strtrim(pPwd->z);  // trim space before and after passwords

  if (pPwd->n <= 0) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pPwd->n >= TSDB_KEY_LEN) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscToSQLCmd(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  if (pInfo == NULL || pSql == NULL) {
    return TSDB_CODE_TSC_APP_ERROR;
  }

  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  int32_t code = TSDB_CODE_SUCCESS;
  if (!pInfo->valid || terrno == TSDB_CODE_TSC_SQL_SYNTAX_ERROR) {
    terrno = TSDB_CODE_SUCCESS;  // clear the error number
    return tscSQLSyntaxErrMsg(tscGetErrorMsgPayload(pCmd), NULL, pInfo->msg);
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetailSafely(pCmd, pCmd->clauseIndex);
  if (pQueryInfo == NULL) {
    pRes->code = terrno;
    return pRes->code;
  }

  STableMetaInfo* pTableMetaInfo = (pQueryInfo->numOfTables == 0)? tscAddEmptyMetaInfo(pQueryInfo) : pQueryInfo->pTableMetaInfo[0];
  if (pTableMetaInfo == NULL) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pRes->code;
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
      const char* msg4 = "table is not super table";

      SStrToken* pzName = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if ((pInfo->type != TSDB_SQL_DROP_DNODE) && (tscValidateName(pzName) != TSDB_CODE_SUCCESS)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      if (pInfo->type == TSDB_SQL_DROP_DB) {
        assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
        code = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pzName);
        if (code != TSDB_CODE_SUCCESS) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
        }

      } else if (pInfo->type == TSDB_SQL_DROP_TABLE) {
        assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);

        code = tscSetTableFullName(pTableMetaInfo, pzName, pSql);
        if(code != TSDB_CODE_SUCCESS) {
          return code; 
        }

        if (pInfo->pMiscInfo->tableType == TSDB_SUPER_TABLE) {
          code = tscGetTableMeta(pSql, pTableMetaInfo);
          if (code != TSDB_CODE_SUCCESS) {
            return code;
          }

          if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
          }
        }
        
      } else if (pInfo->type == TSDB_SQL_DROP_DNODE) {
        pzName->n = strdequote(pzName->z);
        strncpy(pCmd->payload, pzName->z, pzName->n);
      } else {  // drop user/account
        if (pzName->n >= TSDB_USER_LEN) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        strncpy(pCmd->payload, pzName->z, pzName->n);
      }

      break;
    }

    case TSDB_SQL_USE_DB: {
      const char* msg = "invalid db name";
      SStrToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pToken);
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

      SCreateDbInfo* pCreateDB = &(pInfo->pMiscInfo->dbOpt);
      if (tscValidateName(&pCreateDB->dbname) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), &(pCreateDB->dbname));
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      if (parseCreateDBOptions(pCmd, pCreateDB) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      break;
    }

    case TSDB_SQL_CREATE_DNODE: {
      const char* msg = "invalid host name (ip address)";

      if (taosArrayGetSize(pInfo->pMiscInfo->a) > 1) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }

      SStrToken* id = taosArrayGet(pInfo->pMiscInfo->a, 0);
      id->n = strdequote(id->z);
      break;
    }

    case TSDB_SQL_CREATE_ACCT:
    case TSDB_SQL_ALTER_ACCT: {
      const char* msg1 = "invalid state option, available options[no, r, w, all]";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "name too long";

      SStrToken* pName = &pInfo->pMiscInfo->user.user;
      SStrToken* pPwd = &pInfo->pMiscInfo->user.passwd;

      if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      if (pName->n >= TSDB_USER_LEN) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SCreateAcctInfo* pAcctOpt = &pInfo->pMiscInfo->acctOpt;
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
      const char* msg1 = "invalid table name";
      const char* msg2 = "table name too long";

      SStrToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (!tscValidateTableNameLength(pToken->n)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      // additional msg has been attached already
      code = tscSetTableFullName(pTableMetaInfo, pToken, pSql);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      return tscGetTableMeta(pSql, pTableMetaInfo);
    }
    case TSDB_SQL_SHOW_CREATE_TABLE: {
      const char* msg1 = "invalid table name";
      const char* msg2 = "table name is too long";

      SStrToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (!tscValidateTableNameLength(pToken->n)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      code = tscSetTableFullName(pTableMetaInfo, pToken, pSql);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      return tscGetTableMeta(pSql, pTableMetaInfo);
    }
    case TSDB_SQL_SHOW_CREATE_DATABASE: {
      const char* msg1 = "invalid database name";

      SStrToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (pToken->n > TSDB_DB_NAME_LEN) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      return tscSetTableFullName(pTableMetaInfo, pToken, pSql);
    }
    case TSDB_SQL_CFG_DNODE: {
      const char* msg2 = "invalid configure options or values, such as resetlog / debugFlag 135 / balance 'vnode:2-dnode:2' / monitor 1 ";
      const char* msg3 = "invalid dnode ep";

      /* validate the ip address */
      SMiscInfo* pMiscInfo = pInfo->pMiscInfo;

      /* validate the parameter names and options */
      if (validateDNodeConfig(pMiscInfo) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      char* pMsg = pCmd->payload;

      SCfgDnodeMsg* pCfg = (SCfgDnodeMsg*)pMsg;

      SStrToken* t0 = taosArrayGet(pMiscInfo->a, 0);
      SStrToken* t1 = taosArrayGet(pMiscInfo->a, 1);

      t0->n = strdequote(t0->z);
      strncpy(pCfg->ep, t0->z, t0->n);

      if (validateEp(pCfg->ep) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      strncpy(pCfg->config, t1->z, t1->n);

      if (taosArrayGetSize(pMiscInfo->a) == 3) {
        SStrToken* t2 = taosArrayGet(pMiscInfo->a, 2);

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
      SStrToken* pName = &pUser->user;
      SStrToken* pPwd = &pUser->passwd;

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

          SStrToken* pPrivilege = &pUser->privilege;

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
      SMiscInfo  *pMiscInfo = pInfo->pMiscInfo;
      const char *msg = "invalid configure options or values";

      // validate the parameter names and options
      if (validateLocalConfig(pMiscInfo) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }

      int32_t numOfToken = (int32_t) taosArrayGetSize(pMiscInfo->a);
      assert(numOfToken >= 1 && numOfToken <= 2);

      SStrToken* t = taosArrayGet(pMiscInfo->a, 0);
      strncpy(pCmd->payload, t->z, t->n);
      if (numOfToken == 2) {
        SStrToken* t1 = taosArrayGet(pMiscInfo->a, 1);
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
      const char* msg1 = "columns in select clause not identical";

      for (int32_t i = pCmd->numOfClause; i < pInfo->subclauseInfo.numOfClause; ++i) {
        SQueryInfo* pqi = tscGetQueryInfoDetailSafely(pCmd, i);
        if (pqi == NULL) {
          pRes->code = terrno;
          return pRes->code;
        }
      }

      assert(pCmd->numOfClause == pInfo->subclauseInfo.numOfClause);
      for (int32_t i = pCmd->clauseIndex; i < pInfo->subclauseInfo.numOfClause; ++i) {
        SQuerySqlNode* pQuerySqlNode = pInfo->subclauseInfo.pClause[i];
        tscTrace("%p start to parse %dth subclause, total:%d", pSql, i, pInfo->subclauseInfo.numOfClause);
        if ((code = doValidateSqlNode(pSql, pQuerySqlNode, i)) != TSDB_CODE_SUCCESS) {
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
  if (tscBuildMsg[pCmd->command] != NULL) {
    return tscBuildMsg[pCmd->command](pSql, pInfo);
  } else {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), "not support sql expression");
  }
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

// need to add timestamp column in result set, if it is a time window query
static int32_t addPrimaryTsColumnForTimeWindowQuery(SQueryInfo* pQueryInfo) {
  uint64_t uid = tscSqlExprGet(pQueryInfo, 0)->uid;

  int32_t  tableIndex = COLUMN_INDEX_INITIAL_VAL;
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    if (pTableMetaInfo->pTableMeta->id.uid == uid) {
      tableIndex = i;
      break;
    }
  }

  if (tableIndex == COLUMN_INDEX_INITIAL_VAL) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  SSchema s = {.bytes = TSDB_KEYSIZE, .type = TSDB_DATA_TYPE_TIMESTAMP, .colId = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tstrncpy(s.name, aAggs[TSDB_FUNC_TS].name, sizeof(s.name));

  SColumnIndex index = {tableIndex, PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscAddFuncInSelectClause(pQueryInfo, 0, TSDB_FUNC_TS, &index, &s, TSDB_COL_NORMAL);
  return TSDB_CODE_SUCCESS;
}

static int32_t checkInvalidExprForTimeWindow(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg1 = "invalid query expression";
  const char* msg2 = "top/bottom query does not support order by value in time window query";

  // for top/bottom + interval query, we do not add additional timestamp column in the front
  if (isTopBottomQuery(pQueryInfo)) {

    // invalid sql:
    // top(col, k) from table_name [interval(1d)|session(ts, 1d)] order by k asc
    // order by normal column is not supported
    int32_t colId = pQueryInfo->order.orderColId;
    if (isTimeWindowQuery(pQueryInfo) && colId != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    return TSDB_CODE_SUCCESS;
  }

  /*
   * invalid sql:
   * select count(tbname)/count(tag1)/count(tag2) from super_table_name [interval(1d)|session(ts, 1d)];
   */
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_COUNT && TSDB_COL_IS_TAG(pExpr->colInfo.flag)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  /*
   * invalid sql:
   * select tbname, tags_fields from super_table_name [interval(1s)|session(ts,1s)]
   */
  if (tscQueryTags(pQueryInfo) && isTimeWindowQuery(pQueryInfo)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  return addPrimaryTsColumnForTimeWindowQuery(pQueryInfo);
}

int32_t parseIntervalClause(SSqlObj* pSql, SQueryInfo* pQueryInfo, SQuerySqlNode* pQuerySqlNode) {
  const char* msg2 = "interval cannot be less than 10 ms";
  const char* msg3 = "sliding cannot be used without interval";

  SSqlCmd* pCmd = &pSql->cmd;

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  if (!TPARSER_HAS_TOKEN(pQuerySqlNode->interval.interval)) {
    if (TPARSER_HAS_TOKEN(pQuerySqlNode->sliding)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    return TSDB_CODE_SUCCESS;
  }

  // orderby column not set yet, set it to be the primary timestamp column
  if (pQueryInfo->order.orderColId == INT32_MIN) {
    pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
  }

  // interval is not null
  SStrToken *t = &pQuerySqlNode->interval.interval;
  if (parseNatualDuration(t->z, t->n, &pQueryInfo->interval.interval, &pQueryInfo->interval.intervalUnit) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (pQueryInfo->interval.intervalUnit != 'n' && pQueryInfo->interval.intervalUnit != 'y') {
    // if the unit of time window value is millisecond, change the value from microsecond
    if (tinfo.precision == TSDB_TIME_PRECISION_MILLI) {
      pQueryInfo->interval.interval = pQueryInfo->interval.interval / 1000;
    }

    // interval cannot be less than 10 milliseconds
    if (pQueryInfo->interval.interval < tsMinIntervalTime) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  }

  if (parseIntervalOffset(pCmd, pQueryInfo, &pQuerySqlNode->interval.offset) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (parseSlidingClause(pCmd, pQueryInfo, &pQuerySqlNode->sliding) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  // The following part is used to check for the invalid query expression.
  return checkInvalidExprForTimeWindow(pCmd, pQueryInfo);
}

int32_t parseSessionClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySqlNode * pQuerySqlNode) {
  const char* msg1 = "gap should be fixed time window";
  const char* msg2 = "only one type time window allowed";
  const char* msg3 = "invalid column name";
  const char* msg4 = "invalid time window";

  // no session window
  if (!TPARSER_HAS_TOKEN(pQuerySqlNode->sessionVal.gap)) {
    return TSDB_CODE_SUCCESS;
  }

  SStrToken* col = &pQuerySqlNode->sessionVal.col;
  SStrToken* gap = &pQuerySqlNode->sessionVal.gap;

  char timeUnit = 0;
  if (parseNatualDuration(gap->z, gap->n, &pQueryInfo->sessionWindow.gap, &timeUnit) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  if (timeUnit == 'y' || timeUnit == 'n') {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  // if the unit of time window value is millisecond, change the value from microsecond
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  if (tinfo.precision == TSDB_TIME_PRECISION_MILLI) {
    pQueryInfo->sessionWindow.gap = pQueryInfo->sessionWindow.gap / 1000;
  }

  if (pQueryInfo->sessionWindow.gap != 0 && pQueryInfo->interval.interval != 0) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  SColumnIndex index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(pCmd, col, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  pQueryInfo->sessionWindow.primaryColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;

  // The following part is used to check for the invalid query expression.
  return checkInvalidExprForTimeWindow(pCmd, pQueryInfo);
}

int32_t parseIntervalOffset(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SStrToken* offsetToken) {
  const char* msg1 = "interval offset cannot be negative";
  const char* msg2 = "interval offset should be shorter than interval";
  const char* msg3 = "cannot use 'year' as offset when interval is 'month'";

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);

  SStrToken* t = offsetToken;
  if (t->n == 0) {
    pQueryInfo->interval.offsetUnit = pQueryInfo->interval.intervalUnit;
    pQueryInfo->interval.offset = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (parseNatualDuration(t->z, t->n, &pQueryInfo->interval.offset, &pQueryInfo->interval.offsetUnit) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (pQueryInfo->interval.offset < 0) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pQueryInfo->interval.offsetUnit != 'n' && pQueryInfo->interval.offsetUnit != 'y') {
    // if the unit of time window value is millisecond, change the value from microsecond
    if (tinfo.precision == TSDB_TIME_PRECISION_MILLI) {
      pQueryInfo->interval.offset = pQueryInfo->interval.offset / 1000;
    }
    if (pQueryInfo->interval.intervalUnit != 'n' && pQueryInfo->interval.intervalUnit != 'y') {
      if (pQueryInfo->interval.offset >= pQueryInfo->interval.interval) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
    }
  } else if (pQueryInfo->interval.offsetUnit == pQueryInfo->interval.intervalUnit) {
    if (pQueryInfo->interval.offset >= pQueryInfo->interval.interval) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  } else if (pQueryInfo->interval.intervalUnit == 'n' && pQueryInfo->interval.offsetUnit == 'y') {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
  } else if (pQueryInfo->interval.intervalUnit == 'y' && pQueryInfo->interval.offsetUnit == 'n') {
    if (pQueryInfo->interval.interval * 12 <= pQueryInfo->interval.offset) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  } else {
    // TODO: offset should be shorter than interval, but how to check
    // conflicts like 30days offset and 1 month interval
  }

  return TSDB_CODE_SUCCESS;
}

int32_t parseSlidingClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SStrToken* pSliding) {
  const char* msg0 = "sliding value too small";
  const char* msg1 = "sliding value no larger than the interval value";
  const char* msg2 = "sliding value can not less than 1% of interval value";
  const char* msg3 = "does not support sliding when interval is natural month/year";

  const static int32_t INTERVAL_SLIDING_FACTOR = 100;

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);

  if (pSliding->n == 0) {
    pQueryInfo->interval.slidingUnit = pQueryInfo->interval.intervalUnit;
    pQueryInfo->interval.sliding = pQueryInfo->interval.interval;
    return TSDB_CODE_SUCCESS;
  }

  if (pQueryInfo->interval.intervalUnit == 'n' || pQueryInfo->interval.intervalUnit == 'y') {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  parseAbsoluteDuration(pSliding->z, pSliding->n, &pQueryInfo->interval.sliding);
  if (tinfo.precision == TSDB_TIME_PRECISION_MILLI) {
    pQueryInfo->interval.sliding /= 1000;
  }

  if (pQueryInfo->interval.sliding < tsMinSlidingTime) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }

  if (pQueryInfo->interval.sliding > pQueryInfo->interval.interval) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if ((pQueryInfo->interval.interval != 0) && (pQueryInfo->interval.interval/pQueryInfo->interval.sliding > INTERVAL_SLIDING_FACTOR)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

//  if (pQueryInfo->interval.sliding != pQueryInfo->interval.interval && pSql->pStream == NULL) {
//    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
//  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscSetTableFullName(STableMetaInfo* pTableMetaInfo, SStrToken* pTableName, SSqlObj* pSql) {
  const char* msg1 = "name too long";
  const char* msg2 = "acctId too long";

  SSqlCmd* pCmd = &pSql->cmd;
  int32_t  code = TSDB_CODE_SUCCESS;

  if (hasSpecifyDB(pTableName)) { // db has been specified in sql string so we ignore current db path
    code = tNameSetAcctId(&pTableMetaInfo->name, getAccountId(pSql));
    if (code != 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
    
    char name[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, pTableName->n);

    code = tNameFromString(&pTableMetaInfo->name, name, T_NAME_DB|T_NAME_TABLE);
    if (code != 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  } else {  // get current DB name first, and then set it into path
    char* t = cloneCurrentDBName(pSql);
    if (strlen(t) == 0) {
      return TSDB_CODE_TSC_DB_NOT_SELECTED;
    }

    code = tNameFromString(&pTableMetaInfo->name, t, T_NAME_ACCT | T_NAME_DB);
    if (code != 0) {
      free(t);
      return TSDB_CODE_TSC_DB_NOT_SELECTED;
    }

    free(t);

    if (pTableName->n >= TSDB_TABLE_NAME_LEN) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, pTableName->n);

    code = tNameFromString(&pTableMetaInfo->name, name, T_NAME_TABLE);
    if (code != 0) {
      code = invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  return code;
}

static bool validateTableColumnInfo(SArray* pFieldList, SSqlCmd* pCmd) {
  assert(pFieldList != NULL);

  const char* msg = "illegal number of columns";
  const char* msg1 = "first column must be timestamp";
  const char* msg2 = "row length exceeds max length";
  const char* msg3 = "duplicated column names";
  const char* msg4 = "invalid data type";
  const char* msg5 = "invalid binary/nchar column length";
  const char* msg6 = "invalid column name";

  // number of fields no less than 2
  size_t numOfCols = taosArrayGetSize(pFieldList);
  if (numOfCols <= 1 || numOfCols > TSDB_MAX_COLUMNS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
    return false;
  }

  // first column must be timestamp
  TAOS_FIELD* pField = taosArrayGet(pFieldList, 0);
  if (pField->type != TSDB_DATA_TYPE_TIMESTAMP) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    pField = taosArrayGet(pFieldList, i);
    if (!isValidDataType(pField->type)) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
      return false;
    }

    if (pField->bytes == 0) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
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

    // field name must be unique
    if (has(pFieldList, i + 1, pField->name) == true) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return false;
    }

    nLen += pField->bytes;
  }

  // max row length must be less than TSDB_MAX_BYTES_PER_ROW
  if (nLen > TSDB_MAX_BYTES_PER_ROW) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    return false;
  }

  return true;
}

static bool validateTagParams(SArray* pTagsList, SArray* pFieldList, SSqlCmd* pCmd) {
  assert(pTagsList != NULL);

  const char* msg1 = "invalid number of tag columns";
  const char* msg2 = "tag length too long";
  const char* msg3 = "duplicated column names";
  const char* msg4 = "timestamp not allowed in tags";
  const char* msg5 = "invalid data type in tags";
  const char* msg6 = "invalid tag name";
  const char* msg7 = "invalid binary/nchar tag length";

  // number of fields at least 1
  size_t numOfTags = taosArrayGetSize(pTagsList);
  if (numOfTags < 1 || numOfTags > TSDB_MAX_TAGS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  /* timestamp in tag is not allowed */
  for (int32_t i = 0; i < numOfTags; ++i) {
    TAOS_FIELD* p = taosArrayGet(pTagsList, i);

    if (p->type == TSDB_DATA_TYPE_TIMESTAMP) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
      return false;
    }

    if (!isValidDataType(p->type)) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      return false;
    }

    if ((p->type == TSDB_DATA_TYPE_BINARY && p->bytes <= 0) ||
        (p->type == TSDB_DATA_TYPE_NCHAR && p->bytes <= 0)) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
      return false;
    }

    if (validateColumnName(p->name) != TSDB_CODE_SUCCESS) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      return false;
    }

    if (has(pTagsList, i + 1, p->name) == true) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return false;
    }
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < numOfTags; ++i) {
    TAOS_FIELD* p = taosArrayGet(pTagsList, i);
    if (p->bytes == 0) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
      return false;
    }

    nLen += p->bytes;
  }

  // max tag row length must be less than TSDB_MAX_TAGS_LEN
  if (nLen > TSDB_MAX_TAGS_LEN) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < numOfTags; ++i) {
    TAOS_FIELD* p = taosArrayGet(pTagsList, i);

    if (has(pFieldList, 0, p->name) == true) {
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

  if ((pTagField->type < TSDB_DATA_TYPE_BOOL) || (pTagField->type > TSDB_DATA_TYPE_UBIGINT)) {
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
  const char* msg4 = "invalid data type";
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

  if (pColField->type < TSDB_DATA_TYPE_BOOL || pColField->type > TSDB_DATA_TYPE_UBIGINT) {
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
static bool has(SArray* pFieldList, int32_t startIdx, const char* name) {
  size_t numOfCols = taosArrayGetSize(pFieldList);
  for (int32_t j = startIdx; j < numOfCols; ++j) {
    TAOS_FIELD* field = taosArrayGet(pFieldList, j);
    if (strncasecmp(name, field->name, sizeof(field->name) - 1) == 0) return true;
  }

  return false;
}

static char* getAccountId(SSqlObj* pSql) { return pSql->pTscObj->acctId; }

static char* cloneCurrentDBName(SSqlObj* pSql) {
  pthread_mutex_lock(&pSql->pTscObj->mutex);
  char *p = strdup(pSql->pTscObj->db);  
  pthread_mutex_unlock(&pSql->pTscObj->mutex);

  return p;
}

/* length limitation, strstr cannot be applied */
static bool hasSpecifyDB(SStrToken* pTableName) {
  for (uint32_t i = 0; i < pTableName->n; ++i) {
    if (pTableName->z[i] == TS_PATH_DELIMITER[0]) {
      return true;
    }
  }

  return false;
}

int32_t setObjFullName(char* fullName, const char* account, SStrToken* pDB, SStrToken* tableName, int32_t* xlen) {
  int32_t totalLen = 0;

  if (account != NULL) {
    int32_t len = (int32_t)strlen(account);
    strcpy(fullName, account);
    fullName[len] = TS_PATH_DELIMITER[0];
    totalLen += (len + 1);
  }

  /* db name is not specified, the tableName dose not include db name */
  if (pDB != NULL) {
    if (pDB->n >= TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN || pDB->n == 0) {
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

  if (totalLen < TSDB_TABLE_FNAME_LEN) {
    fullName[totalLen] = 0;
  }

  return (totalLen < TSDB_TABLE_FNAME_LEN) ? TSDB_CODE_SUCCESS : TSDB_CODE_TSC_INVALID_SQL;
}

void tscInsertPrimaryTsSourceColumn(SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  SColumnIndex tsCol = {.tableIndex = pIndex->tableIndex, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscColumnListInsert(pQueryInfo->colList, &tsCol);
}

static int32_t handleArithmeticExpr(SSqlCmd* pCmd, int32_t clauseIndex, int32_t exprIndex, tSqlExprItem* pItem) {
  const char* msg1 = "invalid column name, illegal column type, or columns in arithmetic expression from two tables";
  const char* msg2 = "invalid arithmetic expression in select clause";
  const char* msg3 = "tag columns can not be used in arithmetic expression";
  const char* msg4 = "columns from different table mixed up in arithmetic expression";

  // arithmetic function in select clause
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);

  SColumnList columnList = {0};
  int32_t     arithmeticType = NON_ARITHMEIC_EXPR;

  if (validateArithmeticSQLExpr(pCmd, pItem->pNode, pQueryInfo, &columnList, &arithmeticType) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  int32_t tableIndex = columnList.ids[0].tableIndex;
  if (arithmeticType == NORMAL_ARITHMETIC) {
    pQueryInfo->type |= TSDB_QUERY_TYPE_PROJECTION_QUERY;

    // all columns in arithmetic expression must belong to the same table
    for (int32_t f = 1; f < columnList.num; ++f) {
      if (columnList.ids[f].tableIndex != tableIndex) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
      }
    }

    // expr string is set as the parameter of function
    SColumnIndex index = {.tableIndex = tableIndex};

    SSqlExpr* pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_ARITHM, &index, TSDB_DATA_TYPE_DOUBLE, sizeof(double),
                                       getNewResColId(pQueryInfo), sizeof(double), false);

    char* name = (pItem->aliasName != NULL)? pItem->aliasName:pItem->pNode->token.z;
    size_t len = MIN(sizeof(pExpr->aliasName), pItem->pNode->token.n + 1);
    tstrncpy(pExpr->aliasName, name, len);

    tExprNode* pNode = NULL;
    SArray* colList = taosArrayInit(10, sizeof(SColIndex));

    int32_t ret = exprTreeFromSqlExpr(pCmd, &pNode, pItem->pNode, pQueryInfo, colList, NULL);
    if (ret != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(colList);
      tExprTreeDestroy(pNode, NULL);
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    // check for if there is a tag in the arithmetic express
    size_t numOfNode = taosArrayGetSize(colList);
    for(int32_t k = 0; k < numOfNode; ++k) {
      SColIndex* pIndex = taosArrayGet(colList, k);
      if (TSDB_COL_IS_TAG(pIndex->flag)) {
        tExprTreeDestroy(pNode, NULL);
        taosArrayDestroy(colList);

        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }
    }

    SBufferWriter bw = tbufInitWriter(NULL, false);

    TRY(0) {
        exprTreeToBinary(&bw, pNode);
      } CATCH(code) {
        tbufCloseWriter(&bw);
        UNUSED(code);
        // TODO: other error handling
      } END_TRY

    len = tbufTell(&bw);
    char* c = tbufGetData(&bw, false);

    // set the serialized binary string as the parameter of arithmetic expression
    addExprParams(pExpr, c, TSDB_DATA_TYPE_BINARY, (int32_t)len);
    insertResultField(pQueryInfo, exprIndex, &columnList, sizeof(double), TSDB_DATA_TYPE_DOUBLE, pExpr->aliasName, pExpr);

    // add ts column
    tscInsertPrimaryTsSourceColumn(pQueryInfo, &index);

    tbufCloseWriter(&bw);
    taosArrayDestroy(colList);
    tExprTreeDestroy(pNode, NULL);
  } else {
    columnList.num = 0;
    columnList.ids[0] = (SColumnIndex) {0, 0};

    char aliasName[TSDB_COL_NAME_LEN] = {0};
    if (pItem->aliasName != NULL) {
      tstrncpy(aliasName, pItem->aliasName, TSDB_COL_NAME_LEN);
    } else {
      int32_t nameLen = MIN(TSDB_COL_NAME_LEN, pItem->pNode->token.n + 1);
      tstrncpy(aliasName, pItem->pNode->token.z, nameLen);
    }

    insertResultField(pQueryInfo, exprIndex, &columnList, sizeof(double), TSDB_DATA_TYPE_DOUBLE, aliasName, NULL);

    int32_t slot = tscNumOfFields(pQueryInfo) - 1;
    SInternalField* pInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, slot);

    if (pInfo->pSqlExpr == NULL) {
      SExprInfo* pArithExprInfo = calloc(1, sizeof(SExprInfo));

      // arithmetic expression always return result in the format of double float
      pArithExprInfo->bytes      = sizeof(double);
      pArithExprInfo->interBytes = sizeof(double);
      pArithExprInfo->type       = TSDB_DATA_TYPE_DOUBLE;

      pArithExprInfo->base.functionId = TSDB_FUNC_ARITHM;
      pArithExprInfo->base.numOfParams = 1;
      pArithExprInfo->base.resColId = getNewResColId(pQueryInfo);

      int32_t ret = exprTreeFromSqlExpr(pCmd, &pArithExprInfo->pExpr, pItem->pNode, pQueryInfo, NULL, &pArithExprInfo->uid);
      if (ret != TSDB_CODE_SUCCESS) {
        tExprTreeDestroy(pArithExprInfo->pExpr, NULL);
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), "invalid expression in select clause");
      }

      pInfo->pArithExprInfo = pArithExprInfo;
    }

    SBufferWriter bw = tbufInitWriter(NULL, false);

    TRY(0) {
      exprTreeToBinary(&bw, pInfo->pArithExprInfo->pExpr);
    } CATCH(code) {
      tbufCloseWriter(&bw);
      UNUSED(code);
      // TODO: other error handling
    } END_TRY

    SSqlFuncMsg* pFuncMsg = &pInfo->pArithExprInfo->base;
    pFuncMsg->arg[0].argBytes = (int16_t) tbufTell(&bw);
    pFuncMsg->arg[0].argValue.pz = tbufGetData(&bw, true);
    pFuncMsg->arg[0].argType = TSDB_DATA_TYPE_BINARY;

//    tbufCloseWriter(&bw); // TODO there is a memory leak
  }

  return TSDB_CODE_SUCCESS;
}

static void addProjectQueryCol(SQueryInfo* pQueryInfo, int32_t startPos, SColumnIndex* pIndex, tSqlExprItem* pItem) {
  SSqlExpr* pExpr = doAddProjectCol(pQueryInfo, pIndex->columnIndex, pIndex->tableIndex);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pIndex->tableIndex);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

  SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, pIndex->columnIndex);

  char* colName = (pItem->aliasName == NULL) ? pSchema->name : pItem->aliasName;
  tstrncpy(pExpr->aliasName, colName, sizeof(pExpr->aliasName));

  SColumnList ids = {0};
  ids.num = 1;
  ids.ids[0] = *pIndex;

  if (pIndex->columnIndex == TSDB_TBNAME_COLUMN_INDEX || pIndex->columnIndex == TSDB_UD_COLUMN_INDEX ||
      pIndex->columnIndex >= tscGetNumOfColumns(pTableMeta)) {
    ids.num = 0;
  }

  insertResultField(pQueryInfo, startPos, &ids, pExpr->resBytes, (int8_t)pExpr->resType, pExpr->aliasName, pExpr);
}

static void addPrimaryTsColIntoResult(SQueryInfo* pQueryInfo) {
  // primary timestamp column has been added already
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_PRJ && pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return;
    }
  }


  // set the constant column value always attached to first table.
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, PRIMARYKEY_TIMESTAMP_COL_INDEX);

  // add the timestamp column into the output columns
  SColumnIndex index = {0};  // primary timestamp column info
  int32_t numOfCols = (int32_t)tscSqlExprNumOfExprs(pQueryInfo);
  tscAddFuncInSelectClause(pQueryInfo, numOfCols, TSDB_FUNC_PRJ, &index, pSchema, TSDB_COL_NORMAL);

  SInternalField* pSupInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, numOfCols);
  pSupInfo->visible = false;

  pQueryInfo->type |= TSDB_QUERY_TYPE_PROJECTION_QUERY;
}

bool isValidDistinctSql(SQueryInfo* pQueryInfo) {
  if (pQueryInfo == NULL) {
    return false;
  }
  if ((pQueryInfo->type & TSDB_QUERY_TYPE_STABLE_QUERY)  != TSDB_QUERY_TYPE_STABLE_QUERY) {
    return false;
  }
  if (tscQueryTags(pQueryInfo) && tscSqlExprNumOfExprs(pQueryInfo) == 1){
    return true;
  }
  return false;
}

int32_t parseSelectClause(SSqlCmd* pCmd, int32_t clauseIndex, SArray* pSelectList, bool isSTable, bool joinQuery, bool timeWindowQuery) {
  assert(pSelectList != NULL && pCmd != NULL);

  const char* msg2 = "functions or others can not be mixed up";
  const char* msg3 = "not support query expression";
  const char* msg5 = "invalid function name";
  const char* msg6 = "only support distinct one tag";

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }

  bool hasDistinct = false;
  size_t numOfExpr = taosArrayGetSize(pSelectList);
  for (int32_t i = 0; i < numOfExpr; ++i) {
    int32_t outputIndex = (int32_t)tscSqlExprNumOfExprs(pQueryInfo);
    tSqlExprItem* pItem = taosArrayGet(pSelectList, i);
     
    if (hasDistinct == false) {
       hasDistinct = (pItem->distinct == true); 
    }

    int32_t type = pItem->pNode->type;
    if (type == SQL_NODE_SQLFUNCTION) {
      pItem->pNode->functionId = isValidFunction(pItem->pNode->operand.z, pItem->pNode->operand.n);
      if (pItem->pNode->functionId < 0) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      // sql function in selection clause, append sql function info in pSqlCmd structure sequentially
      if (addExprAndResultField(pCmd, pQueryInfo, outputIndex, pItem, true) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    } else if (type == SQL_NODE_TABLE_COLUMN || type == SQL_NODE_VALUE) {
      // use the dynamic array list to decide if the function is valid or not
        // select table_name1.field_name1, table_name2.field_name2  from table_name1, table_name2
        if (addProjectionExprAndResultField(pCmd, pQueryInfo, pItem) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }
    } else if (type == SQL_NODE_EXPR) {
      int32_t code = handleArithmeticExpr(pCmd, clauseIndex, i, pItem);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    if (pQueryInfo->fieldsInfo.numOfOutput > TSDB_MAX_COLUMNS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  if (hasDistinct == true) {
    if (!isValidDistinctSql(pQueryInfo)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
    }
    pQueryInfo->distinctTag = true;
  }
  
  // there is only one user-defined column in the final result field, add the timestamp column.
  size_t numOfSrcCols = taosArrayGetSize(pQueryInfo->colList);
  if (numOfSrcCols <= 0 && !tscQueryTags(pQueryInfo) && !tscQueryBlockInfo(pQueryInfo)) {
    addPrimaryTsColIntoResult(pQueryInfo);
  }

  if (!functionCompatibleCheck(pQueryInfo, joinQuery, timeWindowQuery)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  /*
   * transfer sql functions that need secondary merge into another format
   * in dealing with super table queries such as: count/first/last
   */
  if (isSTable) {
    tscTansformFuncForSTableQuery(pQueryInfo);

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
  SInternalField* pInfo = tscFieldInfoInsert(&pQueryInfo->fieldsInfo, outputIndex, &f);
  pInfo->pSqlExpr = pSqlExpr;
  
  return TSDB_CODE_SUCCESS;
}

SSqlExpr* doAddProjectCol(SQueryInfo* pQueryInfo, int32_t colIndex, int32_t tableIndex) {
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

  int16_t colId = getNewResColId(pQueryInfo);
  return tscSqlExprAppend(pQueryInfo, functionId, &index, pSchema->type, pSchema->bytes, colId, pSchema->bytes,
                          (functionId == TSDB_FUNC_TAGPRJ));
}

SSqlExpr* tscAddFuncInSelectClause(SQueryInfo* pQueryInfo, int32_t outputColIndex, int16_t functionId,
                                  SColumnIndex* pIndex, SSchema* pColSchema, int16_t flag) {
  int16_t colId = getNewResColId(pQueryInfo);

  SSqlExpr* pExpr = tscSqlExprInsert(pQueryInfo, outputColIndex, functionId, pIndex, pColSchema->type,
                                     pColSchema->bytes, colId, pColSchema->bytes, TSDB_COL_IS_TAG(flag));
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

  return pExpr;
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
    SSqlExpr* pExpr = doAddProjectCol(pQueryInfo, j, pIndex->tableIndex);
    tstrncpy(pExpr->aliasName, pSchema[j].name, sizeof(pExpr->aliasName));

    pIndex->columnIndex = j;
    SColumnList ids = {0};
    ids.ids[0] = *pIndex;
    ids.num = 1;

    insertResultField(pQueryInfo, startPos + j, &ids, pSchema[j].bytes, pSchema[j].type, pSchema[j].name, pExpr);
  }

  return numOfTotalColumns;
}

int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExprItem* pItem) {
  const char* msg0 = "invalid column name";
  const char* msg1 = "tag for normal table query is not allowed";

  int32_t startPos = (int32_t)tscSqlExprNumOfExprs(pQueryInfo);
  int32_t optr = pItem->pNode->tokenId;

  if (optr == TK_ALL) {  // project on all fields
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_PROJECTION_QUERY);

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

    // add the primary timestamp column even though it is not required by user
    tscInsertPrimaryTsSourceColumn(pQueryInfo, &index);
  } else if (optr == TK_STRING || optr == TK_INTEGER || optr == TK_FLOAT) {  // simple column projection query
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;

    // user-specified constant value as a new result column
    index.columnIndex = (pQueryInfo->udColumnId--);
    index.tableIndex = 0;

    SSchema colSchema = tGetUserSpecifiedColumnSchema(&pItem->pNode->value, &pItem->pNode->token, pItem->aliasName);
    SSqlExpr* pExpr =
        tscAddFuncInSelectClause(pQueryInfo, startPos, TSDB_FUNC_PRJ, &index, &colSchema, TSDB_COL_UDC);

    // NOTE: the first parameter is reserved for the tag column id during join query process.
    pExpr->numOfParams = 2;
    tVariantAssign(&pExpr->param[1], &pItem->pNode->value);
  } else if (optr == TK_ID) {
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;

    if (getColumnIndexByName(pCmd, &pItem->pNode->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }

    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      SSchema* colSchema = tGetTbnameColumnSchema();
      tscAddFuncInSelectClause(pQueryInfo, startPos, TSDB_FUNC_TAGPRJ, &index, colSchema, TSDB_COL_TAG);
    } else if (index.columnIndex == TSDB_BLOCK_DIST_COLUMN_INDEX) {
      SSchema colSchema = tGetBlockDistColumnSchema();
      tscAddFuncInSelectClause(pQueryInfo, startPos, TSDB_FUNC_PRJ, &index, &colSchema, TSDB_COL_TAG);
    } else {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
      STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

      if (index.columnIndex >= tscGetNumOfColumns(pTableMeta) && UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      addProjectQueryCol(pQueryInfo, startPos, &index, pItem);
      pQueryInfo->type |= TSDB_QUERY_TYPE_PROJECTION_QUERY;
    }

    // add the primary timestamp column even though it is not required by user
    tscInsertPrimaryTsSourceColumn(pQueryInfo, &index);
  } else {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setExprInfoForFunctions(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSchema* pSchema, SConvertFunc cvtFunc,
                                       const char* name, int32_t resColIdx, SColumnIndex* pColIndex, bool finalResult) {
  const char* msg1 = "not support column types";

  int16_t type = 0;
  int16_t bytes = 0;
  int32_t functionID = cvtFunc.execFuncId;

  if (functionID == TSDB_FUNC_SPREAD) {
    int32_t t1 = pSchema->type;
    if (t1 == TSDB_DATA_TYPE_BINARY || t1 == TSDB_DATA_TYPE_NCHAR || t1 == TSDB_DATA_TYPE_BOOL) {
      invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      return -1;
    } else {
      type = TSDB_DATA_TYPE_DOUBLE;
      bytes = tDataTypes[type].bytes;
    }
  } else {
    type = pSchema->type;
    bytes = pSchema->bytes;
  }
  
  SSqlExpr* pExpr = tscSqlExprAppend(pQueryInfo, functionID, pColIndex, type, bytes, getNewResColId(pQueryInfo), bytes, false);
  tstrncpy(pExpr->aliasName, name, tListLen(pExpr->aliasName));

  if (cvtFunc.originFuncId == TSDB_FUNC_LAST_ROW && cvtFunc.originFuncId != functionID) {
    pExpr->colInfo.flag |= TSDB_COL_NULL;
  }

  // set reverse order scan data blocks for last query
  if (functionID == TSDB_FUNC_LAST) {
    pExpr->numOfParams = 1;
    pExpr->param[0].i64 = TSDB_ORDER_DESC;
    pExpr->param[0].nType = TSDB_DATA_TYPE_INT;
  }
  
  // for all queries, the timestamp column needs to be loaded
  SColumnIndex index = {.tableIndex = pColIndex->tableIndex, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscColumnListInsert(pQueryInfo->colList, &index);

  // if it is not in the final result, do not add it
  SColumnList ids = getColumnList(1, pColIndex->tableIndex, pColIndex->columnIndex);
  if (finalResult) {
    insertResultField(pQueryInfo, resColIdx, &ids, bytes, (int8_t)type, pExpr->aliasName, pExpr);
  } else {
    tscColumnListInsert(pQueryInfo->colList, &(ids.ids[0]));
  }

  return TSDB_CODE_SUCCESS;
}

void setResultColName(char* name, tSqlExprItem* pItem, int32_t functionId, SStrToken* pToken, bool multiCols) {
  if (pItem->aliasName != NULL) {
    tstrncpy(name, pItem->aliasName, TSDB_COL_NAME_LEN);
  } else if (multiCols) {
    char uname[TSDB_COL_NAME_LEN] = {0};
    int32_t len = MIN(pToken->n + 1, TSDB_COL_NAME_LEN);
    tstrncpy(uname, pToken->z, len);

    if (tsKeepOriginalColumnName) { // keep the original column name
      tstrncpy(name, uname, TSDB_COL_NAME_LEN);
    } else {
      int32_t size = TSDB_COL_NAME_LEN + tListLen(aAggs[functionId].name) + 2 + 1;
      char tmp[TSDB_COL_NAME_LEN + tListLen(aAggs[functionId].name) + 2 + 1] = {0};
      snprintf(tmp, size, "%s(%s)", aAggs[functionId].name, uname);

      tstrncpy(name, tmp, TSDB_COL_NAME_LEN);
    }
  } else  { // use the user-input result column name
    int32_t len = MIN(pItem->pNode->token.n + 1, TSDB_COL_NAME_LEN);
    tstrncpy(name, pItem->pNode->token.z, len);
  }
}

static void updateLastScanOrderIfNeeded(SQueryInfo* pQueryInfo) {
  if (pQueryInfo->sessionWindow.gap > 0 || tscGroupbyColumn(pQueryInfo)) {
    size_t numOfExpr = tscSqlExprNumOfExprs(pQueryInfo);
    for (int32_t i = 0; i < numOfExpr; ++i) {
      SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
      if (pExpr->functionId != TSDB_FUNC_LAST && pExpr->functionId != TSDB_FUNC_LAST_DST) {
        continue;
      }

      pExpr->numOfParams = 1;
      pExpr->param->i64 = TSDB_ORDER_ASC;
      pExpr->param->nType = TSDB_DATA_TYPE_INT;
    }
  }
}

int32_t addExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t colIndex, tSqlExprItem* pItem, bool finalResult) {
  STableMetaInfo* pTableMetaInfo = NULL;
  int32_t functionId = pItem->pNode->functionId;

  const char* msg1 = "not support column types";
  const char* msg2 = "invalid parameters";
  const char* msg3 = "illegal column name";
  const char* msg4 = "invalid table name";
  const char* msg5 = "parameter is out of range [0, 100]";
  const char* msg6 = "function applied to tags not allowed";
  const char* msg7 = "normal table can not apply this function";
  const char* msg8 = "multi-columns selection does not support alias column name";
  const char* msg9 = "diff can no be applied to unsigned numeric type";

  switch (functionId) {
    case TSDB_FUNC_COUNT: {
        /* more than one parameter for count() function */
      if (pItem->pNode->pParam != NULL && taosArrayGetSize(pItem->pNode->pParam) != 1) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SSqlExpr* pExpr = NULL;
      SColumnIndex index = COLUMN_INDEX_INITIALIZER;

      if (pItem->pNode->pParam != NULL) {
        tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->pParam, 0);
        SStrToken* pToken = &pParamElem->pNode->colInfo;
        int16_t sqlOptr = pParamElem->pNode->tokenId;
        if ((pToken->z == NULL || pToken->n == 0) 
            && (TK_INTEGER != sqlOptr)) /*select count(1) from table*/ {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
        } 
        if (sqlOptr == TK_ALL) {
          // select table.*
          // check if the table name is valid or not
          SStrToken tmpToken = pParamElem->pNode->colInfo;

          if (getTableIndexByName(&tmpToken, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
          }

          index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
          int32_t size = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
          pExpr = tscSqlExprAppend(pQueryInfo, functionId, &index, TSDB_DATA_TYPE_BIGINT, size, getNewResColId(pQueryInfo), size, false);
        } else if (sqlOptr == TK_INTEGER) { // select count(1) from table1
          char buf[8] = {0};  
          int64_t val = -1;
          tVariant* pVariant = &pParamElem->pNode->value;
          if (pVariant->nType == TSDB_DATA_TYPE_BIGINT) {
            tVariantDump(pVariant, buf, TSDB_DATA_TYPE_BIGINT, true);
            val = GET_INT64_VAL(buf); 
          }
          if (val == 1) {
            index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
            int32_t size = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
            pExpr = tscSqlExprAppend(pQueryInfo, functionId, &index, TSDB_DATA_TYPE_BIGINT, size, getNewResColId(pQueryInfo), size, false);
          } else {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }
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

          int32_t size = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
          pExpr = tscSqlExprAppend(pQueryInfo, functionId, &index, TSDB_DATA_TYPE_BIGINT, size, getNewResColId(pQueryInfo), size, isTag);
        }
      } else {  // count(*) is equalled to count(primary_timestamp_key)
        index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
        int32_t size = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
        pExpr = tscSqlExprAppend(pQueryInfo, functionId, &index, TSDB_DATA_TYPE_BIGINT, size, getNewResColId(pQueryInfo), size, false);
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
        tscInsertPrimaryTsSourceColumn(pQueryInfo, &index);
      }

      return TSDB_CODE_SUCCESS;
    }
    case TSDB_FUNC_SUM:
    case TSDB_FUNC_AVG:
    case TSDB_FUNC_RATE:
    case TSDB_FUNC_IRATE:
    case TSDB_FUNC_SUM_RATE:
    case TSDB_FUNC_SUM_IRATE:
    case TSDB_FUNC_AVG_RATE:
    case TSDB_FUNC_AVG_IRATE:
    case TSDB_FUNC_TWA:
    case TSDB_FUNC_MIN:
    case TSDB_FUNC_MAX:
    case TSDB_FUNC_DIFF:
    case TSDB_FUNC_STDDEV:
    case TSDB_FUNC_LEASTSQR: {
      // 1. valid the number of parameters
      if (pItem->pNode->pParam == NULL || (functionId != TSDB_FUNC_LEASTSQR && taosArrayGetSize(pItem->pNode->pParam) != 1) ||
          (functionId == TSDB_FUNC_LEASTSQR && taosArrayGetSize(pItem->pNode->pParam) != 3)) {
        /* no parameters or more than one parameter for function */
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->pParam, 0);
      if (pParamElem->pNode->tokenId != TK_ALL && pParamElem->pNode->tokenId != TK_ID) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if ((getColumnIndexByName(pCmd, &pParamElem->pNode->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      // 2. check if sql function can be applied on this column data type
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
      SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);
      int16_t  colType = pSchema->type;

      if (!IS_NUMERIC_TYPE(colType)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      } else if (IS_UNSIGNED_NUMERIC_TYPE(colType) && functionId == TSDB_FUNC_DIFF) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg9);
      }

      int16_t resultType = 0;
      int16_t resultSize = 0;
      int32_t intermediateResSize = 0;

      if (getResultDataInfo(pSchema->type, pSchema->bytes, functionId, 0, &resultType, &resultSize,
                            &intermediateResSize, 0, false) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      // set the first column ts for diff query
      if (functionId == TSDB_FUNC_DIFF) {
        colIndex += 1;
        SColumnIndex indexTS = {.tableIndex = index.tableIndex, .columnIndex = 0};
        SSqlExpr* pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &indexTS, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE,
                                           getNewResColId(pQueryInfo), TSDB_KEYSIZE, false);

        SColumnList ids = getColumnList(1, 0, 0);
        insertResultField(pQueryInfo, 0, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, aAggs[TSDB_FUNC_TS_DUMMY].name, pExpr);
      }

      // functions can not be applied to tags
      if (index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      SSqlExpr* pExpr = tscSqlExprAppend(pQueryInfo, functionId, &index, resultType, resultSize, getNewResColId(pQueryInfo), resultSize, false);

      if (functionId == TSDB_FUNC_LEASTSQR) {
        /* set the leastsquares parameters */
        char val[8] = {0};
        if (tVariantDump(&pParamElem[1].pNode->value, val, TSDB_DATA_TYPE_DOUBLE, true) < 0) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }

        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, DOUBLE_BYTES);

        memset(val, 0, tListLen(val));
        if (tVariantDump(&pParamElem[2].pNode->value, val, TSDB_DATA_TYPE_DOUBLE, true) < 0) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }

        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
      }

      SColumnList ids = {0};
      ids.num = 1;
      ids.ids[0] = index;
  
      memset(pExpr->aliasName, 0, tListLen(pExpr->aliasName));
      getColumnName(pItem, pExpr->aliasName, sizeof(pExpr->aliasName) - 1);
  
      if (finalResult) {
        int32_t numOfOutput = tscNumOfFields(pQueryInfo);
        insertResultField(pQueryInfo, numOfOutput, &ids, pExpr->resBytes, (int32_t)pExpr->resType, pExpr->aliasName, pExpr);
      } else {
        for (int32_t i = 0; i < ids.num; ++i) {
          tscColumnListInsert(pQueryInfo->colList, &(ids.ids[i]));
        }
      }

      tscInsertPrimaryTsSourceColumn(pQueryInfo, &index);
      return TSDB_CODE_SUCCESS;
    }
    case TSDB_FUNC_FIRST:
    case TSDB_FUNC_LAST:
    case TSDB_FUNC_SPREAD:
    case TSDB_FUNC_LAST_ROW:
    case TSDB_FUNC_INTERP: {
      bool requireAllFields = (pItem->pNode->pParam == NULL);

      // NOTE: has time range condition or normal column filter condition, the last_row query will be transferred to last query
      SConvertFunc cvtFunc = {.originFuncId = functionId, .execFuncId = functionId};
      if (functionId == TSDB_FUNC_LAST_ROW && ((!TSWINDOW_IS_EQUAL(pQueryInfo->window, TSWINDOW_INITIALIZER)) || (hasNormalColumnFilter(pQueryInfo)))) {
        cvtFunc.execFuncId = TSDB_FUNC_LAST;
      }

      if (!requireAllFields) {
        if (taosArrayGetSize(pItem->pNode->pParam) < 1) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        if (taosArrayGetSize(pItem->pNode->pParam) > 1 && (pItem->aliasName != NULL && strlen(pItem->aliasName) > 0)) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
        }

        /* in first/last function, multiple columns can be add to resultset */
        for (int32_t i = 0; i < taosArrayGetSize(pItem->pNode->pParam); ++i) {
          tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->pParam, i);
          if (pParamElem->pNode->tokenId != TK_ALL && pParamElem->pNode->tokenId != TK_ID) {
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }

          SColumnIndex index = COLUMN_INDEX_INITIALIZER;

          if (pParamElem->pNode->tokenId == TK_ALL) { // select table.*
            SStrToken tmpToken = pParamElem->pNode->colInfo;

            if (getTableIndexByName(&tmpToken, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
              return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
            }

            pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
            SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

            char name[TSDB_COL_NAME_LEN] = {0};
            for (int32_t j = 0; j < tscGetNumOfColumns(pTableMetaInfo->pTableMeta); ++j) {
              index.columnIndex = j;
              SStrToken t = {.z = pSchema[j].name, .n = (uint32_t)strnlen(pSchema[j].name, TSDB_COL_NAME_LEN)};
              setResultColName(name, pItem, cvtFunc.originFuncId, &t, true);

              if (setExprInfoForFunctions(pCmd, pQueryInfo, &pSchema[j], cvtFunc, name, colIndex++, &index, finalResult) != 0) {
                return TSDB_CODE_TSC_INVALID_SQL;
              }
            }

          } else {
            if (getColumnIndexByName(pCmd, &pParamElem->pNode->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
              return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
            }

            pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);

            // functions can not be applied to tags
            if ((index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) || (index.columnIndex < 0)) {
              return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
            }

            char name[TSDB_COL_NAME_LEN] = {0};
            SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);

            bool multiColOutput = taosArrayGetSize(pItem->pNode->pParam) > 1;
            setResultColName(name, pItem, cvtFunc.originFuncId, &pParamElem->pNode->colInfo, multiColOutput);

            if (setExprInfoForFunctions(pCmd, pQueryInfo, pSchema, cvtFunc, name, colIndex + i, &index, finalResult) != 0) {
              return TSDB_CODE_TSC_INVALID_SQL;
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

            char name[TSDB_COL_NAME_LEN] = {0};
            SStrToken t = {.z = pSchema[i].name, .n = (uint32_t)strnlen(pSchema[i].name, TSDB_COL_NAME_LEN)};
            setResultColName(name, pItem, cvtFunc.originFuncId, &t, true);

            if (setExprInfoForFunctions(pCmd, pQueryInfo, &pSchema[index.columnIndex], cvtFunc, name, colIndex, &index, finalResult) != 0) {
              return TSDB_CODE_TSC_INVALID_SQL;
            }
            colIndex++;
          }

          numOfFields += tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
        }

        return TSDB_CODE_SUCCESS;
      }
    }

    case TSDB_FUNC_TOP:
    case TSDB_FUNC_BOTTOM:
    case TSDB_FUNC_PERCT:
    case TSDB_FUNC_APERCT: {
      // 1. valid the number of parameters
      if (pItem->pNode->pParam == NULL || taosArrayGetSize(pItem->pNode->pParam) != 2) {
        /* no parameters or more than one parameter for function */
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->pParam, 0);
      if (pParamElem->pNode->tokenId != TK_ID) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
      
      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByName(pCmd, &pParamElem->pNode->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }
      
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
      SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

      // functions can not be applied to tags
      if (index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      // 2. valid the column type
      int16_t colType = pSchema[index.columnIndex].type;
      if (!IS_NUMERIC_TYPE(colType)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      // 3. valid the parameters
      if (pParamElem[1].pNode->tokenId == TK_ID) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tVariant* pVariant = &pParamElem[1].pNode->value;

      int8_t  resultType = pSchema[index.columnIndex].type;
      int16_t resultSize = pSchema[index.columnIndex].bytes;

      char    val[8] = {0};
      SSqlExpr* pExpr = NULL;
      
      if (functionId == TSDB_FUNC_PERCT || functionId == TSDB_FUNC_APERCT) {
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
        tscInsertPrimaryTsSourceColumn(pQueryInfo, &index);
        colIndex += 1;  // the first column is ts

        pExpr = tscSqlExprAppend(pQueryInfo, functionId, &index, resultType, resultSize, getNewResColId(pQueryInfo), resultSize, false);
        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
      } else {
        tVariantDump(pVariant, val, TSDB_DATA_TYPE_BIGINT, true);

        int64_t nTop = GET_INT32_VAL(val);
        if (nTop <= 0 || nTop > 100) {  // todo use macro
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
        }

        // todo REFACTOR
        // set the first column ts for top/bottom query
        SColumnIndex index1 = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
        pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS, &index1, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE, getNewResColId(pQueryInfo),
                                 TSDB_KEYSIZE, false);
        tstrncpy(pExpr->aliasName, aAggs[TSDB_FUNC_TS].name, sizeof(pExpr->aliasName));

        const int32_t TS_COLUMN_INDEX = PRIMARYKEY_TIMESTAMP_COL_INDEX;
        SColumnList   ids = getColumnList(1, 0, TS_COLUMN_INDEX);
        insertResultField(pQueryInfo, TS_COLUMN_INDEX, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP,
                          aAggs[TSDB_FUNC_TS].name, pExpr);

        colIndex += 1;  // the first column is ts

        pExpr = tscSqlExprAppend(pQueryInfo, functionId, &index, resultType, resultSize, getNewResColId(pQueryInfo), resultSize, false);
        addExprParams(pExpr, val, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
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
    
    case TSDB_FUNC_TID_TAG: {
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
      if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
      }
    
      // no parameters or more than one parameter for function
      if (pItem->pNode->pParam == NULL || taosArrayGetSize(pItem->pNode->pParam) != 1) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
      
      tSqlExprItem* pParamItem = taosArrayGet(pItem->pNode->pParam, 0);
      tSqlExpr* pParam = pParamItem->pNode;

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
        s = *tGetTbnameColumnSchema();
      } else {
        s = pTagSchema[index.columnIndex];
      }
      
      int16_t bytes = 0;
      int16_t type  = 0;
      int32_t inter = 0;

      int32_t ret = getResultDataInfo(s.type, s.bytes, TSDB_FUNC_TID_TAG, 0, &type, &bytes, &inter, 0, 0);
      assert(ret == TSDB_CODE_SUCCESS);
      
      s.type = (uint8_t)type;
      s.bytes = bytes;

      TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);
      tscAddFuncInSelectClause(pQueryInfo, 0, TSDB_FUNC_TID_TAG, &index, &s, TSDB_COL_TAG);
      
      return TSDB_CODE_SUCCESS;
    }
    case TSDB_FUNC_BLKINFO: {
      // no parameters or more than one parameter for function
      if (pItem->pNode->pParam != NULL && taosArrayGetSize(pItem->pNode->pParam) != 0) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SColumnIndex index = {.tableIndex = 0, .columnIndex = TSDB_BLOCK_DIST_COLUMN_INDEX,};
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);

      SSchema s = {.name = "block_dist", .type = TSDB_DATA_TYPE_BINARY};
      int32_t inter   = 0;
      int16_t resType = 0;
      int16_t bytes   = 0;
      getResultDataInfo(TSDB_DATA_TYPE_INT, 4, TSDB_FUNC_BLKINFO, 0, &resType, &bytes, &inter, 0, 0);

      s.bytes = bytes;
      s.type = (uint8_t)resType;
      SSqlExpr* pExpr = tscAddFuncInSelectClause(pQueryInfo, 0, TSDB_FUNC_BLKINFO, &index, &s, TSDB_COL_TAG);
      pExpr->numOfParams = 1;
      pExpr->param[0].i64 = pTableMetaInfo->pTableMeta->tableInfo.rowSize;
      pExpr->param[0].nType = TSDB_DATA_TYPE_BIGINT;

      return TSDB_CODE_SUCCESS;
    }

    default:
      return TSDB_CODE_TSC_INVALID_SQL;
  }
  
}

// todo refactor
static SColumnList getColumnList(int32_t num, int16_t tableIndex, int32_t columnIndex) {
  assert(num == 1 && tableIndex >= 0);

  SColumnList columnList = {0};
  columnList.num = num;

  int32_t index = num - 1;
  columnList.ids[index].tableIndex = tableIndex;
  columnList.ids[index].columnIndex = columnIndex;

  return columnList;
}

void getColumnName(tSqlExprItem* pItem, char* resultFieldName, int32_t nameLength) {
  if (pItem->aliasName != NULL) {
    strncpy(resultFieldName, pItem->aliasName, nameLength);
  } else {
    int32_t len = ((int32_t)pItem->pNode->token.n < nameLength) ? (int32_t)pItem->pNode->token.n : nameLength;
    strncpy(resultFieldName, pItem->pNode->token.z, len);
  }
}

static bool isTablenameToken(SStrToken* token) {
  SStrToken tmpToken = *token;
  SStrToken tableToken = {0};

  extractTableNameFromToken(&tmpToken, &tableToken);

  return (strncasecmp(TSQL_TBNAME_L, tmpToken.z, tmpToken.n) == 0 && tmpToken.n == strlen(TSQL_TBNAME_L));
}
static bool isTableBlockDistToken(SStrToken* token) {
  SStrToken tmpToken = *token;
  SStrToken tableToken = {0};

  extractTableNameFromToken(&tmpToken, &tableToken);

  return (strncasecmp(TSQL_BLOCK_DIST, tmpToken.z, tmpToken.n) == 0 && tmpToken.n == strlen(TSQL_BLOCK_DIST_L));
}

static int16_t doGetColumnIndex(SQueryInfo* pQueryInfo, int32_t index, SStrToken* pToken) {
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

int32_t doGetColumnIndexByName(SSqlCmd* pCmd, SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  const char* msg0 = "ambiguous column name";
  const char* msg1 = "invalid column name";

  if (isTablenameToken(pToken)) {
    pIndex->columnIndex = TSDB_TBNAME_COLUMN_INDEX;
  } else if (isTableBlockDistToken(pToken))  {
    pIndex->columnIndex = TSDB_BLOCK_DIST_COLUMN_INDEX; 
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

int32_t getTableIndexImpl(SStrToken* pTableToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  if (pTableToken->n == 0) {  // only one table and no table name prefix in column name
    if (pQueryInfo->numOfTables == 1) {
      pIndex->tableIndex = 0;
    } else {
      pIndex->tableIndex = COLUMN_INDEX_INITIAL_VAL;
    }

    return TSDB_CODE_SUCCESS;
  }

  pIndex->tableIndex = COLUMN_INDEX_INITIAL_VAL;
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    char* name = pTableMetaInfo->aliasName;
    if (strncasecmp(name, pTableToken->z, pTableToken->n) == 0 && strlen(name) == pTableToken->n) {
      pIndex->tableIndex = i;
      break;
    }
  }

  if (pIndex->tableIndex < 0) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getTableIndexByName(SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  SStrToken tableToken = {0};
  extractTableNameFromToken(pToken, &tableToken);

  if (getTableIndexImpl(&tableToken, pQueryInfo, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getColumnIndexByName(SSqlCmd* pCmd, const SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  if (pQueryInfo->pTableMetaInfo == NULL || pQueryInfo->numOfTables == 0) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  SStrToken tmpToken = *pToken;

  if (getTableIndexByName(&tmpToken, pQueryInfo, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  return doGetColumnIndexByName(pCmd, &tmpToken, pQueryInfo, pIndex);
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
   * database prefix in pInfo->pMiscInfo->a[0]
   * wildcard in like clause in pInfo->pMiscInfo->a[1]
   */
  SShowInfo* pShowInfo = &pInfo->pMiscInfo->showOpt;
  int16_t    showType = pShowInfo->showType;
  if (showType == TSDB_MGMT_TABLE_TABLE || showType == TSDB_MGMT_TABLE_METRIC || showType == TSDB_MGMT_TABLE_VGROUP) {
    // db prefix in tagCond, show table conds in payload
    SStrToken* pDbPrefixToken = &pShowInfo->prefix;
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

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pDbPrefixToken);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }
    }

    // show table/stable like 'xxxx', set the like pattern for show tables
    SStrToken* pPattern = &pShowInfo->pattern;
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
    SStrToken* pDnodeIp = &pShowInfo->prefix;
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
  
  SStrToken* idStr = &(pInfo->pMiscInfo->id);
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

  in_addr_t epAddr = taosInetAddr(tmp);

  return epAddr != INADDR_NONE;
}

int32_t tscTansformFuncForSTableQuery(SQueryInfo* pQueryInfo) {
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
        (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_STDDEV_DST) ||
        (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_AVG_IRATE)) {
      if (getResultDataInfo(pSrcSchema->type, pSrcSchema->bytes, functionId, (int32_t)pExpr->param[0].i64, &type, &bytes,
                            &interBytes, 0, true) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      tscSqlExprUpdate(pQueryInfo, k, functionId, pExpr->colInfo.colIndex, TSDB_DATA_TYPE_BINARY, bytes);
      // todo refactor
      pExpr->interBytes = interBytes;
    }
  }

  tscFieldInfoUpdateOffset(pQueryInfo);
  return TSDB_CODE_SUCCESS;
}

/* transfer the field-info back to original input format */
void tscRestoreFuncForSTableQuery(SQueryInfo* pQueryInfo) {
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
    } else if (functionId == TSDB_FUNC_STDDEV_DST) {
      functionId = TSDB_FUNC_STDDEV;
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
    if ((aAggs[functionId].status & TSDB_FUNCSTATE_STABLE) == 0) {
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

static bool groupbyTagsOrNull(SQueryInfo* pQueryInfo) {
  if (pQueryInfo->groupbyExpr.columnInfo == NULL ||
    taosArrayGetSize(pQueryInfo->groupbyExpr.columnInfo) == 0) {
    return true;
  }

  size_t s = taosArrayGetSize(pQueryInfo->groupbyExpr.columnInfo);
  for (int32_t i = 0; i < s; i++) {
    SColIndex* colIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, i);
    if (colIndex->flag != TSDB_COL_TAG) {
      return false;
    }
  }

  return true;
}

static bool functionCompatibleCheck(SQueryInfo* pQueryInfo, bool joinQuery, bool twQuery) {
  int32_t startIdx = 0;

  size_t numOfExpr = tscSqlExprNumOfExprs(pQueryInfo);
  assert(numOfExpr > 0);

  SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, startIdx);

  // ts function can be simultaneously used with any other functions.
  int32_t functionID = pExpr->functionId;
  if (functionID == TSDB_FUNC_TS || functionID == TSDB_FUNC_TS_DUMMY) {
    startIdx++;
  }

  int32_t factor = functionCompatList[tscSqlExprGet(pQueryInfo, startIdx)->functionId];

  if (tscSqlExprGet(pQueryInfo, 0)->functionId == TSDB_FUNC_LAST_ROW && (joinQuery || twQuery || !groupbyTagsOrNull(pQueryInfo))) {
    return false;
  }

  // diff function cannot be executed with other function
  // arithmetic function can be executed with other arithmetic functions
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  for (int32_t i = startIdx + 1; i < size; ++i) {
    SSqlExpr* pExpr1 = tscSqlExprGet(pQueryInfo, i);

    int16_t functionId = pExpr1->functionId;
    if (functionId == TSDB_FUNC_TAGPRJ || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (functionId == TSDB_FUNC_PRJ && (pExpr1->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX || TSDB_COL_IS_UD_COL(pExpr1->colInfo.flag))) {
      continue;
    }

    if (functionCompatList[functionId] != factor) {
      return false;
    } else {
      if (factor == -1) { // two functions with the same -1 flag
        return false;
      }
    }

    if (functionId == TSDB_FUNC_LAST_ROW && (joinQuery || twQuery || !groupbyTagsOrNull(pQueryInfo))) {
      return false;
    }
  }

  return true;
}

int32_t parseGroupbyClause(SQueryInfo* pQueryInfo, SArray* pList, SSqlCmd* pCmd) {
  const char* msg1 = "too many columns in group by clause";
  const char* msg2 = "invalid column name in group by clause";
  const char* msg3 = "columns from one table allowed as group by columns";
  const char* msg4 = "join query does not support group by";
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
  
  pQueryInfo->groupbyExpr.numOfGroupCols = (int16_t)taosArrayGetSize(pList);
  if (pQueryInfo->groupbyExpr.numOfGroupCols > TSDB_MAX_TAGS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pQueryInfo->numOfTables > 1) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  STableMeta* pTableMeta = NULL;
  SSchema*    pSchema = NULL;

  int32_t tableIndex = COLUMN_INDEX_INITIAL_VAL;

  size_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    tVariantListItem * pItem = taosArrayGet(pList, i);
    tVariant* pVar = &pItem->pVar;

    SStrToken token = {pVar->nLen, pVar->nType, pVar->pz};

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(pCmd, &token, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    if (tableIndex == COLUMN_INDEX_INITIAL_VAL) {
      tableIndex = index.tableIndex;
    } else if (tableIndex != index.tableIndex) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
    pTableMeta = pTableMetaInfo->pTableMeta;
  
    int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      pSchema = tGetTbnameColumnSchema();
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
      if (pSchema->type == TSDB_DATA_TYPE_TIMESTAMP || pSchema->type == TSDB_DATA_TYPE_FLOAT || pSchema->type == TSDB_DATA_TYPE_DOUBLE) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
      }

      tscColumnListInsert(pQueryInfo->colList, &index);
      
      SColIndex colIndex = { .colIndex = index.columnIndex, .flag = TSDB_COL_NORMAL, .colId = pSchema->colId };
      taosArrayPush(pGroupExpr->columnInfo, &colIndex);
      pQueryInfo->groupbyExpr.orderType = TSDB_ORDER_ASC;

      if (i == 0 && num > 1) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
      }
    }
  }

  pQueryInfo->groupbyExpr.tableIndex = tableIndex;
  return TSDB_CODE_SUCCESS;
}

static SColumnFilterInfo* addColumnFilterInfo(SColumn* pColumn) {
  if (pColumn == NULL) {
    return NULL;
  }

  int32_t size = pColumn->numOfFilters + 1;

  char* tmp = (char*) realloc((void*)(pColumn->filterInfo), sizeof(SColumnFilterInfo) * (size));
  if (tmp != NULL) {
    pColumn->filterInfo = (SColumnFilterInfo*)tmp;
  } else {
    return NULL;
  }

  pColumn->numOfFilters++;

  SColumnFilterInfo* pColFilterInfo = &pColumn->filterInfo[pColumn->numOfFilters - 1];
  memset(pColFilterInfo, 0, sizeof(SColumnFilterInfo));

  return pColFilterInfo;
}

static int32_t doExtractColumnFilterInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SColumnFilterInfo* pColumnFilter,
                                         SColumnIndex* columnIndex, tSqlExpr* pExpr) {
  const char* msg = "not supported filter condition";

  tSqlExpr*       pRight = pExpr->pRight;
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, columnIndex->tableIndex);

  SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, columnIndex->columnIndex);

  int16_t colType = pSchema->type;
  if (colType >= TSDB_DATA_TYPE_TINYINT && colType <= TSDB_DATA_TYPE_BIGINT) {
    colType = TSDB_DATA_TYPE_BIGINT;
  } else if (colType == TSDB_DATA_TYPE_FLOAT || colType == TSDB_DATA_TYPE_DOUBLE) {
    colType = TSDB_DATA_TYPE_DOUBLE;
  } else if ((colType == TSDB_DATA_TYPE_TIMESTAMP) && (TSDB_DATA_TYPE_BINARY == pRight->value.nType)) {
    int retVal = setColumnFilterInfoForTimestamp(pCmd, pQueryInfo, &pRight->value);
    if (TSDB_CODE_SUCCESS != retVal) {
      return retVal;
    }
  }

  int32_t retVal = TSDB_CODE_SUCCESS;
  if (pExpr->tokenId == TK_LE || pExpr->tokenId == TK_LT) {
    retVal = tVariantDump(&pRight->value, (char*)&pColumnFilter->upperBndd, colType, false);

  // TK_GT,TK_GE,TK_EQ,TK_NE are based on the pColumn->lowerBndd
  } else if (colType == TSDB_DATA_TYPE_BINARY) {
    pColumnFilter->pz = (int64_t)calloc(1, pRight->value.nLen + TSDB_NCHAR_SIZE);
    pColumnFilter->len = pRight->value.nLen;
    retVal = tVariantDump(&pRight->value, (char*)pColumnFilter->pz, colType, false);

  } else if (colType == TSDB_DATA_TYPE_NCHAR) {
    // pRight->value.nLen + 1 is larger than the actual nchar string length
    pColumnFilter->pz = (int64_t)calloc(1, (pRight->value.nLen + 1) * TSDB_NCHAR_SIZE);
    retVal = tVariantDump(&pRight->value, (char*)pColumnFilter->pz, colType, false);
    size_t len = twcslen((wchar_t*)pColumnFilter->pz);
    pColumnFilter->len = len * TSDB_NCHAR_SIZE;

  } else {
    retVal = tVariantDump(&pRight->value, (char*)&pColumnFilter->lowerBndd, colType, false);
  }

  if (retVal != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  } 

  switch (pExpr->tokenId) {
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
    case TK_ISNULL:
      pColumnFilter->lowerRelOptr = TSDB_RELATION_ISNULL;
      break;
    case TK_NOTNULL:
      pColumnFilter->lowerRelOptr = TSDB_RELATION_NOTNULL;
      break;
    default:
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SCondExpr {
  tSqlExpr* pTagCond;
  tSqlExpr* pTimewindow;

  tSqlExpr* pColumnCond;

  tSqlExpr* pTableCond;
  int16_t   relType;  // relation between table name in expression and other tag
                      // filter condition expression, TK_AND or TK_OR
  int16_t tableCondIndex;

  tSqlExpr* pJoinExpr;  // join condition
  bool      tsJoin;
} SCondExpr;

static int32_t getTimeRange(STimeWindow* win, tSqlExpr* pRight, int32_t optr, int16_t timePrecision);

static int32_t tablenameListToString(tSqlExpr* pExpr, SStringBuilder* sb) {
  SArray* pList = pExpr->pParam;

  int32_t size = (int32_t) taosArrayGetSize(pList);
  if (size <= 0) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (size > 0) {
    taosStringBuilderAppendStringLen(sb, QUERY_COND_REL_PREFIX_IN, QUERY_COND_REL_PREFIX_IN_LEN);
  }

  for (int32_t i = 0; i < size; ++i) {
    tSqlExprItem* pSub = taosArrayGet(pList, i);
    tVariant* pVar = &pSub->pNode->value;

    taosStringBuilderAppendStringLen(sb, pVar->pz, pVar->nLen);

    if (i < size - 1) {
      taosStringBuilderAppendString(sb, TBNAME_LIST_SEP);
    }

    if (pVar->nLen <= 0 || !tscValidateTableNameLength(pVar->nLen)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tablenameCondToString(tSqlExpr* pExpr, SStringBuilder* sb) {
  taosStringBuilderAppendStringLen(sb, QUERY_COND_REL_PREFIX_LIKE, QUERY_COND_REL_PREFIX_LIKE_LEN);
  taosStringBuilderAppendString(sb, pExpr->value.pz);

  return TSDB_CODE_SUCCESS;
}

enum {
  TSQL_EXPR_TS = 0,
  TSQL_EXPR_TAG = 1,
  TSQL_EXPR_COLUMN = 2,
  TSQL_EXPR_TBNAME = 3,
};

static int32_t extractColumnFilterInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SColumnIndex* pIndex, tSqlExpr* pExpr, int32_t sqlOptr) {
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

    if (pColFilter == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  } else if (sqlOptr == TK_OR) {
    // TODO fixme: failed to invalid the filter expression: "col1 = 1 OR col2 = 2"
    pColFilter = addColumnFilterInfo(pColumn);
    if (pColFilter == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  } else {  // error;
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  pColFilter->filterstr =
      ((pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) ? 1 : 0);

  if (pColFilter->filterstr) {
    if (pExpr->tokenId != TK_EQ
      && pExpr->tokenId != TK_NE
      && pExpr->tokenId != TK_ISNULL
      && pExpr->tokenId != TK_NOTNULL
      && pExpr->tokenId != TK_LIKE
      ) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  } else {
    if (pExpr->tokenId == TK_LIKE) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
    
    if (pSchema->type == TSDB_DATA_TYPE_BOOL) {
      if (pExpr->tokenId != TK_EQ && pExpr->tokenId != TK_NE) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }
    }
  }

  pColumn->colIndex = *pIndex;
  return doExtractColumnFilterInfo(pCmd, pQueryInfo, pColFilter, pIndex, pExpr);
}

static int32_t getTablenameCond(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr* pTableCond, SStringBuilder* sb) {
  const char* msg0 = "invalid table name list";
  const char* msg1 = "not string following like";

  if (pTableCond == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSqlExpr* pLeft = pTableCond->pLeft;
  tSqlExpr* pRight = pTableCond->pRight;

  if (!isTablenameToken(&pLeft->colInfo)) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  int32_t ret = TSDB_CODE_SUCCESS;

  if (pTableCond->tokenId == TK_IN) {
    ret = tablenameListToString(pRight, sb);
  } else if (pTableCond->tokenId == TK_LIKE) {
    if (pRight->tokenId != TK_STRING) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
    
    ret = tablenameCondToString(pRight, sb);
  }

  if (ret != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }

  return ret;
}

static int32_t getColumnQueryCondInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr* pExpr, int32_t relOptr) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!tSqlExprIsParentOfLeaf(pExpr)) {  // internal node
    int32_t ret = getColumnQueryCondInfo(pCmd, pQueryInfo, pExpr->pLeft, pExpr->tokenId);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    return getColumnQueryCondInfo(pCmd, pQueryInfo, pExpr->pRight, pExpr->tokenId);
  } else {  // handle leaf node
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(pCmd, &pExpr->pLeft->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    return extractColumnFilterInfo(pCmd, pQueryInfo, &index, pExpr, relOptr);
  }
}

static int32_t getJoinCondInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr* pExpr) {
  const char* msg1 = "invalid join query condition";
  const char* msg2 = "invalid table name in join query";
  const char* msg3 = "type of join columns must be identical";
  const char* msg4 = "invalid column name in join condition";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!tSqlExprIsParentOfLeaf(pExpr)) {
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

  pLeft->uid = pTableMetaInfo->pTableMeta->id.uid;
  pLeft->tagColId = pTagSchema1->colId;

  int32_t code = tNameExtractFullName(&pTableMetaInfo->name, pLeft->tableName);
  if (code != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  index = (SColumnIndex)COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(pCmd, &pExpr->pRight->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
  SSchema* pTagSchema2 = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);

  pRight->uid = pTableMetaInfo->pTableMeta->id.uid;
  pRight->tagColId = pTagSchema2->colId;

  code = tNameExtractFullName(&pTableMetaInfo->name, pRight->tableName);
  if (code != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  if (pTagSchema1->type != pTagSchema2->type) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  pTagCond->joinInfo.hasJoin = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t validateSQLExpr(SSqlCmd* pCmd, tSqlExpr* pExpr, SQueryInfo* pQueryInfo, SColumnList* pList,
                               int32_t* type, uint64_t* uid) {
  if (pExpr->type == SQL_NODE_TABLE_COLUMN) {
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
  } else if (pExpr->tokenId == TK_FLOAT && (isnan(pExpr->value.dKey) || isinf(pExpr->value.dKey))) {
    return TSDB_CODE_TSC_INVALID_SQL;
  } else if (pExpr->type == SQL_NODE_SQLFUNCTION) {
    if (*type == NON_ARITHMEIC_EXPR) {
      *type = AGG_ARIGHTMEIC;
    } else if (*type == NORMAL_ARITHMETIC) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    int32_t outputIndex = (int32_t)tscSqlExprNumOfExprs(pQueryInfo);
  
    tSqlExprItem item = {.pNode = pExpr, .aliasName = NULL};
  
    // sql function list in selection clause.
    // Append the sqlExpr into exprList of pQueryInfo structure sequentially
    pExpr->functionId = isValidFunction(pExpr->operand.z, pExpr->operand.n);
    if (pExpr->functionId < 0) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    if (addExprAndResultField(pCmd, pQueryInfo, outputIndex, &item, false) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    // It is invalid in case of more than one sqlExpr, such as first(ts, k) - last(ts, k)
    int32_t inc = (int32_t) tscSqlExprNumOfExprs(pQueryInfo) - outputIndex;
    if (inc > 1) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    // Not supported data type in arithmetic expression
    uint64_t id = -1;
    for(int32_t i = 0; i < inc; ++i) {
      SSqlExpr* p1 = tscSqlExprGet(pQueryInfo, i + outputIndex);
      int16_t t = p1->resType;
      if (t == TSDB_DATA_TYPE_BINARY || t == TSDB_DATA_TYPE_NCHAR || t == TSDB_DATA_TYPE_BOOL || t == TSDB_DATA_TYPE_TIMESTAMP) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }

      if (i == 0) {
        id = p1->uid;
      } else if (id != p1->uid) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    }

    *uid = id;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateArithmeticSQLExpr(SSqlCmd* pCmd, tSqlExpr* pExpr, SQueryInfo* pQueryInfo, SColumnList* pList, int32_t* type) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSqlExpr* pLeft = pExpr->pLeft;
  uint64_t uidLeft = 0;
  uint64_t uidRight = 0;

  if (pLeft->type == SQL_NODE_EXPR) {
    int32_t ret = validateArithmeticSQLExpr(pCmd, pLeft, pQueryInfo, pList, type);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateSQLExpr(pCmd, pLeft, pQueryInfo, pList, type, &uidLeft);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  tSqlExpr* pRight = pExpr->pRight;
  if (pRight->type == SQL_NODE_EXPR) {
    int32_t ret = validateArithmeticSQLExpr(pCmd, pRight, pQueryInfo, pList, type);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateSQLExpr(pCmd, pRight, pQueryInfo, pList, type, &uidRight);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    // the expression not from the same table, return error
    if (uidLeft != uidRight && uidLeft != 0 && uidRight != 0) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool isValidExpr(tSqlExpr* pLeft, tSqlExpr* pRight, int32_t optr) {
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
  if (pLeft->type == SQL_NODE_SQLFUNCTION) {
    return false;
  }

  if (pRight == NULL) {
    return true;
  }
  
  if (pLeft->tokenId >= TK_BOOL && pLeft->tokenId <= TK_BINARY && pRight->tokenId >= TK_BOOL && pRight->tokenId <= TK_BINARY) {
    return false;
  }

  return true;
}

static void exchangeExpr(tSqlExpr* pExpr) {
  tSqlExpr* pLeft  = pExpr->pLeft;
  tSqlExpr* pRight = pExpr->pRight;

  if (pRight->tokenId == TK_ID && (pLeft->tokenId == TK_INTEGER || pLeft->tokenId == TK_FLOAT ||
                                    pLeft->tokenId == TK_STRING || pLeft->tokenId == TK_BOOL)) {
    /*
     * exchange value of the left handside and the value of the right-handside
     * to make sure that the value of filter expression always locates in
     * right-handside and
     * the column-id is at the left handside.
     */
    uint32_t optr = 0;
    switch (pExpr->tokenId) {
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
        optr = pExpr->tokenId;
    }

    pExpr->tokenId = optr;
    SWAP(pExpr->pLeft, pExpr->pRight, void*);
  }
}

static bool validateJoinExprNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr* pExpr, SColumnIndex* pLeftIndex) {
  const char* msg1 = "illegal column name";
  const char* msg2 = "= is expected in join expression";
  const char* msg3 = "join column must have same type";
  const char* msg4 = "self join is not allowed";
  const char* msg5 = "join table must be the same type(table to table, super table to super table)";

  tSqlExpr* pRight = pExpr->pRight;

  if (pRight->tokenId != TK_ID) {
    return true;
  }

  if (pExpr->tokenId != TK_EQ) {
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

static bool validTableNameOptr(tSqlExpr* pExpr) {
  const char nameFilterOptr[] = {TK_IN, TK_LIKE};

  for (int32_t i = 0; i < tListLen(nameFilterOptr); ++i) {
    if (pExpr->tokenId == nameFilterOptr[i]) {
      return true;
    }
  }

  return false;
}

static int32_t setExprToCond(tSqlExpr** parent, tSqlExpr* pExpr, const char* msg, int32_t parentOptr, char* msgBuf) {
  if (*parent != NULL) {
    if (parentOptr == TK_OR && msg != NULL) {
      return invalidSqlErrMsg(msgBuf, msg);
    }

    *parent = tSqlExprCreate((*parent), pExpr, parentOptr);
  } else {
    *parent = pExpr;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t handleExprInQueryCond(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr** pExpr, SCondExpr* pCondExpr,
                                     int32_t* type, int32_t parentOptr) {
  const char* msg1 = "table query cannot use tags filter";
  const char* msg2 = "illegal column name";
  const char* msg3 = "only one query time range allowed";
  const char* msg4 = "only one join condition allowed";
  const char* msg5 = "not support ordinary column join";
  const char* msg6 = "only one query condition on tbname allowed";
  const char* msg7 = "only in/like allowed in filter table name";
  const char* msg8 = "wildcard string should be less than 20 characters";
  
  tSqlExpr* pLeft  = (*pExpr)->pLeft;
  tSqlExpr* pRight = (*pExpr)->pRight;

  int32_t ret = TSDB_CODE_SUCCESS;

  SColumnIndex index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(pCmd, &pLeft->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  assert(tSqlExprIsParentOfLeaf(*pExpr));

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index.tableIndex);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

  if (index.columnIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {  // query on time range
    if (!validateJoinExprNode(pCmd, pQueryInfo, *pExpr, &index)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    // set join query condition
    if (pRight->tokenId == TK_ID) {  // no need to keep the timestamp join condition
      TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_QUERY);
      pCondExpr->tsJoin = true;

      /*
       * to release expression, e.g., m1.ts = m2.ts,
       * since this expression is used to set the join query type
       */
      tSqlExprDestroy(*pExpr);
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
    if ((*pExpr)->tokenId == TK_LIKE) {
      if (pRight->value.nLen > TSDB_PATTERN_STRING_MAX_LEN) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
      }

      SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

      if ((!isTablenameToken(&pLeft->colInfo)) && pSchema[index.columnIndex].type != TSDB_DATA_TYPE_BINARY &&
          pSchema[index.columnIndex].type != TSDB_DATA_TYPE_NCHAR) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
    }

    // in case of in operator, keep it in a seprate attribute
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
      if (pRight != NULL && pRight->tokenId == TK_ID) {  // join on tag columns for stable query
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

    if (pRight->tokenId == TK_ID) {  // other column cannot be served as the join column
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
    }

    ret = setExprToCond(&pCondExpr->pColumnCond, *pExpr, NULL, parentOptr, pQueryInfo->msg);
    *pExpr = NULL;  // remove it from expr tree
  }

  return ret;
}

int32_t getQueryCondExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr** pExpr, SCondExpr* pCondExpr,
                        int32_t* type, int32_t parentOptr) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* msg1 = "query condition between different columns must use 'AND'";

  tSqlExpr* pLeft = (*pExpr)->pLeft;
  tSqlExpr* pRight = (*pExpr)->pRight;

  if (!isValidExpr(pLeft, pRight, (*pExpr)->tokenId)) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  int32_t leftType = -1;
  int32_t rightType = -1;

  if (!tSqlExprIsParentOfLeaf(*pExpr)) {
    int32_t ret = getQueryCondExpr(pCmd, pQueryInfo, &(*pExpr)->pLeft, pCondExpr, &leftType, (*pExpr)->tokenId);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    ret = getQueryCondExpr(pCmd, pQueryInfo, &(*pExpr)->pRight, pCondExpr, &rightType, (*pExpr)->tokenId);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    /*
     *  if left child and right child do not belong to the same group, the sub
     *  expression is not valid for parent node, it must be TK_AND operator.
     */
    if (leftType != rightType) {
      if ((*pExpr)->tokenId == TK_OR && (leftType + rightType != TSQL_EXPR_TBNAME + TSQL_EXPR_TAG)) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }
    }

    *type = rightType;
    return TSDB_CODE_SUCCESS;
  }

  exchangeExpr(*pExpr);

  return handleExprInQueryCond(pCmd, pQueryInfo, pExpr, pCondExpr, type, parentOptr);
}

static void doExtractExprForSTable(SSqlCmd* pCmd, tSqlExpr** pExpr, SQueryInfo* pQueryInfo, tSqlExpr** pOut, int32_t tableIndex) {
  if (tSqlExprIsParentOfLeaf(*pExpr)) {
    tSqlExpr* pLeft = (*pExpr)->pLeft;

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(pCmd, &pLeft->colInfo, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return;
    }

    if (index.tableIndex != tableIndex) {
      return;
    }

    *pOut = *pExpr;
    (*pExpr) = NULL;

  } else {
    *pOut = tSqlExprCreate(NULL, NULL, (*pExpr)->tokenId);

    doExtractExprForSTable(pCmd, &(*pExpr)->pLeft, pQueryInfo, &((*pOut)->pLeft), tableIndex);
    doExtractExprForSTable(pCmd, &(*pExpr)->pRight, pQueryInfo, &((*pOut)->pRight), tableIndex);
  }
}

static tSqlExpr* extractExprForSTable(SSqlCmd* pCmd, tSqlExpr** pExpr, SQueryInfo* pQueryInfo, int32_t tableIndex) {
  tSqlExpr* pResExpr = NULL;

  if (*pExpr != NULL) {
    doExtractExprForSTable(pCmd, pExpr, pQueryInfo, &pResExpr, tableIndex);
    tSqlExprCompact(&pResExpr);
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
                                          tSqlExpr* pExpr, int16_t tableCondIndex, SStringBuilder* sb) {
  const char* msg = "table name too long";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableCondIndex);

  STagCond* pTagCond = &pQueryInfo->tagCond;
  pTagCond->tbnameCond.uid = pTableMetaInfo->pTableMeta->id.uid;

  assert(pExpr->tokenId == TK_LIKE || pExpr->tokenId == TK_IN);

  if (pExpr->tokenId == TK_LIKE) {
    char* str = taosStringBuilderGetResult(sb, NULL);
    pQueryInfo->tagCond.tbnameCond.cond = strdup(str);
    pQueryInfo->tagCond.tbnameCond.len = (int32_t) strlen(str);
    return TSDB_CODE_SUCCESS;
  }

  SStringBuilder sb1; memset(&sb1, 0, sizeof(sb1));
  taosStringBuilderAppendStringLen(&sb1, QUERY_COND_REL_PREFIX_IN, QUERY_COND_REL_PREFIX_IN_LEN);

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

  char name[TSDB_DB_NAME_LEN] = {0};
  tNameGetDbName(&pTableMetaInfo->name, name);
  SStrToken dbToken = { .type = TK_STRING, .z = name, .n = (uint32_t)strlen(name) };
  
  for (int32_t i = 0; i < num; ++i) {
    if (i >= 1) {
      taosStringBuilderAppendStringLen(&sb1, TBNAME_LIST_SEP, 1);
    }

    char      idBuf[TSDB_TABLE_FNAME_LEN] = {0};
    int32_t   xlen = (int32_t)strlen(segments[i]);
    SStrToken t = {.z = segments[i], .n = xlen, .type = TK_STRING};

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
  pQueryInfo->tagCond.tbnameCond.len = (int32_t) strlen(str);

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

static int32_t getTimeRangeFromExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr* pExpr) {
  const char* msg0 = "invalid timestamp";
  const char* msg1 = "only one time stamp window allowed";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!tSqlExprIsParentOfLeaf(pExpr)) {
    if (pExpr->tokenId == TK_OR) {
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
    
    tSqlExpr* pRight = pExpr->pRight;

    STimeWindow win = {.skey = INT64_MIN, .ekey = INT64_MAX};
    if (getTimeRange(&win, pRight, pExpr->tokenId, tinfo.precision) != TSDB_CODE_SUCCESS) {
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
    tSqlExprDestroy(pCondExpr->pTableCond);
  }

  if (pCondExpr->pTagCond) {
    tSqlExprDestroy(pCondExpr->pTagCond);
  }

  if (pCondExpr->pColumnCond) {
    tSqlExprDestroy(pCondExpr->pColumnCond);
  }

  if (pCondExpr->pTimewindow) {
    tSqlExprDestroy(pCondExpr->pTimewindow);
  }

  if (pCondExpr->pJoinExpr) {
    tSqlExprDestroy(pCondExpr->pJoinExpr);
  }
}

static void doAddJoinTagsColumnsIntoTagList(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SCondExpr* pCondExpr) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (QUERY_IS_JOIN_QUERY(pQueryInfo->type) && UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;

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

static int32_t validateTagCondExpr(SSqlCmd* pCmd, tExprNode *p) {
  const char *msg1 = "invalid tag operator";
  const char* msg2 = "not supported filter condition";
  
  do {
    if (p->nodeType != TSQL_NODE_EXPR) {
      break;
    }
    
    if (!p->_node.pLeft || !p->_node.pRight) {
      break;
    }
    
    if (IS_ARITHMETIC_OPTR(p->_node.optr)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1); 
    }
    
    if (!IS_RELATION_OPTR(p->_node.optr)) {
      break;
    }
    
    tVariant * vVariant = NULL;
    int32_t schemaType = -1;
  
    if (p->_node.pLeft->nodeType == TSQL_NODE_VALUE && p->_node.pRight->nodeType == TSQL_NODE_COL) {
      if (!p->_node.pRight->pSchema) {
        break;
      }
      
      vVariant = p->_node.pLeft->pVal;
      schemaType = p->_node.pRight->pSchema->type;
    } else if (p->_node.pLeft->nodeType == TSQL_NODE_COL && p->_node.pRight->nodeType == TSQL_NODE_VALUE) {
      if (!p->_node.pLeft->pSchema) {
        break;
      }

      vVariant = p->_node.pRight->pVal;
      schemaType = p->_node.pLeft->pSchema->type;
    } else {
      break;
    }

    if (schemaType >= TSDB_DATA_TYPE_TINYINT && schemaType <= TSDB_DATA_TYPE_BIGINT) {
      schemaType = TSDB_DATA_TYPE_BIGINT;
    } else if (schemaType == TSDB_DATA_TYPE_FLOAT || schemaType == TSDB_DATA_TYPE_DOUBLE) {
      schemaType = TSDB_DATA_TYPE_DOUBLE;
    }
    
    int32_t retVal = TSDB_CODE_SUCCESS;
    if (schemaType == TSDB_DATA_TYPE_BINARY) {
      char *tmp = calloc(1, vVariant->nLen + TSDB_NCHAR_SIZE);
      retVal = tVariantDump(vVariant, tmp, schemaType, false);
      free(tmp);
    } else if (schemaType == TSDB_DATA_TYPE_NCHAR) {
      // pRight->value.nLen + 1 is larger than the actual nchar string length
      char *tmp = calloc(1, (vVariant->nLen + 1) * TSDB_NCHAR_SIZE);
      retVal = tVariantDump(vVariant, tmp, schemaType, false);
      free(tmp);
    } else {
      double tmp;
      retVal = tVariantDump(vVariant, (char*)&tmp, schemaType, false);
    }
    
    if (retVal != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  }while (0);

  return TSDB_CODE_SUCCESS;
}

static int32_t getTagQueryCondExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SCondExpr* pCondExpr, tSqlExpr** pExpr) {
  int32_t ret = TSDB_CODE_SUCCESS;

  if (pCondExpr->pTagCond == NULL) {
    return ret;
  }
  
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    tSqlExpr* p1 = extractExprForSTable(pCmd, pExpr, pQueryInfo, i);
    if (p1 == NULL) {  // no query condition on this table
      continue;
    }

    tExprNode* p = NULL;
  
    SArray* colList = taosArrayInit(10, sizeof(SColIndex));
    ret = exprTreeFromSqlExpr(pCmd, &p, p1, pQueryInfo, colList, NULL);
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
    int64_t uid = pTableMetaInfo->pTableMeta->id.uid;
    int32_t numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
    
    size_t num = taosArrayGetSize(colList);
    for(int32_t j = 0; j < num; ++j) {
      SColIndex* pIndex = taosArrayGet(colList, j);
      SColumnIndex index = {.tableIndex = i, .columnIndex = pIndex->colIndex - numOfCols};
      tscColumnListInsert(pTableMetaInfo->tagColList, &index);
    }
    
    tsSetSTableQueryCond(&pQueryInfo->tagCond, uid, &bw);
    tSqlExprCompact(pExpr);

    if (ret == TSDB_CODE_SUCCESS) {
      ret = validateTagCondExpr(pCmd, p);
    }

    tSqlExprDestroy(p1);
    tExprTreeDestroy(p, NULL);
    
    taosArrayDestroy(colList);
    if (pQueryInfo->tagCond.pCond != NULL && taosArrayGetSize(pQueryInfo->tagCond.pCond) > 0 && !UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), "filter on tag not supported for normal table");
    }

    if (ret) {
      break;
    }
  }

  pCondExpr->pTagCond = NULL;
  return ret;
}

int32_t parseWhereClause(SQueryInfo* pQueryInfo, tSqlExpr** pExpr, SSqlObj* pSql) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* msg1 = "invalid expression";
  const char* msg2 = "invalid filter expression";

  int32_t ret = TSDB_CODE_SUCCESS;

  // tags query condition may be larger than 512bytes, therefore, we need to prepare enough large space
  SStringBuilder sb; memset(&sb, 0, sizeof(sb));
  SCondExpr      condExpr = {0};

  if ((*pExpr)->pLeft == NULL || (*pExpr)->pRight == NULL) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(&pSql->cmd), msg1);
  }

  int32_t type = 0;
  if ((ret = getQueryCondExpr(&pSql->cmd, pQueryInfo, pExpr, &condExpr, &type, (*pExpr)->tokenId)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  tSqlExprCompact(pExpr);

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

int32_t getTimeRange(STimeWindow* win, tSqlExpr* pRight, int32_t optr, int16_t timePrecision) {
  // this is join condition, do nothing
  if (pRight->tokenId == TK_ID) {
    return TSDB_CODE_SUCCESS;
  }

  /*
   * filter primary ts filter expression like:
   * where ts in ('2015-12-12 4:8:12')
   */
  if (pRight->tokenId == TK_SET || optr == TK_IN) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  int64_t val = 0;
  bool    parsed = false;
  if (pRight->value.nType == TSDB_DATA_TYPE_BINARY) {
    pRight->value.nLen = strdequote(pRight->value.pz);

    char* seg = strnchr(pRight->value.pz, '-', pRight->value.nLen, false);
    if (seg != NULL) {
      if (taosParseTime(pRight->value.pz, &val, pRight->value.nLen, TSDB_TIME_PRECISION_MICRO, tsDaylight) == TSDB_CODE_SUCCESS) {
        parsed = true;
      } else {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    } else {
      SStrToken token = {.z = pRight->value.pz, .n = pRight->value.nLen, .type = TK_ID};
      int32_t   len = tSQLGetToken(pRight->value.pz, &token.type);

      if ((token.type != TK_INTEGER && token.type != TK_FLOAT) || len != pRight->value.nLen) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    }
  } else if (pRight->tokenId == TK_INTEGER && timePrecision == TSDB_TIME_PRECISION_MILLI) {
    /*
     * if the pRight->tokenId == TK_INTEGER/TK_FLOAT, the value is adaptive, we
     * need the time precision in metermeta to transfer the value in MICROSECOND
     *
     * Additional check to avoid data overflow
     */
    if (pRight->value.i64 <= INT64_MAX / 1000) {
      pRight->value.i64 *= 1000;
    }
  } else if (pRight->tokenId == TK_FLOAT && timePrecision == TSDB_TIME_PRECISION_MILLI) {
    pRight->value.dKey *= 1000;
  }

  if (!parsed) {
    /*
     * failed to parse timestamp in regular formation, try next
     * it may be a epoch time in string format
     */
    tVariantDump(&pRight->value, (char*)&val, TSDB_DATA_TYPE_BIGINT, true);

    /*
     * transfer it into MICROSECOND format if it is a string, since for
     * TK_INTEGER/TK_FLOAT the value has been transferred
     *
     * additional check to avoid data overflow
     */
    if (pRight->tokenId == TK_STRING && timePrecision == TSDB_TIME_PRECISION_MILLI) {
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

int32_t parseFillClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySqlNode* pQuerySQL) {
  SArray*     pFillToken = pQuerySQL->fillType;
  tVariantListItem* pItem = taosArrayGet(pFillToken, 0);

  const int32_t START_INTERPO_COL_IDX = 1;

  const char* msg = "illegal value or data overflow";
  const char* msg1 = "value is expected";
  const char* msg2 = "invalid fill option";
  const char* msg3 = "top/bottom not support fill";

  if (pItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }
  
  size_t numOfFields = tscNumOfFields(pQueryInfo);
  
  if (pQueryInfo->fillVal == NULL) {
    pQueryInfo->fillVal = calloc(numOfFields, sizeof(int64_t));
    if (pQueryInfo->fillVal == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  if (strncasecmp(pItem->pVar.pz, "none", 4) == 0 && pItem->pVar.nLen == 4) {
    pQueryInfo->fillType = TSDB_FILL_NONE;
  } else if (strncasecmp(pItem->pVar.pz, "null", 4) == 0 && pItem->pVar.nLen == 4) {
    pQueryInfo->fillType = TSDB_FILL_NULL;
    for (int32_t i = START_INTERPO_COL_IDX; i < numOfFields; ++i) {
      TAOS_FIELD* pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
      setNull((char*)&pQueryInfo->fillVal[i], pField->type, pField->bytes);
    }
  } else if (strncasecmp(pItem->pVar.pz, "prev", 4) == 0 && pItem->pVar.nLen == 4) {
    pQueryInfo->fillType = TSDB_FILL_PREV;
  } else if (strncasecmp(pItem->pVar.pz, "next", 4) == 0 && pItem->pVar.nLen == 4) {
    pQueryInfo->fillType = TSDB_FILL_NEXT;
  } else if (strncasecmp(pItem->pVar.pz, "linear", 6) == 0 && pItem->pVar.nLen == 6) {
    pQueryInfo->fillType = TSDB_FILL_LINEAR;
  } else if (strncasecmp(pItem->pVar.pz, "value", 5) == 0 && pItem->pVar.nLen == 5) {
    pQueryInfo->fillType = TSDB_FILL_SET_VALUE;

    size_t num = taosArrayGetSize(pFillToken);
    if (num == 1) {  // no actual value, return with error code
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    int32_t startPos = 1;
    int32_t numOfFillVal = (int32_t)(num - 1);

    /* for point interpolation query, we do not have the timestamp column */
    if (tscIsPointInterpQuery(pQueryInfo)) {
      startPos = 0;

      if (numOfFillVal > numOfFields) {
        numOfFillVal = (int32_t)numOfFields;
      }
    } else {
      numOfFillVal = (int16_t)((num >  (int32_t)numOfFields) ? (int32_t)numOfFields : num);
    }

    int32_t j = 1;

    for (int32_t i = startPos; i < numOfFillVal; ++i, ++j) {
      TAOS_FIELD* pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);

      if (pField->type == TSDB_DATA_TYPE_BINARY || pField->type == TSDB_DATA_TYPE_NCHAR) {
        setVardataNull((char*) &pQueryInfo->fillVal[i], pField->type);
        continue;
      }

      tVariant* p = taosArrayGet(pFillToken, j);
      int32_t ret = tVariantDump(p, (char*)&pQueryInfo->fillVal[i], pField->type, true);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
      }
    }
    
    if ((num < numOfFields) || ((num - 1 < numOfFields) && (tscIsPointInterpQuery(pQueryInfo)))) {
      tVariantListItem* lastItem = taosArrayGetLast(pFillToken);

      for (int32_t i = numOfFillVal; i < numOfFields; ++i) {
        TAOS_FIELD* pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);

        if (pField->type == TSDB_DATA_TYPE_BINARY || pField->type == TSDB_DATA_TYPE_NCHAR) {
          setVardataNull((char*) &pQueryInfo->fillVal[i], pField->type);
        } else {
          tVariantDump(&lastItem->pVar, (char*)&pQueryInfo->fillVal[i], pField->type, true);
        }
      }
    }
  } else {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  for(int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr->functionId == TSDB_FUNC_TOP || pExpr->functionId == TSDB_FUNC_BOTTOM) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void setDefaultOrderInfo(SQueryInfo* pQueryInfo) {
  /* set default timestamp order information for all queries */
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  pQueryInfo->order.order = TSDB_ORDER_ASC;
  if (isTopBottomQuery(pQueryInfo)) {
    pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
  } else { // in case of select tbname from super_table, the defualt order column can not be the primary ts column
    pQueryInfo->order.orderColId = INT32_MIN;
  }

  /* for super table query, set default ascending order for group output */
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    pQueryInfo->groupbyExpr.orderType = TSDB_ORDER_ASC;
  }
}

int32_t parseOrderbyClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySqlNode* pQuerySqlNode, SSchema* pSchema) {
  const char* msg0 = "only support order by primary timestamp";
  const char* msg1 = "invalid column name";
  const char* msg2 = "order by primary timestamp or first tag in groupby clause allowed";
  const char* msg3 = "invalid column in order by clause, only primary timestamp or first tag in groupby clause allowed";

  setDefaultOrderInfo(pQueryInfo);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);


  if (pQueryInfo->distinctTag == true) {
    pQueryInfo->order.order = TSDB_ORDER_ASC;
    pQueryInfo->order.orderColId = 0; 
    return TSDB_CODE_SUCCESS;
  }
  if (pQuerySqlNode->pSortOrder == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SArray* pSortorder = pQuerySqlNode->pSortOrder;

  /*
   * for table query, there is only one or none order option is allowed, which is the
   * ts or values(top/bottom) order is supported.
   *
   * for super table query, the order option must be less than 3.
   */
  size_t size = taosArrayGetSize(pSortorder);
  if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
    if (size > 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }
  } else {
    if (size > 2) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }
  }

  // handle the first part of order by
  tVariant* pVar = taosArrayGet(pSortorder, 0);

  // e.g., order by 1 asc, return directly with out further check.
  if (pVar->nType >= TSDB_DATA_TYPE_TINYINT && pVar->nType <= TSDB_DATA_TYPE_BIGINT) {
    return TSDB_CODE_SUCCESS;
  }

  SStrToken    columnName = {pVar->nLen, pVar->nType, pVar->pz};
  SColumnIndex index = COLUMN_INDEX_INITIALIZER;

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
    } else {  // order by top/bottom result value column is not supported in case of interval query.
      assert(!(orderByTags && orderByTS));
    }

    size_t s = taosArrayGetSize(pSortorder);
    if (s == 1) {
      if (orderByTags) {
        pQueryInfo->groupbyExpr.orderIndex = index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);

        tVariantListItem* p1 = taosArrayGet(pQuerySqlNode->pSortOrder, 0);
        pQueryInfo->groupbyExpr.orderType = p1->sortOrder;
      } else if (isTopBottomQuery(pQueryInfo)) {
        /* order of top/bottom query in interval is not valid  */
        SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, 0);
        assert(pExpr->functionId == TSDB_FUNC_TS);

        pExpr = tscSqlExprGet(pQueryInfo, 1);
        if (pExpr->colInfo.colIndex != index.columnIndex && index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
        }

        tVariantListItem* p1 = taosArrayGet(pQuerySqlNode->pSortOrder, 0);
        pQueryInfo->order.order = p1->sortOrder;
        pQueryInfo->order.orderColId = pSchema[index.columnIndex].colId;
        return TSDB_CODE_SUCCESS;
      } else {
        tVariantListItem* p1 = taosArrayGet(pQuerySqlNode->pSortOrder, 0);

        pQueryInfo->order.order = p1->sortOrder;
        pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;

        // orderby ts query on super table
        if (tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
          addPrimaryTsColIntoResult(pQueryInfo);
        }
      }
    }

    if (s == 2) {
      tVariantListItem *pItem = taosArrayGet(pQuerySqlNode->pSortOrder, 0);
      if (orderByTags) {
        pQueryInfo->groupbyExpr.orderIndex = index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
        pQueryInfo->groupbyExpr.orderType = pItem->sortOrder;
      } else {
        pQueryInfo->order.order = pItem->sortOrder;
        pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }

      pItem = taosArrayGet(pQuerySqlNode->pSortOrder, 1);
      tVariant* pVar2 = &pItem->pVar;
      SStrToken cname = {pVar2->nLen, pVar2->nType, pVar2->pz};
      if (getColumnIndexByName(pCmd, &cname, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
      } else {
        tVariantListItem* p1 = taosArrayGet(pSortorder, 1);
        pQueryInfo->order.order = p1->sortOrder;
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

      tVariantListItem* pItem = taosArrayGet(pQuerySqlNode->pSortOrder, 0);
      pQueryInfo->order.order = pItem->sortOrder;
      pQueryInfo->order.orderColId = pSchema[index.columnIndex].colId;
      return TSDB_CODE_SUCCESS;
    }

    tVariantListItem* pItem = taosArrayGet(pQuerySqlNode->pSortOrder, 0);
    pQueryInfo->order.order = pItem->sortOrder;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setAlterTableInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  const int32_t DEFAULT_TABLE_INDEX = 0;

  const char* msg1 = "invalid table name";
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
  const char* msg19 = "invalid new tag name";
  const char* msg20 = "table is not super table";

  int32_t code = TSDB_CODE_SUCCESS;

  SSqlCmd*        pCmd = &pSql->cmd;
  SAlterTableInfo* pAlterSQL = pInfo->pAlterInfo;
  SQueryInfo*     pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, DEFAULT_TABLE_INDEX);

  if (tscValidateName(&(pAlterSQL->name)) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  code = tscSetTableFullName(pTableMetaInfo, &(pAlterSQL->name), pSql);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tscGetTableMeta(pSql, pTableMetaInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;

  if (pAlterSQL->tableType == TSDB_SUPER_TABLE && !(UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo))) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg20);
  }

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
    SArray* pFieldList = pAlterSQL->pAddColumns;
    if (taosArrayGetSize(pFieldList) > 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
    }

    TAOS_FIELD* p = taosArrayGet(pFieldList, 0);
    if (!validateOneTags(pCmd, p)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, p);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
    if (tscGetNumOfTags(pTableMeta) == 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
    }

    // numOfTags == 1
    if (taosArrayGetSize(pAlterSQL->varList) > 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg8);
    }

    tVariantListItem* pItem = taosArrayGet(pAlterSQL->varList, 0);
    if (pItem->pVar.nLen >= TSDB_COL_NAME_LEN) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg9);
    }

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    SStrToken    name = {.z = pItem->pVar.pz, .n = pItem->pVar.nLen, .type = TK_STRING};

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
  
    TAOS_FIELD f = tscCreateField(TSDB_DATA_TYPE_INT, name1, tDataTypes[TSDB_DATA_TYPE_INT].bytes);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    SArray* pVarList = pAlterSQL->varList;
    if (taosArrayGetSize(pVarList) > 2) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    tVariantListItem* pSrcItem = taosArrayGet(pAlterSQL->varList, 0);
    tVariantListItem* pDstItem = taosArrayGet(pAlterSQL->varList, 1);

    if (pSrcItem->pVar.nLen >= TSDB_COL_NAME_LEN || pDstItem->pVar.nLen >= TSDB_COL_NAME_LEN) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg9);
    }

    if (pSrcItem->pVar.nType != TSDB_DATA_TYPE_BINARY || pDstItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg10);
    }

    SColumnIndex srcIndex = COLUMN_INDEX_INITIALIZER;
    SColumnIndex destIndex = COLUMN_INDEX_INITIALIZER;

    SStrToken srcToken = {.z = pSrcItem->pVar.pz, .n = pSrcItem->pVar.nLen, .type = TK_STRING};
    if (getColumnIndexByName(pCmd, &srcToken, pQueryInfo, &srcIndex) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg17);
    }

    SStrToken destToken = {.z = pDstItem->pVar.pz, .n = pDstItem->pVar.nLen, .type = TK_STRING};
    if (getColumnIndexByName(pCmd, &destToken, pQueryInfo, &destIndex) == TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg19);
    }

    tVariantListItem* pItem = taosArrayGet(pVarList, 0);

    char name[TSDB_COL_NAME_LEN] = {0};
    strncpy(name, pItem->pVar.pz, pItem->pVar.nLen);
    TAOS_FIELD f = tscCreateField(TSDB_DATA_TYPE_INT, name, tDataTypes[TSDB_DATA_TYPE_INT].bytes);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);

    pItem = taosArrayGet(pVarList, 1);
    memset(name, 0, tListLen(name));

    strncpy(name, pItem->pVar.pz, pItem->pVar.nLen);
    f = tscCreateField(TSDB_DATA_TYPE_INT, name, tDataTypes[TSDB_DATA_TYPE_INT].bytes);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    // Note: update can only be applied to table not super table.
    // the following is used to handle tags value for table created according to super table
    pCmd->command = TSDB_SQL_UPDATE_TAGS_VAL;
    
    SArray* pVarList = pAlterSQL->varList;
    tVariantListItem* item = taosArrayGet(pVarList, 0);
    int16_t       numOfTags = tscGetNumOfTags(pTableMeta);

    SColumnIndex columnIndex = COLUMN_INDEX_INITIALIZER;
    SStrToken    name = {.type = TK_STRING, .z = item->pVar.pz, .n = item->pVar.nLen};
    if (getColumnIndexByName(pCmd, &name, pQueryInfo, &columnIndex) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    if (columnIndex.columnIndex < tscGetNumOfColumns(pTableMeta)) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg12);
    }

    tVariantListItem* pItem = taosArrayGet(pVarList, 1);
    SSchema* pTagsSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, columnIndex.columnIndex);
    pAlterSQL->tagData.data = calloc(1, pTagsSchema->bytes * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);

    if (tVariantDump(&pItem->pVar, pAlterSQL->tagData.data, pTagsSchema->type, true) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg13);
    }
    
    pAlterSQL->tagData.dataLen = pTagsSchema->bytes;

    // validate the length of binary
    if ((pTagsSchema->type == TSDB_DATA_TYPE_BINARY || pTagsSchema->type == TSDB_DATA_TYPE_NCHAR) &&
        varDataTLen(pAlterSQL->tagData.data) > pTagsSchema->bytes) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg14);
    }

    int32_t schemaLen = sizeof(STColumn) * numOfTags;
    int32_t size = sizeof(SUpdateTableTagValMsg) + pTagsSchema->bytes + schemaLen + TSDB_EXTRA_PAYLOAD_SIZE;

    if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
      tscError("%p failed to malloc for alter table msg", pSql);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    SUpdateTableTagValMsg* pUpdateMsg = (SUpdateTableTagValMsg*) pCmd->payload;
    pUpdateMsg->head.vgId = htonl(pTableMeta->vgId);
    pUpdateMsg->tid       = htonl(pTableMeta->id.tid);
    pUpdateMsg->uid       = htobe64(pTableMeta->id.uid);
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
    pItem = taosArrayGet(pVarList, 1);
    tVariantDump(&pItem->pVar, pUpdateMsg->data + schemaLen, pTagsSchema->type, true);
    
    int32_t len = 0;
    if (pTagsSchema->type != TSDB_DATA_TYPE_BINARY && pTagsSchema->type != TSDB_DATA_TYPE_NCHAR) {
      len = tDataTypes[pTagsSchema->type].bytes;
    } else {
      len = varDataTLen(pUpdateMsg->data + schemaLen);
    }
    
    pUpdateMsg->tagValLen = htonl(len);  // length may be changed after dump data
    
    int32_t total = sizeof(SUpdateTableTagValMsg) + len + schemaLen;
    pUpdateMsg->head.contLen = htonl(total);
    
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
    SArray* pFieldList = pAlterSQL->pAddColumns;
    if (taosArrayGetSize(pFieldList) > 1) {
      const char* msg = "only support add one column";
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
    }

    TAOS_FIELD* p = taosArrayGet(pFieldList, 0);
    if (!validateOneColumn(pCmd, p)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, p);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    if (tscGetNumOfColumns(pTableMeta) == TSDB_MIN_COLUMNS) {  //
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg15);
    }

    size_t size = taosArrayGetSize(pAlterSQL->varList);
    if (size > 1) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg16);
    }

    tVariantListItem* pItem = taosArrayGet(pAlterSQL->varList, 0);

    SColumnIndex columnIndex = COLUMN_INDEX_INITIALIZER;
    SStrToken    name = {.type = TK_STRING, .z = pItem->pVar.pz, .n = pItem->pVar.nLen};
    if (getColumnIndexByName(pCmd, &name, pQueryInfo, &columnIndex) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg17);
    }

    if (columnIndex.columnIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg18);
    }

    char name1[TSDB_COL_NAME_LEN] = {0};
    tstrncpy(name1, pItem->pVar.pz, sizeof(name1));
    TAOS_FIELD f = tscCreateField(TSDB_DATA_TYPE_INT, name1, tDataTypes[TSDB_DATA_TYPE_INT].bytes);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateSqlFunctionInStreamSql(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg0 = "sample interval can not be less than 10ms.";
  const char* msg1 = "functions not allowed in select clause";

  if (pQueryInfo->interval.interval != 0 && pQueryInfo->interval.interval < 10 &&
     pQueryInfo->interval.intervalUnit != 'n' &&
     pQueryInfo->interval.intervalUnit != 'y') {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }
  
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  for (int32_t i = 0; i < size; ++i) {
    int32_t functId = tscSqlExprGet(pQueryInfo, i)->functionId;
    if (!IS_STREAM_QUERY_VALID(aAggs[functId].status)) {
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
        if ((aAggs[pEx->functionId].status & TSDB_FUNCSTATE_SELECTIVITY) == TSDB_FUNCSTATE_SELECTIVITY) {
          hasSelectivity = true;
          break;
        }
      }

      if (hasSelectivity) {
        continue;
      }
    }

    if ((pExpr->functionId == TSDB_FUNC_PRJ && pExpr->numOfParams == 0) || pExpr->functionId == TSDB_FUNC_DIFF ||
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

int32_t validateDNodeConfig(SMiscInfo* pOptions) {
  int32_t numOfToken = (int32_t) taosArrayGetSize(pOptions->a);

  if (numOfToken < 2 || numOfToken > 3) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  const int tokenLogEnd = 2;
  const int tokenBalance = 2;
  const int tokenMonitor = 3;
  const int tokenDebugFlag = 4;
  const int tokenDebugFlagEnd = 20;
  const SDNodeDynConfOption cfgOptions[] = {
      {"resetLog", 8},    {"resetQueryCache", 15},  {"balance", 7},     {"monitor", 7},
      {"debugFlag", 9},   {"monDebugFlag", 12},     {"vDebugFlag", 10}, {"mDebugFlag", 10},
      {"cDebugFlag", 10}, {"httpDebugFlag", 13},    {"qDebugflag", 10}, {"sdbDebugFlag", 12},
      {"uDebugFlag", 10}, {"tsdbDebugFlag", 13},    {"sDebugflag", 10}, {"rpcDebugFlag", 12},
      {"dDebugFlag", 10}, {"mqttDebugFlag", 13},    {"wDebugFlag", 10}, {"tmrDebugFlag", 12},
      {"cqDebugFlag", 11},
  };

  SStrToken* pOptionToken = taosArrayGet(pOptions->a, 1);

  if (numOfToken == 2) {
    // reset log and reset query cache does not need value
    for (int32_t i = 0; i < tokenLogEnd; ++i) {
      const SDNodeDynConfOption* pOption = &cfgOptions[i];
      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  } else if ((strncasecmp(cfgOptions[tokenBalance].name, pOptionToken->z, pOptionToken->n) == 0) &&
             (cfgOptions[tokenBalance].len == pOptionToken->n)) {
    SStrToken* pValToken = taosArrayGet(pOptions->a, 2);
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
    SStrToken* pValToken = taosArrayGet(pOptions->a, 2);
    int32_t    val = strtol(pValToken->z, NULL, 10);
    if (val != 0 && val != 1) {
      return TSDB_CODE_TSC_INVALID_SQL;  // options value is invalid
    }
    return TSDB_CODE_SUCCESS;
  } else {
    SStrToken* pValToken = taosArrayGet(pOptions->a, 2);

    int32_t val = strtol(pValToken->z, NULL, 10);
    if (val < 0 || val > 256) {
      /* options value is out of valid range */
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    for (int32_t i = tokenDebugFlag; i < tokenDebugFlagEnd; ++i) {
      const SDNodeDynConfOption* pOption = &cfgOptions[i];

      // options is valid
      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  return TSDB_CODE_TSC_INVALID_SQL;
}

int32_t validateLocalConfig(SMiscInfo* pOptions) {
  int32_t numOfToken = (int32_t) taosArrayGetSize(pOptions->a);
  if (numOfToken < 1 || numOfToken > 2) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  SDNodeDynConfOption LOCAL_DYNAMIC_CFG_OPTIONS[6] = {{"resetLog", 8},    {"rpcDebugFlag", 12}, {"tmrDebugFlag", 12},
                                                      {"cDebugFlag", 10}, {"uDebugFlag", 10},   {"debugFlag", 9}};


  SStrToken* pOptionToken = taosArrayGet(pOptions->a, 0);

  if (numOfToken == 1) {
    // reset log does not need value
    for (int32_t i = 0; i < 1; ++i) {
      SDNodeDynConfOption* pOption = &LOCAL_DYNAMIC_CFG_OPTIONS[i];
      if ((pOption->len == pOptionToken->n) &&
              (strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  } else {
    SStrToken* pValToken = taosArrayGet(pOptions->a, 1);

    int32_t val = strtol(pValToken->z, NULL, 10);
    if (!validateDebugFlag(val)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    for (int32_t i = 1; i < tListLen(LOCAL_DYNAMIC_CFG_OPTIONS); ++i) {
      SDNodeDynConfOption* pOption = &LOCAL_DYNAMIC_CFG_OPTIONS[i];
      if ((pOption->len == pOptionToken->n)
              && (strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  }
  return TSDB_CODE_TSC_INVALID_SQL;
}

int32_t validateColumnName(char* name) {
  bool ret = isKeyWord(name, (int32_t)strlen(name));
  if (ret) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  SStrToken token = {.z = name};
  token.n = tSQLGetToken(name, &token.type);

  if (token.type != TK_STRING && token.type != TK_ID) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (token.type == TK_STRING) {
    strdequote(token.z);
    strntolower(token.z, token.z, token.n);
    token.n = (uint32_t)strtrim(token.z);

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

int32_t parseLimitClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t clauseIndex, SQuerySqlNode* pQuerySqlNode, SSqlObj* pSql) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  const char* msg0 = "soffset/offset can not be less than 0";
  const char* msg1 = "slimit/soffset only available for STable query";
  const char* msg2 = "slimit/soffset can not apply to projection query";

  // handle the limit offset value, validate the limit
  pQueryInfo->limit = pQuerySqlNode->limit;
  pQueryInfo->clauseLimit = pQueryInfo->limit.limit;
  pQueryInfo->slimit = pQuerySqlNode->slimit;
  
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
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
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
    pQueryInfo->prjOffset   = pQueryInfo->limit.offset;
    pQueryInfo->vgroupLimit = -1;

    if (tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
      /*
       * the offset value should be removed during retrieve data from virtual node, since the
       * global order are done in client side, so the offset is applied at the client side
       * However, note that the maximum allowed number of result for each table should be less
       * than or equal to the value of limit.
       */
      if (pQueryInfo->limit.limit > 0) {
        pQueryInfo->vgroupLimit = pQueryInfo->limit.limit + pQueryInfo->limit.offset;
        pQueryInfo->limit.limit = -1;
      }

      pQueryInfo->limit.offset = 0;
    }
  } else {
    if (pQueryInfo->slimit.limit != -1 || pQueryInfo->slimit.offset != 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setKeepOption(SSqlCmd* pCmd, SCreateDbMsg* pMsg, SCreateDbInfo* pCreateDb) {
  const char* msg = "invalid number of options";

  pMsg->daysToKeep = htonl(-1);
  pMsg->daysToKeep1 = htonl(-1);
  pMsg->daysToKeep2 = htonl(-1);

  SArray* pKeep = pCreateDb->keep;
  if (pKeep != NULL) {
    size_t s = taosArrayGetSize(pKeep);
    tVariantListItem* p0 = taosArrayGet(pKeep, 0);
    switch (s) {
      case 1: {
        pMsg->daysToKeep = htonl((int32_t)p0->pVar.i64);
      }
        break;
      case 2: {
        tVariantListItem* p1 = taosArrayGet(pKeep, 1);
        pMsg->daysToKeep = htonl((int32_t)p0->pVar.i64);
        pMsg->daysToKeep1 = htonl((int32_t)p1->pVar.i64);
        break;
      }
      case 3: {
        tVariantListItem* p1 = taosArrayGet(pKeep, 1);
        tVariantListItem* p2 = taosArrayGet(pKeep, 2);

        pMsg->daysToKeep = htonl((int32_t)p0->pVar.i64);
        pMsg->daysToKeep1 = htonl((int32_t)p1->pVar.i64);
        pMsg->daysToKeep2 = htonl((int32_t)p2->pVar.i64);
        break;
      }
      default: { return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg); }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setTimePrecision(SSqlCmd* pCmd, SCreateDbMsg* pMsg, SCreateDbInfo* pCreateDbInfo) {
  const char* msg = "invalid time precision";

  pMsg->precision = TSDB_TIME_PRECISION_MILLI;  // millisecond by default

  SStrToken* pToken = &pCreateDbInfo->precision;
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

static void setCreateDBOption(SCreateDbMsg* pMsg, SCreateDbInfo* pCreateDb) {
  pMsg->maxTables = htonl(-1);  // max tables can not be set anymore
  pMsg->cacheBlockSize = htonl(pCreateDb->cacheBlockSize);
  pMsg->totalBlocks = htonl(pCreateDb->numOfBlocks);
  pMsg->daysPerFile = htonl(pCreateDb->daysPerFile);
  pMsg->commitTime = htonl((int32_t)pCreateDb->commitTime);
  pMsg->minRowsPerFileBlock = htonl(pCreateDb->minRowsPerBlock);
  pMsg->maxRowsPerFileBlock = htonl(pCreateDb->maxRowsPerBlock);
  pMsg->fsyncPeriod = htonl(pCreateDb->fsyncPeriod);
  pMsg->compression = pCreateDb->compressionLevel;
  pMsg->walLevel = (char)pCreateDb->walLevel;
  pMsg->replications = pCreateDb->replica;
  pMsg->quorum = pCreateDb->quorum;
  pMsg->ignoreExist = pCreateDb->ignoreExists;
  pMsg->update = pCreateDb->update;
  pMsg->cacheLastRow = pCreateDb->cachelast;
  pMsg->dbType = pCreateDb->dbType;
  pMsg->partitions = htons(pCreateDb->partitions);
}

int32_t parseCreateDBOptions(SSqlCmd* pCmd, SCreateDbInfo* pCreateDbSql) {
  SCreateDbMsg* pMsg = (SCreateDbMsg *)(pCmd->payload);
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

void addGroupInfoForSubquery(SSqlObj* pParentObj, SSqlObj* pSql, int32_t subClauseIndex, int32_t tableIndex) {
  SQueryInfo* pParentQueryInfo = tscGetQueryInfoDetail(&pParentObj->cmd, subClauseIndex);

  if (pParentQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, subClauseIndex);
    SSqlExpr* pExpr = NULL;

    size_t size = taosArrayGetSize(pQueryInfo->exprList);
    if (size > 0) {
      pExpr = tscSqlExprGet(pQueryInfo, (int32_t)size - 1);
    }

    if (pExpr == NULL || pExpr->functionId != TSDB_FUNC_TAG) {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pParentQueryInfo, tableIndex);

      int16_t colId = tscGetJoinTagColIdByUid(&pQueryInfo->tagCond, pTableMetaInfo->pTableMeta->id.uid);

      SSchema* pTagSchema = tscGetColumnSchemaById(pTableMetaInfo->pTableMeta, colId);
      int16_t colIndex = tscGetTagColIndexById(pTableMetaInfo->pTableMeta, colId);
      SColumnIndex    index = {.tableIndex = 0, .columnIndex = colIndex};

      char*   name = pTagSchema->name;
      int16_t type = pTagSchema->type;
      int16_t bytes = pTagSchema->bytes;

      pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TAG, &index, type, bytes, getNewResColId(pQueryInfo), bytes, true);
      pExpr->colInfo.flag = TSDB_COL_TAG;

      // NOTE: tag column does not add to source column list
      SColumnList ids = {0};
      insertResultField(pQueryInfo, (int32_t)size, &ids, bytes, (int8_t)type, name, pExpr);

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

  tscAddFuncInSelectClause(pQueryInfo, (int32_t)size, TSDB_FUNC_PRJ, &colIndex, pSchema, TSDB_COL_NORMAL);

  int32_t numOfFields = tscNumOfFields(pQueryInfo);
  SInternalField* pInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, numOfFields - 1);

  doLimitOutputNormalColOfGroupby(pInfo->pSqlExpr);
  pInfo->visible = false;
}

static void doUpdateSqlFunctionForTagPrj(SQueryInfo* pQueryInfo) {
  int32_t tagLength = 0;
  size_t size = taosArrayGetSize(pQueryInfo->exprList);

//todo is 0??
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  bool isSTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);

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

  SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if ((pExpr->functionId != TSDB_FUNC_TAG_DUMMY && pExpr->functionId != TSDB_FUNC_TS_DUMMY) &&
       !(pExpr->functionId == TSDB_FUNC_PRJ && TSDB_COL_IS_UD_COL(pExpr->colInfo.flag))) {
      SSchema* pColSchema = &pSchema[pExpr->colInfo.colIndex];
      getResultDataInfo(pColSchema->type, pColSchema->bytes, pExpr->functionId, (int32_t)pExpr->param[0].i64, &pExpr->resType,
                        &pExpr->resBytes, &pExpr->interBytes, tagLength, isSTable);
    }
  }
}

static int32_t doUpdateSqlFunctionForColPrj(SQueryInfo* pQueryInfo) {
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);

    if (pExpr->functionId == TSDB_FUNC_PRJ && (!TSDB_COL_IS_UD_COL(pExpr->colInfo.flag) && (pExpr->colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX))) {
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

      // it is not a tag column/tbname column/user-defined column, return error
      if (!qualifiedCol) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool tagColumnInGroupby(SSqlGroupbyExpr* pGroupbyExpr, int16_t columnId) {
  for (int32_t j = 0; j < pGroupbyExpr->numOfGroupCols; ++j) {
    SColIndex* pColIndex = taosArrayGet(pGroupbyExpr->columnInfo, j);
  
    if (columnId == pColIndex->colId && TSDB_COL_IS_TAG(pColIndex->flag )) {
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

  bool    tagTsColExists = false;
  int16_t numOfSelectivity = 0;
  int16_t numOfAggregation = 0;

  size_t numOfExprs = taosArrayGetSize(pQueryInfo->exprList);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, i);
    if (pExpr->functionId == TSDB_FUNC_TAGPRJ ||
        (pExpr->functionId == TSDB_FUNC_PRJ && pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX)) {
      tagTsColExists = true;  // selectivity + ts/tag column
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

    if ((aAggs[functionId].status & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      numOfSelectivity++;
    } else {
      numOfAggregation++;
    }
  }

  if (tagTsColExists) {  // check if the selectivity function exists
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
      int32_t code = doUpdateSqlFunctionForColPrj(pQueryInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

    } else if (numOfSelectivity > 1) {
      /*
       * If more than one selectivity functions exist, all the selectivity functions must be last_row.
       * Otherwise, return with error code.
       */
      for (int32_t i = 0; i < numOfExprs; ++i) {
        SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
        int16_t functionId = pExpr->functionId;
        if (functionId == TSDB_FUNC_TAGPRJ || (aAggs[functionId].status & TSDB_FUNCSTATE_SELECTIVITY) == 0) {
          continue;
        }

        if ((functionId == TSDB_FUNC_LAST_ROW) ||
             (functionId == TSDB_FUNC_LAST_DST && (pExpr->colInfo.flag & TSDB_COL_NULL) != 0)) {
          // do nothing
        } else {
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
        }
      }

      doUpdateSqlFunctionForTagPrj(pQueryInfo);
      int32_t code = doUpdateSqlFunctionForColPrj(pQueryInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  } else {
    if ((pQueryInfo->type & TSDB_QUERY_TYPE_PROJECTION_QUERY) != 0) {
      if (numOfAggregation > 0 && pQueryInfo->groupbyExpr.numOfGroupCols == 0) {
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (numOfAggregation > 0 || numOfSelectivity > 0) {
        // clear the projection type flag
        pQueryInfo->type &= (~TSDB_QUERY_TYPE_PROJECTION_QUERY);
        int32_t code = doUpdateSqlFunctionForColPrj(pQueryInfo);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doAddGroupbyColumnsOnDemand(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg2 = "interval not allowed in group by normal column";

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  SSchema s = *tGetTbnameColumnSchema();
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
      SSqlExpr* pExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TAG, &index, type, bytes, getNewResColId(pQueryInfo), bytes, true);
      
      memset(pExpr->aliasName, 0, sizeof(pExpr->aliasName));
      tstrncpy(pExpr->aliasName, name, sizeof(pExpr->aliasName));
      
      pExpr->colInfo.flag = TSDB_COL_TAG;

      // NOTE: tag column does not add to source column list
      SColumnList ids = getColumnList(1, 0, pColIndex->colIndex);
      insertResultField(pQueryInfo, (int32_t)size, &ids, bytes, (int8_t)type, name, pExpr);
    } else {
      // if this query is "group by" normal column, time window query is not allowed
      if (isTimeWindowQuery(pQueryInfo)) {
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

static int32_t doTagFunctionCheck(SQueryInfo* pQueryInfo) {
  bool tagProjection = false;
  bool tableCounting = false;

  int32_t numOfCols = (int32_t) tscSqlExprNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    int32_t functionId = pExpr->functionId;

    if (functionId == TSDB_FUNC_TAGPRJ) {
      tagProjection = true;
      continue;
    }

    if (functionId == TSDB_FUNC_COUNT) {
      assert(pExpr->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX);
      tableCounting = true;
    }
  }

  return (tableCounting && tagProjection)? -1:0;
}

int32_t doFunctionsCompatibleCheck(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg1 = "functions/columns not allowed in group by query";
  const char* msg2 = "projection query on columns not allowed";
  const char* msg3 = "group by not allowed on projection query";
  const char* msg4 = "retrieve tags not compatible with group by or interval query";
  const char* msg5 = "functions can not be mixed up";

  // only retrieve tags, group by is not supportted
  if (tscQueryTags(pQueryInfo)) {
    if (doTagFunctionCheck(pQueryInfo) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
    }

    if (pQueryInfo->groupbyExpr.numOfGroupCols > 0 || isTimeWindowQuery(pQueryInfo)) {
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

      if (IS_MULTIOUTPUT(aAggs[functId].status) && functId != TSDB_FUNC_TOP && functId != TSDB_FUNC_BOTTOM &&
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
int32_t doLocalQueryProcess(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SQuerySqlNode* pQuerySqlNode) {
  const char* msg1 = "only one expression allowed";
  const char* msg2 = "invalid expression in select clause";
  const char* msg3 = "invalid function";

  SArray* pExprList = pQuerySqlNode->pSelectList;
  size_t size = taosArrayGetSize(pExprList);
  if (size != 1) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  bool server_status = false;
  tSqlExprItem* pExprItem = taosArrayGet(pExprList, 0);
  tSqlExpr* pExpr = pExprItem->pNode;
  if (pExpr->operand.z == NULL) {
    //handle 'select 1'
    if (pExpr->token.n == 1 && 0 == strncasecmp(pExpr->token.z, "1", 1)) {
      server_status = true; 
    } else {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    } 
  } 
  // TODO redefine the function
   SDNodeDynConfOption functionsInfo[5] = {{"database()", 10},
                                            {"server_version()", 16},
                                            {"server_status()", 15},
                                            {"client_version()", 16},
                                            {"current_user()", 14}};

  int32_t index = -1;
  if (server_status == true) {
    index = 2;
  } else {
    for (int32_t i = 0; i < tListLen(functionsInfo); ++i) {
      if (strncasecmp(functionsInfo[i].name, pExpr->token.z, functionsInfo[i].len) == 0 &&
          functionsInfo[i].len == pExpr->token.n) {
        index = i;
        break;
      }
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
                                      tDataTypes[TSDB_DATA_TYPE_INT].bytes, getNewResColId(pQueryInfo), tDataTypes[TSDB_DATA_TYPE_INT].bytes, false);

  tSqlExprItem* item = taosArrayGet(pExprList, 0);
  const char* name = (item->aliasName != NULL)? item->aliasName:functionsInfo[index].name;
  tstrncpy(pExpr1->aliasName, name, tListLen(pExpr1->aliasName));
  
  return TSDB_CODE_SUCCESS;
}

// can only perform the parameters based on the macro definitation
int32_t tscCheckCreateDbParams(SSqlCmd* pCmd, SCreateDbMsg* pCreate) {
  char msg[512] = {0};

  if (pCreate->walLevel != -1 && (pCreate->walLevel < TSDB_MIN_WAL_LEVEL || pCreate->walLevel > TSDB_MAX_WAL_LEVEL)) {
    snprintf(msg, tListLen(msg), "invalid db option walLevel: %d, only 1-2 allowed", pCreate->walLevel);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (pCreate->replications != -1 &&
      (pCreate->replications < TSDB_MIN_DB_REPLICA_OPTION || pCreate->replications > TSDB_MAX_DB_REPLICA_OPTION)) {
    snprintf(msg, tListLen(msg), "invalid db option replications: %d valid range: [%d, %d]", pCreate->replications,
             TSDB_MIN_DB_REPLICA_OPTION, TSDB_MAX_DB_REPLICA_OPTION);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (pCreate->quorum != -1 &&
      (pCreate->quorum < TSDB_MIN_DB_QUORUM_OPTION || pCreate->quorum > TSDB_MAX_DB_QUORUM_OPTION)) {
    snprintf(msg, tListLen(msg), "invalid db option quorum: %d valid range: [%d, %d]", pCreate->quorum,
             TSDB_MIN_DB_QUORUM_OPTION, TSDB_MAX_DB_QUORUM_OPTION);
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

  val = (int16_t)htons(pCreate->partitions);
  if (val != -1 &&
      (val < TSDB_MIN_DB_PARTITON_OPTION || val > TSDB_MAX_DB_PARTITON_OPTION)) {
    snprintf(msg, tListLen(msg), "invalid topic option partition: %d valid range: [%d, %d]", val,
             TSDB_MIN_DB_PARTITON_OPTION, TSDB_MAX_DB_PARTITON_OPTION);
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg);
  }


  return TSDB_CODE_SUCCESS;
}

// for debug purpose
void tscPrintSelectClause(SSqlObj* pSql, int32_t subClauseIndex) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, subClauseIndex);

  int32_t size = (int32_t)tscSqlExprNumOfExprs(pQueryInfo);
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
        sprintf(tmpBuf, "%s(uid:%" PRId64 ", %d)", aAggs[pExpr->functionId].name, pExpr->uid, pExpr->colInfo.colId);

    if (tmpLen + offset >= totalBufSize - 1) break;


    offset += sprintf(str + offset, "%s", tmpBuf);

    if (i < size - 1) {
      str[offset++] = ',';
    }
  }

  assert(offset < totalBufSize);
  str[offset] = ']';
  assert(offset < totalBufSize);
  tscDebug("%p select clause:%s", pSql, str);
}

int32_t doCheckForCreateTable(SSqlObj* pSql, int32_t subClauseIndex, SSqlInfo* pInfo) {
  const char* msg1 = "invalid table name";

  SSqlCmd*        pCmd = &pSql->cmd;
  SQueryInfo*     pQueryInfo = tscGetQueryInfoDetail(pCmd, subClauseIndex);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;

  SArray* pFieldList = pCreateTable->colInfo.pColumns;
  SArray* pTagList = pCreateTable->colInfo.pTagColumns;

  assert(pFieldList != NULL);

  // if sql specifies db, use it, otherwise use default db
  SStrToken* pzTableName = &(pCreateTable->name);

  if (tscValidateName(pzTableName) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  int32_t code = tscSetTableFullName(pTableMetaInfo, pzTableName, pSql);
  if(code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (!validateTableColumnInfo(pFieldList, pCmd) ||
      (pTagList != NULL && !validateTagParams(pTagList, pFieldList, pCmd))) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  int32_t col = 0;
  size_t numOfFields = taosArrayGetSize(pFieldList);

  for (; col < numOfFields; ++col) {
    TAOS_FIELD* p = taosArrayGet(pFieldList, col);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, p);
  }

  pCmd->numOfCols = (int16_t)numOfFields;

  if (pTagList != NULL) {  // create super table[optional]
    size_t numOfTags = taosArrayGetSize(pTagList);
    for (int32_t i = 0; i < numOfTags; ++i) {
      TAOS_FIELD* p = taosArrayGet(pTagList, i);
      tscFieldInfoAppend(&pQueryInfo->fieldsInfo, p);
    }

    pCmd->count =(int32_t) numOfTags;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doCheckForCreateFromStable(SSqlObj* pSql, SSqlInfo* pInfo) {
  const char* msg1 = "invalid table name";
  const char* msg3 = "tag value too long";
  const char* msg4 = "illegal value or data overflow";
  const char* msg5 = "tags number not matched";

  SSqlCmd* pCmd = &pSql->cmd;

  SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;
  SQueryInfo*      pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  // two table: the first one is for current table, and the secondary is for the super table.
  if (pQueryInfo->numOfTables < 2) {
    tscAddEmptyMetaInfo(pQueryInfo);
  }

  const int32_t TABLE_INDEX = 0;
  const int32_t STABLE_INDEX = 1;

  STableMetaInfo* pStableMetaInfo = tscGetMetaInfo(pQueryInfo, STABLE_INDEX);

  // super table name, create table by using dst
  int32_t numOfTables = (int32_t) taosArrayGetSize(pCreateTable->childTableInfo);
  for(int32_t j = 0; j < numOfTables; ++j) {
    SCreatedTableInfo* pCreateTableInfo = taosArrayGet(pCreateTable->childTableInfo, j);

    SStrToken* pToken = &pCreateTableInfo->stableName;
    if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    int32_t code = tscSetTableFullName(pStableMetaInfo, pToken, pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // get table meta from mnode
    code = tNameExtractFullName(&pStableMetaInfo->name, pCreateTableInfo->tagdata.name);

    SArray* pValList = pCreateTableInfo->pTagVals;
    code = tscGetTableMeta(pSql, pStableMetaInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    size_t valSize = taosArrayGetSize(pValList);


    // too long tag values will return invalid sql, not be truncated automatically
    SSchema  *pTagSchema = tscGetTableTagSchema(pStableMetaInfo->pTableMeta);
    STagData *pTag = &pCreateTableInfo->tagdata;

    SKVRowBuilder kvRowBuilder = {0};
    if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }


    SArray* pNameList = NULL;
    size_t nameSize = 0;
    int32_t schemaSize = tscGetNumOfTags(pStableMetaInfo->pTableMeta);
    int32_t ret = TSDB_CODE_SUCCESS;

    if (pCreateTableInfo->pTagNames) {
      pNameList = pCreateTableInfo->pTagNames;
      nameSize = taosArrayGetSize(pNameList);

      if (valSize != nameSize) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      if (schemaSize < valSize) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      bool findColumnIndex = false;

      for (int32_t i = 0; i < nameSize; ++i) {
        SStrToken* sToken = taosArrayGet(pNameList, i);
        if (TK_STRING == sToken->type) {
          tscDequoteAndTrimToken(sToken);
        }

        tVariantListItem* pItem = taosArrayGet(pValList, i);

        findColumnIndex = false;

        // todo speedup by using hash list
        for (int32_t t = 0; t < schemaSize; ++t) {
          if (strncmp(sToken->z, pTagSchema[t].name, sToken->n) == 0 && strlen(pTagSchema[t].name) == sToken->n) {
            SSchema*          pSchema = &pTagSchema[t];

            char tagVal[TSDB_MAX_TAGS_LEN];
            if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
              if (pItem->pVar.nLen > pSchema->bytes) {
                tdDestroyKVRowBuilder(&kvRowBuilder);
                return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
              }
            }

            ret = tVariantDump(&(pItem->pVar), tagVal, pSchema->type, true);

            // check again after the convert since it may be converted from binary to nchar.
            if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
              int16_t len = varDataTLen(tagVal);
              if (len > pSchema->bytes) {
                tdDestroyKVRowBuilder(&kvRowBuilder);
                return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
              }
            }

            if (ret != TSDB_CODE_SUCCESS) {
              tdDestroyKVRowBuilder(&kvRowBuilder);
              return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
            }

            tdAddColToKVRow(&kvRowBuilder, pSchema->colId, pSchema->type, tagVal);

            findColumnIndex = true;
            break;
          }
        }

        if (!findColumnIndex) {
          tdDestroyKVRowBuilder(&kvRowBuilder);
          return tscInvalidSQLErrMsg(pCmd->payload, "invalid tag name", sToken->z);
        }
      }
    } else {
      if (schemaSize != valSize) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      for (int32_t i = 0; i < valSize; ++i) {
        SSchema*          pSchema = &pTagSchema[i];
        tVariantListItem* pItem = taosArrayGet(pValList, i);

        char tagVal[TSDB_MAX_TAGS_LEN];
        if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
          if (pItem->pVar.nLen > pSchema->bytes) {
            tdDestroyKVRowBuilder(&kvRowBuilder);
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }
        }

        ret = tVariantDump(&(pItem->pVar), tagVal, pSchema->type, true);

        // check again after the convert since it may be converted from binary to nchar.
        if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
          int16_t len = varDataTLen(tagVal);
          if (len > pSchema->bytes) {
            tdDestroyKVRowBuilder(&kvRowBuilder);
            return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }
        }

        if (ret != TSDB_CODE_SUCCESS) {
          tdDestroyKVRowBuilder(&kvRowBuilder);
          return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
        }

        tdAddColToKVRow(&kvRowBuilder, pSchema->colId, pSchema->type, tagVal);
      }
    }

    SKVRow row = tdGetKVRowFromBuilder(&kvRowBuilder);
    tdDestroyKVRowBuilder(&kvRowBuilder);
    if (row == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    tdSortKVRowByColIdx(row);
    pTag->dataLen = kvRowLen(row);

    if (pTag->data == NULL) {
      pTag->data = malloc(pTag->dataLen);
    }

    kvRowCpy(pTag->data, row);
    free(row);

    // table name
    if (tscValidateName(&(pCreateTableInfo->name)) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, TABLE_INDEX);
    ret = tscSetTableFullName(pTableMetaInfo, &pCreateTableInfo->name, pSql);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    pCreateTableInfo->fullname = calloc(1, tNameLen(&pTableMetaInfo->name) + 1);
    ret = tNameExtractFullName(&pTableMetaInfo->name, pCreateTableInfo->fullname);
    if (ret != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doCheckForStream(SSqlObj* pSql, SSqlInfo* pInfo) {
  const char* msg1 = "invalid table name";
  const char* msg2 = "functions not allowed in CQ";
  const char* msg3 = "fill only available for interval query";
  const char* msg4 = "fill option not supported in stream computing";
  const char* msg5 = "sql too long";  // todo ADD support
  const char* msg6 = "from missing in subclause";
  const char* msg7 = "time interval is required";
  
  SSqlCmd*    pCmd = &pSql->cmd;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  assert(pQueryInfo->numOfTables == 1);

  SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;
  STableMetaInfo*  pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  // if sql specifies db, use it, otherwise use default db
  SStrToken* pName = &(pCreateTable->name);
  SQuerySqlNode* pQuerySqlNode = pCreateTable->pSelect;

  if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }
  
  SArray* pSrcMeterName = pInfo->pCreateTableInfo->pSelect->from;
  if (pSrcMeterName == NULL || taosArrayGetSize(pSrcMeterName) == 0) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
  }
  
  tVariantListItem* p1 = taosArrayGet(pSrcMeterName, 0);
  SStrToken srcToken = {.z = p1->pVar.pz, .n = p1->pVar.nLen, .type = TK_STRING};
  if (tscValidateName(&srcToken) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  int32_t code = tscSetTableFullName(pTableMetaInfo, &srcToken, pSql);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tscGetTableMeta(pSql, pTableMetaInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  bool isSTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);
  if (parseSelectClause(&pSql->cmd, 0, pQuerySqlNode->pSelectList, isSTable, false, false) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (pQuerySqlNode->pWhere != NULL) {  // query condition in stream computing
    if (parseWhereClause(pQueryInfo, &pQuerySqlNode->pWhere, pSql) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  // set interval value
  if (parseIntervalClause(pSql, pQueryInfo, pQuerySqlNode) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  if (isTimeWindowQuery(pQueryInfo) && (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  if (!tscIsProjectionQuery(pQueryInfo) && pQueryInfo->interval.interval == 0) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
  }

  // set the created table[stream] name
  code = tscSetTableFullName(pTableMetaInfo, pName, pSql);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pQuerySqlNode->sqlstr.n > TSDB_MAX_SAVED_SQL_LEN) {
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
  if (pQuerySqlNode->fillType != NULL) {
    if (pQueryInfo->interval.interval == 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    tVariantListItem* pItem = taosArrayGet(pQuerySqlNode->fillType, 0);
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

static int32_t checkQueryRangeForFill(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg3 = "start(end) time of query range required or time range too large";

  if (pQueryInfo->interval.interval == 0) {
    return TSDB_CODE_SUCCESS;
  }

    bool initialWindows = TSWINDOW_IS_EQUAL(pQueryInfo->window, TSWINDOW_INITIALIZER);
    if (initialWindows) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    int64_t timeRange = ABS(pQueryInfo->window.skey - pQueryInfo->window.ekey);

    int64_t intervalRange = 0;
    if (pQueryInfo->interval.intervalUnit == 'n' || pQueryInfo->interval.intervalUnit == 'y') {
      int64_t f = 1;
      if (pQueryInfo->interval.intervalUnit == 'n') {
        f = 30L * MILLISECOND_PER_DAY;
      } else if (pQueryInfo->interval.intervalUnit == 'y') {
        f = 365L * MILLISECOND_PER_DAY;
      }

      intervalRange = pQueryInfo->interval.interval * f;
    } else {
      intervalRange = pQueryInfo->interval.interval;
    }
    // number of result is not greater than 10,000,000
    if ((timeRange == 0) || (timeRange / intervalRange) >= MAX_INTERVAL_TIME_WINDOW) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    return TSDB_CODE_SUCCESS;
}

int32_t doValidateSqlNode(SSqlObj* pSql, SQuerySqlNode* pQuerySqlNode, int32_t index) {
  assert(pQuerySqlNode != NULL && (pQuerySqlNode->from == NULL || taosArrayGetSize(pQuerySqlNode->from) > 0));

  const char* msg0 = "invalid table name";
  const char* msg1 = "point interpolation query needs timestamp";
  const char* msg2 = "fill only available for interval query";
  const char* msg3 = "start(end) time of query range required or time range too large";
  const char* msg4 = "illegal number of tables in from clause";
  const char* msg5 = "too many columns in selection clause";
  const char* msg6 = "too many tables in from clause";
  const char* msg7 = "invalid table alias name";

  int32_t code = TSDB_CODE_SUCCESS;

  SSqlCmd* pCmd = &pSql->cmd;

  SQueryInfo*     pQueryInfo = tscGetQueryInfoDetail(pCmd, index);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (pTableMetaInfo == NULL) {
    pTableMetaInfo = tscAddEmptyMetaInfo(pQueryInfo);
  }

  assert(pCmd->clauseIndex == index);

  // too many result columns not support order by in query
  if (taosArrayGetSize(pQuerySqlNode->pSelectList) > TSDB_MAX_COLUMNS) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg5);
  }

  /*
   * handle the sql expression without from subclause
   * select current_database();
   * select server_version();
   * select client_version();
   * select server_state();
   */
  if (pQuerySqlNode->from == NULL) {
    assert(pQuerySqlNode->fillType == NULL && pQuerySqlNode->pGroupby == NULL && pQuerySqlNode->pWhere == NULL &&
           pQuerySqlNode->pSortOrder == NULL);
    return doLocalQueryProcess(pCmd, pQueryInfo, pQuerySqlNode);
  }

  size_t fromSize = taosArrayGetSize(pQuerySqlNode->from);
  if (fromSize > TSDB_MAX_JOIN_TABLE_NUM * 2) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  pQueryInfo->command = TSDB_SQL_SELECT;

  if (fromSize > 4) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg6);
  }

  // set all query tables, which are maybe more than one.
  for (int32_t i = 0; i < fromSize; ) {
    tVariantListItem* item = taosArrayGet(pQuerySqlNode->from, i);
    tVariant* pTableItem = &item->pVar;

    if (pTableItem->nType != TSDB_DATA_TYPE_BINARY) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }

    pTableItem->nLen = strdequote(pTableItem->pz);

    SStrToken tableName = {.z = pTableItem->pz, .n = pTableItem->nLen, .type = TK_STRING};
    if (tscValidateName(&tableName) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg0);
    }

    if (pQueryInfo->numOfTables <= i/2) {  // more than one table
      tscAddEmptyMetaInfo(pQueryInfo);
    }

    STableMetaInfo* pTableMetaInfo1 = tscGetMetaInfo(pQueryInfo, i/2);

    SStrToken t = {.type = TSDB_DATA_TYPE_BINARY, .n = pTableItem->nLen, .z = pTableItem->pz};
    code = tscSetTableFullName(pTableMetaInfo1, &t, pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    tVariantListItem* p1 = taosArrayGet(pQuerySqlNode->from, i + 1);
    if (p1->pVar.nType != TSDB_DATA_TYPE_BINARY) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
    }

    SStrToken aliasName = {.z = p1->pVar.pz, .n = p1->pVar.nLen, .type = TK_STRING};
    if (tscValidateName(&aliasName) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg7);
    }

    // has no table alias name
    if (memcmp(pTableItem->pz, p1->pVar.pz, p1->pVar.nLen) == 0) {
      strncpy(pTableMetaInfo1->aliasName, tNameGetTableName(&pTableMetaInfo1->name), tListLen(pTableMetaInfo->aliasName));
    } else {
      tstrncpy(pTableMetaInfo1->aliasName, p1->pVar.pz, sizeof(pTableMetaInfo1->aliasName));
    }

    code = tscGetTableMeta(pSql, pTableMetaInfo1);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    i += 2;
  }

  assert(pQueryInfo->numOfTables == taosArrayGetSize(pQuerySqlNode->from) / 2);
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
  if (parseGroupbyClause(pQueryInfo, pQuerySqlNode->pGroupby, pCmd) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  // set where info
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);

  if (pQuerySqlNode->pWhere != NULL) {
    if (parseWhereClause(pQueryInfo, &pQuerySqlNode->pWhere, pSql) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    pQuerySqlNode->pWhere = NULL;
    if (tinfo.precision == TSDB_TIME_PRECISION_MILLI) {
      pQueryInfo->window.skey = pQueryInfo->window.skey / 1000;
      pQueryInfo->window.ekey = pQueryInfo->window.ekey / 1000;
    }
  } else {  // set the time rang
    if (taosArrayGetSize(pQuerySqlNode->from) > 2) { // it is a join query, no wher clause is not allowed.
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), "condition missing for join query ");
    }
  }

  int32_t joinQuery = (pQuerySqlNode->from != NULL && taosArrayGetSize(pQuerySqlNode->from) > 2);
  int32_t timeWindowQuery =
      (TPARSER_HAS_TOKEN(pQuerySqlNode->interval.interval) || TPARSER_HAS_TOKEN(pQuerySqlNode->sessionVal.gap));

  if (parseSelectClause(pCmd, index, pQuerySqlNode->pSelectList, isSTable, joinQuery, timeWindowQuery) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  // set order by info
  if (parseOrderbyClause(pCmd, pQueryInfo, pQuerySqlNode, tscGetTableSchema(pTableMetaInfo->pTableMeta)) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  // set interval value
  if (parseIntervalClause(pSql, pQueryInfo, pQuerySqlNode) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  } else {
    if (isTimeWindowQuery(pQueryInfo) &&
        (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }
  }

  if (parseSessionClause(pCmd, pQueryInfo, pQuerySqlNode) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  // no result due to invalid query time range
  if (pQueryInfo->window.skey > pQueryInfo->window.ekey) {
    pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    return TSDB_CODE_SUCCESS;
  }

  if (!hasTimestampForPointInterpQuery(pQueryInfo)) {
    return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  // in case of join query, time range is required.
  if (QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
    int64_t timeRange = ABS(pQueryInfo->window.skey - pQueryInfo->window.ekey);
    if (timeRange == 0 && pQueryInfo->window.skey == 0) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }
  }

  if ((code = parseLimitClause(pCmd, pQueryInfo, index, pQuerySqlNode, pSql)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  if ((code = doFunctionsCompatibleCheck(pCmd, pQueryInfo)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  updateLastScanOrderIfNeeded(pQueryInfo);
  tscFieldInfoUpdateOffset(pQueryInfo);

  if (pQuerySqlNode->fillType != NULL) {
    if (pQueryInfo->interval.interval == 0 && (!tscIsPointInterpQuery(pQueryInfo))) {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    /*
     * fill options are set at the end position, when all columns are set properly
     * the columns may be increased due to group by operation
     */
    if ((code = checkQueryRangeForFill(pCmd, pQueryInfo)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    if ((code = parseFillClause(pCmd, pQueryInfo, pQuerySqlNode)) != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;  // Does not build query message here
}

int32_t exprTreeFromSqlExpr(SSqlCmd* pCmd, tExprNode **pExpr, const tSqlExpr* pSqlExpr, SQueryInfo* pQueryInfo, SArray* pCols, int64_t *uid) {
  tExprNode* pLeft = NULL;
  tExprNode* pRight= NULL;
  
  if (pSqlExpr->pLeft != NULL) {
    int32_t ret = exprTreeFromSqlExpr(pCmd, &pLeft, pSqlExpr->pLeft, pQueryInfo, pCols, uid);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }
  
  if (pSqlExpr->pRight != NULL) {
    int32_t ret = exprTreeFromSqlExpr(pCmd, &pRight, pSqlExpr->pRight, pQueryInfo, pCols, uid);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  if (pSqlExpr->pLeft == NULL && pSqlExpr->pRight == NULL && pSqlExpr->tokenId == 0) {
    *pExpr = calloc(1, sizeof(tExprNode));
    return TSDB_CODE_SUCCESS;
  }
  
  if (pSqlExpr->pLeft == NULL) {
    if (pSqlExpr->type == SQL_NODE_VALUE) {
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_VALUE;
      (*pExpr)->pVal = calloc(1, sizeof(tVariant));
      
      tVariantAssign((*pExpr)->pVal, &pSqlExpr->value);
      return TSDB_CODE_SUCCESS;
    } else if (pSqlExpr->type == SQL_NODE_SQLFUNCTION) {
      // arithmetic expression on the results of aggregation functions
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_COL;
      (*pExpr)->pSchema = calloc(1, sizeof(SSchema));
      strncpy((*pExpr)->pSchema->name, pSqlExpr->token.z, pSqlExpr->token.n);
      
      // set the input column data byte and type.
      size_t size = taosArrayGetSize(pQueryInfo->exprList);
      
      for (int32_t i = 0; i < size; ++i) {
        SSqlExpr* p1 = taosArrayGetP(pQueryInfo->exprList, i);
        
        if (strcmp((*pExpr)->pSchema->name, p1->aliasName) == 0) {
          (*pExpr)->pSchema->type  = (uint8_t)p1->resType;
          (*pExpr)->pSchema->bytes = p1->resBytes;
          (*pExpr)->pSchema->colId = p1->resColId;

          if (uid != NULL) {
            *uid = p1->uid;
          }

          break;
        }
      }
    } else if (pSqlExpr->type == SQL_NODE_TABLE_COLUMN) { // column name, normal column arithmetic expression
      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      int32_t ret = getColumnIndexByName(pCmd, &pSqlExpr->colInfo, pQueryInfo, &index);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, 0)->pTableMeta;
      int32_t numOfColumns = tscGetNumOfColumns(pTableMeta);

      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_COL;
      (*pExpr)->pSchema = calloc(1, sizeof(SSchema));

      SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, index.columnIndex);
      *(*pExpr)->pSchema = *pSchema;
  
      if (pCols != NULL) {  // record the involved columns
        SColIndex colIndex = {0};
        tstrncpy(colIndex.name, pSchema->name, sizeof(colIndex.name));
        colIndex.colId = pSchema->colId;
        colIndex.colIndex = index.columnIndex;
        colIndex.flag = (index.columnIndex >= numOfColumns)? 1:0;

        taosArrayPush(pCols, &colIndex);
      }
      
      return TSDB_CODE_SUCCESS;
    } else {
      return invalidSqlErrMsg(tscGetErrorMsgPayload(pCmd), "not support filter expression");
    }
    
  } else {
    *pExpr = (tExprNode *)calloc(1, sizeof(tExprNode));
    (*pExpr)->nodeType = TSQL_NODE_EXPR;
    
    (*pExpr)->_node.hasPK = false;
    (*pExpr)->_node.pLeft = pLeft;
    (*pExpr)->_node.pRight = pRight;
    
    SStrToken t = {.type = pSqlExpr->tokenId};
    (*pExpr)->_node.optr = convertOptr(&t);
    
    assert((*pExpr)->_node.optr != 0);

    // check for dividing by 0
    if ((*pExpr)->_node.optr == TSDB_BINARY_OP_DIVIDE) {
      if (pRight->nodeType == TSQL_NODE_VALUE) {
        if (pRight->pVal->nType == TSDB_DATA_TYPE_INT && pRight->pVal->i64 == 0) {
          return TSDB_CODE_TSC_INVALID_SQL;
        } else if (pRight->pVal->nType == TSDB_DATA_TYPE_FLOAT && pRight->pVal->dKey == 0) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }
      }
    }

    // NOTE: binary|nchar data allows the >|< type filter
    if ((*pExpr)->_node.optr != TSDB_RELATION_EQUAL && (*pExpr)->_node.optr != TSDB_RELATION_NOT_EQUAL) {
      if (pRight != NULL && pRight->nodeType == TSQL_NODE_VALUE) {
        if (pRight->pVal->nType == TSDB_DATA_TYPE_BOOL) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }
      }
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

bool hasNormalColumnFilter(SQueryInfo* pQueryInfo) {
  size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumn* pCol = taosArrayGetP(pQueryInfo->colList, i);
    if (pCol->numOfFilters > 0) {
      return true;
    }
  }

  return false;
}


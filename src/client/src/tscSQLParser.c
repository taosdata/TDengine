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

#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "os.h"
#include "taos.h"
#include "taosmsg.h"
#include "tstoken.h"
#include "ttime.h"

#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tscSQLParser.h"

#pragma GCC diagnostic ignored "-Wunused-variable"

#define DEFAULT_PRIMARY_TIMESTAMP_COL_NAME "_c0"

// -1 is tbname column index, so here use the -2 as the initial value
#define COLUMN_INDEX_INITIAL_VAL (-2)
#define COLUMN_INDEX_INITIALIZER \
  { COLUMN_INDEX_INITIAL_VAL, COLUMN_INDEX_INITIAL_VAL }
#define COLUMN_INDEX_VALIDE(index) (((index).tableIndex >= 0) && ((index).columnIndex >= TSDB_TBNAME_COLUMN_INDEX))
#define TBNAME_LIST_SEP ","

typedef struct SColumnList {
  int32_t      num;
  SColumnIndex ids[TSDB_MAX_COLUMNS];
} SColumnList;

typedef struct SColumnIdListRes {
  SSchema*    pSchema;
  int32_t     numOfCols;
  SColumnList list;
} SColumnIdListRes;

static SSqlExpr* doAddProjectCol(SSqlCmd* pCmd, int32_t outputIndex, int32_t colIdx, int32_t tableIndex);

static int32_t setShowInfo(SSqlObj* pSql, SSqlInfo* pInfo);
static char* getAccountId(SSqlObj* pSql);

static bool has(tFieldList* pFieldList, int32_t startIdx, const char* name);
static void getCurrentDBName(SSqlObj* pSql, SSQLToken* pDBToken);
static bool hasSpecifyDB(SSQLToken* pTableName);
static bool validateTableColumnInfo(tFieldList* pFieldList, SSqlCmd* pCmd);
static bool validateTagParams(tFieldList* pTagsList, tFieldList* pFieldList, SSqlCmd* pCmd);

static int32_t setObjFullName(char* fullName, char* account, SSQLToken* pDB, SSQLToken* tableName, int32_t* len);

static void getColumnName(tSQLExprItem* pItem, char* resultFieldName, int32_t nameLength);
static void getRevisedName(char* resultFieldName, int32_t functionId, int32_t maxLen, char* columnName);

static int32_t addExprAndResultField(SSqlCmd* pCmd, int32_t colIdx, tSQLExprItem* pItem);
static int32_t insertResultField(SSqlCmd* pCmd, int32_t outputIndex, SColumnList* pIdList, int16_t bytes, int8_t type,
                                 char* fieldName);
static int32_t changeFunctionID(int32_t optr, int16_t* functionId);
static int32_t parseSelectClause(SSqlCmd* pCmd, tSQLExprList* pSelection, bool isMetric);

static bool validateIpAddress(char* ip);
static bool hasUnsupportFunctionsForMetricQuery(SSqlCmd* pCmd);
static bool functionCompatibleCheck(SSqlCmd* pCmd);
static void setColumnOffsetValueInResultset(SSqlCmd* pCmd);

static int32_t parseGroupbyClause(SSqlCmd* pCmd, tVariantList* pList);

static int32_t parseIntervalClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql);
static int32_t setSlidingClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql);

static int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, tSQLExprItem* pItem);

static int32_t parseWhereClause(SSqlObj* pSql, tSQLExpr** pExpr);
static int32_t parseFillClause(SSqlCmd* pCmd, SQuerySQL* pQuerySQL);
static int32_t parseOrderbyClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql, SSchema* pSchema, int32_t numOfCols);

static int32_t tsRewriteFieldNameIfNecessary(SSqlCmd* pCmd);
static int32_t setAlterTableInfo(SSqlObj* pSql, struct SSqlInfo* pInfo);
static int32_t validateSqlFunctionInStreamSql(SSqlCmd* pCmd);
static int32_t buildArithmeticExprString(tSQLExpr* pExpr, char** exprString);
static int32_t validateFunctionsInIntervalOrGroupbyQuery(SSqlCmd* pCmd);
static int32_t validateArithmeticSQLExpr(tSQLExpr* pExpr, SSchema* pSchema, int32_t numOfCols, SColumnIdListRes* pList);
static int32_t validateDNodeConfig(tDCLSQL* pOptions);
static int32_t validateLocalConfig(tDCLSQL* pOptions);
static int32_t validateColumnName(char* name);
static int32_t setKillInfo(SSqlObj* pSql, struct SSqlInfo* pInfo);

static bool validateOneTags(SSqlCmd* pCmd, TAOS_FIELD* pTagField);
static bool hasTimestampForPointInterpQuery(SSqlCmd* pCmd);
static void updateTagColumnIndex(SSqlCmd* pCmd, int32_t tableIndex);

static int32_t parseLimitClause(SSqlObj* pSql, SQuerySQL* pQuerySql);
static int32_t parseCreateDBOptions(SSqlCmd* pCmd, SCreateDBInfo* pCreateDbSql);
static int32_t getColumnIndexByNameEx(SSQLToken* pToken, SSqlCmd* pCmd, SColumnIndex* pIndex);
static int32_t getTableIndexByName(SSQLToken* pToken, SSqlCmd* pCmd, SColumnIndex* pIndex);
static int32_t optrToString(tSQLExpr* pExpr, char** exprString);

static int32_t getMeterIndex(SSQLToken* pTableToken, SSqlCmd* pCmd, SColumnIndex* pIndex);
static int32_t doFunctionsCompatibleCheck(SSqlObj* pSql);
static int32_t doLocalQueryProcess(SQuerySQL* pQuerySql, SSqlCmd* pCmd);
static int32_t tscCheckCreateDbParams(SSqlCmd* pCmd, SCreateDbMsg *pCreate);

static SColumnList getColumnList(int32_t num, int16_t tableIndex, int32_t columnIndex);

/*
 * Used during parsing query sql. Since the query sql usually small in length, error position
 * is not needed in the final error message.
 */
static int32_t invalidSqlErrMsg(SSqlCmd *pCmd, const char* errMsg) {
  return tscInvalidSQLErrMsg(pCmd->payload, errMsg, NULL);
}

static int32_t tscQueryOnlyMetricTags(SSqlCmd* pCmd, bool* queryOnMetricTags) {
  assert(QUERY_IS_STABLE_QUERY(pCmd->type));

  *queryOnMetricTags = true;
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);

    if (pExpr->functionId != TSDB_FUNC_TAGPRJ &&
        !(pExpr->functionId == TSDB_FUNC_COUNT && pExpr->colInfo.colIdx == TSDB_TBNAME_COLUMN_INDEX)) {
      *queryOnMetricTags = false;
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int setColumnFilterInfoForTimestamp(SSqlCmd* pCmd, tVariant* pVar) {
  int64_t     time = 0;
  const char* msg = "invalid timestamp";

  strdequote(pVar->pz);
  char*           seg = strnchr(pVar->pz, '-', pVar->nLen, false);
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  if (seg != NULL) {
    if (taosParseTime(pVar->pz, &time, pVar->nLen, pMeterMetaInfo->pMeterMeta->precision) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(pCmd, msg);
    }
  } else {
    if (tVariantDump(pVar, (char*)&time, TSDB_DATA_TYPE_BIGINT)) {
      return invalidSqlErrMsg(pCmd, msg);
    }
  }

  tVariantDestroy(pVar);
  tVariantCreateFromBinary(pVar, (char*)&time, 0, TSDB_DATA_TYPE_BIGINT);

  return TSDB_CODE_SUCCESS;
}

// todo handle memory leak in error handle function
int32_t tscToSQLCmd(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  if (pInfo == NULL || pSql == NULL || pSql->signature != pSql) {
    return TSDB_CODE_APP_ERROR;
  }

  SSqlCmd* pCmd = &(pSql->cmd);

  if (!pInfo->validSql) {
    return invalidSqlErrMsg(pCmd, pInfo->pzErrMsg);
  }

  SMeterMetaInfo* pMeterMetaInfo = tscAddEmptyMeterMetaInfo(pCmd);

  // transfer pInfo into select operation
  switch (pInfo->sqlType) {
    case DROP_TABLE:
    case DROP_USER:
    case DROP_ACCOUNT:
    case DROP_DNODE:
    case DROP_DATABASE: {
      const char* msg = "param name too long";
      const char* msg1 = "invalid ip address";
      const char* msg2 = "invalid name";

      SSQLToken* pzName = &pInfo->pDCLInfo->a[0];
      if ((pInfo->sqlType != DROP_DNODE) && (tscValidateName(pzName) != TSDB_CODE_SUCCESS)) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      if (pInfo->sqlType == DROP_DATABASE) {
        assert(pInfo->pDCLInfo->nTokens == 2);

        pCmd->command = TSDB_SQL_DROP_DB;
        pCmd->existsCheck = (pInfo->pDCLInfo->a[1].n == 1);

        int32_t code = setObjFullName(pMeterMetaInfo->name, getAccountId(pSql), pzName, NULL, NULL);
        if (code != TSDB_CODE_SUCCESS) {
          invalidSqlErrMsg(pCmd, msg2);
        }

        return code;
      } else if (pInfo->sqlType == DROP_TABLE) {
        assert(pInfo->pDCLInfo->nTokens == 2);

        pCmd->existsCheck = (pInfo->pDCLInfo->a[1].n == 1);
        pCmd->command = TSDB_SQL_DROP_TABLE;

        int32_t ret = setMeterID(pSql, pzName, 0);
        if (ret != TSDB_CODE_SUCCESS) {
          invalidSqlErrMsg(pCmd, msg);
        }
        return ret;
      } else {
        if (pzName->n > TSDB_USER_LEN) {
          return invalidSqlErrMsg(pCmd, msg);
        }

        if (pInfo->sqlType == DROP_USER) {
          pCmd->command = TSDB_SQL_DROP_USER;
        } else if (pInfo->sqlType == DROP_ACCOUNT) {
          pCmd->command = TSDB_SQL_DROP_ACCT;
        } else if (pInfo->sqlType == DROP_DNODE) {
          pCmd->command = TSDB_SQL_DROP_DNODE;
          const int32_t MAX_IP_ADDRESS_LEGNTH = 16;

          if (pzName->n > MAX_IP_ADDRESS_LEGNTH) {
            return invalidSqlErrMsg(pCmd, msg1);
          }

          char str[128] = {0};
          strncpy(str, pzName->z, pzName->n);
          if (!validateIpAddress(str)) {
            return invalidSqlErrMsg(pCmd, msg1);
          }
        }

        strncpy(pMeterMetaInfo->name, pzName->z, pzName->n);
        return TSDB_CODE_SUCCESS;
      }
    }

    case USE_DATABASE: {
      const char* msg = "db name too long";
      pCmd->command = TSDB_SQL_USE_DB;

      SSQLToken* pToken = &pInfo->pDCLInfo->a[0];

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, "invalid db name");
      }

      if (pToken->n > TSDB_DB_NAME_LEN) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      int32_t ret = setObjFullName(pMeterMetaInfo->name, getAccountId(pSql), pToken, NULL, NULL);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      break;
    }

    case RESET_QUERY_CACHE: {
      pCmd->command = TSDB_SQL_RESET_CACHE;
      break;
    }

    case SHOW_DATABASES:
    case SHOW_TABLES:
    case SHOW_STABLES:
    case SHOW_MNODES:
    case SHOW_DNODES:
    case SHOW_ACCOUNTS:
    case SHOW_USERS:
    case SHOW_VGROUPS:
    case SHOW_MODULES:
    case SHOW_CONNECTIONS:
    case SHOW_QUERIES:
    case SHOW_STREAMS:
    case SHOW_SCORES:
    case SHOW_GRANTS:
    case SHOW_CONFIGS: 
    case SHOW_VNODES: {
      return setShowInfo(pSql, pInfo);
    }

    case ALTER_DATABASE:
    case CREATE_DATABASE: {
      const char* msg2 = "name too long";
      const char* msg3 = "invalid db name";

      if (pInfo->sqlType == ALTER_DATABASE) {
        pCmd->command = TSDB_SQL_ALTER_DB;
      } else {
        pCmd->command = TSDB_SQL_CREATE_DB;
        pCmd->existsCheck = (pInfo->pDCLInfo->a[0].n == 1);
      }

      SCreateDBInfo* pCreateDB = &(pInfo->pDCLInfo->dbOpt);
      if (tscValidateName(&pCreateDB->dbname) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg3);
      }

      int32_t ret = setObjFullName(pMeterMetaInfo->name, getAccountId(pSql), &(pCreateDB->dbname), NULL, NULL);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      if (parseCreateDBOptions(pCmd, pCreateDB) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      break;
    }

    case CREATE_DNODE: {
      // todo parse hostname
      pCmd->command = TSDB_SQL_CREATE_DNODE;
      const char* msg = "invalid ip address";

      char          ipAddr[64] = {0};
      const int32_t MAX_IP_ADDRESS_LENGTH = 16;
      if (pInfo->pDCLInfo->nTokens > 1 || pInfo->pDCLInfo->a[0].n > MAX_IP_ADDRESS_LENGTH) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      memcpy(ipAddr, pInfo->pDCLInfo->a[0].z, pInfo->pDCLInfo->a[0].n);
      if (validateIpAddress(ipAddr) == false) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      strncpy(pMeterMetaInfo->name, pInfo->pDCLInfo->a[0].z, pInfo->pDCLInfo->a[0].n);
      break;
    }

    case CREATE_ACCOUNT:
    case CREATE_USER: {
      pCmd->command = (pInfo->sqlType == CREATE_USER) ? TSDB_SQL_CREATE_USER : TSDB_SQL_CREATE_ACCT;
      assert(pInfo->pDCLInfo->nTokens >= 2);

      const char* msg = "name or password too long";
      const char* msg1 = "password can not be empty";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "password needs single quote marks enclosed";
      const char* msg4 = "invalid state option, available options[no, r, w, all]";

      if (pInfo->pDCLInfo->a[1].type != TK_STRING) {
        return invalidSqlErrMsg(pCmd, msg3);
      }

      strdequote(pInfo->pDCLInfo->a[1].z);
      strtrim(pInfo->pDCLInfo->a[1].z);  // trim space before and after passwords
      pInfo->pDCLInfo->a[1].n = strlen(pInfo->pDCLInfo->a[1].z);

      if (pInfo->pDCLInfo->a[1].n <= 0) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      if (pInfo->pDCLInfo->a[0].n > TSDB_USER_LEN || pInfo->pDCLInfo->a[1].n > TSDB_PASSWORD_LEN) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      if (tscValidateName(&pInfo->pDCLInfo->a[0]) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      strncpy(pMeterMetaInfo->name, pInfo->pDCLInfo->a[0].z, pInfo->pDCLInfo->a[0].n);  // name
      strncpy(pCmd->payload, pInfo->pDCLInfo->a[1].z, pInfo->pDCLInfo->a[1].n);         // passwd

      if (pInfo->sqlType == CREATE_ACCOUNT) {
        SCreateAcctSQL* pAcctOpt = &pInfo->pDCLInfo->acctOpt;

        pCmd->defaultVal[0] = pAcctOpt->users;
        pCmd->defaultVal[1] = pAcctOpt->dbs;
        pCmd->defaultVal[2] = pAcctOpt->tseries;
        pCmd->defaultVal[3] = pAcctOpt->streams;
        pCmd->defaultVal[4] = pAcctOpt->pps;
        pCmd->defaultVal[5] = pAcctOpt->storage;
        pCmd->defaultVal[6] = pAcctOpt->qtime;
        pCmd->defaultVal[7] = pAcctOpt->conns;

        if (pAcctOpt->stat.n == 0) {
          pCmd->defaultVal[8] = -1;
        } else {
          strdequote(pAcctOpt->stat.z);
          pAcctOpt->stat.n = strlen(pAcctOpt->stat.z);

          if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
            pCmd->defaultVal[8] = TSDB_VN_READ_ACCCESS;
          } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
            pCmd->defaultVal[8] = TSDB_VN_WRITE_ACCCESS;
          } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
            pCmd->defaultVal[8] = TSDB_VN_ALL_ACCCESS;
          } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
            pCmd->defaultVal[8] = 0;
          } else {
            return invalidSqlErrMsg(pCmd, msg4);
          }
        }
      }
      break;
    }
    case ALTER_ACCT: {
      pCmd->command = TSDB_SQL_ALTER_ACCT;
      int32_t num = pInfo->pDCLInfo->nTokens;
      assert(num >= 1 && num <= 2);

      const char* msg = "password too long";
      const char* msg1 = "password can not be empty";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "password needs single quote marks enclosed";
      const char* msg4 = "invalid state option, available options[no, r, w, all]";

      if (num == 2) {
        if (pInfo->pDCLInfo->a[1].type != TK_STRING) {
          return invalidSqlErrMsg(pCmd, msg3);
        }

        strdequote(pInfo->pDCLInfo->a[1].z);
        strtrim(pInfo->pDCLInfo->a[1].z);  // trim space before and after passwords
        pInfo->pDCLInfo->a[1].n = strlen(pInfo->pDCLInfo->a[1].z);

        if (pInfo->pDCLInfo->a[1].n <= 0) {
          return invalidSqlErrMsg(pCmd, msg1);
        }

        if (pInfo->pDCLInfo->a[1].n > TSDB_PASSWORD_LEN) {
          return invalidSqlErrMsg(pCmd, msg);
        }

        strncpy(pCmd->payload, pInfo->pDCLInfo->a[1].z, pInfo->pDCLInfo->a[1].n);  // passwd
      }

      if (pInfo->pDCLInfo->a[0].n > TSDB_USER_LEN) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      if (tscValidateName(&pInfo->pDCLInfo->a[0]) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      strncpy(pMeterMetaInfo->name, pInfo->pDCLInfo->a[0].z, pInfo->pDCLInfo->a[0].n);  // name

      SCreateAcctSQL* pAcctOpt = &pInfo->pDCLInfo->acctOpt;
      pCmd->defaultVal[0] = pAcctOpt->users;
      pCmd->defaultVal[1] = pAcctOpt->dbs;
      pCmd->defaultVal[2] = pAcctOpt->tseries;
      pCmd->defaultVal[3] = pAcctOpt->streams;
      pCmd->defaultVal[4] = pAcctOpt->pps;
      pCmd->defaultVal[5] = pAcctOpt->storage;
      pCmd->defaultVal[6] = pAcctOpt->qtime;
      pCmd->defaultVal[7] = pAcctOpt->conns;

      if (pAcctOpt->stat.n == 0) {
        pCmd->defaultVal[8] = -1;
      } else {
        strdequote(pAcctOpt->stat.z);
        pAcctOpt->stat.n = strlen(pAcctOpt->stat.z);

        if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
          pCmd->defaultVal[8] = TSDB_VN_READ_ACCCESS;
        } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
          pCmd->defaultVal[8] = TSDB_VN_WRITE_ACCCESS;
        } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
          pCmd->defaultVal[8] = TSDB_VN_ALL_ACCCESS;
        } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
          pCmd->defaultVal[8] = 0;
        } else {
          return invalidSqlErrMsg(pCmd, msg4);
        }
      }
      break;
    }
    case DESCRIBE_TABLE: {
      pCmd->command = TSDB_SQL_DESCRIBE_TABLE;

      SSQLToken*  pToken = &pInfo->pDCLInfo->a[0];
      const char* msg = "table name is too long";
      const char* msg1 = "invalid table name";

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      if (pToken->n > TSDB_METER_NAME_LEN) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      if (setMeterID(pSql, pToken, 0) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      int32_t ret = tscGetMeterMeta(pSql, pMeterMetaInfo->name, 0);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      break;
    }
    case ALTER_DNODE:
    case ALTER_USER_PASSWD:
    case ALTER_USER_PRIVILEGES: {
      pCmd->command = (pInfo->sqlType == ALTER_DNODE) ? TSDB_SQL_CFG_DNODE : TSDB_SQL_ALTER_USER;

      tDCLSQL* pDCL = pInfo->pDCLInfo;

      const char* msg = "parameters too long";
      const char* msg1 = "invalid ip address";
      const char* msg2 = "invalid configure options or values";
      const char* msg3 = "password can not be empty";

      if (pInfo->sqlType != ALTER_DNODE) {
        strdequote(pDCL->a[1].z);
        strtrim(pDCL->a[1].z);
        pDCL->a[1].n = strlen(pDCL->a[1].z);
      }

      if (pDCL->a[1].n <= 0) {
        return invalidSqlErrMsg(pCmd, msg3);
      }

      if (pDCL->a[0].n > TSDB_METER_NAME_LEN || pDCL->a[1].n > TSDB_PASSWORD_LEN) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      if (pCmd->command == TSDB_SQL_CFG_DNODE) {
        char ip[128] = {0};
        strncpy(ip, pDCL->a[0].z, pDCL->a[0].n);

        /* validate the ip address */
        if (!validateIpAddress(ip)) {
          return invalidSqlErrMsg(pCmd, msg1);
        }

        strcpy(pMeterMetaInfo->name, ip);

        /* validate the parameter names and options */
        if (validateDNodeConfig(pDCL) != TSDB_CODE_SUCCESS) {
          return invalidSqlErrMsg(pCmd, msg2);
        }

        strncpy(pCmd->payload, pDCL->a[1].z, pDCL->a[1].n);

        if (pDCL->nTokens == 3) {
          pCmd->payload[pDCL->a[1].n] = ' ';  // add sep
          strncpy(&pCmd->payload[pDCL->a[1].n + 1], pDCL->a[2].z, pDCL->a[2].n);
        }
      } else {
        const char* msg = "invalid user rights";
        const char* msg1 = "password can not be empty or larger than 24 characters";

        strncpy(pMeterMetaInfo->name, pDCL->a[0].z, pDCL->a[0].n);

        if (pInfo->sqlType == ALTER_USER_PASSWD) {
          /* update the password for user */
          pCmd->order.order |= TSDB_ALTER_USER_PASSWD;

          strdequote(pDCL->a[1].z);
          pDCL->a[1].n = strlen(pDCL->a[1].z);

          if (pDCL->a[1].n <= 0 || pInfo->pDCLInfo->a[1].n > TSDB_PASSWORD_LEN) {
            /* password cannot be empty string */
            return invalidSqlErrMsg(pCmd, msg1);
          }

          strncpy(pCmd->payload, pDCL->a[1].z, pDCL->a[1].n);
        } else if (pInfo->sqlType == ALTER_USER_PRIVILEGES) {
          pCmd->order.order |= TSDB_ALTER_USER_PRIVILEGES;

          if (strncasecmp(pDCL->a[1].z, "super", 5) == 0 && pDCL->a[1].n == 5) {
            pCmd->count = 1;
          } else if (strncasecmp(pDCL->a[1].z, "read", 4) == 0 && pDCL->a[1].n == 4) {
            pCmd->count = 2;
          } else if (strncasecmp(pDCL->a[1].z, "write", 5) == 0 && pDCL->a[1].n == 5) {
            pCmd->count = 3;
          } else {
            return invalidSqlErrMsg(pCmd, msg);
          }
        } else {
          return TSDB_CODE_INVALID_SQL;
        }
      }
      break;
    }
    case ALTER_LOCAL: {
      pCmd->command = TSDB_SQL_CFG_LOCAL;
      tDCLSQL*    pDCL = pInfo->pDCLInfo;
      const char* msg = "invalid configure options or values";

      // validate the parameter names and options
      if (validateLocalConfig(pDCL) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      strncpy(pCmd->payload, pDCL->a[0].z, pDCL->a[0].n);
      if (pDCL->nTokens == 2) {
        pCmd->payload[pDCL->a[0].n] = ' ';  // add sep
        strncpy(&pCmd->payload[pDCL->a[0].n + 1], pDCL->a[1].z, pDCL->a[1].n);
      }

      break;
    }
    case TSQL_CREATE_NORMAL_METER:
    case TSQL_CREATE_NORMAL_METRIC: {
      const char* msg = "table name too long";
      const char* msg1 = "invalid table name";

      tFieldList* pFieldList = pInfo->pCreateTableInfo->colInfo.pColumns;
      tFieldList* pTagList = pInfo->pCreateTableInfo->colInfo.pTagColumns;
      assert(pFieldList != NULL);

      pCmd->command = TSDB_SQL_CREATE_TABLE;
      pCmd->existsCheck = pInfo->pCreateTableInfo->existCheck;

      // if sql specifies db, use it, otherwise use default db
      SSQLToken* pzTableName = &(pInfo->pCreateTableInfo->name);

      if (tscValidateName(pzTableName) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      if (setMeterID(pSql, pzTableName, 0) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      if (!validateTableColumnInfo(pFieldList, pCmd) ||
          (pTagList != NULL && !validateTagParams(pTagList, pFieldList, pCmd))) {
        return TSDB_CODE_INVALID_SQL;
      }

      int32_t col = 0;
      for (; col < pFieldList->nField; ++col) {
        tscFieldInfoSetValFromField(&pCmd->fieldsInfo, col, &pFieldList->p[col]);
      }
      pCmd->numOfCols = (int16_t)pFieldList->nField;

      if (pTagList != NULL) {  // create metric[optional]
        for (int32_t i = 0; i < pTagList->nField; ++i) {
          tscFieldInfoSetValFromField(&pCmd->fieldsInfo, col++, &pTagList->p[i]);
        }
        pCmd->count = pTagList->nField;
      }

      break;
    }
    case TSQL_CREATE_METER_FROM_METRIC: {
      pCmd->command = TSDB_SQL_CREATE_TABLE;
      pCmd->existsCheck = pInfo->pCreateTableInfo->existCheck;

      const char* msg = "invalid table name";
      const char* msg1 = "illegal value or data overflow";
      const char* msg2 = "illegal number of tags";
      const char* msg3 = "tag value too long";

      // table name
      // metric name, create table by using dst
      SSQLToken* pToken = &(pInfo->pCreateTableInfo->usingInfo.metricName);

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      if (setMeterID(pSql, pToken, 0) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      // get meter meta from mnode
      STagData* pTag = (STagData*)pCmd->payload;
      strncpy(pTag->name, pMeterMetaInfo->name, TSDB_METER_ID_LEN);

      tVariantList* pList = pInfo->pCreateTableInfo->usingInfo.pTagVals;

      int32_t code = tscGetMeterMeta(pSql, pTag->name, 0);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      if (pMeterMetaInfo->pMeterMeta->numOfTags != pList->nExpr) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      // too long tag values will return invalid sql, not be truncated automatically
      SSchema* pTagSchema = tsGetTagSchema(pMeterMetaInfo->pMeterMeta);

      char* tagVal = pTag->data;
      for (int32_t i = 0; i < pList->nExpr; ++i) {
        int32_t ret = tVariantDump(&(pList->a[i].pVar), tagVal, pTagSchema[i].type);
        if (ret != TSDB_CODE_SUCCESS) {
          return invalidSqlErrMsg(pCmd, msg1);
        }

        // validate the length of binary
        if ((pTagSchema[i].type == TSDB_DATA_TYPE_BINARY || pTagSchema[i].type == TSDB_DATA_TYPE_NCHAR) &&
            pList->a[i].pVar.nLen > pTagSchema[i].bytes) {
          return invalidSqlErrMsg(pCmd, msg3);
        }

        tagVal += pTagSchema[i].bytes;
      }

      if (tscValidateName(&pInfo->pCreateTableInfo->name) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      int32_t ret = setMeterID(pSql, &pInfo->pCreateTableInfo->name, 0);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      pCmd->numOfCols = 0;
      pCmd->count = 0;
      break;
    }
    case TSQL_CREATE_STREAM: {
      pCmd->command = TSDB_SQL_CREATE_TABLE;
      const char* msg1 = "invalid table name";
      const char* msg2 = "table name too long";
      const char* msg3 = "fill only available for interval query";
      const char* msg4 = "fill option not supported in stream computing";
      const char* msg5 = "sql too long";  // todo ADD support

      // if sql specifies db, use it, otherwise use default db
      SSQLToken* pzTableName = &(pInfo->pCreateTableInfo->name);
      SQuerySQL* pQuerySql = pInfo->pCreateTableInfo->pSelect;

      if (tscValidateName(pzTableName) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      tVariantList* pSrcMeterName = pInfo->pCreateTableInfo->pSelect->from;
      tVariant*     pVar = &pSrcMeterName->a[0].pVar;

      SSQLToken srcToken = {.z = pVar->pz, .n = pVar->nLen, .type = TK_STRING};
      if (tscValidateName(&srcToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      if (setMeterID(pSql, &srcToken, 0) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      int32_t code = tscGetMeterMeta(pSql, pMeterMetaInfo->name, 0);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      bool isMetric = UTIL_METER_IS_METRIC(pMeterMetaInfo);
      if (parseSelectClause(pCmd, pQuerySql->pSelection, isMetric) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      if (pQuerySql->pWhere != NULL) {  // query condition in stream computing
        if (parseWhereClause(pSql, &pQuerySql->pWhere) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_INVALID_SQL;
        }
      }

      // set interval value
      if (parseIntervalClause(pCmd, pQuerySql) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      } else {
        if ((pCmd->nAggTimeInterval > 0) && (validateFunctionsInIntervalOrGroupbyQuery(pCmd) != TSDB_CODE_SUCCESS)) {
          return TSDB_CODE_INVALID_SQL;
        }
      }

      if (setSlidingClause(pCmd, pQuerySql) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      // set the created table[stream] name
      if (setMeterID(pSql, pzTableName, 0) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      // copy sql length
      int ret = tscAllocPayload(pCmd, pQuerySql->selectToken.n + 8);
      if (TSDB_CODE_SUCCESS != ret) {
        invalidSqlErrMsg(pCmd, "client out of memory");
        return ret;
      }

      strncpy(pCmd->payload, pQuerySql->selectToken.z, pQuerySql->selectToken.n);
      if (pQuerySql->selectToken.n > TSDB_MAX_SAVED_SQL_LEN) {
        return invalidSqlErrMsg(pCmd, msg5);
      }

      if (tsRewriteFieldNameIfNecessary(pCmd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      pCmd->numOfCols = pCmd->fieldsInfo.numOfOutputCols;

      if (validateSqlFunctionInStreamSql(pCmd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      /*
       * check if fill operation is available, the fill operation is parsed and executed during query execution, not
       * here.
       */
      if (pQuerySql->fillType != NULL) {
        if (pCmd->nAggTimeInterval == 0) {
          return invalidSqlErrMsg(pCmd, msg3);
        }

        tVariantListItem* pItem = &pQuerySql->fillType->a[0];
        if (pItem->pVar.nType == TSDB_DATA_TYPE_BINARY) {
          if (!((strncmp(pItem->pVar.pz, "none", 4) == 0 && pItem->pVar.nLen == 4) ||
                (strncmp(pItem->pVar.pz, "null", 4) == 0 && pItem->pVar.nLen == 4))) {
            return invalidSqlErrMsg(pCmd, msg4);
          }
        }
      }

      break;
    }

    case TSQL_QUERY_METER: {
      SQuerySQL* pQuerySql = pInfo->pQueryInfo;
      assert(pQuerySql != NULL && (pQuerySql->from == NULL || pQuerySql->from->nExpr > 0));

      const char* msg0 = "invalid table name";
      const char* msg1 = "table name too long";
      const char* msg2 = "point interpolation query needs timestamp";
      const char* msg3 = "sliding value too small";
      const char* msg4 = "sliding value no larger than the interval value";
      const char* msg5 = "fill only available for interval query";
      const char* msg6 = "start(end) time of query range required or time range too large";
      const char* msg7 = "illegal number of tables in from clause";
      const char* msg8 = "too many columns in selection clause";
      const char* msg9 = "TWA query requires both the start and end time";
      
      int32_t code = TSDB_CODE_SUCCESS;

      // too many result columns not support order by in query
      if (pQuerySql->pSelection->nExpr > TSDB_MAX_COLUMNS) {
        return invalidSqlErrMsg(pCmd, msg8);
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
        return doLocalQueryProcess(pQuerySql, pCmd);
      }

      if (pQuerySql->from->nExpr > TSDB_MAX_JOIN_TABLE_NUM) {
        return invalidSqlErrMsg(pCmd, msg7);
      }

      // set all query tables, which are maybe more than one.
      for (int32_t i = 0; i < pQuerySql->from->nExpr; ++i) {
        tVariant* pTableItem = &pQuerySql->from->a[i].pVar;

        if (pTableItem->nType != TSDB_DATA_TYPE_BINARY) {
          return invalidSqlErrMsg(pCmd, msg0);
        }

        pTableItem->nLen = strdequote(pTableItem->pz);

        SSQLToken tableName = {.z = pTableItem->pz, .n = pTableItem->nLen, .type = TK_STRING};
        if (tscValidateName(&tableName) != TSDB_CODE_SUCCESS) {
          return invalidSqlErrMsg(pCmd, msg0);
        }

        if (pCmd->numOfTables <= i) {
          tscAddEmptyMeterMetaInfo(pCmd);
        }

        SSQLToken t = {.type = TSDB_DATA_TYPE_BINARY, .n = pTableItem->nLen, .z = pTableItem->pz};
        if (setMeterID(pSql, &t, i) != TSDB_CODE_SUCCESS) {
          return invalidSqlErrMsg(pCmd, msg1);
        }

        SMeterMetaInfo* pMeterInfo1 = tscGetMeterMetaInfo(pCmd, i);
        code = tscGetMeterMeta(pSql, pMeterInfo1->name, i);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }

      pSql->cmd.command = TSDB_SQL_SELECT;

      // parse the group by clause in the first place
      if (parseGroupbyClause(pCmd, pQuerySql->pGroupby) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      bool isMetric = UTIL_METER_IS_METRIC(pMeterMetaInfo);
      if (parseSelectClause(pCmd, pQuerySql->pSelection, isMetric) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      // set interval value
      if (parseIntervalClause(pCmd, pQuerySql) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      } else {
        if ((pCmd->nAggTimeInterval > 0) && (validateFunctionsInIntervalOrGroupbyQuery(pCmd) != TSDB_CODE_SUCCESS)) {
          return TSDB_CODE_INVALID_SQL;
        }
      }

      // set sliding value
      SSQLToken* pSliding = &pQuerySql->sliding;
      if (pSliding->n != 0) {
        // TODO refactor pCmd->count == 1 means sql in stream function
        if (!tscEmbedded && pCmd->count == 0) {
          const char* msg = "not support sliding in query";
          return invalidSqlErrMsg(pCmd, msg);
        }

        getTimestampInUsFromStr(pSliding->z, pSliding->n, &pCmd->nSlidingTime);
        if (pMeterMetaInfo->pMeterMeta->precision == TSDB_TIME_PRECISION_MILLI) {
          pCmd->nSlidingTime /= 1000;
        }

        if (pCmd->nSlidingTime < tsMinSlidingTime) {
          return invalidSqlErrMsg(pCmd, msg3);
        }

        if (pCmd->nSlidingTime > pCmd->nAggTimeInterval) {
          return invalidSqlErrMsg(pCmd, msg4);
        }
      }

      // set order by info
      if (parseOrderbyClause(pCmd, pQuerySql, tsGetSchema(pMeterMetaInfo->pMeterMeta),
                             pMeterMetaInfo->pMeterMeta->numOfColumns) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      // set where info
      if (pQuerySql->pWhere != NULL) {
        if (parseWhereClause(pSql, &pQuerySql->pWhere) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_INVALID_SQL;
        }

        pQuerySql->pWhere = NULL;

        if (pMeterMetaInfo->pMeterMeta->precision == TSDB_TIME_PRECISION_MILLI) {
          pCmd->stime = pCmd->stime / 1000;
          pCmd->etime = pCmd->etime / 1000;
        }
      } else {  // set the time rang
        pCmd->stime = 0;
        pCmd->etime = INT64_MAX;
      }

      // user does not specified the query time window, twa is not allowed in such case.
      if ((pCmd->stime == 0 || pCmd->etime == INT64_MAX ||
           (pCmd->etime == INT64_MAX / 1000 && pMeterMetaInfo->pMeterMeta->precision == TSDB_TIME_PRECISION_MILLI)) &&
          tscIsTWAQuery(pCmd)) {
        return invalidSqlErrMsg(pCmd, msg9);
      }

      // no result due to invalid query time range
      if (pCmd->stime > pCmd->etime) {
        pCmd->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
        return TSDB_CODE_SUCCESS;
      }

      if (!hasTimestampForPointInterpQuery(pCmd)) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      if (pQuerySql->fillType != NULL) {
        if (pCmd->nAggTimeInterval == 0 && (!tscIsPointInterpQuery(pCmd))) {
          return invalidSqlErrMsg(pCmd, msg5);
        }

        if (pCmd->nAggTimeInterval > 0) {
          int64_t timeRange = labs(pCmd->stime - pCmd->etime);
          // number of result is not greater than 10,000,000
          if ((timeRange == 0) || (timeRange / pCmd->nAggTimeInterval) > MAX_RETRIEVE_ROWS_IN_INTERVAL_QUERY) {
            return invalidSqlErrMsg(pCmd, msg6);
          }
        }

        int32_t ret = parseFillClause(pCmd, pQuerySql);
        if (ret != TSDB_CODE_SUCCESS) {
          return ret;
        }
      }

      // in case of join query, time range is required.
      if (QUERY_IS_JOIN_QUERY(pCmd->type)) {
        int64_t timeRange = labs(pCmd->stime - pCmd->etime);

        if (timeRange == 0 && pCmd->stime == 0) {
          return invalidSqlErrMsg(pCmd, msg6);
        }
      }

      // handle the limit offset value, validate the limit
      pCmd->limit = pQuerySql->limit;

      // temporarily save the original limitation value
      if ((code = parseLimitClause(pSql, pQuerySql)) != TSDB_CODE_SUCCESS) {
        return code;
      }

      if ((code = doFunctionsCompatibleCheck(pSql)) != TSDB_CODE_SUCCESS) {
        return code;
      }

      setColumnOffsetValueInResultset(pCmd);
      updateTagColumnIndex(pCmd, 0);

      break;
    }
    case TSQL_INSERT: {
      assert(false);
    }
    case ALTER_TABLE_ADD_COLUMN:
    case ALTER_TABLE_DROP_COLUMN:
    case ALTER_TABLE_TAGS_ADD:
    case ALTER_TABLE_TAGS_DROP:
    case ALTER_TABLE_TAGS_CHG:
    case ALTER_TABLE_TAGS_SET: {
      return setAlterTableInfo(pSql, pInfo);
    }

    case KILL_CONNECTION:
    case KILL_QUERY:
    case KILL_STREAM: {
      return setKillInfo(pSql, pInfo);
    }

    default:
      return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * if the top/bottom exists, only tags columns, tbname column, and primary timestamp column
 * are available.
 */
static bool isTopBottomQuery(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    int32_t functionId = tscSqlExprGet(pCmd, i)->functionId;

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      return true;
    }
  }

  return false;
}

int32_t parseIntervalClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql) {
  const char* msg1 = "invalid query expression";
  const char* msg2 = "interval cannot be less than 10 ms";

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  if (pQuerySql->interval.type == 0 || pQuerySql->interval.n == 0) {
    return TSDB_CODE_SUCCESS;
  }

  // interval is not null
  SSQLToken* t = &pQuerySql->interval;
  if (getTimestampInUsFromStr(t->z, t->n, &pCmd->nAggTimeInterval) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }

  /* revised the time precision according to the flag */
  if (pMeterMetaInfo->pMeterMeta->precision == TSDB_TIME_PRECISION_MILLI) {
    pCmd->nAggTimeInterval = pCmd->nAggTimeInterval / 1000;
  }

  /* parser has filter the illegal type, no need to check here */
  pCmd->intervalTimeUnit = pQuerySql->interval.z[pQuerySql->interval.n - 1];

  // interval cannot be less than 10 milliseconds
  if (pCmd->nAggTimeInterval < tsMinIntervalTime) {
    return invalidSqlErrMsg(pCmd, msg2);
  }

  // for top/bottom + interval query, we do not add additional timestamp column in the front
  if (isTopBottomQuery(pCmd)) {
    return TSDB_CODE_SUCCESS;
  }

  // check the invalid sql expresssion: select count(tbname)/count(tag1)/count(tag2) from super_table interval(1d);
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr->functionId == TSDB_FUNC_COUNT && TSDB_COL_IS_TAG(pExpr->colInfo.flag)) {
      return invalidSqlErrMsg(pCmd, msg1);
    }
  }

  // need to add timestamp column in result set, if interval is existed
  uint64_t uid = tscSqlExprGet(pCmd, 0)->uid;

  int32_t tableIndex = COLUMN_INDEX_INITIAL_VAL;
  for (int32_t i = 0; i < pCmd->numOfTables; ++i) {
    pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, i);
    if (pMeterMetaInfo->pMeterMeta->uid == uid) {
      tableIndex = i;
      break;
    }
  }

  if (tableIndex == COLUMN_INDEX_INITIAL_VAL) {
    return TSDB_CODE_INVALID_SQL;
  }

  SColumnIndex index = {tableIndex, PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscSqlExprInsert(pCmd, 0, TSDB_FUNC_TS, &index, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE, TSDB_KEYSIZE);

  SColumnList ids = getColumnList(1, 0, PRIMARYKEY_TIMESTAMP_COL_INDEX);
  int32_t     ret = insertResultField(pCmd, 0, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, aAggs[TSDB_FUNC_TS].aName);

  return ret;
}

int32_t setSlidingClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql) {
  const char* msg0 = "sliding value too small";
  const char* msg1 = "sliding value no larger than the interval value";

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  SSQLToken*      pSliding = &pQuerySql->sliding;

  if (pSliding->n != 0) {
    getTimestampInUsFromStr(pSliding->z, pSliding->n, &pCmd->nSlidingTime);
    if (pMeterMetaInfo->pMeterMeta->precision == TSDB_TIME_PRECISION_MILLI) {
      pCmd->nSlidingTime /= 1000;
    }

    if (pCmd->nSlidingTime < tsMinSlidingTime) {
      return invalidSqlErrMsg(pCmd, msg0);
    }

    if (pCmd->nSlidingTime > pCmd->nAggTimeInterval) {
      return invalidSqlErrMsg(pCmd, msg1);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setMeterID(SSqlObj* pSql, SSQLToken* pzTableName, int32_t tableIndex) {
  const char* msg = "name too long";

  SSqlCmd*        pCmd = &pSql->cmd;
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, tableIndex);
  int32_t         code = TSDB_CODE_SUCCESS;

  if (hasSpecifyDB(pzTableName)) {
    /*
     * db has been specified in sql string
     * so we ignore current db path
     */
    code = setObjFullName(pMeterMetaInfo->name, getAccountId(pSql), NULL, pzTableName, NULL);
  } else {  // get current DB name first, then set it into path
    SSQLToken t = {0};
    getCurrentDBName(pSql, &t);

    code = setObjFullName(pMeterMetaInfo->name, NULL, &t, pzTableName, NULL);
  }

  if (code != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(pCmd, msg);
  }

  return code;
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
    invalidSqlErrMsg(pCmd, msg);
    return false;
  }

  // first column must be timestamp
  if (pFieldList->p[0].type != TSDB_DATA_TYPE_TIMESTAMP) {
    invalidSqlErrMsg(pCmd, msg1);
    return false;
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < pFieldList->nField; ++i) {
    nLen += pFieldList->p[i].bytes;
  }

  // max row length must be less than TSDB_MAX_BYTES_PER_ROW
  if (nLen > TSDB_MAX_BYTES_PER_ROW) {
    invalidSqlErrMsg(pCmd, msg2);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < pFieldList->nField; ++i) {
    TAOS_FIELD* pField = &pFieldList->p[i];
    if (pField->type < TSDB_DATA_TYPE_BOOL || pField->type > TSDB_DATA_TYPE_NCHAR) {
      invalidSqlErrMsg(pCmd, msg4);
      return false;
    }

    if ((pField->type == TSDB_DATA_TYPE_BINARY && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_BINARY_LEN)) ||
        (pField->type == TSDB_DATA_TYPE_NCHAR && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_NCHAR_LEN))) {
      invalidSqlErrMsg(pCmd, msg5);
      return false;
    }

    if (validateColumnName(pField->name) != TSDB_CODE_SUCCESS) {
      invalidSqlErrMsg(pCmd, msg6);
      return false;
    }

    if (has(pFieldList, i + 1, pFieldList->p[i].name) == true) {
      invalidSqlErrMsg(pCmd, msg3);
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
    invalidSqlErrMsg(pCmd, msg1);
    return false;
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < pTagsList->nField; ++i) {
    nLen += pTagsList->p[i].bytes;
  }

  // max tag row length must be less than TSDB_MAX_TAGS_LEN
  if (nLen > TSDB_MAX_TAGS_LEN) {
    invalidSqlErrMsg(pCmd, msg2);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < pTagsList->nField; ++i) {
    if (has(pFieldList, 0, pTagsList->p[i].name) == true) {
      invalidSqlErrMsg(pCmd, msg3);
      return false;
    }
  }

  /* timestamp in tag is not allowed */
  for (int32_t i = 0; i < pTagsList->nField; ++i) {
    if (pTagsList->p[i].type == TSDB_DATA_TYPE_TIMESTAMP) {
      invalidSqlErrMsg(pCmd, msg4);
      return false;
    }

    if (pTagsList->p[i].type < TSDB_DATA_TYPE_BOOL || pTagsList->p[i].type > TSDB_DATA_TYPE_NCHAR) {
      invalidSqlErrMsg(pCmd, msg5);
      return false;
    }

    if ((pTagsList->p[i].type == TSDB_DATA_TYPE_BINARY && pTagsList->p[i].bytes <= 0) ||
        (pTagsList->p[i].type == TSDB_DATA_TYPE_NCHAR && pTagsList->p[i].bytes <= 0)) {
      invalidSqlErrMsg(pCmd, msg7);
      return false;
    }

    if (validateColumnName(pTagsList->p[i].name) != TSDB_CODE_SUCCESS) {
      invalidSqlErrMsg(pCmd, msg6);
      return false;
    }

    if (has(pTagsList, i + 1, pTagsList->p[i].name) == true) {
      invalidSqlErrMsg(pCmd, msg3);
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

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  SMeterMeta*     pMeterMeta = pMeterMetaInfo->pMeterMeta;

  // no more than 6 tags
  if (pMeterMeta->numOfTags == TSDB_MAX_TAGS) {
    char msg[128] = {0};
    sprintf(msg, "tags no more than %d", TSDB_MAX_TAGS);

    invalidSqlErrMsg(pCmd, msg);
    return false;
  }

  // no timestamp allowable
  if (pTagField->type == TSDB_DATA_TYPE_TIMESTAMP) {
    invalidSqlErrMsg(pCmd, msg1);
    return false;
  }

  if (pTagField->type < TSDB_DATA_TYPE_BOOL && pTagField->type > TSDB_DATA_TYPE_NCHAR) {
    invalidSqlErrMsg(pCmd, msg6);
    return false;
  }

  SSchema* pTagSchema = tsGetTagSchema(pMeterMetaInfo->pMeterMeta);
  int32_t  nLen = 0;

  for (int32_t i = 0; i < pMeterMeta->numOfTags; ++i) {
    nLen += pTagSchema[i].bytes;
  }

  // length less than TSDB_MAX_TASG_LEN
  if (nLen + pTagField->bytes > TSDB_MAX_TAGS_LEN) {
    invalidSqlErrMsg(pCmd, msg3);
    return false;
  }

  // tags name can not be a keyword
  if (validateColumnName(pTagField->name) != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(pCmd, msg4);
    return false;
  }

  // binary(val), val can not be equalled to or less than 0
  if ((pTagField->type == TSDB_DATA_TYPE_BINARY || pTagField->type == TSDB_DATA_TYPE_NCHAR) && pTagField->bytes <= 0) {
    invalidSqlErrMsg(pCmd, msg5);
    return false;
  }

  // field name must be unique
  SSchema* pSchema = tsGetSchema(pMeterMeta);

  for (int32_t i = 0; i < pMeterMeta->numOfTags + pMeterMeta->numOfColumns; ++i) {
    if (strncasecmp(pTagField->name, pSchema[i].name, TSDB_COL_NAME_LEN) == 0) {
      invalidSqlErrMsg(pCmd, msg2);
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

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  SMeterMeta*     pMeterMeta = pMeterMetaInfo->pMeterMeta;

  // no more max columns
  if (pMeterMeta->numOfColumns >= TSDB_MAX_COLUMNS ||
      pMeterMeta->numOfTags + pMeterMeta->numOfColumns >= TSDB_MAX_COLUMNS) {
    invalidSqlErrMsg(pCmd, msg1);
    return false;
  }

  if (pColField->type < TSDB_DATA_TYPE_BOOL || pColField->type > TSDB_DATA_TYPE_NCHAR) {
    invalidSqlErrMsg(pCmd, msg4);
    return false;
  }

  if (validateColumnName(pColField->name) != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(pCmd, msg5);
    return false;
  }

  SSchema* pSchema = tsGetSchema(pMeterMeta);
  int32_t  nLen = 0;

  for (int32_t i = 0; i < pMeterMeta->numOfColumns; ++i) {
    nLen += pSchema[i].bytes;
  }

  if (pColField->bytes <= 0) {
    invalidSqlErrMsg(pCmd, msg6);
    return false;
  }

  // length less than TSDB_MAX_BYTES_PER_ROW
  if (nLen + pColField->bytes > TSDB_MAX_BYTES_PER_ROW) {
    invalidSqlErrMsg(pCmd, msg3);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < pMeterMeta->numOfTags + pMeterMeta->numOfColumns; ++i) {
    if (strncasecmp(pColField->name, pSchema[i].name, TSDB_COL_NAME_LEN) == 0) {
      invalidSqlErrMsg(pCmd, msg2);
      return false;
    }
  }

  return true;
}

/* is contained in pFieldList or not */
static bool has(tFieldList* pFieldList, int32_t startIdx, const char* name) {
  for (int32_t j = startIdx; j < pFieldList->nField; ++j) {
    if (strncasecmp(name, pFieldList->p[j].name, TSDB_COL_NAME_LEN) == 0) return true;
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

static int32_t setObjFullName(char* fullName, char* account, SSQLToken* pDB, SSQLToken* tableName, int32_t* xlen) {
  int32_t totalLen = 0;

  if (account != NULL) {
    int32_t len = strlen(account);
    strcpy(fullName, account);
    fullName[len] = TS_PATH_DELIMITER[0];
    totalLen += (len + 1);
  }

  /* db name is not specified, the tableName dose not include db name */
  if (pDB != NULL) {
    if (pDB->n > TSDB_DB_NAME_LEN) {
      return TSDB_CODE_INVALID_SQL;
    }

    memcpy(&fullName[totalLen], pDB->z, pDB->n);
    totalLen += pDB->n;
  }

  if (tableName != NULL) {
    if (pDB != NULL) {
      fullName[totalLen] = TS_PATH_DELIMITER[0];
      totalLen += 1;

      /* here we only check the table name length limitation */
      if (tableName->n > TSDB_METER_NAME_LEN) {
        return TSDB_CODE_INVALID_SQL;
      }
    } else {  // pDB == NULL, the db prefix name is specified in tableName
      /* the length limitation includes tablename + dbname + sep */
      if (tableName->n > TSDB_METER_NAME_LEN + TSDB_DB_NAME_LEN + tListLen(TS_PATH_DELIMITER)) {
        return TSDB_CODE_INVALID_SQL;
      }
    }

    memcpy(&fullName[totalLen], tableName->z, tableName->n);
    totalLen += tableName->n;
  }

  if (xlen != NULL) {
    *xlen = totalLen;
  }

  if (totalLen < TSDB_METER_ID_LEN) {
    fullName[totalLen] = 0;
  }

  return (totalLen <= TSDB_METER_ID_LEN) ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_SQL;
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

int32_t parseSelectClause(SSqlCmd* pCmd, tSQLExprList* pSelection, bool isMetric) {
  assert(pSelection != NULL && pCmd != NULL);

  const char* msg1 = "invalid column name/illegal column type in arithmetic expression";
  const char* msg2 = "functions can not be mixed up";
  const char* msg3 = "not support query expression";

  for (int32_t i = 0; i < pSelection->nExpr; ++i) {
    int32_t       outputIndex = pCmd->fieldsInfo.numOfOutputCols;
    tSQLExprItem* pItem = &pSelection->a[i];

    // project on all fields
    if (pItem->pNode->nSQLOptr == TK_ALL || pItem->pNode->nSQLOptr == TK_ID || pItem->pNode->nSQLOptr == TK_STRING) {
      // it is actually a function, but the function name is invalid
      if (pItem->pNode->nSQLOptr == TK_ID && (pItem->pNode->colInfo.z == NULL && pItem->pNode->colInfo.n == 0)) {
        return TSDB_CODE_INVALID_SQL;
      }

      // if the name of column is quoted, remove it and set the right information for later process
      extractColumnNameFromString(pItem);

      pCmd->type |= TSDB_QUERY_TYPE_PROJECTION_QUERY;

      // select table_name1.field_name1, table_name2.field_name2  from table_name1, table_name2
      if (addProjectionExprAndResultField(pCmd, pItem) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }
    } else if (pItem->pNode->nSQLOptr >= TK_COUNT && pItem->pNode->nSQLOptr <= TK_LAST_ROW) {
      // sql function in selection clause, append sql function info in pSqlCmd structure sequentially
      if (addExprAndResultField(pCmd, outputIndex, pItem) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

    } else if (pItem->pNode->nSQLOptr >= TK_PLUS && pItem->pNode->nSQLOptr <= TK_REM) {
      // arithmetic function in select
      SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
      SSchema*        pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

      SColumnIdListRes columnList = {.pSchema = pSchema, .numOfCols = pMeterMetaInfo->pMeterMeta->numOfColumns};

      int32_t ret =
          validateArithmeticSQLExpr(pItem->pNode, pSchema, pMeterMetaInfo->pMeterMeta->numOfColumns, &columnList);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      char  arithmeticExprStr[1024] = {0};
      char* p = arithmeticExprStr;

      if (buildArithmeticExprString(pItem->pNode, &p) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      // expr string is set as the parameter of function
      SColumnIndex index = {0};
      SSqlExpr*    pExpr = tscSqlExprInsert(pCmd, outputIndex, TSDB_FUNC_ARITHM, &index, TSDB_DATA_TYPE_DOUBLE,
                                         sizeof(double), sizeof(double));
      addExprParams(pExpr, arithmeticExprStr, TSDB_DATA_TYPE_BINARY, strlen(arithmeticExprStr), 0);

      /* todo alias name should use the original sql string */
      if (pItem->aliasName != NULL) {
        strncpy(pExpr->aliasName, pItem->aliasName, TSDB_COL_NAME_LEN);
      } else {
        strncpy(pExpr->aliasName, arithmeticExprStr, TSDB_COL_NAME_LEN);
      }

      insertResultField(pCmd, i, &columnList.list, sizeof(double), TSDB_DATA_TYPE_DOUBLE, pExpr->aliasName);
    } else {
      /*
       * not support such expression
       * e.g., select 12+5 from table_name
       */
      return invalidSqlErrMsg(pCmd, msg3);
    }

    if (pCmd->fieldsInfo.numOfOutputCols > TSDB_MAX_COLUMNS) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  if (!functionCompatibleCheck(pCmd)) {
    return invalidSqlErrMsg(pCmd, msg2);
  }

  if (isMetric) {
    pCmd->type |= TSDB_QUERY_TYPE_STABLE_QUERY;
    SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

    if (tscQueryMetricTags(pCmd)) {  // local handle the metric tag query
      pCmd->command = TSDB_SQL_RETRIEVE_TAGS;
      pCmd->count = pMeterMetaInfo->pMeterMeta->numOfColumns;  // the number of meter schema, tricky.
    }

    /*
     * transfer sql functions that need secondary merge into another format
     * in dealing with metric queries such as: count/first/last
     */
    tscTansformSQLFunctionForMetricQuery(pCmd);

    if (hasUnsupportFunctionsForMetricQuery(pCmd)) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t insertResultField(SSqlCmd* pCmd, int32_t outputIndex, SColumnList* pIdList, int16_t bytes, int8_t type,
                          char* fieldName) {
  for (int32_t i = 0; i < pIdList->num; ++i) {
    tscColumnBaseInfoInsert(pCmd, &(pIdList->ids[i]));
  }

  tscFieldInfoSetValue(&pCmd->fieldsInfo, outputIndex, type, fieldName, bytes);
  return TSDB_CODE_SUCCESS;
}

SSqlExpr* doAddProjectCol(SSqlCmd* pCmd, int32_t outputIndex, int32_t colIdx, int32_t tableIndex) {
  SMeterMeta* pMeterMeta = tscGetMeterMetaInfo(pCmd, tableIndex)->pMeterMeta;

  SSchema* pSchema = tsGetColumnSchema(pMeterMeta, colIdx);
  int32_t  numOfCols = pMeterMeta->numOfColumns;

  int16_t functionId = (int16_t)((colIdx >= numOfCols) ? TSDB_FUNC_TAGPRJ : TSDB_FUNC_PRJ);

  if (functionId == TSDB_FUNC_TAGPRJ) {
    addRequiredTagColumn(pCmd, colIdx - numOfCols, tableIndex);
    pCmd->type = TSDB_QUERY_TYPE_STABLE_QUERY;
  } else {
    pCmd->type = TSDB_QUERY_TYPE_PROJECTION_QUERY;
  }

  SColumnIndex index = {tableIndex, colIdx};
  SSqlExpr*    pExpr =
      tscSqlExprInsert(pCmd, outputIndex, functionId, &index, pSchema->type, pSchema->bytes, pSchema->bytes);

  return pExpr;
}

void addRequiredTagColumn(SSqlCmd* pCmd, int32_t tagColIndex, int32_t tableIndex) {
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, tableIndex);

  if (pMeterMetaInfo->numOfTags == 0 || pMeterMetaInfo->tagColumnIndex[pMeterMetaInfo->numOfTags - 1] < tagColIndex) {
    pMeterMetaInfo->tagColumnIndex[pMeterMetaInfo->numOfTags++] = tagColIndex;
  } else {  // find the appropriate position
    for (int32_t i = 0; i < pMeterMetaInfo->numOfTags; ++i) {
      if (tagColIndex > pMeterMetaInfo->tagColumnIndex[i]) {
        continue;
      } else if (tagColIndex == pMeterMetaInfo->tagColumnIndex[i]) {
        break;
      } else {
        memmove(&pMeterMetaInfo->tagColumnIndex[i + 1], &pMeterMetaInfo->tagColumnIndex[i],
                sizeof(pMeterMetaInfo->tagColumnIndex[0]) * (pMeterMetaInfo->numOfTags - i));

        pMeterMetaInfo->tagColumnIndex[i] = tagColIndex;

        pMeterMetaInfo->numOfTags++;
        break;
      }
    }
  }

  // plus one means tbname
  assert(tagColIndex >= -1 && tagColIndex < TSDB_MAX_TAGS && pMeterMetaInfo->numOfTags <= TSDB_MAX_TAGS + 1);
}

static void addProjectQueryCol(SSqlCmd* pCmd, int32_t startPos, SColumnIndex* pIndex, tSQLExprItem* pItem) {
  SSqlExpr* pExpr = doAddProjectCol(pCmd, startPos, pIndex->columnIndex, pIndex->tableIndex);

  SMeterMeta* pMeterMeta = tscGetMeterMetaInfo(pCmd, pIndex->tableIndex)->pMeterMeta;

  SSchema* pSchema = tsGetColumnSchema(pMeterMeta, pIndex->columnIndex);

  char* colName = (pItem->aliasName == NULL) ? pSchema->name : pItem->aliasName;

  SColumnList ids = {0};
  ids.num = 1;
  ids.ids[0] = *pIndex;

  if (pIndex->columnIndex >= pMeterMeta->numOfColumns || pIndex->columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    ids.num = 0;
  }

  insertResultField(pCmd, startPos, &ids, pExpr->resBytes, pExpr->resType, colName);
}

void tscAddSpecialColumnForSelect(SSqlCmd* pCmd, int32_t outputColIndex, int16_t functionId, SColumnIndex* pIndex,
                                  SSchema* pColSchema, int16_t flag) {
  SSqlExpr* pExpr = tscSqlExprInsert(pCmd, outputColIndex, functionId, pIndex, pColSchema->type, pColSchema->bytes,
                                     pColSchema->bytes);

  SColumnList ids = getColumnList(1, pIndex->tableIndex, pIndex->columnIndex);
  if (TSDB_COL_IS_TAG(flag)) {
    ids.num = 0;
  }

  insertResultField(pCmd, outputColIndex, &ids, pColSchema->bytes, pColSchema->type, pColSchema->name);

  pExpr->colInfo.flag = flag;
  if (TSDB_COL_IS_TAG(flag)) {
    addRequiredTagColumn(pCmd, pIndex->columnIndex, pIndex->tableIndex);
  }
}

static int32_t doAddProjectionExprAndResultFields(SSqlCmd* pCmd, SColumnIndex* pIndex, int32_t startPos) {
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pIndex->tableIndex);

  int32_t     numOfTotalColumns = 0;
  SMeterMeta* pMeterMeta = pMeterMetaInfo->pMeterMeta;
  SSchema*    pSchema = tsGetSchema(pMeterMeta);

  if (UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
    numOfTotalColumns = pMeterMeta->numOfColumns + pMeterMeta->numOfTags;
  } else {
    numOfTotalColumns = pMeterMeta->numOfColumns;
  }

  for (int32_t j = 0; j < numOfTotalColumns; ++j) {
    doAddProjectCol(pCmd, startPos + j, j, pIndex->tableIndex);

    pIndex->columnIndex = j;
    SColumnList ids = {0};
    ids.ids[0] = *pIndex;

    // tag columns do not add to source list
    ids.num = (j >= pMeterMeta->numOfColumns) ? 0 : 1;

    insertResultField(pCmd, startPos + j, &ids, pSchema[j].bytes, pSchema[j].type, pSchema[j].name);
  }

  return numOfTotalColumns;
}

int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, tSQLExprItem* pItem) {
  const char* msg0 = "invalid column name";
  const char* msg1 = "tag for table query is not allowed";

  int32_t startPos = pCmd->fieldsInfo.numOfOutputCols;

  if (pItem->pNode->nSQLOptr == TK_ALL) {  // project on all fields
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getTableIndexByName(&pItem->pNode->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    // all meters columns are required
    if (index.tableIndex == COLUMN_INDEX_INITIAL_VAL) {  // all table columns are required.
      for (int32_t i = 0; i < pCmd->numOfTables; ++i) {
        index.tableIndex = i;
        int32_t inc = doAddProjectionExprAndResultFields(pCmd, &index, startPos);
        startPos += inc;
      }
    } else {
      doAddProjectionExprAndResultFields(pCmd, &index, startPos);
    }
  } else if (pItem->pNode->nSQLOptr == TK_ID) {  // simple column projection query
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;

    if (getColumnIndexByNameEx(&pItem->pNode->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(pCmd, msg0);
    }

    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      SColumnIndex index1 = {0, TSDB_TBNAME_COLUMN_INDEX};
      SSchema      colSchema = {.type = TSDB_DATA_TYPE_BINARY, .bytes = TSDB_METER_NAME_LEN};
      strcpy(colSchema.name, TSQL_TBNAME_L);

      pCmd->type = TSDB_QUERY_TYPE_STABLE_QUERY;
      tscAddSpecialColumnForSelect(pCmd, startPos, TSDB_FUNC_TAGPRJ, &index1, &colSchema, true);
    } else {
      SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
      SMeterMeta*     pMeterMeta = pMeterMetaInfo->pMeterMeta;

      if (index.columnIndex >= pMeterMeta->numOfColumns && UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      addProjectQueryCol(pCmd, startPos, &index, pItem);
    }
  } else {
    return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setExprInfoForFunctions(SSqlCmd* pCmd, SSchema* pSchema, int32_t functionID, char* aliasName,
                                       int32_t resColIdx, SColumnIndex* pColIndex) {
  int16_t type = 0;
  int16_t bytes = 0;

  char        columnName[TSDB_COL_NAME_LEN] = {0};
  const char* msg1 = "not support column types";

  if (functionID == TSDB_FUNC_SPREAD) {
    if (pSchema[pColIndex->columnIndex].type == TSDB_DATA_TYPE_BINARY ||
        pSchema[pColIndex->columnIndex].type == TSDB_DATA_TYPE_NCHAR ||
        pSchema[pColIndex->columnIndex].type == TSDB_DATA_TYPE_BOOL) {
      invalidSqlErrMsg(pCmd, msg1);
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
    strcpy(columnName, aliasName);
  } else {
    getRevisedName(columnName, functionID, TSDB_COL_NAME_LEN, pSchema[pColIndex->columnIndex].name);
  }

  tscSqlExprInsert(pCmd, resColIdx, functionID, pColIndex, type, bytes, bytes);

  // for point interpolation/last_row query, we need the timestamp column to be loaded
  SColumnIndex index = {.tableIndex = pColIndex->tableIndex, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  if (functionID == TSDB_FUNC_INTERP || functionID == TSDB_FUNC_LAST_ROW) {
    tscColumnBaseInfoInsert(pCmd, &index);
  }

  SColumnList ids = getColumnList(1, pColIndex->tableIndex, pColIndex->columnIndex);
  insertResultField(pCmd, resColIdx, &ids, bytes, type, columnName);

  return TSDB_CODE_SUCCESS;
}

int32_t addExprAndResultField(SSqlCmd* pCmd, int32_t colIdx, tSQLExprItem* pItem) {
  SMeterMetaInfo* pMeterMetaInfo = NULL;
  int32_t         optr = pItem->pNode->nSQLOptr;

  int32_t numOfAddedColumn = 1;

  const char* msg1 = "not support column types";
  const char* msg2 = "invalid parameters";
  const char* msg3 = "illegal column name";
  const char* msg4 = "invalid table name";
  const char* msg5 = "parameter is out of range [0, 100]";
  const char* msg6 = "function applied to tags not allowed";

  switch (optr) {
    case TK_COUNT: {
      if (pItem->pNode->pParam != NULL && pItem->pNode->pParam->nExpr != 1) {
        /* more than one parameter for count() function */
        return invalidSqlErrMsg(pCmd, msg2);
      }

      int16_t functionID = 0;
      if (changeFunctionID(optr, &functionID) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      SColumnIndex index = COLUMN_INDEX_INITIALIZER;

      if (pItem->pNode->pParam != NULL) {
        SSQLToken* pToken = &pItem->pNode->pParam->a[0].pNode->colInfo;
        if (pToken->z == NULL || pToken->n == 0) {
          return invalidSqlErrMsg(pCmd, msg3);
        }

        tSQLExprItem* pParamElem = &pItem->pNode->pParam->a[0];
        if (pParamElem->pNode->nSQLOptr == TK_ALL) {
          // select table.*
          // check if the table name is valid or not
          SSQLToken tmpToken = pParamElem->pNode->colInfo;

          if (getTableIndexByName(&tmpToken, pCmd, &index) != TSDB_CODE_SUCCESS) {
            return invalidSqlErrMsg(pCmd, msg4);
          }

          index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
          int32_t size = tDataTypeDesc[TSDB_DATA_TYPE_BIGINT].nSize;
          tscSqlExprInsert(pCmd, colIdx, functionID, &index, TSDB_DATA_TYPE_BIGINT, size, size);
        } else {
          // count the number of meters created according to the metric
          if (getColumnIndexByNameEx(pToken, pCmd, &index) != TSDB_CODE_SUCCESS) {
            return invalidSqlErrMsg(pCmd, msg3);
          }

          pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);

          // count tag is equalled to count(tbname)
          if (index.columnIndex >= pMeterMetaInfo->pMeterMeta->numOfColumns) {
            index.columnIndex = TSDB_TBNAME_COLUMN_INDEX;
          }

          int32_t size = tDataTypeDesc[TSDB_DATA_TYPE_BIGINT].nSize;
          tscSqlExprInsert(pCmd, colIdx, functionID, &index, TSDB_DATA_TYPE_BIGINT, size, size);
        }
      } else {  // count(*) is equalled to count(primary_timestamp_key)
        index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX};

        int32_t size = tDataTypeDesc[TSDB_DATA_TYPE_BIGINT].nSize;
        tscSqlExprInsert(pCmd, colIdx, functionID, &index, TSDB_DATA_TYPE_BIGINT, size, size);
      }

      char columnName[TSDB_COL_NAME_LEN] = {0};
      getColumnName(pItem, columnName, TSDB_COL_NAME_LEN);

      // count always use the primary timestamp key column, which is 0.
      SColumnList ids = getColumnList(1, index.tableIndex, index.columnIndex);

      insertResultField(pCmd, colIdx, &ids, sizeof(int64_t), TSDB_DATA_TYPE_BIGINT, columnName);
      return TSDB_CODE_SUCCESS;
    }
    case TK_SUM:
    case TK_AVG:
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
        return invalidSqlErrMsg(pCmd, msg2);
      }

      tSQLExprItem* pParamElem = &(pItem->pNode->pParam->a[0]);
      if (pParamElem->pNode->nSQLOptr != TK_ALL && pParamElem->pNode->nSQLOptr != TK_ID) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByNameEx(&pParamElem->pNode->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg3);
      }

      // 2. check if sql function can be applied on this column data type
      pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
      SSchema* pSchema = tsGetColumnSchema(pMeterMetaInfo->pMeterMeta, index.columnIndex);
      int16_t  colType = pSchema->type;

      if (colType == TSDB_DATA_TYPE_BOOL || colType >= TSDB_DATA_TYPE_BINARY) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      char columnName[TSDB_COL_NAME_LEN] = {0};
      getColumnName(pItem, columnName, TSDB_COL_NAME_LEN);

      int16_t resultType = 0;
      int16_t resultSize = 0;
      int16_t intermediateResSize = 0;

      int16_t functionID = 0;
      if (changeFunctionID(optr, &functionID) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      if (getResultDataInfo(pSchema->type, pSchema->bytes, functionID, 0, &resultType, &resultSize,
                            &intermediateResSize, 0, false) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      // set the first column ts for diff query
      if (optr == TK_DIFF) {
        colIdx += 1;
        SColumnIndex indexTS = {.tableIndex = index.tableIndex, .columnIndex = 0};
        tscSqlExprInsert(pCmd, 0, TSDB_FUNC_TS_DUMMY, &indexTS, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE, TSDB_KEYSIZE);

        SColumnList ids = getColumnList(1, 0, 0);
        insertResultField(pCmd, 0, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, aAggs[TSDB_FUNC_TS_DUMMY].aName);
      }

      // functions can not be applied to tags
      if (index.columnIndex >= pMeterMetaInfo->pMeterMeta->numOfColumns) {
        return invalidSqlErrMsg(pCmd, msg6);
      }

      SSqlExpr* pExpr = tscSqlExprInsert(pCmd, colIdx, functionID, &index, resultType, resultSize, resultSize);

      if (optr == TK_LEASTSQUARES) {
        /* set the leastsquares parameters */
        char val[8] = {0};
        if (tVariantDump(&pParamElem[1].pNode->val, val, TSDB_DATA_TYPE_DOUBLE) < 0) {
          return TSDB_CODE_INVALID_SQL;
        }

        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, DOUBLE_BYTES, 0);

        memset(val, 0, tListLen(val));
        if (tVariantDump(&pParamElem[2].pNode->val, val, TSDB_DATA_TYPE_DOUBLE) < 0) {
          return TSDB_CODE_INVALID_SQL;
        }

        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double), 0);
      }

      SColumnList ids = {0};
      ids.num = 1;
      ids.ids[0] = index;

      insertResultField(pCmd, colIdx, &ids, pExpr->resBytes, pExpr->resType, columnName);

      return TSDB_CODE_SUCCESS;
    }
    case TK_FIRST:
    case TK_LAST:
    case TK_SPREAD:
    case TK_LAST_ROW:
    case TK_INTERP: {
      bool requireAllFields = (pItem->pNode->pParam == NULL);

      int16_t functionID = 0;
      changeFunctionID(optr, &functionID);

      if (!requireAllFields) {
        if (pItem->pNode->pParam->nExpr < 1) {
          return invalidSqlErrMsg(pCmd, msg3);
        }

        /* in first/last function, multiple columns can be add to resultset */

        for (int32_t i = 0; i < pItem->pNode->pParam->nExpr; ++i) {
          tSQLExprItem* pParamElem = &(pItem->pNode->pParam->a[i]);
          if (pParamElem->pNode->nSQLOptr != TK_ALL && pParamElem->pNode->nSQLOptr != TK_ID) {
            return invalidSqlErrMsg(pCmd, msg3);
          }

          SColumnIndex index = COLUMN_INDEX_INITIALIZER;

          if (pParamElem->pNode->nSQLOptr == TK_ALL) {
            // select table.*
            SSQLToken tmpToken = pParamElem->pNode->colInfo;

            if (getTableIndexByName(&tmpToken, pCmd, &index) != TSDB_CODE_SUCCESS) {
              return invalidSqlErrMsg(pCmd, msg4);
            }

            pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
            SSchema* pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

            for (int32_t j = 0; j < pMeterMetaInfo->pMeterMeta->numOfColumns; ++j) {
              index.columnIndex = j;
              if (setExprInfoForFunctions(pCmd, pSchema, functionID, pItem->aliasName, colIdx++, &index) != 0) {
                return TSDB_CODE_INVALID_SQL;
              }
            }

          } else {
            if (getColumnIndexByNameEx(&pParamElem->pNode->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
              return invalidSqlErrMsg(pCmd, msg3);
            }

            pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
            SSchema* pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

            // functions can not be applied to tags
            if (index.columnIndex >= pMeterMetaInfo->pMeterMeta->numOfColumns) {
              return invalidSqlErrMsg(pCmd, msg6);
            }

            if (setExprInfoForFunctions(pCmd, pSchema, functionID, pItem->aliasName, colIdx + i, &index) != 0) {
              return TSDB_CODE_INVALID_SQL;
            }
          }
        }

        return TSDB_CODE_SUCCESS;
      } else {  // select * from xxx
        int32_t numOfFields = 0;

        for (int32_t j = 0; j < pCmd->numOfTables; ++j) {
          pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, j);
          SSchema* pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

          for (int32_t i = 0; i < pMeterMetaInfo->pMeterMeta->numOfColumns; ++i) {
            SColumnIndex index = {.tableIndex = j, .columnIndex = i};
            if (setExprInfoForFunctions(pCmd, pSchema, functionID, pItem->aliasName, colIdx + i + j, &index) != 0) {
              return TSDB_CODE_INVALID_SQL;
            }
          }

          numOfFields += pMeterMetaInfo->pMeterMeta->numOfColumns;
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
        return invalidSqlErrMsg(pCmd, msg2);
      }

      tSQLExprItem* pParamElem = &(pItem->pNode->pParam->a[0]);
      if (pParamElem->pNode->nSQLOptr != TK_ID) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      char columnName[TSDB_COL_NAME_LEN] = {0};
      getColumnName(pItem, columnName, TSDB_COL_NAME_LEN);

      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByNameEx(&pParamElem->pNode->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg3);
      }

      pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
      SSchema* pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

      // functions can not be applied to tags
      if (index.columnIndex >= pMeterMetaInfo->pMeterMeta->numOfColumns) {
        return invalidSqlErrMsg(pCmd, msg6);
      }

      // 2. valid the column type
      int16_t colType = pSchema[index.columnIndex].type;
      if (colType == TSDB_DATA_TYPE_BOOL || colType >= TSDB_DATA_TYPE_BINARY) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      // 3. valid the parameters
      if (pParamElem[1].pNode->nSQLOptr == TK_ID) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      tVariant* pVariant = &pParamElem[1].pNode->val;

      int8_t  resultType = pSchema[index.columnIndex].type;
      int16_t resultSize = pSchema[index.columnIndex].bytes;

      char val[8] = {0};
      if (optr == TK_PERCENTILE || optr == TK_APERCENTILE) {
        tVariantDump(pVariant, val, TSDB_DATA_TYPE_DOUBLE);

        double dp = *((double*)val);
        if (dp < 0 || dp > TOP_BOTTOM_QUERY_LIMIT) {
          return invalidSqlErrMsg(pCmd, msg5);
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
          return TSDB_CODE_INVALID_SQL;
        }

        SSqlExpr* pExpr = tscSqlExprInsert(pCmd, colIdx, functionId, &index, resultType, resultSize, resultSize);
        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double), 0);
      } else {
        tVariantDump(pVariant, val, TSDB_DATA_TYPE_BIGINT);

        int64_t nTop = *((int32_t*)val);
        if (nTop <= 0 || nTop > 100) {  // todo use macro
          return invalidSqlErrMsg(pCmd, msg5);
        }

        int16_t functionId = 0;
        if (changeFunctionID(optr, &functionId) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_INVALID_SQL;
        }

        // set the first column ts for top/bottom query
        SColumnIndex index1 = {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
        tscSqlExprInsert(pCmd, 0, TSDB_FUNC_TS, &index1, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE, TSDB_KEYSIZE);

        const int32_t TS_COLUMN_INDEX = 0;
        SColumnList   ids = getColumnList(1, 0, TS_COLUMN_INDEX);
        insertResultField(pCmd, TS_COLUMN_INDEX, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP,
                          aAggs[TSDB_FUNC_TS].aName);

        colIdx += 1;  // the first column is ts
        numOfAddedColumn += 1;

        SSqlExpr* pExpr = tscSqlExprInsert(pCmd, colIdx, functionId, &index, resultType, resultSize, resultSize);
        addExprParams(pExpr, val, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 0);
      }

      SColumnList ids = getColumnList(1, 0, index.columnIndex);
      insertResultField(pCmd, colIdx, &ids, resultSize, resultType, columnName);

      return TSDB_CODE_SUCCESS;
    }
    default:
      return TSDB_CODE_INVALID_SQL;
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

static int16_t doGetColumnIndex(SSqlCmd* pCmd, int32_t index, SSQLToken* pToken) {
  SMeterMeta* pMeterMeta = tscGetMeterMetaInfo(pCmd, index)->pMeterMeta;

  int32_t  numOfCols = pMeterMeta->numOfColumns + pMeterMeta->numOfTags;
  SSchema* pSchema = tsGetSchema(pMeterMeta);

  int16_t columnIndex = COLUMN_INDEX_INITIAL_VAL;

  for (int16_t i = 0; i < numOfCols; ++i) {
    if (pToken->n != strlen(pSchema[i].name)) {
      continue;
    }

    if (strncasecmp(pSchema[i].name, pToken->z, pToken->n) == 0) {
      columnIndex = i;
    }
  }

  return columnIndex;
}

int32_t doGetColumnIndexByName(SSQLToken* pToken, SSqlCmd* pCmd, SColumnIndex* pIndex) {
  const char* msg0 = "ambiguous column name";
  const char* msg1 = "invalid column name";

  if (isTablenameToken(pToken)) {
    pIndex->columnIndex = TSDB_TBNAME_COLUMN_INDEX;
  } else if (strncasecmp(pToken->z, DEFAULT_PRIMARY_TIMESTAMP_COL_NAME, pToken->n) == 0) {
    pIndex->columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX;
  } else {
    // not specify the table name, try to locate the table index by column name
    if (pIndex->tableIndex == COLUMN_INDEX_INITIAL_VAL) {
      for (int16_t i = 0; i < pCmd->numOfTables; ++i) {
        int16_t colIndex = doGetColumnIndex(pCmd, i, pToken);

        if (colIndex != COLUMN_INDEX_INITIAL_VAL) {
          if (pIndex->columnIndex != COLUMN_INDEX_INITIAL_VAL) {
            return invalidSqlErrMsg(pCmd, msg0);
          } else {
            pIndex->tableIndex = i;
            pIndex->columnIndex = colIndex;
          }
        }
      }
    } else {  // table index is valid, get the column index
      int16_t colIndex = doGetColumnIndex(pCmd, pIndex->tableIndex, pToken);
      if (colIndex != COLUMN_INDEX_INITIAL_VAL) {
        pIndex->columnIndex = colIndex;
      }
    }

    if (pIndex->columnIndex == COLUMN_INDEX_INITIAL_VAL) {
      return invalidSqlErrMsg(pCmd, msg1);
    }
  }

  if (COLUMN_INDEX_VALIDE(*pIndex)) {
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_INVALID_SQL;
  }
}

static int32_t getMeterIndex(SSQLToken* pTableToken, SSqlCmd* pCmd, SColumnIndex* pIndex) {
  if (pTableToken->n == 0) {  // only one table and no table name prefix in column name
    if (pCmd->numOfTables == 1) {
      pIndex->tableIndex = 0;
    }

    return TSDB_CODE_SUCCESS;
  }

  pIndex->tableIndex = COLUMN_INDEX_INITIAL_VAL;
  char tableName[TSDB_METER_ID_LEN + 1] = {0};

  for (int32_t i = 0; i < pCmd->numOfTables; ++i) {
    SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, i);
    extractMeterName(pMeterMetaInfo->name, tableName);

    if (strncasecmp(tableName, pTableToken->z, pTableToken->n) == 0 && strlen(tableName) == pTableToken->n) {
      pIndex->tableIndex = i;
      break;
    }
  }

  if (pIndex->tableIndex < 0) {
    return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getTableIndexByName(SSQLToken* pToken, SSqlCmd* pCmd, SColumnIndex* pIndex) {
  SSQLToken tableToken = {0};
  extractTableNameFromToken(pToken, &tableToken);

  if (getMeterIndex(&tableToken, pCmd, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getColumnIndexByNameEx(SSQLToken* pToken, SSqlCmd* pCmd, SColumnIndex* pIndex) {
  if (pCmd->pMeterInfo == NULL || pCmd->numOfTables == 0) {
    return TSDB_CODE_INVALID_SQL;
  }

  SSQLToken tmpToken = *pToken;

  if (getTableIndexByName(&tmpToken, pCmd, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }

  return doGetColumnIndexByName(&tmpToken, pCmd, pIndex);
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

// TODO support like for showing metrics, there are show meters with like ops
int32_t setShowInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  SSqlCmd*        pCmd = &pSql->cmd;
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  pCmd->command = TSDB_SQL_SHOW;
  int8_t type = pInfo->sqlType;

  const char* msg = "database name too long";
  const char* msg1 = "invalid database name";
  const char* msg2 = "pattern filter string too long";

  switch (type) {
    case SHOW_VGROUPS:
      pCmd->showType = TSDB_MGMT_TABLE_VGROUP;
      break;
    case SHOW_TABLES:
      pCmd->showType = TSDB_MGMT_TABLE_TABLE;
      break;
    case SHOW_STABLES:
      pCmd->showType = TSDB_MGMT_TABLE_METRIC;
      break;

    case SHOW_DATABASES:
      pCmd->showType = TSDB_MGMT_TABLE_DB;
      break;
    case SHOW_MNODES:
      pCmd->showType = TSDB_MGMT_TABLE_MNODE;
      break;
    case SHOW_DNODES:
      pCmd->showType = TSDB_MGMT_TABLE_PNODE;
      break;
    case SHOW_ACCOUNTS:
      pCmd->showType = TSDB_MGMT_TABLE_ACCT;
      break;
    case SHOW_USERS:
      pCmd->showType = TSDB_MGMT_TABLE_USER;
      break;
    case SHOW_MODULES:
      pCmd->showType = TSDB_MGMT_TABLE_MODULE;
      break;
    case SHOW_CONNECTIONS:
      pCmd->showType = TSDB_MGMT_TABLE_CONNS;
      break;
    case SHOW_QUERIES:
      pCmd->showType = TSDB_MGMT_TABLE_QUERIES;
      break;
    case SHOW_SCORES:
      pCmd->showType = TSDB_MGMT_TABLE_SCORES;
      break;
    case SHOW_GRANTS:
      pCmd->showType = TSDB_MGMT_TABLE_GRANTS;
      break;
    case SHOW_STREAMS:
      pCmd->showType = TSDB_MGMT_TABLE_STREAMS;
      break;
    case SHOW_CONFIGS:
      pCmd->showType = TSDB_MGMT_TABLE_CONFIGS;
      break;
    case SHOW_VNODES:
      pCmd->showType = TSDB_MGMT_TABLE_VNODES;
      break;
    default:
      return TSDB_CODE_INVALID_SQL;
  }

  /*
   * database prefix in pInfo->pDCLInfo->a[0]
   * wildcard in like clause in pInfo->pDCLInfo->a[1]
   */
  if (type == SHOW_TABLES || type == SHOW_STABLES || type == SHOW_VGROUPS) {
    // db prefix in tagCond, show table conds in payload
    if (pInfo->pDCLInfo->nTokens > 0) {
      SSQLToken* pDbPrefixToken = &pInfo->pDCLInfo->a[0];

      if (pDbPrefixToken->n > TSDB_DB_NAME_LEN) {  // db name is too long
        return invalidSqlErrMsg(pCmd, msg);
      }

      if (pDbPrefixToken->n > 0 && tscValidateName(pDbPrefixToken) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      int32_t ret = 0;
      if (pDbPrefixToken->n > 0) {  // has db prefix
        ret = setObjFullName(pMeterMetaInfo->name, getAccountId(pSql), pDbPrefixToken, NULL, NULL);
      }

      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      if (type != SHOW_VGROUPS && pInfo->pDCLInfo->nTokens == 2) {
        // set the like conds for show tables
        SSQLToken* likeToken = &pInfo->pDCLInfo->a[1];

        strncpy(pCmd->payload, likeToken->z, likeToken->n);
        pCmd->payloadLen = strdequote(pCmd->payload);

        if (pCmd->payloadLen > TSDB_METER_NAME_LEN) {
          return invalidSqlErrMsg(pCmd, msg2);
        }
      }
    }
  }else if (type == SHOW_VNODES) {
    if (NULL == pInfo->pDCLInfo) {
      return invalidSqlErrMsg(pCmd, "No specified ip of dnode");
    }

    // show vnodes may be ip addr of dnode in payload
    if (pInfo->pDCLInfo->nTokens > 0) {
      SSQLToken* pDnodeIp = &pInfo->pDCLInfo->a[0];

      if (pDnodeIp->n > TSDB_IPv4ADDR_LEN) {  // ip addr is too long
        return invalidSqlErrMsg(pCmd, msg);
      }

      strncpy(pCmd->payload, pDnodeIp->z, pDnodeIp->n);
      pCmd->payloadLen = strdequote(pCmd->payload);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setKillInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  SSqlCmd* pCmd = &pSql->cmd;

  switch (pInfo->sqlType) {
    case KILL_QUERY:
      pCmd->command = TSDB_SQL_KILL_QUERY;
      break;
    case KILL_STREAM:
      pCmd->command = TSDB_SQL_KILL_STREAM;
      break;
    case KILL_CONNECTION:
      pCmd->command = TSDB_SQL_KILL_CONNECTION;
      break;
    default:
      return TSDB_CODE_INVALID_SQL;
  }

  SSQLToken* pToken = &(pInfo->pDCLInfo->a[0]);
  if (pToken->n > TSDB_KILL_MSG_LEN) {
    return TSDB_CODE_INVALID_SQL;
  }

  strncpy(pCmd->payload, pToken->z, pToken->n);

  const char delim = ':';
  char*      ipStr = strtok(pToken->z, &delim);
  char*      portStr = strtok(NULL, &delim);

  if (!validateIpAddress(ipStr)) {
    memset(pCmd->payload, 0, tListLen(pCmd->payload));

    const char* msg = "invalid ip address";
    return invalidSqlErrMsg(pCmd, msg);
  }

  uint16_t port = (uint16_t)strtol(portStr, NULL, 10);
  if (port <= 0 || port > 65535) {
    memset(pCmd->payload, 0, tListLen(pCmd->payload));

    const char* msg = "invalid port";
    return invalidSqlErrMsg(pCmd, msg);
  }

  return TSDB_CODE_SUCCESS;
}

bool validateIpAddress(char* ip) {
  in_addr_t ipAddr = inet_addr(ip);
  return (ipAddr != 0) && (ipAddr != 0xffffffff);
}

int32_t tscTansformSQLFunctionForMetricQuery(SSqlCmd* pCmd) {
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  if (pMeterMetaInfo->pMeterMeta == NULL || !UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
    return TSDB_CODE_INVALID_SQL;
  }

  assert(pMeterMetaInfo->pMeterMeta->numOfTags >= 0);

  int16_t bytes = 0;
  int16_t type = 0;
  int16_t intermediateBytes = 0;

  for (int32_t k = 0; k < pCmd->fieldsInfo.numOfOutputCols; ++k) {
    SSqlExpr*   pExpr = tscSqlExprGet(pCmd, k);
    TAOS_FIELD* pField = tscFieldInfoGetField(pCmd, k);

    int16_t functionId = aAggs[pExpr->functionId].stableFuncId;

    if ((functionId >= TSDB_FUNC_SUM && functionId <= TSDB_FUNC_TWA) ||
        (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_LAST_DST)) {
      if (getResultDataInfo(pField->type, pField->bytes, functionId, pExpr->param[0].i64Key, &type, &bytes,
                            &intermediateBytes, 0, true) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      tscSqlExprUpdate(pCmd, k, functionId, pExpr->colInfo.colIdx, TSDB_DATA_TYPE_BINARY, bytes);
      // todo refactor
      pExpr->interResBytes = intermediateBytes;
    }
  }

  tscFieldInfoUpdateOffset(pCmd);
  return TSDB_CODE_SUCCESS;
}

/* transfer the field-info back to original input format */
void tscRestoreSQLFunctionForMetricQuery(SSqlCmd* pCmd) {
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  if (!UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
    return;
  }

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr*   pExpr = tscSqlExprGet(pCmd, i);
    TAOS_FIELD* pField = tscFieldInfoGetField(pCmd, i);

    if ((pExpr->functionId >= TSDB_FUNC_FIRST_DST && pExpr->functionId <= TSDB_FUNC_LAST_DST) ||
        (pExpr->functionId >= TSDB_FUNC_SUM && pExpr->functionId <= TSDB_FUNC_MAX)) {
      pExpr->resBytes = pField->bytes;
      pExpr->resType = pField->type;
    }
  }
}

bool hasUnsupportFunctionsForMetricQuery(SSqlCmd* pCmd) {
  const char* msg1 = "TWA not allowed to apply to super table directly";
  const char* msg2 = "functions not supported for super table";
  const char* msg3 = "TWA only support group by tbname for super table query";

  // filter sql function not supported by metric query yet.
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    int32_t functionId = tscSqlExprGet(pCmd, i)->functionId;
    if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_METRIC) == 0) {
      return true;
    }
  }

  if (tscIsTWAQuery(pCmd)) {
    if (pCmd->groupbyExpr.numOfGroupCols == 0) {
      invalidSqlErrMsg(pCmd, msg1);
      return true;
    }

    if (pCmd->groupbyExpr.numOfGroupCols != 1 || pCmd->groupbyExpr.columnInfo[0].colIdx != TSDB_TBNAME_COLUMN_INDEX) {
      invalidSqlErrMsg(pCmd, msg3);
      return true;
    }
  }

  return false;
}

static bool functionCompatibleCheck(SSqlCmd* pCmd) {
  const char* msg1 = "column on select clause not allowed";

  int32_t startIdx = 0;
  int32_t functionID = tscSqlExprGet(pCmd, startIdx)->functionId;

  // ts function can be simultaneously used with any other functions.
  if (functionID == TSDB_FUNC_TS || functionID == TSDB_FUNC_TS_DUMMY) {
    startIdx++;
  }

  int32_t factor = funcCompatDefList[tscSqlExprGet(pCmd, startIdx)->functionId];

  // diff function cannot be executed with other function
  // arithmetic function can be executed with other arithmetic functions
  for (int32_t i = startIdx + 1; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);

    int16_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TAGPRJ || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TS) {
      continue;
    }

    if (functionId == TSDB_FUNC_PRJ && pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      continue;
    }

    if (funcCompatDefList[functionId] != factor) {
      return false;
    }
  }

  return true;
}

void updateTagColumnIndex(SSqlCmd* pCmd, int32_t tableIndex) {
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, tableIndex);

  // update tags column index for group by tags
  for (int32_t i = 0; i < pCmd->groupbyExpr.numOfGroupCols; ++i) {
    int32_t index = pCmd->groupbyExpr.columnInfo[i].colIdx;

    for (int32_t j = 0; j < pMeterMetaInfo->numOfTags; ++j) {
      int32_t tagColIndex = pMeterMetaInfo->tagColumnIndex[j];
      if (tagColIndex == index) {
        pCmd->groupbyExpr.columnInfo[i].colIdx = j;
        break;
      }
    }
  }

  // update tags column index for expression
  for (int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (!TSDB_COL_IS_TAG(pExpr->colInfo.flag)) {  // not tags, continue
      continue;
    }

    for (int32_t j = 0; j < pMeterMetaInfo->numOfTags; ++j) {
      if (pExpr->colInfo.colIdx == pMeterMetaInfo->tagColumnIndex[j]) {
        pExpr->colInfo.colIdx = j;
        break;
      }
    }
  }
}

int32_t parseGroupbyClause(SSqlCmd* pCmd, tVariantList* pList) {
  const char* msg1 = "too many columns in group by clause";
  const char* msg2 = "invalid column name in group by clause";
  const char* msg4 = "group by only available for STable query";
  const char* msg5 = "group by columns must belong to one table";
  const char* msg6 = "only support group by one ordinary column";
  const char* msg7 = "not support group by expression";
  const char* msg8 = "not allowed column type for group by";
  const char* msg9 = "tags not allowed for table query";

  // todo : handle two meter situation
  SMeterMetaInfo* pMeterMetaInfo = NULL;

  if (pList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pCmd->groupbyExpr.numOfGroupCols = pList->nExpr;
  if (pList->nExpr > TSDB_MAX_TAGS) {
    return invalidSqlErrMsg(pCmd, msg1);
  }

  SMeterMeta* pMeterMeta = NULL;
  SSchema*    pSchema = NULL;

  SSchema s = {0};
  int32_t numOfReqTags = 0;
  int32_t tableIndex = COLUMN_INDEX_INITIAL_VAL;

  for (int32_t i = 0; i < pList->nExpr; ++i) {
    tVariant* pVar = &pList->a[i].pVar;
    SSQLToken token = {pVar->nLen, pVar->nType, pVar->pz};

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;

    if (getColumnIndexByNameEx(&token, pCmd, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(pCmd, msg2);
    }

    if (tableIndex != index.tableIndex && tableIndex >= 0) {
      return invalidSqlErrMsg(pCmd, msg5);
    }

    tableIndex = index.tableIndex;

    pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
    pMeterMeta = pMeterMetaInfo->pMeterMeta;

    // TODO refactor!!!!!!!!!!!!!!1
    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      s.colId = TSDB_TBNAME_COLUMN_INDEX;
      s.type = TSDB_DATA_TYPE_BINARY;
      s.bytes = TSDB_METER_NAME_LEN;
      strcpy(s.name, TSQL_TBNAME_L);

      pSchema = &s;
    } else {
      pSchema = tsGetColumnSchema(pMeterMeta, index.columnIndex);
    }

    int16_t type = 0;
    int16_t bytes = 0;
    char*   name = NULL;

    bool groupTag = false;
    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX || index.columnIndex >= pMeterMeta->numOfColumns) {
      groupTag = true;
    }

    if (groupTag) {
      if (!UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
        return invalidSqlErrMsg(pCmd, msg9);
      }

      int32_t relIndex = index.columnIndex;
      if (index.columnIndex != TSDB_TBNAME_COLUMN_INDEX) {
        relIndex -= pMeterMeta->numOfColumns;
      }

      pCmd->groupbyExpr.columnInfo[i] =
          (SColIndexEx){.colIdx = relIndex, .flag = TSDB_COL_TAG, .colId = pSchema->colId};  // relIndex;
      addRequiredTagColumn(pCmd, pCmd->groupbyExpr.columnInfo[i].colIdx, index.tableIndex);
    } else {
      // check if the column type is valid, here only support the bool/tinyint/smallint/bigint group by
      if (pSchema->type > TSDB_DATA_TYPE_BIGINT) {
        return invalidSqlErrMsg(pCmd, msg8);
      }

      tscColumnBaseInfoInsert(pCmd, &index);
      pCmd->groupbyExpr.columnInfo[i] =
          (SColIndexEx){.colIdx = index.columnIndex, .flag = TSDB_COL_NORMAL, .colId = pSchema->colId};  // relIndex;
      pCmd->groupbyExpr.orderType = TSQL_SO_ASC;

      if (i == 0 && pList->nExpr > 1) {
        return invalidSqlErrMsg(pCmd, msg7);
      }
    }
  }

  pCmd->groupbyExpr.tableIndex = tableIndex;

  return TSDB_CODE_SUCCESS;
}

void setColumnOffsetValueInResultset(SSqlCmd* pCmd) {
  if (QUERY_IS_STABLE_QUERY(pCmd->type)) {
    tscFieldInfoUpdateOffset(pCmd);
  } else {
    tscFieldInfoCalOffset(pCmd);
  }
}

static SColumnFilterInfo* addColumnFilterInfo(SColumnBase* pColumn) {
  if (pColumn == NULL) {
    return NULL;
  }

  int32_t size = pColumn->numOfFilters + 1;
  char*   tmp = realloc(pColumn->filterInfo, sizeof(SColumnFilterInfo) * (size));
  if (tmp != NULL) {
    pColumn->filterInfo = (SColumnFilterInfo*)tmp;
  }

  pColumn->numOfFilters++;

  SColumnFilterInfo* pColFilterInfo = &pColumn->filterInfo[pColumn->numOfFilters - 1];
  memset(pColFilterInfo, 0, sizeof(SColumnFilterInfo));

  return pColFilterInfo;
}

static int32_t doExtractColumnFilterInfo(SSqlCmd* pCmd, SColumnFilterInfo* pColumnFilter, SColumnIndex* columnIndex,
                                         tSQLExpr* pExpr) {
  const char* msg = "not supported filter condition";

  tSQLExpr*       pRight = pExpr->pRight;
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, columnIndex->tableIndex);

  SSchema* pSchema = tsGetColumnSchema(pMeterMetaInfo->pMeterMeta, columnIndex->columnIndex);

  int16_t colType = pSchema->type;
  if (colType >= TSDB_DATA_TYPE_TINYINT && colType <= TSDB_DATA_TYPE_BIGINT) {
    colType = TSDB_DATA_TYPE_BIGINT;
  } else if (colType == TSDB_DATA_TYPE_FLOAT || colType == TSDB_DATA_TYPE_DOUBLE) {
    colType = TSDB_DATA_TYPE_DOUBLE;
  } else if ((colType == TSDB_DATA_TYPE_TIMESTAMP) && (TSDB_DATA_TYPE_BINARY == pRight->val.nType)) {
    int retVal = setColumnFilterInfoForTimestamp(pCmd, &pRight->val);
    if (TSDB_CODE_SUCCESS != retVal) {
      return retVal;
    }
  }

  if (pExpr->nSQLOptr == TK_LE || pExpr->nSQLOptr == TK_LT) {
    tVariantDump(&pRight->val, (char*)&pColumnFilter->upperBndd, colType);
  } else {  // TK_GT,TK_GE,TK_EQ,TK_NE are based on the pColumn->lowerBndd
    if (colType == TSDB_DATA_TYPE_BINARY) {
      pColumnFilter->pz = (int64_t)calloc(1, pRight->val.nLen + 1);
      pColumnFilter->len = pRight->val.nLen;

      tVariantDump(&pRight->val, (char*)pColumnFilter->pz, colType);
    } else if (colType == TSDB_DATA_TYPE_NCHAR) {
      // pRight->val.nLen + 1 is larger than the actual nchar string length
      pColumnFilter->pz = (int64_t)calloc(1, (pRight->val.nLen + 1) * TSDB_NCHAR_SIZE);

      tVariantDump(&pRight->val, (char*)pColumnFilter->pz, colType);

      size_t len = wcslen((wchar_t*)pColumnFilter->pz);
      pColumnFilter->len = len * TSDB_NCHAR_SIZE;
    } else {
      tVariantDump(&pRight->val, (char*)&pColumnFilter->lowerBndd, colType);
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
      pColumnFilter->lowerRelOptr = TSDB_RELATION_LARGE;
      break;
    case TK_GE:
      pColumnFilter->lowerRelOptr = TSDB_RELATION_LARGE_EQUAL;
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
      return invalidSqlErrMsg(pCmd, msg);
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

static int32_t getTimeRange(int64_t* stime, int64_t* etime, tSQLExpr* pRight, int32_t optr, int16_t timePrecision);

static int32_t doParseWhereClause(SSqlObj* pSql, tSQLExpr** pExpr, SCondExpr* condExpr);

static int32_t tSQLExprNodeToString(tSQLExpr* pExpr, char** str) {
  if (pExpr->nSQLOptr == TK_ID) {  // column name
    strncpy(*str, pExpr->colInfo.z, pExpr->colInfo.n);
    *str += pExpr->colInfo.n;

  } else if (pExpr->nSQLOptr >= TK_BOOL && pExpr->nSQLOptr <= TK_STRING) {  // value
    *str += tVariantToString(&pExpr->val, *str);

  } else {
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
    return TSDB_CODE_INVALID_SQL;
  }

  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;

  if (addParentheses) {
    *(*output) = '(';
    *output += 1;
  }

  tSQLExprNodeToString(pLeft, output);
  if (optrToString(pExpr, output) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
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
      return TSDB_CODE_INVALID_SQL;
  }

  *exprString += 1;

  return TSDB_CODE_SUCCESS;
}

static int32_t tablenameListToString(tSQLExpr* pExpr, char* str) {
  tSQLExprList* pList = pExpr->pParam;
  if (pList->nExpr <= 0) {
    return TSDB_CODE_INVALID_SQL;
  }

  if (pList->nExpr > 0) {
    strcpy(str, QUERY_COND_REL_PREFIX_IN);
    str += QUERY_COND_REL_PREFIX_IN_LEN;
  }

  int32_t len = 0;
  for (int32_t i = 0; i < pList->nExpr; ++i) {
    tSQLExpr* pSub = pList->a[i].pNode;
    strncpy(str + len, pSub->val.pz, pSub->val.nLen);

    len += pSub->val.nLen;

    if (i < pList->nExpr - 1) {
      str[len++] = TBNAME_LIST_SEP[0];
    }

    if (pSub->val.nLen <= 0 || pSub->val.nLen > TSDB_METER_NAME_LEN) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tablenameCondToString(tSQLExpr* pExpr, char* str) {
  strcpy(str, QUERY_COND_REL_PREFIX_LIKE);
  str += strlen(QUERY_COND_REL_PREFIX_LIKE);

  strcpy(str, pExpr->val.pz);

  return TSDB_CODE_SUCCESS;
}

enum {
  TSQL_EXPR_TS = 0,
  TSQL_EXPR_TAG = 1,
  TSQL_EXPR_COLUMN = 2,
  TSQL_EXPR_TBNAME = 3,
};

static int32_t extractColumnFilterInfo(SSqlCmd* pCmd, SColumnIndex* pIndex, tSQLExpr* pExpr, int32_t sqlOptr) {
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, pIndex->tableIndex);

  SMeterMeta* pMeterMeta = pMeterMetaInfo->pMeterMeta;
  SSchema*    pSchema = tsGetColumnSchema(pMeterMeta, pIndex->columnIndex);

  const char* msg1 = "non binary column not support like operator";
  const char* msg2 = "binary column not support this operator";
  const char* msg3 = "OR is not supported on different column filter";

  SColumnBase*       pColumn = tscColumnBaseInfoInsert(pCmd, pIndex);
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
    return TSDB_CODE_INVALID_SQL;
  }

  pColFilter->filterOnBinary =
      ((pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) ? 1 : 0);

  if (pColFilter->filterOnBinary) {
    if (pExpr->nSQLOptr != TK_EQ && pExpr->nSQLOptr != TK_NE && pExpr->nSQLOptr != TK_LIKE) {
      return invalidSqlErrMsg(pCmd, msg2);
    }
  } else {
    if (pExpr->nSQLOptr == TK_LIKE) {
      return invalidSqlErrMsg(pCmd, msg1);
    }
  }

  pColumn->colIndex = *pIndex;
  return doExtractColumnFilterInfo(pCmd, pColFilter, pIndex, pExpr);
}

static void relToString(SSqlCmd* pCmd, tSQLExpr* pExpr, char** str) {
  assert(pExpr->nSQLOptr == TK_AND || pExpr->nSQLOptr == TK_OR);

  const char* or = "OR";
  const char*and = "AND";

  //    if (pCmd->tagCond.relType == TSQL_STABLE_QTYPE_COND) {
  if (pExpr->nSQLOptr == TK_AND) {
    strcpy(*str, and);
    *str += strlen(and);
  } else {
    strcpy(*str, or);
    *str += strlen(or);
  }
  //    }
}

static int32_t getTagCondString(SSqlCmd* pCmd, tSQLExpr* pExpr, char** str) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isExprDirectParentOfLeaftNode(pExpr)) {
    *(*str) = '(';
    *str += 1;

    int32_t ret = getTagCondString(pCmd, pExpr->pLeft, str);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    relToString(pCmd, pExpr, str);

    ret = getTagCondString(pCmd, pExpr->pRight, str);

    *(*str) = ')';
    *str += 1;

    return ret;
  }

  return tSQLExprLeafToString(pExpr, true, str);
}

static int32_t getTablenameCond(SSqlCmd* pCmd, tSQLExpr* pTableCond, char* str) {
  const char* msg0 = "invalid table name list";

  if (pTableCond == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSQLExpr* pLeft = pTableCond->pLeft;
  tSQLExpr* pRight = pTableCond->pRight;

  if (!isTablenameToken(&pLeft->colInfo)) {
    return TSDB_CODE_INVALID_SQL;
  }

  int32_t ret = TSDB_CODE_SUCCESS;

  if (pTableCond->nSQLOptr == TK_IN) {
    ret = tablenameListToString(pRight, str);
  } else if (pTableCond->nSQLOptr == TK_LIKE) {
    ret = tablenameCondToString(pRight, str);
  }

  if (ret != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(pCmd, msg0);
  }

  return ret;
}

static int32_t getColumnQueryCondInfo(SSqlCmd* pCmd, tSQLExpr* pExpr, int32_t relOptr) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isExprDirectParentOfLeaftNode(pExpr)) {  // internal node
    int32_t ret = getColumnQueryCondInfo(pCmd, pExpr->pLeft, pExpr->nSQLOptr);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    return getColumnQueryCondInfo(pCmd, pExpr->pRight, pExpr->nSQLOptr);
  } else {  // handle leaf node
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByNameEx(&pExpr->pLeft->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    return extractColumnFilterInfo(pCmd, &index, pExpr, relOptr);
  }
}

static int32_t getJoinCondInfo(SSqlObj* pSql, tSQLExpr* pExpr) {
  const char* msg = "invalid join query condition";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SSqlCmd* pCmd = &pSql->cmd;

  if (!isExprDirectParentOfLeaftNode(pExpr)) {
    return invalidSqlErrMsg(pCmd, msg);
  }

  STagCond*  pTagCond = &pCmd->tagCond;
  SJoinNode* pLeft = &pTagCond->joinInfo.left;
  SJoinNode* pRight = &pTagCond->joinInfo.right;

  SColumnIndex index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByNameEx(&pExpr->pLeft->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
  int16_t         tagColIndex = index.columnIndex - pMeterMetaInfo->pMeterMeta->numOfColumns;

  pLeft->uid = pMeterMetaInfo->pMeterMeta->uid;
  pLeft->tagCol = tagColIndex;
  strcpy(pLeft->meterId, pMeterMetaInfo->name);

  index = (SColumnIndex)COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByNameEx(&pExpr->pRight->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }

  pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
  tagColIndex = index.columnIndex - pMeterMetaInfo->pMeterMeta->numOfColumns;

  pRight->uid = pMeterMetaInfo->pMeterMeta->uid;
  pRight->tagCol = tagColIndex;
  strcpy(pRight->meterId, pMeterMetaInfo->name);

  pTagCond->joinInfo.hasJoin = true;
  return TSDB_CODE_SUCCESS;
}

// todo error handle / such as and /or mixed with +/-/*/
int32_t buildArithmeticExprString(tSQLExpr* pExpr, char** exprString) {
  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;

  *(*exprString) = '(';
  *exprString += 1;

  if (pLeft->nSQLOptr >= TK_PLUS && pLeft->nSQLOptr <= TK_REM) {
    buildArithmeticExprString(pLeft, exprString);
  } else {
    int32_t ret = tSQLExprNodeToString(pLeft, exprString);
    if (ret != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  optrToString(pExpr, exprString);

  if (pRight->nSQLOptr >= TK_PLUS && pRight->nSQLOptr <= TK_REM) {
    buildArithmeticExprString(pRight, exprString);
  } else {
    int32_t ret = tSQLExprNodeToString(pRight, exprString);
    if (ret != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  *(*exprString) = ')';
  *exprString += 1;

  return TSDB_CODE_SUCCESS;
}

static int32_t validateSQLExpr(tSQLExpr* pExpr, SSchema* pSchema, int32_t numOfCols, SColumnIdListRes* pList) {
  if (pExpr->nSQLOptr == TK_ID) {
    bool validColumnName = false;

    SColumnList* list = &pList->list;

    for (int32_t i = 0; i < numOfCols; ++i) {
      if (strncasecmp(pExpr->colInfo.z, pSchema[i].name, pExpr->colInfo.n) == 0 &&
          pExpr->colInfo.n == strlen(pSchema[i].name)) {
        if (pSchema[i].type < TSDB_DATA_TYPE_TINYINT || pSchema[i].type > TSDB_DATA_TYPE_DOUBLE) {
          return TSDB_CODE_INVALID_SQL;
        }

        if (pList != NULL) {
          list->ids[list->num++].columnIndex = (int16_t)i;
        }

        validColumnName = true;
      }
    }

    if (!validColumnName) {
      return TSDB_CODE_INVALID_SQL;
    }

  } else if (pExpr->nSQLOptr == TK_FLOAT && (isnan(pExpr->val.dKey) || isinf(pExpr->val.dKey))) {
    return TSDB_CODE_INVALID_SQL;
  } else if (pExpr->nSQLOptr >= TK_MIN && pExpr->nSQLOptr <= TK_LAST_ROW) {
    return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateArithmeticSQLExpr(tSQLExpr* pExpr, SSchema* pSchema, int32_t numOfCols,
                                         SColumnIdListRes* pList) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSQLExpr* pLeft = pExpr->pLeft;
  if (pLeft->nSQLOptr >= TK_PLUS && pLeft->nSQLOptr <= TK_REM) {
    int32_t ret = validateArithmeticSQLExpr(pLeft, pSchema, numOfCols, pList);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateSQLExpr(pLeft, pSchema, numOfCols, pList);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  tSQLExpr* pRight = pExpr->pRight;
  if (pRight->nSQLOptr >= TK_PLUS && pRight->nSQLOptr <= TK_REM) {
    int32_t ret = validateArithmeticSQLExpr(pRight, pSchema, numOfCols, pList);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateSQLExpr(pRight, pSchema, numOfCols, pList);
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
  if ((pLeft->nSQLOptr >= TK_COUNT && pLeft->nSQLOptr <= TK_LAST_ROW) ||
      (pRight->nSQLOptr >= TK_COUNT && pRight->nSQLOptr <= TK_LAST_ROW) ||
      (pLeft->nSQLOptr >= TK_BOOL && pLeft->nSQLOptr <= TK_BINARY && pRight->nSQLOptr >= TK_BOOL &&
       pRight->nSQLOptr <= TK_BINARY)) {
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

static bool validateJoinExprNode(SSqlCmd* pCmd, tSQLExpr* pExpr, SColumnIndex* pLeftIndex) {
  const char* msg1 = "illegal column name";
  const char* msg2 = "= is expected in join expression";
  const char* msg3 = "join column must have same type";
  const char* msg4 = "self join is not allowed";
  const char* msg5 = "join table must be the same type(table to table, super table to super table)";
  const char* msg6 = "tags in join condition not support binary/nchar types";

  tSQLExpr* pRight = pExpr->pRight;

  if (pRight->nSQLOptr != TK_ID) {
    return true;
  }

  if (pExpr->nSQLOptr != TK_EQ) {
    invalidSqlErrMsg(pCmd, msg2);
    return false;
  }

  SColumnIndex rightIndex = COLUMN_INDEX_INITIALIZER;

  if (getColumnIndexByNameEx(&pRight->colInfo, pCmd, &rightIndex) != TSDB_CODE_SUCCESS) {
    invalidSqlErrMsg(pCmd, msg1);
    return false;
  }

  // todo extract function
  SMeterMetaInfo* pLeftMeterMeta = tscGetMeterMetaInfo(pCmd, pLeftIndex->tableIndex);
  SSchema*        pLeftSchema = tsGetSchema(pLeftMeterMeta->pMeterMeta);
  int16_t         leftType = pLeftSchema[pLeftIndex->columnIndex].type;

  SMeterMetaInfo* pRightMeterMeta = tscGetMeterMetaInfo(pCmd, rightIndex.tableIndex);
  SSchema*        pRightSchema = tsGetSchema(pRightMeterMeta->pMeterMeta);
  int16_t         rightType = pRightSchema[rightIndex.columnIndex].type;

  if (leftType != rightType) {
    invalidSqlErrMsg(pCmd, msg3);
    return false;
  } else if (pLeftIndex->tableIndex == rightIndex.tableIndex) {
    invalidSqlErrMsg(pCmd, msg4);
    return false;
  } else if (leftType == TSDB_DATA_TYPE_BINARY || leftType == TSDB_DATA_TYPE_NCHAR) {
    invalidSqlErrMsg(pCmd, msg6);
    return false;
  }

  // table to table/ super table to super table are allowed
  if (UTIL_METER_IS_METRIC(pLeftMeterMeta) != UTIL_METER_IS_METRIC(pRightMeterMeta)) {
    invalidSqlErrMsg(pCmd, msg5);
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

static int32_t setExprToCond(SSqlCmd* pCmd, tSQLExpr** parent, tSQLExpr* pExpr, const char* msg, int32_t parentOptr) {
  if (*parent != NULL) {
    if (parentOptr == TK_OR && msg != NULL) {
      return invalidSqlErrMsg(pCmd, msg);
    }

    *parent = tSQLExprCreate((*parent), pExpr, parentOptr);
  } else {
    *parent = pExpr;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t handleExprInQueryCond(SSqlCmd* pCmd, tSQLExpr** pExpr, SCondExpr* pCondExpr, int32_t* type,
                                     int32_t parentOptr) {
  const char* msg1 = "meter query cannot use tags filter";
  const char* msg2 = "illegal column name";
  const char* msg3 = "only one query time range allowed";
  const char* msg4 = "only one join condition allowed";
  const char* msg5 = "AND is allowed to filter on different ordinary columns";
  const char* msg6 = "not support ordinary column join";
  const char* msg7 = "only one query condition on tbname allowed";
  const char* msg8 = "only in/like allowed in filter table name";

  tSQLExpr* pLeft = (*pExpr)->pLeft;
  tSQLExpr* pRight = (*pExpr)->pRight;

  int32_t ret = TSDB_CODE_SUCCESS;

  SColumnIndex index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByNameEx(&pLeft->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
    return invalidSqlErrMsg(pCmd, msg2);
  }

  assert(isExprDirectParentOfLeaftNode(*pExpr));

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
  SMeterMeta*     pMeterMeta = pMeterMetaInfo->pMeterMeta;

  if (index.columnIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {  // query on time range
    if (!validateJoinExprNode(pCmd, *pExpr, &index)) {
      return TSDB_CODE_INVALID_SQL;
    }

    // set join query condition
    if (pRight->nSQLOptr == TK_ID) {  // no need to keep the timestamp join condition
      pCmd->type |= TSDB_QUERY_TYPE_JOIN_QUERY;
      pCondExpr->tsJoin = true;

      /*
       * to release expression, e.g., m1.ts = m2.ts,
       * since this expression is used to set the join query type
       */
      tSQLExprDestroy(*pExpr);
    } else {
      ret = setExprToCond(pCmd, &pCondExpr->pTimewindow, *pExpr, msg3, parentOptr);
    }

    *pExpr = NULL;  // remove this expression
    *type = TSQL_EXPR_TS;
  } else if (index.columnIndex >= pMeterMeta->numOfColumns ||
             index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {  // query on tags
    // check for tag query condition
    if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
      return invalidSqlErrMsg(pCmd, msg1);
    }

    // check for like expression
    if ((*pExpr)->nSQLOptr == TK_LIKE) {
      if (pRight->val.nLen > TSDB_PATTERN_STRING_MAX_LEN) {
        return TSDB_CODE_INVALID_SQL;
      }

      SSchema* pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

      if ((!isTablenameToken(&pLeft->colInfo)) && pSchema[index.columnIndex].type != TSDB_DATA_TYPE_BINARY &&
          pSchema[index.columnIndex].type != TSDB_DATA_TYPE_NCHAR) {
        return invalidSqlErrMsg(pCmd, msg2);
      }
    }

    // in case of in operator, keep it in a seperate attribute
    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      if (!validTableNameOptr(*pExpr)) {
        return invalidSqlErrMsg(pCmd, msg8);
      }

      if (pCondExpr->pTableCond == NULL) {
        pCondExpr->pTableCond = *pExpr;
        pCondExpr->relType = parentOptr;
        pCondExpr->tableCondIndex = index.tableIndex;
      } else {
        return invalidSqlErrMsg(pCmd, msg7);
      }

      *type = TSQL_EXPR_TBNAME;
      *pExpr = NULL;
    } else {
      if (pRight->nSQLOptr == TK_ID) {  // join on tag columns for stable query
        if (!validateJoinExprNode(pCmd, *pExpr, &index)) {
          return TSDB_CODE_INVALID_SQL;
        }

        if (pCondExpr->pJoinExpr != NULL) {
          return invalidSqlErrMsg(pCmd, msg4);
        }

        pCmd->type |= TSDB_QUERY_TYPE_JOIN_QUERY;
        ret = setExprToCond(pCmd, &pCondExpr->pJoinExpr, *pExpr, NULL, parentOptr);
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
      return invalidSqlErrMsg(pCmd, msg6);
    }

    ret = setExprToCond(pCmd, &pCondExpr->pColumnCond, *pExpr, NULL, parentOptr);
    *pExpr = NULL;  // remove it from expr tree
  }

  return ret;
}

int32_t getQueryCondExpr(SSqlCmd* pCmd, tSQLExpr** pExpr, SCondExpr* pCondExpr, int32_t* type, int32_t parentOptr) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* msg1 = "query condition between different columns must use 'AND'";

  tSQLExpr* pLeft = (*pExpr)->pLeft;
  tSQLExpr* pRight = (*pExpr)->pRight;

  if (!isValidExpr(pLeft, pRight, (*pExpr)->nSQLOptr)) {
    return TSDB_CODE_INVALID_SQL;
  }

  int32_t leftType = -1;
  int32_t rightType = -1;

  if (!isExprDirectParentOfLeaftNode(*pExpr)) {
    int32_t ret = getQueryCondExpr(pCmd, &(*pExpr)->pLeft, pCondExpr, &leftType, (*pExpr)->nSQLOptr);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    ret = getQueryCondExpr(pCmd, &(*pExpr)->pRight, pCondExpr, &rightType, (*pExpr)->nSQLOptr);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    /*
     *  if left child and right child do not belong to the same group, the sub
     *  expression is not valid for parent node, it must be TK_AND operator.
     */
    if (leftType != rightType) {
      if ((*pExpr)->nSQLOptr == TK_OR && (leftType + rightType != TSQL_EXPR_TBNAME + TSQL_EXPR_TAG)) {
        return invalidSqlErrMsg(pCmd, msg1);
      }
    }

    *type = rightType;
    return TSDB_CODE_SUCCESS;
  }

  exchangeExpr(*pExpr);

  return handleExprInQueryCond(pCmd, pExpr, pCondExpr, type, parentOptr);
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

static void doExtractExprForSTable(tSQLExpr** pExpr, SSqlCmd* pCmd, tSQLExpr** pOut, int32_t tableIndex) {
  if (isExprDirectParentOfLeaftNode(*pExpr)) {
    tSQLExpr* pLeft = (*pExpr)->pLeft;

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByNameEx(&pLeft->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
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

    doExtractExprForSTable(&(*pExpr)->pLeft, pCmd, &((*pOut)->pLeft), tableIndex);
    doExtractExprForSTable(&(*pExpr)->pRight, pCmd, &((*pOut)->pRight), tableIndex);
  }
}

static tSQLExpr* extractExprForSTable(tSQLExpr** pExpr, SSqlCmd* pCmd, int32_t tableIndex) {
  tSQLExpr* pResExpr = NULL;

  if (*pExpr != NULL) {
    doExtractExprForSTable(pExpr, pCmd, &pResExpr, tableIndex);
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

static int32_t setTableCondForMetricQuery(SSqlObj* pSql, tSQLExpr* pExpr, int16_t tableCondIndex,
                                          char* tmpTableCondBuf) {
  SSqlCmd*    pCmd = &pSql->cmd;
  const char* msg = "meter name too long";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, tableCondIndex);

  STagCond* pTagCond = &pSql->cmd.tagCond;
  pTagCond->tbnameCond.uid = pMeterMetaInfo->pMeterMeta->uid;

  SString* pTableCond = &pCmd->tagCond.tbnameCond.cond;
  SStringAlloc(pTableCond, 4096);

  assert(pExpr->nSQLOptr == TK_LIKE || pExpr->nSQLOptr == TK_IN);

  if (pExpr->nSQLOptr == TK_LIKE) {
    strcpy(pTableCond->z, tmpTableCondBuf);
    pTableCond->n = strlen(pTableCond->z);
    return TSDB_CODE_SUCCESS;
  }

  strcpy(pTableCond->z, QUERY_COND_REL_PREFIX_IN);
  pTableCond->n += strlen(QUERY_COND_REL_PREFIX_IN);

  char db[TSDB_METER_ID_LEN] = {0};

  // remove the duplicated input table names
  int32_t num = 0;
  char**  segments = strsplit(tmpTableCondBuf + QUERY_COND_REL_PREFIX_IN_LEN, TBNAME_LIST_SEP, &num);
  qsort(segments, num, sizeof(void*), tableNameCompar);

  int32_t j = 1;
  for (int32_t i = 1; i < num; ++i) {
    if (strcmp(segments[i], segments[i - 1]) != 0) {
      segments[j++] = segments[i];
    }
  }
  num = j;

  SSQLToken dbToken = extractDBName(pMeterMetaInfo->name, db);
  char*     acc = getAccountId(pSql);

  for (int32_t i = 0; i < num; ++i) {
    SStringEnsureRemain(pTableCond, TSDB_METER_ID_LEN);

    if (i >= 1) {
      pTableCond->z[pTableCond->n++] = TBNAME_LIST_SEP[0];
    }

    int32_t   xlen = strlen(segments[i]);
    SSQLToken t = {.z = segments[i], .n = xlen, .type = TK_STRING};

    int32_t ret = setObjFullName(pTableCond->z + pTableCond->n, acc, &dbToken, &t, &xlen);
    if (ret != TSDB_CODE_SUCCESS) {
      tfree(segments);
      invalidSqlErrMsg(pCmd, msg);
      return ret;
    }

    pTableCond->n += xlen;
  }

  tfree(segments);
  return TSDB_CODE_SUCCESS;
}

static bool validateFilterExpr(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->colList.numOfCols; ++i) {
    SColumnBase* pColBase = &pCmd->colList.pColList[i];

    for (int32_t j = 0; j < pColBase->numOfFilters; ++j) {
      SColumnFilterInfo* pColFilter = &pColBase->filterInfo[j];
      int32_t            lowerOptr = pColFilter->lowerRelOptr;
      int32_t            upperOptr = pColFilter->upperRelOptr;

      if ((lowerOptr == TSDB_RELATION_LARGE_EQUAL || lowerOptr == TSDB_RELATION_LARGE) &&
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

static int32_t getTimeRangeFromExpr(SSqlCmd* pCmd, tSQLExpr* pExpr) {
  const char* msg0 = "invalid timestamp";
  const char* msg1 = "only one time stamp window allowed";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!isExprDirectParentOfLeaftNode(pExpr)) {
    if (pExpr->nSQLOptr == TK_OR) {
      return invalidSqlErrMsg(pCmd, msg1);
    }

    getTimeRangeFromExpr(pCmd, pExpr->pLeft);

    return getTimeRangeFromExpr(pCmd, pExpr->pRight);
  } else {
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByNameEx(&pExpr->pLeft->colInfo, pCmd, &index) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);
    SMeterMeta*     pMeterMeta = pMeterMetaInfo->pMeterMeta;

    tSQLExpr* pRight = pExpr->pRight;

    TSKEY stime = 0;
    TSKEY etime = INT64_MAX;

    if (getTimeRange(&stime, &etime, pRight, pExpr->nSQLOptr, pMeterMeta->precision) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(pCmd, msg0);
    }

    // update the timestamp query range
    if (pCmd->stime < stime) {
      pCmd->stime = stime;
    }

    if (pCmd->etime > etime) {
      pCmd->etime = etime;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateJoinExpr(SSqlCmd* pCmd, SCondExpr* pCondExpr) {
  const char* msg1 = "super table join requires tags column";
  const char* msg2 = "timestamp join condition missing";
  const char* msg3 = "condition missing for join query";

  if (!QUERY_IS_JOIN_QUERY(pCmd->type)) {
    if (pCmd->numOfTables == 1) {
      return TSDB_CODE_SUCCESS;
    } else {
      return invalidSqlErrMsg(pCmd, msg3);
    }
  }

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  if (UTIL_METER_IS_METRIC(pMeterMetaInfo)) {  // for stable join, tag columns
                                               // must be present for join
    if (pCondExpr->pJoinExpr == NULL) {
      return invalidSqlErrMsg(pCmd, msg1);
    }
  }

  if (!pCondExpr->tsJoin) {
    return invalidSqlErrMsg(pCmd, msg2);
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

int32_t parseWhereClause(SSqlObj* pSql, tSQLExpr** pExpr) {
  SSqlCmd* pCmd = &pSql->cmd;

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pCmd->stime = 0;
  pCmd->etime = INT64_MAX;

  int32_t ret = TSDB_CODE_SUCCESS;

  const char* msg1 = "invalid expression";
  SCondExpr   condExpr = {0};

  if ((*pExpr)->pLeft == NULL || (*pExpr)->pRight == NULL) {
    return invalidSqlErrMsg(pCmd, msg1);
  }

  ret = doParseWhereClause(pSql, pExpr, &condExpr);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  if (QUERY_IS_JOIN_QUERY(pCmd->type) && UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
    SColumnIndex index = {0};

    getColumnIndexByNameEx(&condExpr.pJoinExpr->pLeft->colInfo, pCmd, &index);
    pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);

    int32_t columnInfo = index.columnIndex - pMeterMetaInfo->pMeterMeta->numOfColumns;
    addRequiredTagColumn(pCmd, columnInfo, index.tableIndex);

    getColumnIndexByNameEx(&condExpr.pJoinExpr->pRight->colInfo, pCmd, &index);
    pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, index.tableIndex);

    columnInfo = index.columnIndex - pMeterMetaInfo->pMeterMeta->numOfColumns;
    addRequiredTagColumn(pCmd, columnInfo, index.tableIndex);
  }

  cleanQueryExpr(&condExpr);
  return ret;
}

int32_t doParseWhereClause(SSqlObj* pSql, tSQLExpr** pExpr, SCondExpr* condExpr) {
  const char* msg = "invalid filter expression";

  int32_t  type = 0;
  SSqlCmd* pCmd = &pSql->cmd;

  /*
   * tags query condition may be larger than 512bytes,
   * therefore, we need to prepare enough large space
   */
  char tableNameCond[TSDB_MAX_SQL_LEN] = {0};

  int32_t ret = TSDB_CODE_SUCCESS;
  if ((ret = getQueryCondExpr(pCmd, pExpr, condExpr, &type, (*pExpr)->nSQLOptr)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  doCompactQueryExpr(pExpr);

  // after expression compact, the expression tree is only include tag query condition
  condExpr->pTagCond = (*pExpr);

  // 1. check if it is a join query
  if ((ret = validateJoinExpr(pCmd, condExpr)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 2. get the query time range
  if ((ret = getTimeRangeFromExpr(pCmd, condExpr->pTimewindow)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 3. get the tag query condition
  if (condExpr->pTagCond != NULL) {
    for (int32_t i = 0; i < pCmd->numOfTables; ++i) {
      tSQLExpr* p1 = extractExprForSTable(pExpr, pCmd, i);

      SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, i);

      char  c[TSDB_MAX_TAGS_LEN] = {0};
      char* str = c;
      if ((ret = getTagCondString(pCmd, p1, &str)) != TSDB_CODE_SUCCESS) {
        return ret;
      }

      tsSetMetricQueryCond(&pCmd->tagCond, pMeterMetaInfo->pMeterMeta->uid, c);

      doCompactQueryExpr(pExpr);
      tSQLExprDestroy(p1);
    }

    condExpr->pTagCond = NULL;
  }

  // 4. get the table name query condition
  if ((ret = getTablenameCond(pCmd, condExpr->pTableCond, tableNameCond)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 5. other column query condition
  if ((ret = getColumnQueryCondInfo(pCmd, condExpr->pColumnCond, TK_AND)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 6. join condition
  if ((ret = getJoinCondInfo(pSql, condExpr->pJoinExpr)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // 7. query condition for table name
  pCmd->tagCond.relType = (condExpr->relType == TK_AND) ? TSDB_RELATION_AND : TSDB_RELATION_OR;
  ret = setTableCondForMetricQuery(pSql, condExpr->pTableCond, condExpr->tableCondIndex, tableNameCond);
  if (!validateFilterExpr(pCmd)) {
    return invalidSqlErrMsg(pCmd, msg);
  }

  return ret;
}

int32_t getTimeRange(int64_t* stime, int64_t* etime, tSQLExpr* pRight, int32_t optr, int16_t timePrecision) {
  // this is join condition, do nothing
  if (pRight->nSQLOptr == TK_ID) {
    return TSDB_CODE_SUCCESS;
  }

  /*
   * filter primary ts filter expression like:
   * where ts in ('2015-12-12 4:8:12')
   */
  if (pRight->nSQLOptr == TK_SET || optr == TK_IN) {
    return TSDB_CODE_INVALID_SQL;
  }

  int64_t val = 0;
  bool    parsed = false;
  if (pRight->val.nType == TSDB_DATA_TYPE_BINARY) {
    pRight->val.nLen = strdequote(pRight->val.pz);

    char* seg = strnchr(pRight->val.pz, '-', pRight->val.nLen, false);
    if (seg != NULL) {
      if (taosParseTime(pRight->val.pz, &val, pRight->val.nLen, TSDB_TIME_PRECISION_MICRO) == TSDB_CODE_SUCCESS) {
        parsed = true;
      } else {
        return TSDB_CODE_INVALID_SQL;
      }
    } else {
      SSQLToken token = {.z = pRight->val.pz, .n = pRight->val.nLen, .type = TK_ID};
      int32_t   len = tSQLGetToken(pRight->val.pz, &token.type);

      if ((token.type != TK_INTEGER && token.type != TK_FLOAT) || len != pRight->val.nLen) {
        return TSDB_CODE_INVALID_SQL;
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
    tVariantDump(&pRight->val, (char*)&val, TSDB_DATA_TYPE_BIGINT);

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
    *etime = val;
  } else if (optr == TK_LT) {
    *etime = val - delta;
  } else if (optr == TK_GT) {
    *stime = val + delta;
  } else if (optr == TK_GE) {
    *stime = val;
  } else if (optr == TK_EQ) {
    *stime = val;
    *etime = *stime;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tsRewriteFieldNameIfNecessary(SSqlCmd* pCmd) {
  const char rep[] = {'(', ')', '*', ',', '.', '/', '\\', '+', '-', '%', ' '};

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    char* fieldName = tscFieldInfoGetField(pCmd, i)->name;
    for (int32_t j = 0; j < TSDB_COL_NAME_LEN && fieldName[j] != 0; ++j) {
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
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    char* fieldName = tscFieldInfoGetField(pCmd, i)->name;
    for (int32_t j = i + 1; j < pCmd->fieldsInfo.numOfOutputCols; ++j) {
      if (strncasecmp(fieldName, tscFieldInfoGetField(pCmd, j)->name, TSDB_COL_NAME_LEN) == 0) {
        const char* msg = "duplicated column name in new table";
        return invalidSqlErrMsg(pCmd, msg);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t parseFillClause(SSqlCmd* pCmd, SQuerySQL* pQuerySQL) {
  tVariantList*     pFillToken = pQuerySQL->fillType;
  tVariantListItem* pItem = &pFillToken->a[0];

  const int32_t START_INTERPO_COL_IDX = 1;
  const char*   msg = "illegal value or data overflow";
  const char*   msg1 = "value is expected";
  const char*   msg2 = "invalid fill option";

  if (pItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
    return invalidSqlErrMsg(pCmd, msg2);
  }

  if (strncasecmp(pItem->pVar.pz, "none", 4) == 0 && pItem->pVar.nLen == 4) {
    pCmd->interpoType = TSDB_INTERPO_NONE;
  } else if (strncasecmp(pItem->pVar.pz, "null", 4) == 0 && pItem->pVar.nLen == 4) {
    pCmd->interpoType = TSDB_INTERPO_NULL;
    for (int32_t i = START_INTERPO_COL_IDX; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
      TAOS_FIELD* pFields = tscFieldInfoGetField(pCmd, i);
      setNull((char*)&pCmd->defaultVal[i], pFields->type, pFields->bytes);
    }
  } else if (strncasecmp(pItem->pVar.pz, "prev", 4) == 0 && pItem->pVar.nLen == 4) {
    pCmd->interpoType = TSDB_INTERPO_PREV;
  } else if (strncasecmp(pItem->pVar.pz, "linear", 6) == 0 && pItem->pVar.nLen == 6) {
    //         not support yet
    pCmd->interpoType = TSDB_INTERPO_LINEAR;
  } else if (strncasecmp(pItem->pVar.pz, "value", 5) == 0 && pItem->pVar.nLen == 5) {
    pCmd->interpoType = TSDB_INTERPO_SET_VALUE;

    if (pFillToken->nExpr == 1) {
      return invalidSqlErrMsg(pCmd, msg1);
    }

    int32_t startPos = 1;
    int32_t numOfFillVal = pFillToken->nExpr - 1;

    /* for point interpolation query, we do not have the timestamp column */
    if (tscIsPointInterpQuery(pCmd)) {
      startPos = 0;

      if (numOfFillVal > pCmd->fieldsInfo.numOfOutputCols) {
        numOfFillVal = pCmd->fieldsInfo.numOfOutputCols;
      }
    } else {
      numOfFillVal =
          (pFillToken->nExpr > pCmd->fieldsInfo.numOfOutputCols) ? pCmd->fieldsInfo.numOfOutputCols : pFillToken->nExpr;
    }

    int32_t j = 1;

    for (int32_t i = startPos; i < numOfFillVal; ++i, ++j) {
      TAOS_FIELD* pFields = tscFieldInfoGetField(pCmd, i);

      int32_t ret = tVariantDump(&pFillToken->a[j].pVar, (char*)&pCmd->defaultVal[i], pFields->type);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg);
      }

      if (pFields->type == TSDB_DATA_TYPE_BINARY || pFields->type == TSDB_DATA_TYPE_NCHAR) {
        setNull((char*)(&pCmd->defaultVal[i]), pFields->type, pFields->bytes);
      }
    }

    if ((pFillToken->nExpr < pCmd->fieldsInfo.numOfOutputCols) ||
        ((pFillToken->nExpr - 1 < pCmd->fieldsInfo.numOfOutputCols) && (tscIsPointInterpQuery(pCmd)))) {
      tVariantListItem* lastItem = &pFillToken->a[pFillToken->nExpr - 1];

      for (int32_t i = numOfFillVal; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
        TAOS_FIELD* pFields = tscFieldInfoGetField(pCmd, i);
        tVariantDump(&lastItem->pVar, (char*)&pCmd->defaultVal[i], pFields->type);

        if (pFields->type == TSDB_DATA_TYPE_BINARY || pFields->type == TSDB_DATA_TYPE_NCHAR) {
          setNull((char*)(&pCmd->defaultVal[i]), pFields->type, pFields->bytes);
        }
      }
    }
  } else {
    return invalidSqlErrMsg(pCmd, msg2);
  }

  return TSDB_CODE_SUCCESS;
}

static void setDefaultOrderInfo(SSqlCmd* pCmd) {
  /* set default timestamp order information for all queries */
  pCmd->order.order = TSQL_SO_ASC;
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  if (isTopBottomQuery(pCmd)) {
    pCmd->order.order = TSQL_SO_ASC;
    pCmd->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
  } else {
    pCmd->order.orderColId = -1;
  }

  /* for metric query, set default ascending order for group output */
  if (UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
    pCmd->groupbyExpr.orderType = TSQL_SO_ASC;
  }
}

int32_t parseOrderbyClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql, SSchema* pSchema, int32_t numOfCols) {
  const char* msg0 = "only support order by primary timestamp";
  const char* msg1 = "invalid column name";
  const char* msg2 = "only support order by primary timestamp and queried column";
  const char* msg3 = "only support order by primary timestamp and first tag in groupby clause";

  setDefaultOrderInfo(pCmd);
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

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
  if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
    if (pSortorder->nExpr > 1) {
      return invalidSqlErrMsg(pCmd, msg0);
    }
  } else {
    if (pSortorder->nExpr > 2) {
      return invalidSqlErrMsg(pCmd, msg3);
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

  if (UTIL_METER_IS_METRIC(pMeterMetaInfo)) {  // metric query
    if (getColumnIndexByNameEx(&columnName, pCmd, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(pCmd, msg1);
    }

    bool orderByTags = false;
    bool orderByTS = false;
    bool orderByCol = false;

    if (index.columnIndex >= pMeterMetaInfo->pMeterMeta->numOfColumns) {
      int32_t relTagIndex = index.columnIndex - pMeterMetaInfo->pMeterMeta->numOfColumns;
      if (relTagIndex == pCmd->groupbyExpr.columnInfo[0].colIdx) {
        orderByTags = true;
      }
    } else if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      orderByTags = true;
    }

    if (PRIMARYKEY_TIMESTAMP_COL_INDEX == index.columnIndex) {
      orderByTS = true;
    }

    if (!(orderByTags || orderByTS) && !isTopBottomQuery(pCmd)) {
      return invalidSqlErrMsg(pCmd, msg3);
    } else {
      assert(!(orderByTags && orderByTS));
    }

    if (pSortorder->nExpr == 1) {
      if (orderByTags) {
        pCmd->groupbyExpr.orderIndex = index.columnIndex - pMeterMetaInfo->pMeterMeta->numOfColumns;
        pCmd->groupbyExpr.orderType = pQuerySql->pSortOrder->a[0].sortOrder;
      } else if (isTopBottomQuery(pCmd)) {
        /* order of top/bottom query in interval is not valid  */
        SSqlExpr* pExpr = tscSqlExprGet(pCmd, 0);
        assert(pExpr->functionId == TSDB_FUNC_TS);

        pExpr = tscSqlExprGet(pCmd, 1);
        if (pExpr->colInfo.colIdx != index.columnIndex && index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
          return invalidSqlErrMsg(pCmd, msg2);
        }

        pCmd->order.order = pQuerySql->pSortOrder->a[0].sortOrder;
        pCmd->order.orderColId = pSchema[index.columnIndex].colId;
        return TSDB_CODE_SUCCESS;
      } else {
        pCmd->order.order = pSortorder->a[0].sortOrder;
        pCmd->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }
    }

    if (pSortorder->nExpr == 2) {
      if (orderByTags) {
        pCmd->groupbyExpr.orderIndex = index.columnIndex - pMeterMetaInfo->pMeterMeta->numOfColumns;
        pCmd->groupbyExpr.orderType = pQuerySql->pSortOrder->a[0].sortOrder;
      } else {
        pCmd->order.order = pSortorder->a[0].sortOrder;
        pCmd->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }

      tVariant* pVar2 = &pSortorder->a[1].pVar;
      SSQLToken cname = {pVar2->nLen, pVar2->nType, pVar2->pz};
      if (getColumnIndexByNameEx(&cname, pCmd, &index) != TSDB_CODE_SUCCESS) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      if (index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        return invalidSqlErrMsg(pCmd, msg2);
      } else {
        pCmd->order.order = pSortorder->a[1].sortOrder;
        pCmd->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }
    }

  } else {  // meter query
    if (getColumnIndexByNameEx(&columnName, pCmd, &index) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(pCmd, msg1);
    }

    if (index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX && !isTopBottomQuery(pCmd)) {
      return invalidSqlErrMsg(pCmd, msg2);
    }

    if (isTopBottomQuery(pCmd)) {
      /* order of top/bottom query in interval is not valid  */
      SSqlExpr* pExpr = tscSqlExprGet(pCmd, 0);
      assert(pExpr->functionId == TSDB_FUNC_TS);

      pExpr = tscSqlExprGet(pCmd, 1);
      if (pExpr->colInfo.colIdx != index.columnIndex && index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      pCmd->order.order = pQuerySql->pSortOrder->a[0].sortOrder;
      pCmd->order.orderColId = pSchema[index.columnIndex].colId;
      return TSDB_CODE_SUCCESS;
    }

    pCmd->order.order = pQuerySql->pSortOrder->a[0].sortOrder;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setAlterTableInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  const int32_t DEFAULT_TABLE_INDEX = 0;

  SSqlCmd*        pCmd = &pSql->cmd;
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, DEFAULT_TABLE_INDEX);

  SAlterTableSQL* pAlterSQL = pInfo->pAlterInfo;
  pCmd->command = TSDB_SQL_ALTER_TABLE;

  if (tscValidateName(&(pAlterSQL->name)) != TSDB_CODE_SUCCESS) {
    const char* msg = "invalid table name";
    return invalidSqlErrMsg(pCmd, msg);
  }

  if (setMeterID(pSql, &(pAlterSQL->name), 0) != TSDB_CODE_SUCCESS) {
    const char* msg = "table name too long";
    return invalidSqlErrMsg(pCmd, msg);
  }

  int32_t ret = tscGetMeterMeta(pSql, pMeterMetaInfo->name, DEFAULT_TABLE_INDEX);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  SMeterMeta* pMeterMeta = pMeterMetaInfo->pMeterMeta;
  SSchema*    pSchema = tsGetSchema(pMeterMeta);

  if (pInfo->sqlType == ALTER_TABLE_TAGS_ADD || pInfo->sqlType == ALTER_TABLE_TAGS_DROP ||
      pInfo->sqlType == ALTER_TABLE_TAGS_CHG) {
    if (UTIL_METER_IS_NOMRAL_METER(pMeterMetaInfo)) {
      const char* msg = "manipulation of tag available for metric";
      return invalidSqlErrMsg(pCmd, msg);
    }
  } else if ((pInfo->sqlType == ALTER_TABLE_TAGS_SET) && (UTIL_METER_IS_METRIC(pMeterMetaInfo))) {
    const char* msg = "set tag value only available for table";
    return invalidSqlErrMsg(pCmd, msg);
  } else if ((pInfo->sqlType == ALTER_TABLE_ADD_COLUMN || pInfo->sqlType == ALTER_TABLE_DROP_COLUMN) &&
             UTIL_METER_IS_CREATE_FROM_METRIC(pMeterMetaInfo)) {
    const char* msg = "column can only be modified by metric";
    return invalidSqlErrMsg(pCmd, msg);
  }

  if (pInfo->sqlType == ALTER_TABLE_TAGS_ADD) {
    pCmd->count = TSDB_ALTER_TABLE_ADD_TAG_COLUMN;

    tFieldList* pFieldList = pAlterSQL->pAddColumns;
    if (pFieldList->nField > 1) {
      const char* msg = "only support add one tag";
      return invalidSqlErrMsg(pCmd, msg);
    }

    if (!validateOneTags(pCmd, &pFieldList->p[0])) {
      return TSDB_CODE_INVALID_SQL;
    }

    tscFieldInfoSetValFromField(&pCmd->fieldsInfo, 0, &pFieldList->p[0]);
    pCmd->numOfCols = 1;  // only one column

  } else if (pInfo->sqlType == ALTER_TABLE_TAGS_DROP) {
    pCmd->count = TSDB_ALTER_TABLE_DROP_TAG_COLUMN;

    const char* msg1 = "no tags can be dropped";
    const char* msg2 = "only support one tag";
    const char* msg3 = "tag name too long";
    const char* msg4 = "illegal tag name";
    const char* msg5 = "primary tag cannot be dropped";

    if (pMeterMeta->numOfTags == 1) {
      return invalidSqlErrMsg(pCmd, msg1);
    }

    // numOfTags == 1
    if (pAlterSQL->varList->nExpr > 1) {
      return invalidSqlErrMsg(pCmd, msg2);
    }

    tVariantListItem* pItem = &pAlterSQL->varList->a[0];
    if (pItem->pVar.nLen > TSDB_COL_NAME_LEN) {
      return invalidSqlErrMsg(pCmd, msg3);
    }

    int32_t idx = -1;
    for (int32_t i = 0; i < pMeterMeta->numOfTags; ++i) {
      int32_t tagIdx = i + pMeterMeta->numOfColumns;
      char*   tagName = pSchema[tagIdx].name;
      size_t  nLen = strlen(tagName);

      if ((strncasecmp(tagName, pItem->pVar.pz, nLen) == 0) && (pItem->pVar.nLen == nLen)) {
        idx = i;
        break;
      }
    }

    if (idx == -1) {
      return invalidSqlErrMsg(pCmd, msg4);
    } else if (idx == 0) {
      return invalidSqlErrMsg(pCmd, msg5);
    }

    char name[128] = {0};
    strncpy(name, pItem->pVar.pz, pItem->pVar.nLen);
    tscFieldInfoSetValue(&pCmd->fieldsInfo, 0, TSDB_DATA_TYPE_INT, name, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize);

    pCmd->numOfCols = 1;  // only one column

  } else if (pInfo->sqlType == ALTER_TABLE_TAGS_CHG) {
    const char* msg1 = "tag name too long";
    const char* msg2 = "invalid tag name";

    pCmd->count = TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN;
    tVariantList* pVarList = pAlterSQL->varList;
    if (pVarList->nExpr > 2) {
      return TSDB_CODE_INVALID_SQL;
    }

    tVariantListItem* pSrcItem = &pAlterSQL->varList->a[0];
    tVariantListItem* pDstItem = &pAlterSQL->varList->a[1];

    if (pSrcItem->pVar.nLen >= TSDB_COL_NAME_LEN || pDstItem->pVar.nLen >= TSDB_COL_NAME_LEN) {
      return invalidSqlErrMsg(pCmd, msg1);
    }

    if (pSrcItem->pVar.nType != TSDB_DATA_TYPE_BINARY || pDstItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
      return invalidSqlErrMsg(pCmd, msg2);
    }

    SColumnIndex srcIndex = COLUMN_INDEX_INITIALIZER;
    SColumnIndex destIndex = COLUMN_INDEX_INITIALIZER;

    SSQLToken srcToken = {.z = pSrcItem->pVar.pz, .n = pSrcItem->pVar.nLen, .type = TK_STRING};
    if (getColumnIndexByNameEx(&srcToken, pCmd, &srcIndex) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    SSQLToken destToken = {.z = pDstItem->pVar.pz, .n = pDstItem->pVar.nLen, .type = TK_STRING};
    if (getColumnIndexByNameEx(&destToken, pCmd, &destIndex) == TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    char name[128] = {0};
    strncpy(name, pVarList->a[0].pVar.pz, pVarList->a[0].pVar.nLen);
    tscFieldInfoSetValue(&pCmd->fieldsInfo, 0, TSDB_DATA_TYPE_INT, name, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize);

    memset(name, 0, tListLen(name));
    strncpy(name, pVarList->a[1].pVar.pz, pVarList->a[1].pVar.nLen);
    tscFieldInfoSetValue(&pCmd->fieldsInfo, 1, TSDB_DATA_TYPE_INT, name, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize);

    pCmd->numOfCols = 2;
  } else if (pInfo->sqlType == ALTER_TABLE_TAGS_SET) {
    const char* msg0 = "tag name too long";
    const char* msg1 = "invalid tag value";
    const char* msg2 = "invalid tag name";
    const char* msg3 = "tag value too long";

    pCmd->count = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;

    // Note: update can only be applied to meter not metric.
    // the following is handle display tags value for meters created according to metric

    tVariantList* pVarList = pAlterSQL->varList;
    tVariant*     pTagName = &pVarList->a[0].pVar;

    if (pTagName->nLen > TSDB_COL_NAME_LEN) {
      return invalidSqlErrMsg(pCmd, msg0);
    }

    int32_t  tagsIndex = -1;
    SSchema* pTagsSchema = tsGetTagSchema(pMeterMetaInfo->pMeterMeta);

    for (int32_t i = 0; i < pMeterMetaInfo->pMeterMeta->numOfTags; ++i) {
      if (strcmp(pTagName->pz, pTagsSchema[i].name) == 0 && strlen(pTagsSchema[i].name) == pTagName->nLen) {
        tagsIndex = i;
        break;
      }
    }

    if (tagsIndex == -1) {
      return invalidSqlErrMsg(pCmd, msg2);
    }

    if (tVariantDump(&pVarList->a[1].pVar, pCmd->payload, pTagsSchema[tagsIndex].type) != TSDB_CODE_SUCCESS) {
      return invalidSqlErrMsg(pCmd, msg1);
    }

    // validate the length of binary
    if ((pTagsSchema[tagsIndex].type == TSDB_DATA_TYPE_BINARY || pTagsSchema[tagsIndex].type == TSDB_DATA_TYPE_NCHAR) &&
        pVarList->a[1].pVar.nLen > pTagsSchema[tagsIndex].bytes) {
      return invalidSqlErrMsg(pCmd, msg3);
    }

    char name[128] = {0};
    strncpy(name, pTagName->pz, pTagName->nLen);
    tscFieldInfoSetValue(&pCmd->fieldsInfo, 0, TSDB_DATA_TYPE_INT, name, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize);

    pCmd->numOfCols = 1;
  } else if (pInfo->sqlType == ALTER_TABLE_ADD_COLUMN) {
    pCmd->count = TSDB_ALTER_TABLE_ADD_COLUMN;

    tFieldList* pFieldList = pAlterSQL->pAddColumns;
    if (pFieldList->nField > 1) {
      const char* msg = "only support add one column";
      return invalidSqlErrMsg(pCmd, msg);
    }

    if (!validateOneColumn(pCmd, &pFieldList->p[0])) {
      return TSDB_CODE_INVALID_SQL;
    }

    tscFieldInfoSetValFromField(&pCmd->fieldsInfo, 0, &pFieldList->p[0]);
    pCmd->numOfCols = 1;  // only one column
  } else if (pInfo->sqlType == ALTER_TABLE_DROP_COLUMN) {
    pCmd->count = TSDB_ALTER_TABLE_DROP_COLUMN;

    const char* msg1 = "no columns can be dropped";
    const char* msg2 = "only support one column";
    const char* msg3 = "column name too long";
    const char* msg4 = "illegal column name";
    const char* msg5 = "primary timestamp column cannot be dropped";

    if (pMeterMeta->numOfColumns == TSDB_MIN_COLUMNS) {  //
      return invalidSqlErrMsg(pCmd, msg1);
    }

    if (pAlterSQL->varList->nExpr > 1) {
      return invalidSqlErrMsg(pCmd, msg2);
    }

    tVariantListItem* pItem = &pAlterSQL->varList->a[0];
    if (pItem->pVar.nLen > TSDB_COL_NAME_LEN) {
      return invalidSqlErrMsg(pCmd, msg3);
    }

    int32_t idx = -1;
    for (int32_t i = 0; i < pMeterMeta->numOfColumns; ++i) {
      char*  colName = pSchema[i].name;
      size_t len = strlen(colName);

      if ((strncasecmp(colName, pItem->pVar.pz, len) == 0) && (len == pItem->pVar.nLen)) {
        idx = i;
        break;
      }
    }

    if (idx == -1) {
      return invalidSqlErrMsg(pCmd, msg4);
    } else if (idx == 0) {
      return invalidSqlErrMsg(pCmd, msg5);
    }

    char name[128] = {0};
    strncpy(name, pItem->pVar.pz, pItem->pVar.nLen);
    tscFieldInfoSetValue(&pCmd->fieldsInfo, 0, TSDB_DATA_TYPE_INT, name, tDataTypeDesc[TSDB_DATA_TYPE_INT].nSize);

    pCmd->numOfCols = 1;  // only one column
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateSqlFunctionInStreamSql(SSqlCmd* pCmd) {
  const char* msg0 = "sample interval can not be less than 10ms.";
  const char* msg1 = "functions not allowed in select clause";

  if (pCmd->nAggTimeInterval != 0 && pCmd->nAggTimeInterval < 10) {
    return invalidSqlErrMsg(pCmd, msg0);
  }

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    int32_t functId = tscSqlExprGet(pCmd, i)->functionId;
    if (!IS_STREAM_QUERY_VALID(aAggs[functId].nStatus)) {
      return invalidSqlErrMsg(pCmd, msg1);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateFunctionsInIntervalOrGroupbyQuery(SSqlCmd* pCmd) {
  bool        isProjectionFunction = false;
  const char* msg1 = "column projection is not compatible with interval";
  const char* msg2 = "interval not allowed for tag queries";

  // multi-output set/ todo refactor
  for (int32_t k = 0; k < pCmd->fieldsInfo.numOfOutputCols; ++k) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, k);

    // projection query on primary timestamp, the selectivity function needs to be present.
    if (pExpr->functionId == TSDB_FUNC_PRJ && pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      bool hasSelectivity = false;
      for (int32_t j = 0; j < pCmd->fieldsInfo.numOfOutputCols; ++j) {
        SSqlExpr* pEx = tscSqlExprGet(pCmd, j);
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
    invalidSqlErrMsg(pCmd, msg1);
  }

  return isProjectionFunction == true ? TSDB_CODE_INVALID_SQL : TSDB_CODE_SUCCESS;
}

typedef struct SDNodeDynConfOption {
  char*   name;  // command name
  int32_t len;   // name string length
} SDNodeDynConfOption;

int32_t validateDNodeConfig(tDCLSQL* pOptions) {
  if (pOptions->nTokens < 2 || pOptions->nTokens > 3) {
    return TSDB_CODE_INVALID_SQL;
  }

  const SDNodeDynConfOption DNODE_DYNAMIC_CFG_OPTIONS[14] = {
      {"resetLog", 8},      {"resetQueryCache", 15}, {"dDebugFlag", 10},       {"rpcDebugFlag", 12},
      {"tmrDebugFlag", 12}, {"cDebugFlag", 10},      {"uDebugFlag", 10},       {"mDebugFlag", 10},
      {"sdbDebugFlag", 12}, {"httpDebugFlag", 13},   {"monitorDebugFlag", 16}, {"qDebugflag", 10},
      {"debugFlag", 9},     {"monitor", 7}};

  SSQLToken* pOptionToken = &pOptions->a[1];

  if (pOptions->nTokens == 2) {
    // reset log and reset query cache does not need value
    for (int32_t i = 0; i < 2; ++i) {
      const SDNodeDynConfOption* pOption = &DNODE_DYNAMIC_CFG_OPTIONS[i];
      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  } else if ((strncasecmp(DNODE_DYNAMIC_CFG_OPTIONS[13].name, pOptionToken->z, pOptionToken->n) == 0) &&
             (DNODE_DYNAMIC_CFG_OPTIONS[13].len == pOptionToken->n)) {
    SSQLToken* pValToken = &pOptions->a[2];
    int32_t    val = strtol(pValToken->z, NULL, 10);
    if (val != 0 && val != 1) {
      return TSDB_CODE_INVALID_SQL;  // options value is invalid
    }
    return TSDB_CODE_SUCCESS;
  } else {
    SSQLToken* pValToken = &pOptions->a[2];

    int32_t val = strtol(pValToken->z, NULL, 10);
    if (val < 131 || val > 199) {
      /* options value is out of valid range */
      return TSDB_CODE_INVALID_SQL;
    }

    for (int32_t i = 2; i < tListLen(DNODE_DYNAMIC_CFG_OPTIONS) - 1; ++i) {
      const SDNodeDynConfOption* pOption = &DNODE_DYNAMIC_CFG_OPTIONS[i];

      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        /* options is valid */
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  return TSDB_CODE_INVALID_SQL;
}

int32_t validateLocalConfig(tDCLSQL* pOptions) {
  if (pOptions->nTokens < 1 || pOptions->nTokens > 2) {
    return TSDB_CODE_INVALID_SQL;
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
      return TSDB_CODE_INVALID_SQL;
    }

    for (int32_t i = 1; i < tListLen(LOCAL_DYNAMIC_CFG_OPTIONS); ++i) {
      SDNodeDynConfOption* pOption = &LOCAL_DYNAMIC_CFG_OPTIONS[i];
      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        // options is valid
        return TSDB_CODE_SUCCESS;
      }
    }
  }
  return TSDB_CODE_INVALID_SQL;
}

int32_t validateColumnName(char* name) {
  bool ret = isKeyWord(name, strlen(name));
  if (ret) {
    return TSDB_CODE_INVALID_SQL;
  }

  SSQLToken token = {.z = name};
  token.n = tSQLGetToken(name, &token.type);

  if (token.type != TK_STRING && token.type != TK_ID) {
    return TSDB_CODE_INVALID_SQL;
  }

  if (token.type == TK_STRING) {
    strdequote(token.z);
    strtrim(token.z);
    token.n = (uint32_t)strlen(token.z);

    int32_t k = tSQLGetToken(token.z, &token.type);
    if (k != token.n) {
      return TSDB_CODE_INVALID_SQL;
    }

    return validateColumnName(token.z);
  } else {
    if (isNumber(&token)) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

bool hasTimestampForPointInterpQuery(SSqlCmd* pCmd) {
  if (!tscIsPointInterpQuery(pCmd)) {
    return true;
  }

  return (pCmd->stime == pCmd->etime) && (pCmd->stime != 0);
}

int32_t parseLimitClause(SSqlObj* pSql, SQuerySQL* pQuerySql) {
  SSqlCmd*        pCmd = &pSql->cmd;
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  const char* msg0 = "soffset/offset can not be less than 0";
  const char* msg1 = "slimit/soffset only available for STable query";
  const char* msg2 = "function not supported on table";
  const char* msg3 = "slimit/soffset can not apply to projection query";

  // handle the limit offset value, validate the limit
  pCmd->limit = pQuerySql->limit;
  pCmd->slimit = pQuerySql->slimit;

  if (pCmd->slimit.offset < 0 || pCmd->limit.offset < 0) {
    return invalidSqlErrMsg(pCmd, msg0);
  }

  if (pCmd->limit.limit == 0) {
    tscTrace("%p limit 0, no output result", pSql);
    pCmd->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
  }

  if (UTIL_METER_IS_METRIC(pMeterMetaInfo)) {
    bool queryOnTags = false;
    if (tscQueryOnlyMetricTags(pCmd, &queryOnTags) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    if (queryOnTags == true) {  // local handle the metric tag query
      pCmd->command = TSDB_SQL_RETRIEVE_TAGS;
    } else {
      if (tscProjectionQueryOnMetric(pCmd) && (pCmd->slimit.limit > 0 || pCmd->slimit.offset > 0)) {
        return invalidSqlErrMsg(pCmd, msg3);
      }
    }

    if (pCmd->slimit.limit == 0) {
      tscTrace("%p limit 0, no output result", pSql);
      pCmd->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      return TSDB_CODE_SUCCESS;
    }

    /*
     * get the distribution of all tables among available virtual nodes that satisfy query condition and
     * created according to this super table from management node.
     * And then launching multiple async-queries on required virtual nodes, which is the first-stage query operation.
     */
    int32_t code = tscGetMetricMeta(pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // No tables included. No results generated. Query results are empty.
    SMetricMeta* pMetricMeta = pMeterMetaInfo->pMetricMeta;
    if (pMeterMetaInfo->pMeterMeta == NULL || pMetricMeta == NULL || pMetricMeta->numOfMeters == 0) {
      tscTrace("%p no table in metricmeta, no output result", pSql);
      pCmd->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    }

    // keep original limitation value in globalLimit
    pCmd->globalLimit = pCmd->limit.limit;
  } else {
    if (pCmd->slimit.limit != -1 || pCmd->slimit.offset != 0) {
      return invalidSqlErrMsg(pCmd, msg1);
    }

    // filter the query functions operating on "tbname" column that are not supported by normal columns.
    for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
      SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
      if (pExpr->colInfo.colIdx == TSDB_TBNAME_COLUMN_INDEX) {
        return invalidSqlErrMsg(pCmd, msg2);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setKeepOption(SSqlCmd* pCmd, SCreateDbMsg* pMsg, SCreateDBInfo* pCreateDb) {
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
      default: {
        return invalidSqlErrMsg(pCmd, msg);
      }
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

static int32_t setTimePrecisionOption(SSqlCmd* pCmd, SCreateDbMsg* pMsg, SCreateDBInfo* pCreateDbInfo) {
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
      return invalidSqlErrMsg(pCmd, msg);
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

static void setCreateDBOption(SCreateDbMsg* pMsg, SCreateDBInfo* pCreateDb) {
  pMsg->blocksPerMeter = htons(pCreateDb->numOfBlocksPerTable);
  pMsg->compression = pCreateDb->compressionLevel;

  pMsg->commitLog = (char) pCreateDb->commitLog;
  pMsg->commitTime = htonl(pCreateDb->commitTime);
  pMsg->maxSessions = htonl(pCreateDb->tablesPerVnode);
  pMsg->cacheNumOfBlocks.fraction = pCreateDb->numOfAvgCacheBlocks;
  pMsg->cacheBlockSize = htonl(pCreateDb->cacheBlockSize);
  pMsg->rowsInFileBlock = htonl(pCreateDb->rowPerFileBlock);
  pMsg->daysPerFile = htonl(pCreateDb->daysPerFile);
  pMsg->replications = pCreateDb->replica;
}

int32_t parseCreateDBOptions(SSqlCmd* pCmd, SCreateDBInfo* pCreateDbSql) {
  SCreateDbMsg* pMsg = (SCreateDbMsg*)(pCmd->payload + tsRpcHeadSize + sizeof(SMgmtHead));
  setCreateDBOption(pMsg, pCreateDbSql);
  
  if (setKeepOption(pCmd, pMsg, pCreateDbSql) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }
  
  if (setTimePrecisionOption(pCmd, pMsg, pCreateDbSql) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }
  
  if (tscCheckCreateDbParams(pCmd, pMsg) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }
  
  return TSDB_CODE_SUCCESS;
}

void tscAddTimestampColumn(SSqlCmd* pCmd, int16_t functionId, int16_t tableIndex) {
  // the first column not timestamp column, add it
  SSqlExpr* pExpr = NULL;
  if (pCmd->exprsInfo.numOfExprs > 0) {
    pExpr = tscSqlExprGet(pCmd, 0);
  }

  if (pExpr == NULL || pExpr->colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX || pExpr->functionId != functionId) {
    SColumnIndex index = {tableIndex, PRIMARYKEY_TIMESTAMP_COL_INDEX};

    pExpr = tscSqlExprInsert(pCmd, 0, functionId, &index, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE, TSDB_KEYSIZE);
    pExpr->colInfo.flag = TSDB_COL_NORMAL;

    // NOTE: tag column does not add to source column list
    SColumnList ids = getColumnList(1, tableIndex, PRIMARYKEY_TIMESTAMP_COL_INDEX);

    insertResultField(pCmd, 0, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, "ts");
  }
}

void addGroupInfoForSubquery(SSqlObj* pParentObj, SSqlObj* pSql, int32_t tableIndex) {
  if (pParentObj->cmd.groupbyExpr.numOfGroupCols > 0) {
    int32_t   num = pSql->cmd.exprsInfo.numOfExprs;
    SSqlExpr* pExpr = tscSqlExprGet(&pSql->cmd, num - 1);
    SSqlCmd*  pCmd = &pSql->cmd;

    if (pExpr->functionId != TSDB_FUNC_TAG) {
      SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(&pSql->cmd, 0);
      int16_t         columnInfo = tscGetJoinTagColIndexByUid(pCmd, pMeterMetaInfo->pMeterMeta->uid);
      SColumnIndex    index = {.tableIndex = 0, .columnIndex = columnInfo};
      SSchema*        pSchema = tsGetTagSchema(pMeterMetaInfo->pMeterMeta);

      int16_t type = pSchema[index.columnIndex].type;
      int16_t bytes = pSchema[index.columnIndex].bytes;
      char*   name = pSchema[index.columnIndex].name;

      pExpr = tscSqlExprInsert(pCmd, pCmd->fieldsInfo.numOfOutputCols, TSDB_FUNC_TAG, &index, type, bytes, bytes);
      pExpr->colInfo.flag = TSDB_COL_TAG;

      // NOTE: tag column does not add to source column list
      SColumnList ids = {0};
      insertResultField(pCmd, pCmd->fieldsInfo.numOfOutputCols, &ids, bytes, type, name);

      int32_t relIndex = index.columnIndex;

      pExpr->colInfo.colIdx = relIndex;
      pCmd->groupbyExpr.columnInfo[0].colIdx = relIndex;

      addRequiredTagColumn(pCmd, pCmd->groupbyExpr.columnInfo[0].colIdx, 0);
    }
  }
}

void doAddGroupColumnForSubquery(SSqlCmd* pCmd, int32_t tagIndex) {
  int32_t index = pCmd->groupbyExpr.columnInfo[tagIndex].colIdx;

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  SSchema*     pSchema = tsGetColumnSchema(pMeterMetaInfo->pMeterMeta, index);
  SColumnIndex colIndex = {.tableIndex = 0, .columnIndex = index};

  SSqlExpr* pExpr = tscSqlExprInsert(pCmd, pCmd->fieldsInfo.numOfOutputCols, TSDB_FUNC_PRJ, &colIndex, pSchema->type,
                                     pSchema->bytes, pSchema->bytes);

  pExpr->colInfo.flag = TSDB_COL_NORMAL;
  pExpr->param[0].i64Key = 1;
  pExpr->numOfParams = 1;

  // NOTE: tag column does not add to source column list
  SColumnList list = {0};
  list.num = 1;
  list.ids[0] = colIndex;

  insertResultField(pCmd, pCmd->fieldsInfo.numOfOutputCols, &list, pSchema->bytes, pSchema->type, pSchema->name);
  tscFieldInfoUpdateVisible(&pCmd->fieldsInfo, pCmd->fieldsInfo.numOfOutputCols - 1, false);
}

static void doUpdateSqlFunctionForTagPrj(SSqlCmd* pCmd) {
  int32_t tagLength = 0;
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr->functionId == TSDB_FUNC_TAGPRJ || pExpr->functionId == TSDB_FUNC_TAG) {
      pExpr->functionId = TSDB_FUNC_TAG_DUMMY;
      tagLength += pExpr->resBytes;
    } else if (pExpr->functionId == TSDB_FUNC_PRJ && pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      pExpr->functionId = TSDB_FUNC_TS_DUMMY;
      tagLength += pExpr->resBytes;
    }
  }

  int16_t resType = 0;
  int16_t resBytes = 0;

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);
  SSchema*        pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr->functionId != TSDB_FUNC_TAG_DUMMY && pExpr->functionId != TSDB_FUNC_TS_DUMMY) {
      SSchema* pColSchema = &pSchema[pExpr->colInfo.colIdx];
      getResultDataInfo(pColSchema->type, pColSchema->bytes, pExpr->functionId, pExpr->param[0].i64Key, &pExpr->resType,
                        &pExpr->resBytes, &pExpr->interResBytes, tagLength, true);
    }
  }
}

static void doUpdateSqlFunctionForColPrj(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr->functionId == TSDB_FUNC_PRJ) {
      bool qualifiedCol = false;
      for (int32_t j = 0; j < pCmd->groupbyExpr.numOfGroupCols; ++j) {
        if (pExpr->colInfo.colId == pCmd->groupbyExpr.columnInfo[j].colId) {
          qualifiedCol = true;

          pExpr->param[0].i64Key = 1;  // limit the output to be 1 for each state value
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
    if (columnId == pGroupbyExpr->columnInfo[j].colId && pGroupbyExpr->columnInfo[j].flag == TSDB_COL_TAG) {
      return true;
    }
  }

  return false;
}

static bool onlyTagPrjFunction(SSqlCmd* pCmd) {
  bool hasTagPrj = false;
  bool hasColumnPrj = false;

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr->functionId == TSDB_FUNC_PRJ) {
      hasColumnPrj = true;
    } else if (pExpr->functionId == TSDB_FUNC_TAGPRJ) {
      hasTagPrj = true;
    }
  }

  return (hasTagPrj) && (hasColumnPrj == false);
}

// check if all the tags prj columns belongs to the group by columns
static bool allTagPrjInGroupby(SSqlCmd* pCmd) {
  bool allInGroupby = true;

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr->functionId != TSDB_FUNC_TAGPRJ) {
      continue;
    }

    if (!tagColumnInGroupby(&pCmd->groupbyExpr, pExpr->colInfo.colId)) {
      allInGroupby = false;
      break;
    }
  }

  // all selected tag columns belong to the group by columns set, always correct
  return allInGroupby;
}

static void updateTagPrjFunction(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
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
static int32_t checkUpdateTagPrjFunctions(SSqlCmd* pCmd) {
  const char* msg1 = "only one selectivity function allowed in presence of tags function";
  const char* msg2 = "functions not allowed";
  const char* msg3 = "aggregation function should not be mixed up with projection";

  bool    tagColExists = false;
  int16_t numOfTimestamp = 0;  // primary timestamp column
  int16_t numOfSelectivity = 0;
  int16_t numOfAggregation = 0;

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (pExpr->functionId == TSDB_FUNC_TAGPRJ ||
        (pExpr->functionId == TSDB_FUNC_PRJ && pExpr->colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX)) {
      tagColExists = true;
      break;
    }
  }

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    int16_t functionId = tscSqlExprGet(pCmd, i)->functionId;
    if (functionId == TSDB_FUNC_TAGPRJ || functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_TS) {
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
      return invalidSqlErrMsg(pCmd, msg1);
    }

    /*
     *  if numOfSelectivity equals to 0, it is a super table projection query
     */
    if (numOfSelectivity == 1) {
      doUpdateSqlFunctionForTagPrj(pCmd);
      doUpdateSqlFunctionForColPrj(pCmd);
    } else if (numOfSelectivity > 1) {
      /*
       * If more than one selectivity functions exist, all the selectivity functions must be last_row.
       * Otherwise, return with error code.
       */
      for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
        int16_t functionId = tscSqlExprGet(pCmd, i)->functionId;
        if (functionId == TSDB_FUNC_TAGPRJ) {
          continue;
        }

        if (((aAggs[functionId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) && (functionId != TSDB_FUNC_LAST_ROW)) {
          return invalidSqlErrMsg(pCmd, msg1);
        }
      }

      doUpdateSqlFunctionForTagPrj(pCmd);
      doUpdateSqlFunctionForColPrj(pCmd);
    }
  } else {
    if ((pCmd->type & TSDB_QUERY_TYPE_PROJECTION_QUERY) == TSDB_QUERY_TYPE_PROJECTION_QUERY) {
      if (numOfAggregation > 0 && pCmd->groupbyExpr.numOfGroupCols == 0) {
        return invalidSqlErrMsg(pCmd, msg3);
      }

      if (numOfAggregation > 0 || numOfSelectivity > 0) {
        // clear the projection type flag
        pCmd->type &= (~TSDB_QUERY_TYPE_PROJECTION_QUERY);
        doUpdateSqlFunctionForColPrj(pCmd);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doAddGroupbyColumnsOnDemand(SSqlCmd* pCmd) {
  const char* msg2 = "interval not allowed in group by normal column";

  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  SSchema* pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);
  int16_t  bytes = 0;
  int16_t  type = 0;
  char*    name = NULL;

  for (int32_t i = 0; i < pCmd->groupbyExpr.numOfGroupCols; ++i) {
    SColIndexEx* pColIndex = &pCmd->groupbyExpr.columnInfo[i];

    int16_t colIndex = pColIndex->colIdx;
    if (pColIndex->colIdx == TSDB_TBNAME_COLUMN_INDEX) {
      type = TSDB_DATA_TYPE_BINARY;
      bytes = TSDB_METER_NAME_LEN;
      name = TSQL_TBNAME_L;
    } else {
      colIndex = (TSDB_COL_IS_TAG(pColIndex->flag)) ? pMeterMetaInfo->pMeterMeta->numOfColumns + pColIndex->colIdx
                                                    : pColIndex->colIdx;

      type = pSchema[colIndex].type;
      bytes = pSchema[colIndex].bytes;
      name = pSchema[colIndex].name;
    }

    if (TSDB_COL_IS_TAG(pColIndex->flag)) {
      SColumnIndex index = {.tableIndex = pCmd->groupbyExpr.tableIndex, .columnIndex = colIndex};

      SSqlExpr* pExpr =
          tscSqlExprInsert(pCmd, pCmd->fieldsInfo.numOfOutputCols, TSDB_FUNC_TAG, &index, type, bytes, bytes);

      pExpr->colInfo.flag = TSDB_COL_TAG;

      // NOTE: tag column does not add to source column list
      SColumnList ids = {0};
      insertResultField(pCmd, pCmd->fieldsInfo.numOfOutputCols, &ids, bytes, type, name);
    } else {
      // if this query is "group by" normal column, interval is not allowed
      if (pCmd->nAggTimeInterval > 0) {
        return invalidSqlErrMsg(pCmd, msg2);
      }

      bool hasGroupColumn = false;
      for (int32_t j = 0; j < pCmd->fieldsInfo.numOfOutputCols; ++j) {
        SSqlExpr* pExpr = tscSqlExprGet(pCmd, j);
        if (pExpr->colInfo.colId == pColIndex->colId) {
          break;
        }
      }

      /*
       * if the group by column does not required by user, add this column into the final result set
       * but invisible to user
       */
      if (!hasGroupColumn) {
        doAddGroupColumnForSubquery(pCmd, i);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doFunctionsCompatibleCheck(SSqlObj* pSql) {
  const char* msg1 = "functions/columns not allowed in group by query";
  const char* msg2 = "interval not allowed in group by normal column";
  const char* msg3 = "group by not allowed on projection query";
  const char* msg4 = "tags retrieve not compatible with group by";
  const char* msg5 = "retrieve tags not compatible with group by or interval query";

  SSqlCmd*        pCmd = &pSql->cmd;
  SMeterMetaInfo* pMeterMetaInfo = tscGetMeterMetaInfo(pCmd, 0);

  // only retrieve tags, group by is not supportted
  if (pCmd->command == TSDB_SQL_RETRIEVE_TAGS) {
    if (pCmd->groupbyExpr.numOfGroupCols > 0 || pCmd->nAggTimeInterval > 0) {
      return invalidSqlErrMsg(pCmd, msg5);
    } else {
      return TSDB_CODE_SUCCESS;
    }
  }

  if (pCmd->groupbyExpr.numOfGroupCols > 0) {
    SSchema* pSchema = tsGetSchema(pMeterMetaInfo->pMeterMeta);
    int16_t  bytes = 0;
    int16_t  type = 0;
    char*    name = NULL;

    // check if all the tags prj columns belongs to the group by columns
    if (onlyTagPrjFunction(pCmd) && allTagPrjInGroupby(pCmd)) {
      updateTagPrjFunction(pCmd);
      return doAddGroupbyColumnsOnDemand(pCmd);
    }

    // check all query functions in selection clause, multi-output functions are not allowed
    for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
      SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
      int32_t   functId = pExpr->functionId;

      /*
       * group by normal columns.
       * Check if the column projection is identical to the group by column or not
       */
      if (functId == TSDB_FUNC_PRJ && pExpr->colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        bool qualified = false;
        for (int32_t j = 0; j < pCmd->groupbyExpr.numOfGroupCols; ++j) {
          SColIndexEx* pColIndex = &pCmd->groupbyExpr.columnInfo[j];
          if (pColIndex->colId == pExpr->colInfo.colId) {
            qualified = true;
            break;
          }
        }

        if (!qualified) {
          return TSDB_CODE_INVALID_SQL;
        }
      }

      if (IS_MULTIOUTPUT(aAggs[functId].nStatus) && functId != TSDB_FUNC_TOP && functId != TSDB_FUNC_BOTTOM &&
          functId != TSDB_FUNC_TAGPRJ && functId != TSDB_FUNC_PRJ) {
        return invalidSqlErrMsg(pCmd, msg1);
      }

      if (functId == TSDB_FUNC_COUNT && pExpr->colInfo.colIdx == TSDB_TBNAME_COLUMN_INDEX) {
        return invalidSqlErrMsg(pCmd, msg1);
      }
    }

    if (checkUpdateTagPrjFunctions(pCmd) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    /*
     * group by tag function must be not changed the function name, otherwise, the group operation may fail to
     * divide the subset of final result.
     */
    if (doAddGroupbyColumnsOnDemand(pCmd) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    // projection query on metric does not compatible with "group by" syntax
    if (tscProjectionQueryOnMetric(pCmd)) {
      return invalidSqlErrMsg(pCmd, msg3);
    }

    return TSDB_CODE_SUCCESS;
  } else {
    return checkUpdateTagPrjFunctions(pCmd);
  }
}

int32_t doLocalQueryProcess(SQuerySQL* pQuerySql, SSqlCmd* pCmd) {
  const char* msg1 = "only one expression allowed";
  const char* msg2 = "invalid expression in select clause";
  const char* msg3 = "invalid function";

  tSQLExprList* pExprList = pQuerySql->pSelection;
  if (pExprList->nExpr != 1) {
    return invalidSqlErrMsg(pCmd, msg1);
  }

  tSQLExpr* pExpr = pExprList->a[0].pNode;
  if (pExpr->operand.z == NULL) {
    return invalidSqlErrMsg(pCmd, msg2);
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

  SSqlExpr* pExpr1 = tscSqlExprInsertEmpty(pCmd, 0, TSDB_FUNC_TAG_DUMMY);
  if (pExprList->a[0].aliasName != NULL) {
    strncpy(pExpr1->aliasName, pExprList->a[0].aliasName, tListLen(pExpr1->aliasName));
  } else {
    strncpy(pExpr1->aliasName, functionsInfo[index].name, tListLen(pExpr1->aliasName));
  }

  switch (index) {
    case 0:
      pCmd->command = TSDB_SQL_CURRENT_DB;
      return TSDB_CODE_SUCCESS;
    case 1:
      pCmd->command = TSDB_SQL_SERV_VERSION;
      return TSDB_CODE_SUCCESS;
    case 2:
      pCmd->command = TSDB_SQL_SERV_STATUS;
      return TSDB_CODE_SUCCESS;
    case 3:
      pCmd->command = TSDB_SQL_CLI_VERSION;
      return TSDB_CODE_SUCCESS;
    case 4:
      pCmd->command = TSDB_SQL_CURRENT_USER;
      return TSDB_CODE_SUCCESS;
    default: {
      return invalidSqlErrMsg(pCmd, msg3);
    }
  }
}

// can only perform the parameters based on the macro definitation
int32_t tscCheckCreateDbParams(SSqlCmd* pCmd, SCreateDbMsg *pCreate) {
  char msg[512] = {0};
  
  if (pCreate->commitLog != -1 && (pCreate->commitLog < 0 || pCreate->commitLog > 1)) {
    snprintf(msg, tListLen(msg), "invalid db option commitLog: %d, only 0 or 1 allowed", pCreate->commitLog);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  if (pCreate->replications != -1 &&
      (pCreate->replications < TSDB_REPLICA_MIN_NUM || pCreate->replications > TSDB_REPLICA_MAX_NUM)) {
    snprintf(msg, tListLen(msg), "invalid db option replications: %d valid range: [%d, %d]", pCreate->replications, TSDB_REPLICA_MIN_NUM,
             TSDB_REPLICA_MAX_NUM);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  int32_t val = htonl(pCreate->daysPerFile);
  if (val != -1 && (val < TSDB_FILE_MIN_PARTITION_RANGE || val > TSDB_FILE_MAX_PARTITION_RANGE)) {
    snprintf(msg, tListLen(msg), "invalid db option daysPerFile: %d valid range: [%d, %d]", val,
             TSDB_FILE_MIN_PARTITION_RANGE, TSDB_FILE_MAX_PARTITION_RANGE);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  val = htonl(pCreate->rowsInFileBlock);
  if (val != -1 && (val < TSDB_MIN_ROWS_IN_FILEBLOCK || val > TSDB_MAX_ROWS_IN_FILEBLOCK)) {
    snprintf(msg, tListLen(msg), "invalid db option rowsInFileBlock: %d valid range: [%d, %d]", val,
             TSDB_MIN_ROWS_IN_FILEBLOCK, TSDB_MAX_ROWS_IN_FILEBLOCK);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  val = htonl(pCreate->cacheBlockSize);
  if (val != -1 && (val < TSDB_MIN_CACHE_BLOCK_SIZE || val > TSDB_MAX_CACHE_BLOCK_SIZE)) {
    snprintf(msg, tListLen(msg), "invalid db option cacheBlockSize: %d valid range: [%d, %d]", val,
             TSDB_MIN_CACHE_BLOCK_SIZE, TSDB_MAX_CACHE_BLOCK_SIZE);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  val = htonl(pCreate->maxSessions);
  if (val != -1 && (val < TSDB_MIN_TABLES_PER_VNODE || val > TSDB_MAX_TABLES_PER_VNODE)) {
    snprintf(msg, tListLen(msg), "invalid db option maxSessions: %d valid range: [%d, %d]", val, TSDB_MIN_TABLES_PER_VNODE,
             TSDB_MAX_TABLES_PER_VNODE);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  if (pCreate->precision != -1 &&
      (pCreate->precision != TSDB_TIME_PRECISION_MILLI && pCreate->precision != TSDB_TIME_PRECISION_MICRO)) {
    snprintf(msg, tListLen(msg), "invalid db option timePrecision: %d valid value: [%d, %d]", pCreate->precision, TSDB_TIME_PRECISION_MILLI,
             TSDB_TIME_PRECISION_MICRO);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  if (pCreate->cacheNumOfBlocks.fraction != -1 && (pCreate->cacheNumOfBlocks.fraction < TSDB_MIN_AVG_BLOCKS ||
      pCreate->cacheNumOfBlocks.fraction > TSDB_MAX_AVG_BLOCKS)) {
    snprintf(msg, tListLen(msg), "invalid db option ablocks: %f valid value: [%d, %d]", pCreate->cacheNumOfBlocks.fraction,
             TSDB_MIN_AVG_BLOCKS, TSDB_MAX_AVG_BLOCKS);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  val = htonl(pCreate->commitTime);
  if (val != -1 && (val < TSDB_MIN_COMMIT_TIME_INTERVAL || val > TSDB_MAX_COMMIT_TIME_INTERVAL)) {
    snprintf(msg, tListLen(msg), "invalid db option commitTime: %d valid range: [%d, %d]", val,
             TSDB_MIN_COMMIT_TIME_INTERVAL, TSDB_MAX_COMMIT_TIME_INTERVAL);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  if (pCreate->compression != -1 &&
      (pCreate->compression < TSDB_MIN_COMPRESSION_LEVEL || pCreate->compression > TSDB_MAX_COMPRESSION_LEVEL)) {
    snprintf(msg, tListLen(msg), "invalid db option compression: %d valid range: [%d, %d]", pCreate->compression, TSDB_MIN_COMPRESSION_LEVEL,
             TSDB_MAX_COMPRESSION_LEVEL);
    return invalidSqlErrMsg(pCmd, msg);
  }
  
  return TSDB_CODE_SUCCESS;
}

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

#include <stdio.h>
#include <stdlib.h>
#ifdef LINUX
#include <strings.h>
#endif
#include <float.h>
#include <math.h>
#include <string.h>

#include "os.h"
#include "taos.h"
#include "tstoken.h"
#include "ttime.h"

#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "tsql.h"
#pragma GCC diagnostic ignored "-Wunused-variable"

typedef struct SColumnIdList {
  SSchema* pSchema;
  int32_t  numOfCols;
  int32_t  numOfRecordedCols;
  int32_t  ids[TSDB_MAX_COLUMNS];
} SColumnIdList;

typedef struct SColumnList {
  int32_t numOfCols;
  int32_t ids[TSDB_MAX_COLUMNS];
} SColumnList;

static void setProjExprForMetricQuery(SSqlCmd* pCmd, int32_t fieldIDInResult, int32_t colIdx);

static int32_t setShowInfo(SSqlObj* pSql, SSqlInfo* pInfo);

static bool has(tFieldList* pFieldList, int32_t offset, char* name);

static char* getAccountId(SSqlObj* pSql);

static void getCurrentDBName(SSqlObj* pSql, SSQLToken* pDBToken);
static bool hasSpecifyDB(SSQLToken* pTableName);
static bool validateTableColumnInfo(tFieldList* pFieldList, SSqlCmd* pCmd);

static bool validateTagParams(tFieldList* pTagsList, tFieldList* pFieldList, SSqlCmd* pCmd);

static int32_t setObjFullName(char* fullName, char* account, SSQLToken* pDB, SSQLToken* tableName, int32_t* len);

static int32_t getColumnIndexByName(SSQLToken* pToken, SSchema* pSchema, int32_t numOfCols);

static void getColumnName(tSQLExprItem* pItem, char* resultFieldName, int32_t nLen);
static void getRevisedName(char* resultFieldName, int32_t functionId, int32_t maxLen, char* columnName);

static int32_t addExprAndResultField(SSqlCmd* pCmd, int32_t colIdx, tSQLExprItem* pItem);

static int32_t insertResultField(SSqlCmd* pCmd, int32_t fieldIDInResult, SColumnList* pIdList, int16_t bytes,
                                 int8_t type, char* fieldName);
static int32_t changeFunctionID(int32_t optr, int16_t* pExpr);

static void setErrMsg(SSqlCmd* pCmd, const char* pzErrMsg);

static int32_t buildSelectionClause(SSqlCmd* pCmd, tSQLExprList* pSelection, bool isMetric);

static bool validateIpAddress(char* ip);
static bool onlyQueryMetricTags(SSqlCmd* pCmd);
static bool hasUnsupportFunctionsForMetricQuery(SSqlCmd* pCmd);
static bool functionCompatibleCheck(SSqlCmd* pCmd);

static void setColumnOffsetValueInResultset(SSqlCmd* pCmd);
static int32_t setGroupByClause(SSqlCmd* pCmd, tVariantList* pList);

static int32_t setIntervalClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql);
static int32_t setSlidingClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql);

static int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, SSchema* pSchema, tSQLExprItem* pItem, bool isMet);

static int32_t buildQueryCond(SSqlObj* pSql, tSQLExpr* pExpr);
static int32_t setFillPolicy(SSqlCmd* pCmd, SQuerySQL* pQuerySQL);
static int32_t setOrderByClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql, SSchema* pSchema, int32_t numOfCols);

static int32_t tsRewriteFieldNameIfNecessary(SSqlCmd* pCmd);
static bool validateOneTags(SSqlCmd* pCmd, TAOS_FIELD* pTagField);
static int32_t setAlterTableInfo(SSqlObj* pSql, struct SSqlInfo* pInfo);
static int32_t validateSqlFunctionInStreamSql(SSqlCmd* pCmd);
static int32_t buildArithmeticExprString(tSQLExpr* pExpr, char** exprString, SColumnIdList* colIdList);
static int32_t validateFunctionsInIntervalOrGroupbyQuery(SSqlCmd* pCmd);
static int32_t validateArithmeticSQLExpr(tSQLExpr* pExpr, SSchema* pSchema, int32_t numOfCols);
static int32_t validateDNodeConfig(tDCLSQL* pOptions);
static int32_t validateColumnName(char* name);
static int32_t setKillInfo(SSqlObj* pSql, struct SSqlInfo* pInfo);
static bool hasTimestampForPointInterpQuery(SSqlCmd* pCmd);
static void updateTagColumnIndex(SSqlCmd* pCmd);
static int32_t setLimitOffsetValueInfo(SSqlObj* pSql, SQuerySQL* pQuerySql);
static void addRequiredTagColumn(SSqlCmd* pCmd, int32_t tagColIndex);
static int32_t parseCreateDBOptions(SCreateDBInfo* pCreateDbSql, SSqlCmd* pCmd);

static int32_t tscQueryOnlyMetricTags(SSqlCmd* pCmd, bool* queryOnMetricTags) {
  assert(pCmd->metricQuery == 1);

  // here colIdx == -1 means the special column tbname that is the name of each table
  *queryOnMetricTags = true;
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);

    if (pExpr->sqlFuncId != TSDB_FUNC_TAGPRJ &&
        !(pExpr->sqlFuncId == TSDB_FUNC_COUNT && pExpr->colInfo.colIdx == -1)) {  // 23 == "tagprj" function
      *queryOnMetricTags = false;
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// todo handle memory leak in error handle function
int32_t tscToSQLCmd(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  if (pInfo == NULL || pSql == NULL || pSql->signature != pSql) {
    return TSDB_CODE_APP_ERROR;
  }

  SSqlCmd* pCmd = &(pSql->cmd);

  if (!pInfo->validSql) {
    setErrMsg(pCmd, pInfo->pzErrMsg);
    return TSDB_CODE_INVALID_SQL;
  }

  // transfer pInfo into select operation
  switch (pInfo->sqlType) {
    case DROP_TABLE:
    case DROP_USER:
    case DROP_ACCOUNT:
    case DROP_DATABASE: {
      const char* msg = "param name too long";
      const char* msg1 = "invalid ip address";
      const char* msg2 = "invalid name";

      SSQLToken* pzName = &pInfo->pDCLInfo->a[0];
      if ((pInfo->sqlType != DROP_DNODE) && (tscValidateName(pzName) != TSDB_CODE_SUCCESS)) {
        setErrMsg(pCmd, msg2);
        return TSDB_CODE_INVALID_SQL;
      }

      if (pInfo->sqlType == DROP_DATABASE) {
        assert(pInfo->pDCLInfo->nTokens == 2);

        pCmd->command = TSDB_SQL_DROP_DB;
        pCmd->existsCheck = (pInfo->pDCLInfo->a[1].n == 1);

        int32_t code = setObjFullName(pCmd->name, getAccountId(pSql), pzName, NULL, NULL);
        if (code != TSDB_CODE_SUCCESS) {
          setErrMsg(pCmd, msg2);
        }

        return code;
      } else if (pInfo->sqlType == DROP_TABLE) {
        assert(pInfo->pDCLInfo->nTokens == 2);

        pCmd->existsCheck = (pInfo->pDCLInfo->a[1].n == 1);
        pCmd->command = TSDB_SQL_DROP_TABLE;

        int32_t ret = setMeterID(pSql, pzName);
        if (ret != TSDB_CODE_SUCCESS) {
          setErrMsg(pCmd, msg);
        }
        return ret;
      } else {
        if (pzName->n > TSDB_USER_LEN) {
          setErrMsg(pCmd, msg);
          return TSDB_CODE_INVALID_SQL;
        }

        if (pInfo->sqlType == DROP_USER) {
          pCmd->command = TSDB_SQL_DROP_USER;
        }

        strncpy(pCmd->name, pzName->z, pzName->n);
        return TSDB_CODE_SUCCESS;
      }
    }

    case USE_DATABASE: {
      const char* msg = "db name too long";
      pCmd->command = TSDB_SQL_USE_DB;

      SSQLToken* pToken = &pInfo->pDCLInfo->a[0];
	  
	  if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
	  	const char* msg1 = "invalid db name";
        setErrMsg(pCmd, msg1);
        return TSDB_CODE_INVALID_SQL;
      }
	  
      if (pToken->n > TSDB_DB_NAME_LEN) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      int32_t ret = setObjFullName(pCmd->name, getAccountId(pSql), pToken, NULL, NULL);
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
    case SHOW_DNODES:
    case SHOW_USERS:
    case SHOW_VGROUPS:
    case SHOW_CONNECTIONS:
    case SHOW_QUERIES:
    case SHOW_STREAMS:
    case SHOW_SCORES:
    case SHOW_CONFIGS: {
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
        setErrMsg(pCmd, msg3);
        return TSDB_CODE_INVALID_SQL;
      }

      int32_t ret = setObjFullName(pCmd->name, getAccountId(pSql), &(pCreateDB->dbname), NULL, NULL);
      if (ret != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg2);
        return ret;
      }

      if (parseCreateDBOptions(pCreateDB, pCmd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      break;
    }

    case CREATE_USER: {
      pCmd->command = (pInfo->sqlType == CREATE_USER) ? TSDB_SQL_CREATE_USER : TSDB_SQL_CREATE_ACCT;
      assert(pInfo->pDCLInfo->nTokens >= 2);

      const char* msg = "name or password too long";
      const char* msg1 = "password can not be empty";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "password needs single quote marks enclosed";
      const char* msg4 = "invalid state option, available options[no, r, w, all]";

      if (pInfo->pDCLInfo->a[1].type != TK_STRING) {
        setErrMsg(pCmd, msg3);
        return TSDB_CODE_INVALID_SQL;
      }

      strdequote(pInfo->pDCLInfo->a[1].z);
      strtrim(pInfo->pDCLInfo->a[1].z);  // trim space before and after passwords
      pInfo->pDCLInfo->a[1].n = strlen(pInfo->pDCLInfo->a[1].z);

      if (pInfo->pDCLInfo->a[1].n <= 0) {
        setErrMsg(pCmd, msg1);
        return TSDB_CODE_INVALID_SQL;
      }

      if (pInfo->pDCLInfo->a[0].n > TSDB_USER_LEN || pInfo->pDCLInfo->a[1].n > TSDB_PASSWORD_LEN) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      if (tscValidateName(&pInfo->pDCLInfo->a[0]) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg2);
        return TSDB_CODE_INVALID_SQL;
      }

      strncpy(pCmd->name, pInfo->pDCLInfo->a[0].z, pInfo->pDCLInfo->a[0].n);     // name
      strncpy(pCmd->payload, pInfo->pDCLInfo->a[1].z, pInfo->pDCLInfo->a[1].n);  // passwd
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
          setErrMsg(pCmd, msg3);
          return TSDB_CODE_INVALID_SQL;
        }

        strdequote(pInfo->pDCLInfo->a[1].z);
        strtrim(pInfo->pDCLInfo->a[1].z);  // trim space before and after passwords
        pInfo->pDCLInfo->a[1].n = strlen(pInfo->pDCLInfo->a[1].z);

        if (pInfo->pDCLInfo->a[1].n <= 0) {
          setErrMsg(pCmd, msg1);
          return TSDB_CODE_INVALID_SQL;
        }

        if (pInfo->pDCLInfo->a[1].n > TSDB_PASSWORD_LEN) {
          setErrMsg(pCmd, msg);
          return TSDB_CODE_INVALID_SQL;
        }

        strncpy(pCmd->payload, pInfo->pDCLInfo->a[1].z, pInfo->pDCLInfo->a[1].n);  // passwd
      }

      if (pInfo->pDCLInfo->a[0].n > TSDB_USER_LEN) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      if (tscValidateName(&pInfo->pDCLInfo->a[0]) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg2);
        return TSDB_CODE_INVALID_SQL;
      }

      strncpy(pCmd->name, pInfo->pDCLInfo->a[0].z, pInfo->pDCLInfo->a[0].n);  // name

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
          setErrMsg(pCmd, msg4);
          return TSDB_CODE_INVALID_SQL;
        }
      }
      break;
    }
    case DESCRIBE_TABLE: {
      pCmd->command = TSDB_SQL_DESCRIBE_TABLE;

      SSQLToken* pToken = &pInfo->pDCLInfo->a[0];
      const char* msg = "table name is too long";

	  if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
	  	const char* msg1 = "invalid table name";
        setErrMsg(pCmd, msg1);
        return TSDB_CODE_INVALID_SQL;
      }
	  
      if (pToken->n > TSDB_METER_NAME_LEN) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      if (setMeterID(pSql, pToken) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      int32_t ret = tscGetMeterMeta(pSql, pSql->cmd.name);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      break;
    }
    case ALTER_DNODE:
    case ALTER_USER_PASSWD:
    case ALTER_USER_PRIVILEGES: {
      pCmd->command = (pInfo->sqlType == ALTER_DNODE) ? TSDB_SQL_CFG_PNODE : TSDB_SQL_ALTER_USER;

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
        setErrMsg(pCmd, msg3);
        return TSDB_CODE_INVALID_SQL;
      }

      if (pDCL->a[0].n > TSDB_METER_NAME_LEN || pDCL->a[1].n > TSDB_PASSWORD_LEN) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      if (pCmd->command == TSDB_SQL_CFG_PNODE) {
        char ip[128] = {0};
        strncpy(ip, pDCL->a[0].z, pDCL->a[0].n);

        /* validate the ip address */
        if (!validateIpAddress(ip)) {
          setErrMsg(pCmd, msg1);
          return TSDB_CODE_INVALID_SQL;
        }

        strcpy(pCmd->name, ip);

        /* validate the parameter names and options */
        if (validateDNodeConfig(pDCL) != TSDB_CODE_SUCCESS) {
          setErrMsg(pCmd, msg2);
          return TSDB_CODE_INVALID_SQL;
        }

        strncpy(pCmd->payload, pDCL->a[1].z, pDCL->a[1].n);

        if (pDCL->nTokens == 3) {
          pCmd->payload[pDCL->a[1].n] = ' ';  // add sep
          strncpy(&pCmd->payload[pDCL->a[1].n + 1], pDCL->a[2].z, pDCL->a[2].n);
        }
      } else {
        const char* msg = "invalid user rights";
        const char* msg1 = "password can not be empty or larger than 24 characters";

        strncpy(pCmd->name, pDCL->a[0].z, pDCL->a[0].n);

        if (pInfo->sqlType == ALTER_USER_PASSWD) {
          /* update the password for user */
          pCmd->order.order |= TSDB_ALTER_USER_PASSWD;

          strdequote(pDCL->a[1].z);
          pDCL->a[1].n = strlen(pDCL->a[1].z);

          if (pDCL->a[1].n <= 0 || pInfo->pDCLInfo->a[1].n > TSDB_PASSWORD_LEN) {
            /* password cannot be empty string */
            setErrMsg(pCmd, msg1);
            return TSDB_CODE_INVALID_SQL;
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
            setErrMsg(pCmd, msg);
            return TSDB_CODE_INVALID_SQL;
          }
        } else {
          return TSDB_CODE_INVALID_SQL;
        }
      }
      break;
    }
    case ALTER_LOCAL: {
      pCmd->command = TSDB_SQL_CFG_LOCAL;
      const char* msg = "parameter too long";
      if (pInfo->pDCLInfo->a[0].n > TSDB_METER_ID_LEN) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      strncpy(pCmd->payload, pInfo->pDCLInfo->a[0].z, pInfo->pDCLInfo->a[0].n);
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
        setErrMsg(pCmd, msg1);
        return TSDB_CODE_INVALID_SQL;
      }

      if (setMeterID(pSql, pzTableName) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
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

      // table name
      // metric name, create table by using dst
      SSQLToken* pToken = &(pInfo->pCreateTableInfo->usingInfo.metricName);

	  if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      int32_t    ret = setMeterID(pSql, pToken);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      // get meter meta from mnode
      STagData* pTag = (STagData*)pCmd->payload;
      strncpy(pTag->name, pCmd->name, TSDB_METER_ID_LEN);

      tVariantList* pList = pInfo->pCreateTableInfo->usingInfo.pTagVals;

      int32_t code = tscGetMeterMeta(pSql, pTag->name);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      if (pSql->cmd.pMeterMeta->numOfTags != pList->nExpr) {
        setErrMsg(pCmd, msg2);
        return TSDB_CODE_INVALID_SQL;
      }

      /* too long tag values will be truncated automatically */
      SSchema* pTagSchema = tsGetTagSchema(pCmd->pMeterMeta);

      char* tagVal = pTag->data;
      for (int32_t i = 0; i < pList->nExpr; ++i) {
        int32_t ret = tVariantDump(&(pList->a[i].pVar), tagVal, pTagSchema[i].type);
        if (ret != TSDB_CODE_SUCCESS) {
          setErrMsg(pCmd, msg1);
          return TSDB_CODE_INVALID_SQL;
        }

        tagVal += pTagSchema[i].bytes;
      }

      if (tscValidateName(&pInfo->pCreateTableInfo->name) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      ret = setMeterID(pSql, &pInfo->pCreateTableInfo->name);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      pCmd->numOfCols = 0;
      pCmd->count = 0;
      break;
    }
    case TSQL_CREATE_STREAM: {
      pCmd->command = TSDB_SQL_CREATE_TABLE;
      const char* msg = "table name too long";
      const char* msg1 = "invalid table name";

      // if sql specifies db, use it, otherwise use default db
      SSQLToken* pzTableName = &(pInfo->pCreateTableInfo->name);
      SQuerySQL* pQuerySql = pInfo->pCreateTableInfo->pSelect;

      if (tscValidateName(pzTableName) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg1);
        return TSDB_CODE_INVALID_SQL;
      }

      SSQLToken* pSrcMeterName = &pInfo->pCreateTableInfo->pSelect->from;
      if (tscValidateName(pSrcMeterName) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg1);
        return TSDB_CODE_INVALID_SQL;
      }

      if (setMeterID(pSql, pSrcMeterName) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      int32_t code = tscGetMeterMeta(pSql, pCmd->name);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      bool isMetric = UTIL_METER_IS_METRIC(pCmd);
      if (buildSelectionClause(pCmd, pQuerySql->pSelection, isMetric) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      if (pQuerySql->pWhere != NULL) {  // query condition in stream computing
        if (buildQueryCond(pSql, pQuerySql->pWhere) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_INVALID_SQL;
        }
      }

      // set interval value
      if (setIntervalClause(pCmd, pQuerySql) != TSDB_CODE_SUCCESS) {
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
      if (setMeterID(pSql, pzTableName) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      // copy sql length
      tscAllocPayloadWithSize(pCmd, pQuerySql->selectToken.n + 8);

      strncpy(pCmd->payload, pQuerySql->selectToken.z, pQuerySql->selectToken.n);
      if (pQuerySql->selectToken.n > TSDB_MAX_SAVED_SQL_LEN) {
        const char* msg4 = "sql too long";  // todo ADD support
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      if (tsRewriteFieldNameIfNecessary(pCmd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      pCmd->numOfCols = pCmd->fieldsInfo.numOfOutputCols;

      if (validateSqlFunctionInStreamSql(pCmd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      /*
       * check if fill operation is available, the fill operation is parsed and executed during query execution, not here.
       */
      if (pQuerySql->fillType != NULL) {
        if (pCmd->nAggTimeInterval == 0) {
          const char* msg1 = "fill only available for interval query";
          setErrMsg(pCmd, msg1);

          return TSDB_CODE_INVALID_SQL;
        }

        tVariantListItem* pItem = &pQuerySql->fillType->a[0];
        if (pItem->pVar.nType == TSDB_DATA_TYPE_BINARY) {
          if (!((strncmp(pItem->pVar.pz, "none", 4) == 0 && pItem->pVar.nLen == 4) ||
                (strncmp(pItem->pVar.pz, "null", 4) == 0 && pItem->pVar.nLen == 4))) {
            const char* msg2 = "fill option not supported in stream computing";
            setErrMsg(pCmd, msg2);

            return TSDB_CODE_INVALID_SQL;
          }
        }
      }

      break;
    }

    case TSQL_QUERY_METER: {
      SQuerySQL* pQuerySql = pInfo->pQueryInfo;
      assert(pQuerySql != NULL);

      // too many result columns not support order by in query
      if (pQuerySql->pSelection->nExpr > TSDB_MAX_COLUMNS) {
        const char* msg = "too many columns in selection clause";
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }
 
	  if (tscValidateName(&(pQuerySql->from)) != TSDB_CODE_SUCCESS) {
	  	const char* msg = "invalid table name";
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }
	  
      if (setMeterID(pSql, &pQuerySql->from) != TSDB_CODE_SUCCESS) {
        const char* msg = "table name too long";
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      pSql->cmd.command = TSDB_SQL_SELECT;

      int32_t code = tscGetMeterMeta(pSql, pCmd->name);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      bool isMetric = UTIL_METER_IS_METRIC(pCmd);
      if (buildSelectionClause(pCmd, pQuerySql->pSelection, isMetric) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      // set interval value
      if (setIntervalClause(pCmd, pQuerySql) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      } else {
        if ((pCmd->nAggTimeInterval > 0) && (validateFunctionsInIntervalOrGroupbyQuery(pCmd) != TSDB_CODE_SUCCESS)) {
          return TSDB_CODE_INVALID_SQL;
        }
      }

      // set sliding value
      SSQLToken* pSliding = &pQuerySql->sliding;
      if (pSliding->n != 0) {
        // pCmd->count == 1 means sql in stream function
        if (!tscEmbedded && pCmd->count == 0) {
          const char* msg = "not support sliding in query";
          setErrMsg(pCmd, msg);
          return TSDB_CODE_INVALID_SQL;
        }

        code = getTimestampInUsFromStr(pSliding->z, pSliding->n, &pCmd->nSlidingTime);
        if (pCmd->pMeterMeta->precision == TSDB_TIME_PRECISION_MILLI) {
          pCmd->nSlidingTime /= 1000;
        }

        const char* msg3 = "sliding value too small";
        const char* msg4 = "sliding value no larger than the interval value";

        if (pCmd->nSlidingTime < tsMinSlidingTime) {
          setErrMsg(pCmd, msg3);
          return TSDB_CODE_INVALID_SQL;
        }

        if (pCmd->nSlidingTime > pCmd->nAggTimeInterval) {
          setErrMsg(pCmd, msg4);
          return TSDB_CODE_INVALID_SQL;
        }
      }

      if (setGroupByClause(pCmd, pQuerySql->pGroupby) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      // set order by info
      if (setOrderByClause(pCmd, pQuerySql, tsGetSchema(pCmd->pMeterMeta), pCmd->pMeterMeta->numOfColumns) !=
          TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      // set where info
      if (pQuerySql->pWhere != NULL) {
        if (buildQueryCond(pSql, pQuerySql->pWhere) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_INVALID_SQL;
        }

        if (pCmd->pMeterMeta->precision == TSDB_TIME_PRECISION_MILLI) {
          pCmd->stime = pCmd->stime / 1000;
          pCmd->etime = pCmd->etime / 1000;
        }
      } else {  // set the time range
        pCmd->stime = 0;
        pCmd->etime = INT64_MAX;
      }

      // no result due to invalid query time range
      if (pCmd->stime > pCmd->etime) {
        pCmd->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
        return TSDB_CODE_SUCCESS;
      }

      if (!hasTimestampForPointInterpQuery(pCmd)) {
        const char* msg = "point interpolation query needs timestamp";
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      if (pQuerySql->fillType != NULL) {
        const char* msg1 = "fill only available for interval query";
        const char* msg2 = "start(end) time of query range required or time range too large";

        if (pCmd->nAggTimeInterval == 0 && (!tscIsPointInterpQuery(pCmd))) {
          setErrMsg(pCmd, msg1);
          return TSDB_CODE_INVALID_SQL;
        }

        if (pCmd->nAggTimeInterval > 0) {
          int64_t timeRange = labs(pCmd->stime - pCmd->etime);
          // number of result is not greater than 10,000,000

          // TODO define macro
          if ((timeRange == 0) || (timeRange / pCmd->nAggTimeInterval) > 10000000) {
            setErrMsg(pCmd, msg2);
            return TSDB_CODE_INVALID_SQL;
          }
        }

        int32_t ret = setFillPolicy(pCmd, pQuerySql);
        if (ret != TSDB_CODE_SUCCESS) {
          return ret;
        }
      }

      // handle the limit offset value, validate the limit
      pCmd->limit = pQuerySql->limit;

      /* temporarily save the original limitation value */
      if ((code = setLimitOffsetValueInfo(pSql, pQuerySql)) != TSDB_CODE_SUCCESS) {
        return code;
      }

      setColumnOffsetValueInResultset(pCmd);
      updateTagColumnIndex(pCmd);
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

static bool isTopBottomQuery(SSqlCmd* pCmd) {
  if (pCmd->exprsInfo.numOfExprs != 2) {
    return false;
  }

  int32_t functionId = tscSqlExprGet(pCmd, 1)->sqlFuncId;
  return functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_TOP_DST ||
         functionId == TSDB_FUNC_BOTTOM_DST;
}

int32_t setIntervalClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql) {
  if (pQuerySql->interval.type == 0) {
    return TSDB_CODE_SUCCESS;
  }

  // interval is not null
  SSQLToken* t = &pQuerySql->interval;
  if (getTimestampInUsFromStr(t->z, t->n, &pCmd->nAggTimeInterval) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }

  /* revised the time precision according to the flag */
  if (pCmd->pMeterMeta->precision == TSDB_TIME_PRECISION_MILLI) {
    pCmd->nAggTimeInterval = pCmd->nAggTimeInterval / 1000;
  }

  /* parser has filter the illegal type, no need to check here */
  pCmd->intervalTimeUnit = pQuerySql->interval.z[pQuerySql->interval.n - 1];

  // interval cannot be less than 10 milliseconds
  if (pCmd->nAggTimeInterval < tsMinIntervalTime) {
    const char* msg = "interval cannot be less than 10 ms";
    setErrMsg(pCmd, msg);
    return TSDB_CODE_INVALID_SQL;
  }

  // for top/bottom + interval query, we do not add additional timestamp column in the front
  if (isTopBottomQuery(pCmd)) {
    return TSDB_CODE_SUCCESS;
  }

  // need to add timestamp column in resultset, if interval is existed
  tscSqlExprInsert(pCmd, 0, TSDB_FUNC_TS, 0, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE);

  SColumnList ids = {.numOfCols = 1, .ids = {0}};
  int32_t ret = insertResultField(pCmd, 0, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, aAggs[TSDB_FUNC_TS].aName);

  return ret;
}

int32_t setSlidingClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql) {
  const char* msg0 = "sliding value too small";
  const char* msg1 = "sliding value no larger than the interval value";

  SSQLToken* pSliding = &pQuerySql->sliding;

  if (pSliding->n != 0) {
    getTimestampInUsFromStr(pSliding->z, pSliding->n, &pCmd->nSlidingTime);
    if (pCmd->pMeterMeta->precision == TSDB_TIME_PRECISION_MILLI) {
      pCmd->nSlidingTime /= 1000;
    }

    if (pCmd->nSlidingTime < tsMinSlidingTime) {
      setErrMsg(pCmd, msg0);
      return TSDB_CODE_INVALID_SQL;
    }

    if (pCmd->nSlidingTime > pCmd->nAggTimeInterval) {
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setMeterID(SSqlObj *pSql, SSQLToken *pzTableName) {
  SSqlCmd *pCmd = &(pSql->cmd);
  int32_t ret = TSDB_CODE_SUCCESS;

  //clear array
  memset(pCmd->name, 0, tListLen(pCmd->name));
  const char* msg = "name too long";

  if (hasSpecifyDB(pzTableName)) {
    /*
     * db has been specified in sql string
     * so we ignore current db path
     */
    ret = setObjFullName(pCmd->name, getAccountId(pSql), NULL, pzTableName, NULL);
  } else {
    /* get current DB name first, then set it into path */
    SSQLToken t = {0};
    getCurrentDBName(pSql, &t);

    ret = setObjFullName(pCmd->name, NULL, &t, pzTableName, NULL);
  }

  if (ret != TSDB_CODE_SUCCESS) {
    setErrMsg(pCmd, msg);
  }

  return ret;
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
    setErrMsg(pCmd, msg);
    return false;
  }

  // first column must be timestamp
  if (pFieldList->p[0].type != TSDB_DATA_TYPE_TIMESTAMP) {
    setErrMsg(pCmd, msg1);
    return false;
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < pFieldList->nField; ++i) {
    nLen += pFieldList->p[i].bytes;
  }

  // max row length must be less than TSDB_MAX_BYTES_PER_ROW
  if (nLen > TSDB_MAX_BYTES_PER_ROW) {
    setErrMsg(pCmd, msg2);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < pFieldList->nField; ++i) {
    TAOS_FIELD* pField = &pFieldList->p[i];
    if (pField->type < TSDB_DATA_TYPE_BOOL || pField->type > TSDB_DATA_TYPE_NCHAR) {
      setErrMsg(pCmd, msg4);
      return false;
    }

    if ((pField->type == TSDB_DATA_TYPE_BINARY && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_BINARY_LEN)) ||
        (pField->type == TSDB_DATA_TYPE_NCHAR && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_NCHAR_LEN))) {
      setErrMsg(pCmd, msg5);
      return false;
    }

    if (validateColumnName(pField->name) != TSDB_CODE_SUCCESS) {
      setErrMsg(pCmd, msg6);
      return false;
    }

    if (has(pFieldList, i + 1, pFieldList->p[i].name) == true) {
      setErrMsg(pCmd, msg3);
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
    setErrMsg(pCmd, msg1);
    return false;
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < pTagsList->nField; ++i) {
    nLen += pTagsList->p[i].bytes;
  }

  // max tag row length must be less than TSDB_MAX_TAGS_LEN
  if (nLen > TSDB_MAX_TAGS_LEN) {
    setErrMsg(pCmd, msg2);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < pTagsList->nField; ++i) {
    if (has(pFieldList, 0, pTagsList->p[i].name) == true) {
      setErrMsg(pCmd, msg3);
      return false;
    }
  }

  /* timestamp in tag is not allowed */
  for (int32_t i = 0; i < pTagsList->nField; ++i) {
    if (pTagsList->p[i].type == TSDB_DATA_TYPE_TIMESTAMP) {
      setErrMsg(pCmd, msg4);
      return false;
    }

    if (pTagsList->p[i].type < TSDB_DATA_TYPE_BOOL || pTagsList->p[i].type > TSDB_DATA_TYPE_NCHAR) {
      setErrMsg(pCmd, msg5);
      return false;
    }

    if ((pTagsList->p[i].type == TSDB_DATA_TYPE_BINARY && pTagsList->p[i].bytes <= 0) ||
        (pTagsList->p[i].type == TSDB_DATA_TYPE_NCHAR && pTagsList->p[i].bytes <= 0)) {
      setErrMsg(pCmd, msg7);
      return false;
    }

    if (validateColumnName(pTagsList->p[i].name) != TSDB_CODE_SUCCESS) {
      setErrMsg(pCmd, msg6);
      return false;
    }

    if (has(pTagsList, i + 1, pTagsList->p[i].name) == true) {
      setErrMsg(pCmd, msg3);
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

  SMeterMeta* pMeterMeta = pCmd->pMeterMeta;

  // no more than 6 tags
  if (pMeterMeta->numOfTags == TSDB_MAX_TAGS) {
    char msg[128] = {0};
    sprintf(msg, "tags no more than %d", TSDB_MAX_TAGS);

    setErrMsg(pCmd, msg);
    return false;
  }

  // no timestamp allowable
  if (pTagField->type == TSDB_DATA_TYPE_TIMESTAMP) {
    setErrMsg(pCmd, msg1);
    return false;
  }

  if (pTagField->type < TSDB_DATA_TYPE_BOOL && pTagField->type > TSDB_DATA_TYPE_NCHAR) {
    setErrMsg(pCmd, msg6);
    return false;
  }

  SSchema* pTagSchema = tsGetTagSchema(pCmd->pMeterMeta);
  int32_t  nLen = 0;

  for (int32_t i = 0; i < pMeterMeta->numOfTags; ++i) {
    nLen += pTagSchema[i].bytes;
  }

  // length less than TSDB_MAX_TASG_LEN
  if (nLen + pTagField->bytes > TSDB_MAX_TAGS_LEN) {
    setErrMsg(pCmd, msg3);
    return false;
  }

  // tags name can not be a keyword
  if (validateColumnName(pTagField->name) != TSDB_CODE_SUCCESS) {
    setErrMsg(pCmd, msg4);
    return false;
  }

  // binary(val), val can not be equalled to or less than 0
  if ((pTagField->type == TSDB_DATA_TYPE_BINARY || pTagField->type == TSDB_DATA_TYPE_NCHAR) && pTagField->bytes <= 0) {
    setErrMsg(pCmd, msg5);
    return false;
  }

  // field name must be unique
  SSchema* pSchema = tsGetSchema(pMeterMeta);

  for (int32_t i = 0; i < pMeterMeta->numOfTags + pMeterMeta->numOfColumns; ++i) {
    if (strncasecmp(pTagField->name, pSchema[i].name, TSDB_COL_NAME_LEN) == 0) {
      setErrMsg(pCmd, msg2);
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

  SMeterMeta* pMeterMeta = pCmd->pMeterMeta;

  // no more max columns
  if (pMeterMeta->numOfColumns >= TSDB_MAX_COLUMNS ||
      pMeterMeta->numOfTags + pMeterMeta->numOfColumns >= TSDB_MAX_COLUMNS) {
    setErrMsg(pCmd, msg1);
    return false;
  }

  if (pColField->type < TSDB_DATA_TYPE_BOOL || pColField->type > TSDB_DATA_TYPE_NCHAR) {
    setErrMsg(pCmd, msg4);
    return false;
  }

  if (validateColumnName(pColField->name) != TSDB_CODE_SUCCESS) {
    setErrMsg(pCmd, msg5);
    return false;
  }

  SSchema* pSchema = tsGetSchema(pMeterMeta);
  int32_t  nLen = 0;

  for (int32_t i = 0; i < pMeterMeta->numOfColumns; ++i) {
    nLen += pSchema[i].bytes;
  }

  if (pColField->bytes <= 0) {
    setErrMsg(pCmd, msg6);
    return false;
  }

  // length less than TSDB_MAX_BYTES_PER_ROW
  if (nLen + pColField->bytes > TSDB_MAX_BYTES_PER_ROW) {
    setErrMsg(pCmd, msg3);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < pMeterMeta->numOfTags + pMeterMeta->numOfColumns; ++i) {
    if (strncasecmp(pColField->name, pSchema[i].name, TSDB_COL_NAME_LEN) == 0) {
      setErrMsg(pCmd, msg2);
      return false;
    }
  }

  return true;
}

/* is contained in pFieldList or not */
static bool has(tFieldList* pFieldList, int32_t startIdx, char* name) {
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

  fullName[totalLen] = 0;

  if (xlen != NULL) {
    *xlen = totalLen;
  }
  return (totalLen <= TSDB_METER_ID_LEN) ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_SQL;
}

static void extractColumnNameFromString(tSQLExprItem* pItem, char* tmpBuf) {
  if (pItem->pNode->nSQLOptr == TK_STRING) {
    strdequote(pItem->pNode->val.pz);
    strcpy(tmpBuf, pItem->pNode->val.pz);

    tVariantDestroy(&pItem->pNode->val);
    pItem->pNode->nSQLOptr = TK_ID;

    SSQLToken* pIdToken = &pItem->pNode->colInfo;
    pIdToken->type = TK_ID;
    pIdToken->z = tmpBuf;
    pIdToken->n = strlen(pIdToken->z);
  }
}

int32_t buildSelectionClause(SSqlCmd* pCmd, tSQLExprList* pSelection, bool isMetric) {
  assert(pSelection != NULL && pCmd != NULL);

  const char* msg1 = "invalid column name/illegal column type in arithmetic expression";
  const char* msg2 = "functions can not be mixed up";
  const char* msg3 = "not support query expression";
  const char* msg4 = "function not support in STable query";

  SSchema* pSchema = tsGetSchema(pCmd->pMeterMeta);

  for (int32_t i = 0; i < pSelection->nExpr; ++i) {
    int32_t outputIndex = pCmd->fieldsInfo.numOfOutputCols;

    tSQLExprItem* pItem = &pSelection->a[i];
    if (pItem->pNode->nSQLOptr == TK_ALL || pItem->pNode->nSQLOptr == TK_ID ||
        pItem->pNode->nSQLOptr == TK_STRING) {  // project on all fields

      if (pItem->pNode->nSQLOptr == TK_ID && (pItem->pNode->colInfo.z == NULL && pItem->pNode->colInfo.n == 0)) {
        /* it is actually a function, but the function name is invalid */
        return TSDB_CODE_INVALID_SQL;
      }

      /* if the name of column is quoted, remove it and set the right
       * information for later process */
      char tmpName[TSDB_METER_NAME_LEN + 1] = {0};
      extractColumnNameFromString(pItem, tmpName);

      /* select * / select field_name1, field_name2  from table_name */
      int32_t ret = addProjectionExprAndResultField(pCmd, pSchema, pItem, isMetric);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
    } else if (pItem->pNode->nSQLOptr >= TK_COUNT && pItem->pNode->nSQLOptr <= TK_LAST_ROW) {
      // sql function optr
      /* sql function in selection clause, append sql function info in pSqlCmd structure sequentially */
      if (addExprAndResultField(pCmd, outputIndex, pItem) == -1) {
        return TSDB_CODE_INVALID_SQL;
      }

    } else if (pItem->pNode->nSQLOptr >= TK_PLUS && pItem->pNode->nSQLOptr <= TK_REM) {
      /* arithmetic function in select*/
      int32_t ret = validateArithmeticSQLExpr(pItem->pNode, pSchema, pCmd->pMeterMeta->numOfColumns);
      if (ret != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg1);
        return TSDB_CODE_INVALID_SQL;
      }

      SColumnIdList ids = {0};
      ids.pSchema = pSchema;
      ids.numOfCols = pCmd->pMeterMeta->numOfColumns;

      char  arithmeticExprStr[1024] = {0};
      char* p = arithmeticExprStr;

      if (buildArithmeticExprString(pItem->pNode, &p, &ids) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_INVALID_SQL;
      }

      // expr string is set as the parameter of function
      SSqlExpr* pExpr = tscSqlExprInsert(pCmd, outputIndex, TSDB_FUNC_ARITHM, 0, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
      addExprParams(pExpr, arithmeticExprStr, TSDB_DATA_TYPE_BINARY, strlen(arithmeticExprStr));

      /* todo alias name should use the original sql string */
      if (pItem->aliasName != NULL) {
        strncpy(pExpr->aliasName, pItem->aliasName, TSDB_COL_NAME_LEN);
      } else {
        strncpy(pExpr->aliasName, arithmeticExprStr, TSDB_COL_NAME_LEN);
      }

      SColumnList idx = {.numOfCols = ids.numOfRecordedCols, .ids = {0}};
      memcpy(idx.ids, ids.ids, ids.numOfRecordedCols * sizeof(ids.ids[0]));

      insertResultField(pCmd, i, &idx, sizeof(double), TSDB_DATA_TYPE_DOUBLE, pExpr->aliasName);
    } else {
      /*
       * not support such expression
       * e.g., select 12+5 from table_name
       */
      setErrMsg(pCmd, msg3);
      return TSDB_CODE_INVALID_SQL;
    }

    if (pCmd->fieldsInfo.numOfOutputCols > TSDB_MAX_COLUMNS) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  if (!functionCompatibleCheck(pCmd)) {
    setErrMsg(pCmd, msg2);
    return TSDB_CODE_INVALID_SQL;
  }

  if (isMetric) {
    pCmd->metricQuery = 1;

    if (onlyQueryMetricTags(pCmd)) {  // local handle the metric tag query
      pCmd->command = TSDB_SQL_RETRIEVE_TAGS;
      pCmd->count = pCmd->pMeterMeta->numOfColumns;  // the number of meter schema, tricky.
    }

    /*
     * transfer sql functions that need secondary merge into another format
     * in dealing with metric queries such as: count/first/last
     */
    tscTansformSQLFunctionForMetricQuery(pCmd);

    if (hasUnsupportFunctionsForMetricQuery(pCmd)) {
      setErrMsg(pCmd, msg4);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t insertResultField(SSqlCmd* pCmd, int32_t outputIndex, SColumnList* pIdList, int16_t bytes, int8_t type,
                          char* fieldName) {
  for (int32_t i = 0; i < pIdList->numOfCols; ++i) {
    tscColumnInfoInsert(pCmd, pIdList->ids[i]);
  }

  tscFieldInfoSetValue(&pCmd->fieldsInfo, outputIndex, type, fieldName, bytes);
  return TSDB_CODE_SUCCESS;
}

void setProjExprForMetricQuery(SSqlCmd* pCmd, int32_t outputIndex, int32_t colIdx) {
  pCmd->metricQuery = 1;
  SSchema* pSchema = tsGetSchemaColIdx(pCmd->pMeterMeta, colIdx);

  int16_t functionId = (int16_t)((colIdx >= pCmd->pMeterMeta->numOfColumns) ? TSDB_FUNC_TAGPRJ :  // tagPrj function
                                     TSDB_FUNC_PRJ);                                              // colprj function

  int32_t numOfCols = pCmd->pMeterMeta->numOfColumns;

  bool isTag = false;
  if (colIdx >= numOfCols) {
    colIdx -= numOfCols;
    addRequiredTagColumn(pCmd, colIdx);
    isTag = true;
  }

  SSqlExpr* pExpr = tscSqlExprInsert(pCmd, outputIndex, functionId, colIdx, pSchema->type, pSchema->bytes);
  pExpr->colInfo.isTag = isTag;
}

void addRequiredTagColumn(SSqlCmd* pCmd, int32_t tagColIndex) {
  if (pCmd->numOfReqTags == 0 || pCmd->tagColumnIndex[pCmd->numOfReqTags - 1] < tagColIndex) {
    pCmd->tagColumnIndex[pCmd->numOfReqTags++] = tagColIndex;
  } else {  // find the appropriate position
    for (int32_t i = 0; i < pCmd->numOfReqTags; ++i) {
      if (tagColIndex > pCmd->tagColumnIndex[i]) {
        continue;
      } else if (tagColIndex == pCmd->tagColumnIndex[i]) {
        break;
      } else {
        memmove(&pCmd->tagColumnIndex[i + 1], &pCmd->tagColumnIndex[i],
                sizeof(pCmd->tagColumnIndex[0]) * (pCmd->numOfReqTags - i));
        pCmd->tagColumnIndex[i] = tagColIndex;

        pCmd->numOfReqTags++;
        break;
      }
    }
  }

  // plus one means tbname
  assert(tagColIndex >= -1 && tagColIndex < TSDB_MAX_TAGS && pCmd->numOfReqTags <= TSDB_MAX_TAGS + 1);
}

int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, SSchema* pSchema, tSQLExprItem* pItem, bool isMetric) {
  int32_t startPos = pCmd->fieldsInfo.numOfOutputCols;

  if (pItem->pNode->nSQLOptr == TK_ALL) {  // project on all fields
    int32_t     numOfTotalColumns = 0;
    SMeterMeta* pMeterMeta = pCmd->pMeterMeta;

    if (isMetric) {  // metric query
      numOfTotalColumns = pMeterMeta->numOfColumns + pMeterMeta->numOfTags;

      for (int32_t j = 0; j < numOfTotalColumns; ++j) {
        setProjExprForMetricQuery(pCmd, startPos + j, j);

        SColumnList ids = {.numOfCols = 1, .ids = {j}};

        // tag columns do not add to source list
        if (j >= pMeterMeta->numOfColumns) {
          ids.numOfCols = 0;
        }
        insertResultField(pCmd, startPos + j, &ids, pSchema[j].bytes, pSchema[j].type, pSchema[j].name);
      }
    } else {  // meter query
      numOfTotalColumns = pMeterMeta->numOfColumns;
      for (int32_t j = 0; j < numOfTotalColumns; ++j) {
        tscSqlExprInsert(pCmd, j, TSDB_FUNC_PRJ, j, pSchema[j].type, pSchema[j].bytes);

        SColumnList ids = {.numOfCols = 1, .ids = {j}};
        insertResultField(pCmd, startPos + j, &ids, pSchema[j].bytes, pSchema[j].type, pSchema[j].name);
      }
    }

  } else if (pItem->pNode->nSQLOptr == TK_ID) {  // simple column projection query
    int32_t numOfAllCols = pCmd->pMeterMeta->numOfColumns + pCmd->pMeterMeta->numOfTags;
    int32_t idx = getColumnIndexByName(&pItem->pNode->colInfo, pSchema, numOfAllCols);
    if (idx == -1) {
      if (strncmp(pItem->pNode->colInfo.z, TSQL_TBNAME_L, 6) == 0 && pItem->pNode->colInfo.n == 6) {
        SSqlExpr* pExpr =
            tscSqlExprInsert(pCmd, startPos, TSDB_FUNC_TAGPRJ, -1, TSDB_DATA_TYPE_BINARY, TSDB_METER_NAME_LEN);

        SColumnList ids = {.numOfCols = 1, .ids = {idx}};
        insertResultField(pCmd, startPos, &ids, TSDB_METER_NAME_LEN, TSDB_DATA_TYPE_BINARY, TSQL_TBNAME_L);

        pCmd->metricQuery = 1;
        addRequiredTagColumn(pCmd, -1);
        pExpr->colInfo.isTag = true;
      } else {
        const char* msg = "invalid column name";
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }
    } else {
      if (isMetric) {
        setProjExprForMetricQuery(pCmd, startPos, idx);
      } else {
        tscSqlExprInsert(pCmd, startPos, TSDB_FUNC_PRJ, idx, pSchema[idx].type, pSchema[idx].bytes);
      }

      char*       colName = (pItem->aliasName == NULL) ? pSchema[idx].name : pItem->aliasName;
      SColumnList ids = {.numOfCols = 1, .ids = {idx}};

      if (idx >= pCmd->pMeterMeta->numOfColumns || idx == -1) {
        ids.numOfCols = 0;
      }

      insertResultField(pCmd, startPos, &ids, pSchema[idx].bytes, pSchema[idx].type, colName);
    }
  } else {
    return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setExprInfoForFunctions(SSqlCmd* pCmd, SSchema* pSchema, int32_t functionID, char* aliasName,
                                       int32_t resColIdx, int32_t idx) {
  int16_t type = 0;
  int16_t bytes = 0;

  char columnName[TSDB_COL_NAME_LEN] = {0};
  const char* msg1 = "not support column types";

  if (functionID == TSDB_FUNC_SPREAD) {
    if (pSchema[idx].type == TSDB_DATA_TYPE_BINARY || pSchema[idx].type == TSDB_DATA_TYPE_NCHAR ||
        pSchema[idx].type == TSDB_DATA_TYPE_BOOL) {
      setErrMsg(pCmd, msg1);
      return -1;
    } else {
      type = TSDB_DATA_TYPE_DOUBLE;
      bytes = tDataTypeDesc[type].nSize;
    }
  } else {
    type = pSchema[idx].type;
    bytes = pSchema[idx].bytes;
  }

  if (aliasName != NULL) {
    strcpy(columnName, aliasName);
  } else {
    getRevisedName(columnName, functionID, TSDB_COL_NAME_LEN, pSchema[idx].name);
  }

  tscSqlExprInsert(pCmd, resColIdx, functionID, idx, type, bytes);

  /* for point interpolation/last_row query, we need the timestamp column to be
   * loaded */
  if (functionID == TSDB_FUNC_INTERP || functionID == TSDB_FUNC_LAST_ROW) {
    tscColumnInfoInsert(pCmd, PRIMARYKEY_TIMESTAMP_COL_INDEX);
  }

  SColumnList ids = {.numOfCols = 1, .ids = {idx}};
  insertResultField(pCmd, resColIdx, &ids, bytes, type, columnName);

  return TSDB_CODE_SUCCESS;
}

int32_t addExprAndResultField(SSqlCmd* pCmd, int32_t colIdx, tSQLExprItem* pItem) {
  int32_t  optr = pItem->pNode->nSQLOptr;
  SSchema* pSchema = tsGetSchema(pCmd->pMeterMeta);
  int32_t  numOfAddedColumn = 1;

  const char* msg = "invalid parameters";
  const char* msg1 = "not support column types";
  const char* msg3 = "illegal column name";
  const char* msg5 = "parameter is out of range [0, 100]";

  switch (optr) {
    case TK_COUNT: {
      if (pItem->pNode->pParam != NULL && pItem->pNode->pParam->nExpr != 1) {
        /* more than one parameter for count() function */
        setErrMsg(pCmd, msg);
        return -1;
      }

      int16_t functionID = 0;
      if (changeFunctionID(optr, &functionID) != TSDB_CODE_SUCCESS) {
        return -1;
      }

      int32_t columnId = 0;

      if (pItem->pNode->pParam != NULL) {
        SSQLToken* pToken = &pItem->pNode->pParam->a[0].pNode->colInfo;
        if (pToken->z == NULL || pToken->n == 0) {
          setErrMsg(pCmd, msg3);
          return -1;
        }

        /* count the number of meters created according to the metric */
        if (strncmp(pToken->z, "tbname", 6) == 0 && pToken->n == 6) {
          tscSqlExprInsert(pCmd, colIdx, functionID, -1, TSDB_DATA_TYPE_BIGINT,
                           tDataTypeDesc[TSDB_DATA_TYPE_BIGINT].nSize);
        } else {
          columnId = getColumnIndexByName(pToken, pSchema, pCmd->pMeterMeta->numOfColumns);
          if (columnId < 0) {  // invalid column name
            setErrMsg(pCmd, msg3);
            return -1;
          }

          tscSqlExprInsert(pCmd, colIdx, functionID, columnId, TSDB_DATA_TYPE_BIGINT,
                           tDataTypeDesc[TSDB_DATA_TYPE_BIGINT].nSize);
        }
      } else {
        /* count(*) is equalled to count(primary_timestamp_key) */
        tscSqlExprInsert(pCmd, colIdx, functionID, PRIMARYKEY_TIMESTAMP_COL_INDEX, TSDB_DATA_TYPE_BIGINT,
                         tDataTypeDesc[TSDB_DATA_TYPE_BIGINT].nSize);
      }

      char columnName[TSDB_COL_NAME_LEN] = {0};
      getColumnName(pItem, columnName, TSDB_COL_NAME_LEN);

      // count always use the primary timestamp key column, which is 0.
      SColumnList ids = {.numOfCols = 1, .ids = {columnId}};
      insertResultField(pCmd, colIdx, &ids, sizeof(int64_t), TSDB_DATA_TYPE_BIGINT, columnName);
      return numOfAddedColumn;
    }
    case TK_SUM:
    case TK_AVG:
    case TK_WAVG:
    case TK_MIN:
    case TK_MAX:
    case TK_DIFF:
    case TK_STDDEV:
    case TK_LEASTSQUARES: {
      // 1. valid the number of parameters
      if (pItem->pNode->pParam == NULL || (optr != TK_LEASTSQUARES && pItem->pNode->pParam->nExpr != 1) ||
          (optr == TK_LEASTSQUARES && pItem->pNode->pParam->nExpr != 3)) {
        /* no parameters or more than one parameter for function */
        setErrMsg(pCmd, msg);
        return -1;
      }

      tSQLExprItem* pParamElem = &(pItem->pNode->pParam->a[0]);
      if (pParamElem->pNode->nSQLOptr != TK_ALL && pParamElem->pNode->nSQLOptr != TK_ID) {
        setErrMsg(pCmd, msg);
        return -1;
      }

      int32_t idx = getColumnIndexByName(&pParamElem->pNode->colInfo, pSchema, pCmd->pMeterMeta->numOfColumns);
      if (idx < 0) {  // invalid column name
        setErrMsg(pCmd, msg3);
        return -1;
      }

      // 2. check if sql function can be applied on this column data type
      int16_t colType = pSchema[idx].type;
      if (colType == TSDB_DATA_TYPE_BOOL || colType >= TSDB_DATA_TYPE_BINARY) {
        setErrMsg(pCmd, msg1);
        return -1;
      }

      char columnName[TSDB_COL_NAME_LEN] = {0};
      getColumnName(pItem, columnName, TSDB_COL_NAME_LEN);

      int16_t resultType = 0;
      int16_t resultSize = 0;

      int16_t functionID = 0;
      if (changeFunctionID(optr, &functionID) != TSDB_CODE_SUCCESS) {
        return -1;
      }

      getResultInfo(pSchema[idx].type, pSchema[idx].bytes, functionID, 0, &resultType, &resultSize);

      if (optr == TK_DIFF) {
        // set the first column ts for diff query
        colIdx += 1;
        tscSqlExprInsert(pCmd, 0, TSDB_FUNC_TS_DUMMY, 0, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE);

        SColumnList ids = {.numOfCols = 1, .ids = {0}};
        insertResultField(pCmd, 0, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, aAggs[TSDB_FUNC_TS_DUMMY].aName);
      }

      SSqlExpr* pExpr = tscSqlExprInsert(pCmd, colIdx, functionID, idx, resultType, resultSize);

      if (optr == TK_LEASTSQUARES) {
        /* set the leastsquares parameters */
        char val[8] = {0};
        if (tVariantDump(&pParamElem[1].pNode->val, val, TSDB_DATA_TYPE_DOUBLE) < 0) {
          return TSDB_CODE_INVALID_SQL;
        }

        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double));

        memset(val, 0, tListLen(val));
        if (tVariantDump(&pParamElem[2].pNode->val, val, TSDB_DATA_TYPE_DOUBLE) < 0) {
          return TSDB_CODE_INVALID_SQL;
        }

        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
      }

      SColumnList ids = {.numOfCols = 1, .ids = {idx}};
      insertResultField(pCmd, colIdx, &ids, pExpr->resBytes, pExpr->resType, columnName);

      return numOfAddedColumn;
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
          setErrMsg(pCmd, msg3);
          return -1;
        }

        /* in first/last function, multiple columns can be add to resultset */

        for (int32_t i = 0; i < pItem->pNode->pParam->nExpr; ++i) {
          tSQLExprItem* pParamElem = &(pItem->pNode->pParam->a[i]);
          if (pParamElem->pNode->nSQLOptr != TK_ALL && pParamElem->pNode->nSQLOptr != TK_ID) {
            setErrMsg(pCmd, msg3);
            return -1;
          }

          int32_t idx = getColumnIndexByName(&pParamElem->pNode->colInfo, pSchema, pCmd->pMeterMeta->numOfColumns);
          if (idx == -1) {
            return -1;
          }

          if (setExprInfoForFunctions(pCmd, pSchema, functionID, pItem->aliasName, colIdx + i, idx) != 0) {
            return -1;
          }
        }

        return pItem->pNode->pParam->nExpr;
      } else {
        for (int32_t i = 0; i < pCmd->pMeterMeta->numOfColumns; ++i) {
          if (setExprInfoForFunctions(pCmd, pSchema, functionID, pItem->aliasName, colIdx + i, i) != 0) {
            return -1;
          }
        }

        return pCmd->pMeterMeta->numOfColumns;
      }
    }
    case TK_TOP:
    case TK_BOTTOM:
    case TK_PERCENTILE:
    case TK_APERCENTILE: {
      // 1. valid the number of parameters
      if (pItem->pNode->pParam == NULL || pItem->pNode->pParam->nExpr != 2) {
        /* no parameters or more than one parameter for function */
        setErrMsg(pCmd, msg);
        return -1;
      }

      tSQLExprItem* pParamElem = &(pItem->pNode->pParam->a[0]);
      if (pParamElem->pNode->nSQLOptr != TK_ID) {
        setErrMsg(pCmd, msg);
      }

      char columnName[TSDB_COL_NAME_LEN] = {0};
      getColumnName(pItem, columnName, TSDB_COL_NAME_LEN);

      int32_t idx = getColumnIndexByName(&pParamElem->pNode->colInfo, pSchema, pCmd->pMeterMeta->numOfColumns);
      if (idx == -1) {
        return -1;
      }

      // 2. valid the column type
      int16_t colType = pSchema[idx].type;
      if (colType == TSDB_DATA_TYPE_BOOL || colType >= TSDB_DATA_TYPE_BINARY) {
        setErrMsg(pCmd, msg1);
        return -1;
      }

      // 3. valid the parameters
      if (pParamElem[1].pNode->nSQLOptr == TK_ID) {
        setErrMsg(pCmd, msg);
        return -1;
      }

      tVariant* pVariant = &pParamElem[1].pNode->val;

      int8_t  resultType = pSchema[idx].type;
      int16_t resultSize = pSchema[idx].bytes;

      char val[8] = {0};
      if (optr == TK_PERCENTILE || optr == TK_APERCENTILE) {
        tVariantDump(pVariant, val, TSDB_DATA_TYPE_DOUBLE);

        double dp = *((double*)val);
        if (dp < 0 || dp > 100) {  // todo use macro
          setErrMsg(pCmd, msg5);
          return -1;
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
          return -1;
        }

        SSqlExpr* pExpr = tscSqlExprInsert(pCmd, colIdx, functionId, idx, resultType, resultSize);
        addExprParams(pExpr, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
      } else {
        tVariantDump(pVariant, val, TSDB_DATA_TYPE_BIGINT);

        int64_t nTop = *((int32_t*)val);
        if (nTop <= 0 || nTop > 100) {  // todo use macro
          return -1;
        }

        int16_t functionId = 0;
        if (changeFunctionID(optr, &functionId) != TSDB_CODE_SUCCESS) {
          return -1;
        }
        // set the first column ts for top/bottom query
        tscSqlExprInsert(pCmd, 0, TSDB_FUNC_TS, 0, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE);
        SColumnList ids = {.numOfCols = 1, .ids = {0}};
        insertResultField(pCmd, 0, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, aAggs[TSDB_FUNC_TS].aName);

        colIdx += 1;  // the first column is ts
        numOfAddedColumn += 1;

        SSqlExpr* pExpr = tscSqlExprInsert(pCmd, colIdx, functionId, idx, resultType, resultSize);
        addExprParams(pExpr, val, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
      }

      SColumnList ids = {.numOfCols = 1, .ids = {idx}};
      insertResultField(pCmd, colIdx, &ids, resultSize, resultType, columnName);
      return numOfAddedColumn;
    }
    default:
      return -1;
  }
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

int32_t getColumnIndexByName(SSQLToken* pToken, SSchema* pSchema, int32_t numOfCols) {
  if (pToken->z == NULL || pToken->n == 0) {
    return -1;
  }

  char* r = strnchr(pToken->z, '.', pToken->n, false);
  if (r != NULL) {
    r += 1;

    pToken->n -= (r - pToken->z);
    pToken->z = r;
  }
  if (strncasecmp(pToken->z, "_c0", pToken->n) == 0) return 0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    if (pToken->n != strlen(pSchema[i].name)) continue;
    if (strncasecmp(pSchema[i].name, pToken->z, pToken->n) == 0) return i;
  }

  return -1;
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
    case TK_WAVG:
      *functionId = TSDB_FUNC_WAVG;
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
  SSqlCmd* pCmd = &pSql->cmd;
  pCmd->command = TSDB_SQL_SHOW;
  int8_t type = pInfo->sqlType;

  const char* msg = "database name too long";
  const char* msg1 = "invalid database name";
  const char* msg2 = "pattern filter string too long";

  switch (type) {
    case SHOW_VGROUPS:
      pCmd->type = TSDB_MGMT_TABLE_VGROUP;
      break;
    case SHOW_TABLES:
      pCmd->type = TSDB_MGMT_TABLE_TABLE;
      break;
    case SHOW_STABLES:
      pCmd->type = TSDB_MGMT_TABLE_METRIC;
      break;

    case SHOW_DATABASES:
      pCmd->type = TSDB_MGMT_TABLE_DB;
      break;
    case SHOW_DNODES:
      pCmd->type = TSDB_MGMT_TABLE_PNODE;
      break;
    case SHOW_USERS:
      pCmd->type = TSDB_MGMT_TABLE_USER;
      break;
    case SHOW_CONNECTIONS:
      pCmd->type = TSDB_MGMT_TABLE_CONNS;
      break;
    case SHOW_QUERIES:
      pCmd->type = TSDB_MGMT_TABLE_QUERIES;
      break;
    case SHOW_STREAMS:
      pCmd->type = TSDB_MGMT_TABLE_STREAMS;
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
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      if (pDbPrefixToken->n > 0 && tscValidateName(pDbPrefixToken) != TSDB_CODE_SUCCESS) {
        setErrMsg(pCmd, msg1);
        return TSDB_CODE_INVALID_SQL;
      }

      int32_t ret = 0;
      if (pDbPrefixToken->n > 0) {  // has db prefix
        if (pCmd->tagCond.allocSize < TSDB_MAX_TAGS_LEN) {
          pCmd->tagCond.pData = realloc(pCmd->tagCond.pData, TSDB_MAX_TAGS_LEN);
          pCmd->tagCond.allocSize = TSDB_MAX_TAGS_LEN;
        }
        ret = setObjFullName(pCmd->tagCond.pData, getAccountId(pSql), pDbPrefixToken, NULL, &pCmd->tagCond.len);
      } else {
        ret = setObjFullName(pCmd->name, getAccountId(pSql), NULL, NULL, NULL);
      }

      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      if (type != SHOW_VGROUPS && pInfo->pDCLInfo->nTokens == 2) {
        // set the like conds for show tables
        SSQLToken* likeToken = &pInfo->pDCLInfo->a[1];
        strncpy(pCmd->payload, likeToken->z, likeToken->n);
        strdequote(pCmd->payload);
        pCmd->payloadLen = strlen(pCmd->payload);

        if (pCmd->payloadLen > TSDB_METER_NAME_LEN) {
          setErrMsg(pCmd, msg2);
          return TSDB_CODE_INVALID_SQL;  // wildcard is too long
        }
      }
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
    setErrMsg(pCmd, msg);
    return TSDB_CODE_INVALID_SQL;
  }

  int32_t port = strtol(portStr, NULL, 10);
  if (port <= 0 || port > 65535) {
    memset(pCmd->payload, 0, tListLen(pCmd->payload));

    const char* msg = "invalid port";
    setErrMsg(pCmd, msg);
    return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

void setErrMsg(SSqlCmd* pCmd, const char* pzErrMsg) {
  strncpy(pCmd->payload, pzErrMsg, pCmd->allocSize);
  pCmd->payload[pCmd->allocSize - 1] = 0;
}

bool validateIpAddress(char* ip) {
  in_addr_t ipAddr = inet_addr(ip);
  return (ipAddr != 0) && (ipAddr != 0xffffffff);
}

void tscTansformSQLFunctionForMetricQuery(SSqlCmd* pCmd) {
  if (pCmd->pMeterMeta == NULL || !UTIL_METER_IS_METRIC(pCmd)) {
    return;
  }

  assert(pCmd->pMeterMeta->numOfTags >= 0);

  int16_t bytes = 0;
  int16_t type = 0;

  for (int32_t k = 0; k < pCmd->fieldsInfo.numOfOutputCols; ++k) {
    SSqlExpr*   pExpr = tscSqlExprGet(pCmd, k);
    TAOS_FIELD* pField = tscFieldInfoGetField(pCmd, k);

    int16_t functionId = aAggs[pExpr->sqlFuncId].stableFuncId;

    if (functionId >= TSDB_FUNC_SUM_DST && functionId <= TSDB_FUNC_APERCT_DST) {
      getResultInfo(pField->type, pField->bytes, functionId, pExpr->param[0].i64Key, &type, &bytes);
      tscSqlExprUpdate(pCmd, k, functionId, pExpr->colInfo.colIdx, TSDB_DATA_TYPE_BINARY, bytes);
    }
  }

  tscFieldInfoRenewOffsetForInterResult(pCmd);
}

/* transfer the field-info back to original input format */
void tscRestoreSQLFunctionForMetricQuery(SSqlCmd* pCmd) {
  if (!UTIL_METER_IS_METRIC(pCmd)) {
    return;
  }

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr*   pExpr = tscSqlExprGet(pCmd, i);
    TAOS_FIELD* pField = tscFieldInfoGetField(pCmd, i);

    if (pExpr->sqlFuncId >= TSDB_FUNC_SUM_DST && pExpr->sqlFuncId <= TSDB_FUNC_WAVG_DST) {
      pExpr->resBytes = pField->bytes;
      pExpr->resType = pField->type;
    }
  }
}

bool onlyQueryMetricTags(SSqlCmd* pCmd) {
  assert(pCmd->metricQuery == 1);

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    if (tscSqlExprGet(pCmd, i)->sqlFuncId != TSDB_FUNC_TAGPRJ) {  // 18 == "tagprj" function
      return false;
    }
  }

  return true;
}

bool hasUnsupportFunctionsForMetricQuery(SSqlCmd* pCmd) {
  // filter sql function not supported by metric query yet.
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    int32_t functionId = tscSqlExprGet(pCmd, i)->sqlFuncId;
    if ((aAggs[functionId].nStatus & TSDB_FUNCSTATE_METRIC) == 0) {
      return true;
    }
  }
  return false;
}

static bool functionCompatibleCheck(SSqlCmd* pCmd) {
  int32_t startIdx = 0;
  int32_t functionID = tscSqlExprGet(pCmd, startIdx)->sqlFuncId;
  if (functionID == TSDB_FUNC_TS || functionID == TSDB_FUNC_TS_DUMMY) {
    startIdx++;  // ts function can be simultaneously used with any other
                 // functions.
  }

  int32_t nRetCount = funcCompatList[tscSqlExprGet(pCmd, startIdx)->sqlFuncId];

  // diff function cannot be executed with other function
  // arithmetic function can be executed with other arithmetic functions
  for (int32_t i = startIdx + 1; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
    if (funcCompatList[pExpr->sqlFuncId] != nRetCount) {
      return false;
    }
  }

  return true;
}

void updateTagColumnIndex(SSqlCmd* pCmd) {
  // update tags column index for group by tags
  for (int32_t i = 0; i < pCmd->groupbyExpr.numOfGroupbyCols; ++i) {
    int32_t index = pCmd->groupbyExpr.tagIndex[i];

    for (int32_t j = 0; j < pCmd->numOfReqTags; ++j) {
      int32_t tagColIndex = pCmd->tagColumnIndex[j];
      if (tagColIndex == index) {
        pCmd->groupbyExpr.tagIndex[i] = j;
        break;
      }
    }
  }

  // update tags column index for expression
  for (int32_t i = 0; i < pCmd->exprsInfo.numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);

    // not tags, continue
    if (!pExpr->colInfo.isTag) {
      continue;
    }

    for (int32_t j = 0; j < pCmd->numOfReqTags; ++j) {
      if (pExpr->colInfo.colIdx == pCmd->tagColumnIndex[j]) {
        pExpr->colInfo.colIdx = j;
        break;
      }
    }
  }
}

int32_t setGroupByClause(SSqlCmd* pCmd, tVariantList* pList) {
  const char* msg1 = "too many columns in group by clause";
  const char* msg2 = "invalid column name in group by clause";
  const char* msg3 = "functions are not available in group by query";
  const char* msg4 = "group by only available for STable query";

  if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {
    if (pList == NULL) {
      return TSDB_CODE_SUCCESS;
    } else {
      setErrMsg(pCmd, msg4);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  if (pList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pCmd->groupbyExpr.numOfGroupbyCols = pList->nExpr;
  if (pList->nExpr > TSDB_MAX_TAGS) {
    setErrMsg(pCmd, msg1);
    return TSDB_CODE_INVALID_SQL;
  }

  SMeterMeta* pMeterMeta = pCmd->pMeterMeta;
  SSchema*    pSchema = tsGetSchema(pMeterMeta);

  int32_t numOfReqTags = 0;

  for (int32_t i = 0; i < pList->nExpr; ++i) {
    tVariant* pVar = &pList->a[i].pVar;
    SSQLToken token = {pVar->nLen, pVar->nType, pVar->pz};

    int32_t colIdx = 0;
    int16_t type = 0;
    int16_t bytes = 0;
    char*   name = NULL;

    /* group by tbname*/
    if (strncasecmp(pVar->pz, TSQL_TBNAME_L, pVar->nLen) == 0) {
      colIdx = -1;
      type = TSDB_DATA_TYPE_BINARY;
      bytes = TSDB_METER_NAME_LEN;
      name = TSQL_TBNAME_L;
    } else {
      colIdx = getColumnIndexByName(&token, pSchema, pMeterMeta->numOfTags + pMeterMeta->numOfColumns);
      if (colIdx < pMeterMeta->numOfColumns || colIdx > TSDB_MAX_TAGS + pMeterMeta->numOfColumns) {
        setErrMsg(pCmd, msg2);
        return TSDB_CODE_INVALID_SQL;
      }

      type = pSchema[colIdx].type;
      bytes = pSchema[colIdx].bytes;
      name = pSchema[colIdx].name;
      numOfReqTags++;
    }

    SSqlExpr* pExpr = tscSqlExprInsert(pCmd, pCmd->fieldsInfo.numOfOutputCols, TSDB_FUNC_TAG, colIdx, type, bytes);
    pExpr->colInfo.isTag = true;

    // NOTE: tag column does not add to source column list
    SColumnList ids = {0};
    insertResultField(pCmd, pCmd->fieldsInfo.numOfOutputCols, &ids, bytes, type, name);

    int32_t relIndex = 0;
    if (colIdx != -1) {
      relIndex = colIdx - pMeterMeta->numOfColumns;
    } else {  // tbname
      relIndex = colIdx;
    }

    pExpr->colInfo.colIdx = relIndex;
    pCmd->groupbyExpr.tagIndex[i] = relIndex;

    assert(pCmd->groupbyExpr.tagIndex[i] >= -1);
    addRequiredTagColumn(pCmd, pCmd->groupbyExpr.tagIndex[i]);
  }

  /*
   * check all query functions in selection clause, multi-output functions are not available in query function
   */
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    int32_t functId = tscSqlExprGet(pCmd, i)->sqlFuncId;
    if (IS_MULTIOUTPUT(aAggs[functId].nStatus) && functId != TSDB_FUNC_TOP_DST && functId != TSDB_FUNC_BOTTOM_DST &&
        functId != TSDB_FUNC_TOP && functId != TSDB_FUNC_BOTTOM) {
      setErrMsg(pCmd, msg3);
      return TSDB_CODE_INVALID_SQL;
    }

    if (functId == TSDB_FUNC_COUNT && tscSqlExprGet(pCmd, i)->colInfo.colIdx == -1) {
      setErrMsg(pCmd, msg3);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void setColumnOffsetValueInResultset(SSqlCmd* pCmd) {
  if (pCmd->metricQuery == 0) {
    tscFieldInfoCalOffset(pCmd);
  } else {
    tscFieldInfoRenewOffsetForInterResult(pCmd);
  }
}

static void setColumnFilterInfo(SSqlCmd* pCmd, SColumnBase* pColFilter, int32_t colIdx, tSQLExpr* pExpr) {
  tSQLExpr* pRight = pExpr->pRight;
  SSchema*  pSchema = tsGetSchema(pCmd->pMeterMeta);

  pColFilter->filterOn = 1;

  pColFilter->colIndex = colIdx;

  int16_t colType = pSchema[colIdx].type;
  if ((colType >= TSDB_DATA_TYPE_TINYINT && colType <= TSDB_DATA_TYPE_BIGINT) || colType == TSDB_DATA_TYPE_TIMESTAMP) {
    colType = TSDB_DATA_TYPE_BIGINT;
  } else if (colType == TSDB_DATA_TYPE_FLOAT || colType == TSDB_DATA_TYPE_DOUBLE) {
    colType = TSDB_DATA_TYPE_DOUBLE;
  }

  if (pExpr->nSQLOptr == TK_LE || pExpr->nSQLOptr == TK_LT) {
    tVariantDump(&pRight->val, (char*)&pColFilter->upperBndd, colType);
  } else {  // TK_GT,TK_GE,TK_EQ,TK_NE are based on the pColFilter->lowerBndd
    if (colType == TSDB_DATA_TYPE_BINARY) {
      pColFilter->pz = (int64_t)malloc(pRight->val.nLen + 1);
      pColFilter->len = pRight->val.nLen;

      tVariantDump(&pRight->val, (char*)pColFilter->pz, colType);
      ((char*)pColFilter->pz)[pColFilter->len] = 0;
    } else {
      tVariantDump(&pRight->val, (char*)&pColFilter->lowerBndd, colType);
    }
  }

  switch (pExpr->nSQLOptr) {
    case TK_LE:
      pColFilter->upperRelOptr = TSDB_RELATION_LESS_EQUAL;
      break;
    case TK_LT:
      pColFilter->upperRelOptr = TSDB_RELATION_LESS;
      break;
    case TK_GT:
      pColFilter->lowerRelOptr = TSDB_RELATION_LARGE;
      break;
    case TK_GE:
      pColFilter->lowerRelOptr = TSDB_RELATION_LARGE_EQUAL;
      break;
    case TK_EQ:
      pColFilter->lowerRelOptr = TSDB_RELATION_EQUAL;
      break;
    case TK_NE:
      pColFilter->lowerRelOptr = TSDB_RELATION_NOT_EQUAL;
      break;
    case TK_LIKE:
      pColFilter->lowerRelOptr = TSDB_RELATION_LIKE;
      break;
  }
}

static int32_t getTimeRange(int64_t* stime, int64_t* etime, tSQLExpr* pRight, int32_t optr, int16_t precision);

static int32_t exprToString(tSQLExpr* pExpr, char** exprString, SColumnIdList* pIdList) {
  if (pExpr->nSQLOptr == TK_ID) {  // column name
    strncpy(*exprString, pExpr->colInfo.z, pExpr->colInfo.n);
    *exprString += pExpr->colInfo.n;

    if (pIdList) {
      bool validColumn = false;
      // record and check the column name
      for (int32_t i = 0; i < pIdList->numOfCols; ++i) {
        int32_t len = strlen(pIdList->pSchema[i].name);

        if (pExpr->colInfo.n == len && strncasecmp(pExpr->colInfo.z, pIdList->pSchema[i].name, len) == 0) {
          pIdList->ids[pIdList->numOfRecordedCols++] = (int16_t)i;
          validColumn = true;
          break;
        }
      }

      if (!validColumn) {
        return TSDB_CODE_INVALID_SQL;
      }
    }
  } else if (pExpr->nSQLOptr >= TK_BOOL && pExpr->nSQLOptr <= TK_STRING) {  // value
    *exprString += tVariantToString(&pExpr->val, *exprString);
  } else {
    return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t optrToString(tSQLExpr* pExpr, char** exprString) {
  char le[] = "<=";
  char ge[] = ">=";
  char ne[] = "<>";
  char likeOptr[] = "LIKE";

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

static int32_t createTableNameList(tSQLExpr* pExpr, char** queryStr) {
  tSQLExprList* pList = pExpr->pParam;
  if (pList->nExpr <= 0) {
    return TSDB_CODE_INVALID_SQL;
  }

  int32_t len = 0;
  for (int32_t i = 0; i < pList->nExpr; ++i) {
    tSQLExpr* pSub = pList->a[i].pNode;
    strncpy(*queryStr + len, pSub->val.pz, pSub->val.nLen);

    len += pSub->val.nLen;
    (*queryStr)[len++] = ',';

    if (pSub->val.nLen <= 0 || pSub->val.nLen > TSDB_METER_NAME_LEN) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  *queryStr += len;

  return TSDB_CODE_SUCCESS;
}

static bool isTbnameToken(SSQLToken* token) {
  return (strncasecmp(TSQL_TBNAME_L, token->z, token->n) == 0 && token->n == strlen(TSQL_TBNAME_L));
}

static int32_t buildTagQueryCondString(SSqlCmd* pCmd, tSQLExpr* pExpr, char** queryStr) {
  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;

  const char* msg0 = "invalid table name list";
  const char* msg1 = "like operation is not allowed on numeric tags";
  const char* msg2 = "in and query condition cannot be mixed up";
  STagCond* pCond = &pCmd->tagCond;

  if (pExpr->nSQLOptr == TK_IN && pRight->nSQLOptr == TK_SET) {
    /* table name array list, invoke another routine */
    if (pCond->type == TSQL_STABLE_QTYPE_COND) {
      setErrMsg(pCmd, msg2);
      return TSDB_CODE_INVALID_SQL;
    }

    pCond->type = TSQL_STABLE_QTYPE_SET;

    if (!isTbnameToken(&pLeft->colInfo)) {
      return TSDB_CODE_INVALID_SQL;
    }

    int32_t ret = createTableNameList(pRight, queryStr);
    if (ret != TSDB_CODE_SUCCESS) {
      setErrMsg(pCmd, msg0);
    }
    return ret;
  }

  // already use IN predicates
  if (pCond->type == TSQL_STABLE_QTYPE_SET) {
    setErrMsg(pCmd, msg2);
    return TSDB_CODE_INVALID_SQL;
  } else {
    pCond->type = TSQL_STABLE_QTYPE_COND;
  }

  *(*queryStr) = '(';
  *queryStr += 1;

  exprToString(pLeft, queryStr, NULL);
  if (optrToString(pExpr, queryStr) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }

  /* pattern string is too long */
  if (pExpr->nSQLOptr == TK_LIKE) {
    if (pRight->val.nLen > TSDB_PATTERN_STRING_MAX_LEN) {
      return TSDB_CODE_INVALID_SQL;
    }

    SSchema* pSchema = tsGetSchema(pCmd->pMeterMeta);

    int32_t numOfCols = pCmd->pMeterMeta->numOfColumns;
    int32_t numOfTags = pCmd->pMeterMeta->numOfTags;

    int32_t colIdx = getColumnIndexByName(&pLeft->colInfo, pSchema, numOfCols + numOfTags);
    if ((!isTbnameToken(&pLeft->colInfo)) && pSchema[colIdx].type != TSDB_DATA_TYPE_BINARY &&
        pSchema[colIdx].type != TSDB_DATA_TYPE_NCHAR) {
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  exprToString(pRight, queryStr, NULL);

  *(*queryStr) = ')';
  *queryStr += 1;

  return TSDB_CODE_SUCCESS;
}

// todo error handle / such as and /or mixed with +/-/*/
int32_t buildArithmeticExprString(tSQLExpr* pExpr, char** exprString, SColumnIdList* colIdList) {
  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;

  *(*exprString) = '(';
  *exprString += 1;

  if (pLeft->nSQLOptr >= TK_PLUS && pLeft->nSQLOptr <= TK_REM) {
    buildArithmeticExprString(pLeft, exprString, colIdList);
  } else {
    int32_t ret = exprToString(pLeft, exprString, colIdList);
    if (ret != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  optrToString(pExpr, exprString);

  if (pRight->nSQLOptr >= TK_PLUS && pRight->nSQLOptr <= TK_REM) {
    buildArithmeticExprString(pRight, exprString, colIdList);
  } else {
    int32_t ret = exprToString(pRight, exprString, colIdList);
    if (ret != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }
  }

  *(*exprString) = ')';
  *exprString += 1;

  return TSDB_CODE_SUCCESS;
}

static int32_t validateSQLExpr(tSQLExpr* pExpr, SSchema* pSchema, int32_t numOfCols) {
  if (pExpr->nSQLOptr == TK_ID) {
    bool validColumnName = false;
    for (int32_t i = 0; i < numOfCols; ++i) {
      if (strncasecmp(pExpr->colInfo.z, pSchema[i].name, pExpr->colInfo.n) == 0 &&
          pExpr->colInfo.n == strlen(pSchema[i].name)) {
        if (pSchema[i].type < TSDB_DATA_TYPE_TINYINT || pSchema[i].type > TSDB_DATA_TYPE_DOUBLE) {
          return TSDB_CODE_INVALID_SQL;
        }
        validColumnName = true;
      }
    }

    if (!validColumnName) {
      return TSDB_CODE_INVALID_SQL;
    }

  } else if (pExpr->nSQLOptr == TK_FLOAT && (isnan(pExpr->val.dKey) || isinf(pExpr->val.dKey))) {
    return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateArithmeticSQLExpr(tSQLExpr* pExpr, SSchema* pSchema, int32_t numOfCols) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSQLExpr* pLeft = pExpr->pLeft;
  if (pLeft->nSQLOptr >= TK_PLUS && pLeft->nSQLOptr <= TK_REM) {
    int32_t ret = validateArithmeticSQLExpr(pLeft, pSchema, numOfCols);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateSQLExpr(pLeft, pSchema, numOfCols);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  tSQLExpr* pRight = pExpr->pRight;
  if (pRight->nSQLOptr >= TK_PLUS && pRight->nSQLOptr <= TK_REM) {
    int32_t ret = validateArithmeticSQLExpr(pRight, pSchema, numOfCols);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateSQLExpr(pRight, pSchema, numOfCols);
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
   * 1. columnA = columnB
   * 2. count(*) > 12
   * 3. sum(columnA) > sum(columnB)
   * 4. 4 < 5,  'ABC'>'abc'
   *
   * However, columnA < 4+12 is valid
   */
  if ((pLeft->nSQLOptr == TK_ID && pRight->nSQLOptr == TK_ID) ||
      (pLeft->nSQLOptr >= TK_COUNT && pLeft->nSQLOptr <= TK_WAVG) ||
      (pRight->nSQLOptr >= TK_COUNT && pRight->nSQLOptr <= TK_WAVG) ||
      (pLeft->nSQLOptr >= TK_BOOL && pLeft->nSQLOptr <= TK_BINARY && pRight->nSQLOptr >= TK_BOOL &&
       pRight->nSQLOptr <= TK_BINARY)) {
    return false;
  }

  return true;
}

static int32_t getColumnFilterInfo(SSqlCmd* pCmd, int32_t colIdx, tSQLExpr* pExpr) {
  SMeterMeta* pMeterMeta = pCmd->pMeterMeta;
  SSchema*    pSchema = tsGetSchema(pMeterMeta);

  const char* msg = "nchar column not available for filter";
  const char* msg1 = "non binary column not support like operator";
  const char* msg2 = "binary column not support this operator";
  const char* msg3 = "column not support in operator";

  if (pSchema[colIdx].type == TSDB_DATA_TYPE_NCHAR) {
    setErrMsg(pCmd, msg);
    return -1;
  }

  if (pExpr->nSQLOptr == TK_IN) {
    setErrMsg(pCmd, msg3);
    return -1;
  }

  SColumnBase* pColFilter = tscColumnInfoInsert(pCmd, colIdx);

  pColFilter->filterOnBinary = ((pSchema[colIdx].type == TSDB_DATA_TYPE_BINARY) ? 1 : 0);

  if (pColFilter->filterOnBinary) {
    if (pExpr->nSQLOptr != TK_EQ && pExpr->nSQLOptr != TK_NE && pExpr->nSQLOptr != TK_LIKE) {
      setErrMsg(pCmd, msg2);
      return TSDB_CODE_INVALID_SQL;
    }
  } else {
    if (pExpr->nSQLOptr == TK_LIKE) {
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  setColumnFilterInfo(pCmd, pColFilter, colIdx, pExpr);
  return TSDB_CODE_SUCCESS;
}

static int32_t handleExprInQueryCond(SSqlCmd* pCmd, bool* queryTimeRangeIsSet, char** queryStr, int64_t* stime,
                                     int64_t* etime, tSQLExpr* pExpr) {
  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;

  SMeterMeta* pMeterMeta = pCmd->pMeterMeta;
  SSchema*    pSchema = tsGetSchema(pMeterMeta);

  int32_t numOfCols = pMeterMeta->numOfColumns;
  int32_t numOfTags = pMeterMeta->numOfTags;

  const char* msg = "meter query cannot use tags filter";
  const char* msg1 = "illegal column name";
  const char* msg2 = "invalid timestamp";

  int32_t colIdx = getColumnIndexByName(&pLeft->colInfo, pSchema, numOfCols + numOfTags);
  bool    istbname = isTbnameToken(&pLeft->colInfo);

  if (colIdx < 0 && (!istbname)) {
    setErrMsg(pCmd, msg1);
    return TSDB_CODE_INVALID_SQL;
  }

  if (colIdx == 0) {  // query on time range
    *queryTimeRangeIsSet = true;
    if (getTimeRange(stime, etime, pRight, pExpr->nSQLOptr, pMeterMeta->precision) != TSDB_CODE_SUCCESS) {
      setErrMsg(pCmd, msg2);
      return TSDB_CODE_INVALID_SQL;
    }
  } else if (colIdx >= numOfCols || istbname) {  // query on tags
    if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {
      setErrMsg(pCmd, msg);
      return TSDB_CODE_INVALID_SQL;
    }
    return buildTagQueryCondString(pCmd, pExpr, queryStr);
  } else {  // query on other columns
    return getColumnFilterInfo(pCmd, colIdx, pExpr);
  }

  return TSDB_CODE_SUCCESS;
}

static void insertLeftParentheses(char** queryStr, char* p) {
  int32_t len = (*queryStr - p);
  memmove(p + 1, p, len);
  p[0] = '(';
  *queryStr += 1;
}

static void removeLeftParentheses(char** queryStr, char* p) {
  // remove the left parentheses
  memmove(p, p + 1, *queryStr - p - 1);
  *queryStr -= 1;
}

int32_t getQueryCondExprImpl(SSqlCmd* pCmd, tSQLExpr* pExpr, int64_t* stime, int64_t* etime, bool* queryTimeRangeIsSet,
                             char** queryStr) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;

  if (!isValidExpr(pLeft, pRight, pExpr->nSQLOptr)) {
    return TSDB_CODE_INVALID_SQL;
  }

  if (pExpr->nSQLOptr == TK_AND || pExpr->nSQLOptr == TK_OR) {
    int64_t stime1 = 0, etime1 = INT64_MAX;
    bool    tmRangeIsSet = false;

    char*   p = *queryStr;
    int32_t ret = getQueryCondExprImpl(pCmd, pExpr->pLeft, &stime1, &etime1, &tmRangeIsSet, queryStr);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    if (tmRangeIsSet) {
      *stime = stime1;
      *etime = etime1;

      *queryTimeRangeIsSet = true;
    }

    if (p == *queryStr) {
      /*
       * query on timestamp or filter on normal columns
       * no data serialize to string
       *
       * do nothing
       */
    } else {  // serialize relational operator for tag filter operation
      if (pCmd->tagCond.type == TSQL_STABLE_QTYPE_SET) {
        /* using id in clause, and/or is not needed */

      } else {
        assert(pCmd->tagCond.type == TSQL_STABLE_QTYPE_COND);
        insertLeftParentheses(queryStr, p);

        char*   optr = (pExpr->nSQLOptr == TK_AND) ? "and" : "or";
        int32_t len = (pExpr->nSQLOptr == TK_AND) ? 3 : 2;
        strcpy(*queryStr, optr);

        *queryStr += len;
      }
    }

    int64_t stime2 = 0, etime2 = INT64_MAX;
    tmRangeIsSet = false;
    char* p2 = *queryStr;
    ret = getQueryCondExprImpl(pCmd, pExpr->pRight, &stime2, &etime2, &tmRangeIsSet, queryStr);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    if (tmRangeIsSet) {
      *queryTimeRangeIsSet = true;
      if (pExpr->nSQLOptr == TK_AND) {
        *stime = stime2 > (*stime) ? stime2 : (*stime);
        *etime = etime2 < (*etime) ? etime2 : (*etime);

      } else {
        const char* msg1 = "not support multi-segments query time ranges";
        setErrMsg(pCmd, msg1);
        return TSDB_CODE_INVALID_SQL;
      }
    }

    if (p != *queryStr) {  // either the left and right hand side has tags
                           // filter
      if (p2 == *queryStr && p != p2 && pCmd->tagCond.type == TSQL_STABLE_QTYPE_COND) {
        /*
         * has no tags filter info on the right hand side
         * has filter on the left hand side
         *
         * rollback string
         */
        int32_t len = (pExpr->nSQLOptr == TK_AND) ? 3 : 2;
        *queryStr -= len;

        removeLeftParentheses(queryStr, p);
      } else if (p2 != *queryStr && p == p2) {
        // do nothing
      } else {
        if (pCmd->tagCond.type == TSQL_STABLE_QTYPE_COND) {
          *(*queryStr) = ')';
          *queryStr += 1;
        }
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  if (pLeft->nSQLOptr == TK_ID && (pRight->nSQLOptr == TK_INTEGER || pRight->nSQLOptr == TK_FLOAT ||
                                   pRight->nSQLOptr == TK_STRING || pRight->nSQLOptr == TK_BOOL)) {
    // do nothing
  } else if (pRight->nSQLOptr == TK_ID && (pLeft->nSQLOptr == TK_INTEGER || pLeft->nSQLOptr == TK_FLOAT ||
                                           pLeft->nSQLOptr == TK_STRING || pLeft->nSQLOptr == TK_BOOL)) {
    /*
     * exchange value of the left-handside and the value of the right-handside
     * to make sure that the value of filter expression always locates in right-handside and
     * the column-id is at the left hande side.
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

    tSQLExpr* pTmpExpr = pExpr->pLeft;
    pExpr->pLeft = pExpr->pRight;
    pExpr->pRight = pTmpExpr;
  }

  return handleExprInQueryCond(pCmd, queryTimeRangeIsSet, queryStr, stime, etime, pExpr);
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

static int32_t setMetersIDForMetricQuery(SSqlObj* pSql, char* tmpTagCondBuf) {
  SSqlCmd* pCmd = &pSql->cmd;
  const char* msg = "meter name too long";

  pCmd->tagCond.allocSize = 4096;
  pCmd->tagCond.pData = realloc(pCmd->tagCond.pData, pCmd->tagCond.allocSize);

  char db[TSDB_METER_ID_LEN] = {0};

  /* remove the duplicated input table names */
  int32_t num = 0;
  char**  segments = strsplit(tmpTagCondBuf, ",", &num);
  qsort(segments, num, POINTER_BYTES, tableNameCompar);

  int32_t j = 1;
  for (int32_t i = 1; i < num; ++i) {
    if (strcmp(segments[i], segments[i - 1]) != 0) {
      segments[j++] = segments[i];
    }
  }
  num = j;

  extractDBName(pCmd->name, db);
  SSQLToken tDB = {
      .z = db, .n = strlen(db), .type = TK_STRING,
  };

  char* acc = getAccountId(pSql);
  for (int32_t i = 0; i < num; ++i) {
    if (pCmd->tagCond.allocSize - pCmd->tagCond.len < (TSDB_METER_ID_LEN + 1)) {
      /* remain space is insufficient, buy more spaces */
      pCmd->tagCond.allocSize = (pCmd->tagCond.allocSize << 1);
      pCmd->tagCond.pData = realloc(pCmd->tagCond.pData, pCmd->tagCond.allocSize);
    }

    if (i >= 1) {
      pCmd->tagCond.pData[pCmd->tagCond.len++] = ',';
    }

    int32_t xlen = strlen(segments[i]);

    SSQLToken t = {.z = segments[i], .n = xlen, .type = TK_STRING};
    int32_t   ret = setObjFullName(pCmd->tagCond.pData + pCmd->tagCond.len, acc, &tDB, &t, &xlen);

    if (ret != TSDB_CODE_SUCCESS) {
      setErrMsg(pCmd, msg);
      tfree(segments);
      return ret;
    }

    pCmd->tagCond.len += xlen;
  }

  tfree(segments);
  return TSDB_CODE_SUCCESS;
}

static bool validateFilterExpr(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->colList.numOfCols; ++i) {
    SColumnBase* pColBase = &pCmd->colList.pColList[i];

    if (pColBase->filterOn > 0) {
      int32_t lowerOptr = pColBase->lowerRelOptr;
      int32_t upperOptr = pColBase->upperRelOptr;

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

int32_t buildQueryCond(SSqlObj* pSql, tSQLExpr* pExpr) {
  SSqlCmd* pCmd = &pSql->cmd;

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* msg1 = "invalid expression";
  const char* msg2 = "meter is not allowed";
  const char* msg3 = "invalid filter expression";

  tSQLExpr* pLeft = pExpr->pLeft;
  tSQLExpr* pRight = pExpr->pRight;
  if (pLeft == NULL || pRight == NULL || (pLeft->nSQLOptr == TK_ID && pRight->nSQLOptr == TK_ID)) {
    setErrMsg(pCmd, msg1);
    return TSDB_CODE_INVALID_SQL;
  }

  bool setTimeRange = false;

  /* tags query condition may be larger than 512bytes, therefore, we need to prepare enough large space */
  char  tmpTagCondBuf[TSDB_MAX_SQL_LEN] = {0};
  char* q = tmpTagCondBuf;

  pCmd->stime = 0;
  pCmd->etime = INT64_MAX;
  int32_t ret = getQueryCondExprImpl(pCmd, pExpr, &pCmd->stime, &pCmd->etime, &setTimeRange, &q);
  if (ret != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_INVALID_SQL;
  }

  // query condition for tags
  if (q == tmpTagCondBuf) {
    pCmd->tagCond.len = 0;
  } else {
    int32_t qlen = (q - tmpTagCondBuf) + 1;
    tmpTagCondBuf[qlen - 1] = 0;

    if (pCmd->tagCond.type == TSQL_STABLE_QTYPE_SET) {
      if (!UTIL_METER_IS_METRIC(pCmd)) {
        setErrMsg(pCmd, msg2);
        return TSDB_CODE_INVALID_SQL;
      }

      ret = setMetersIDForMetricQuery(pSql, tmpTagCondBuf);
    } else {
      if (pCmd->tagCond.allocSize < qlen + 1) {
        pCmd->tagCond.allocSize = qlen + 1;
        pCmd->tagCond.pData = realloc(pCmd->tagCond.pData, pCmd->tagCond.allocSize);
      }

      strcpy(pCmd->tagCond.pData, tmpTagCondBuf);
      pCmd->tagCond.len = qlen;  // plus one null-terminated symbol
    }

    pCmd->tagCond.pData[pCmd->tagCond.len] = 0;
  }

  if (!validateFilterExpr(pCmd)) {
    setErrMsg(pCmd, msg3);
    return TSDB_CODE_INVALID_SQL;
  }

  return ret;
}

int32_t getTimeRange(int64_t* stime, int64_t* etime, tSQLExpr* pRight, int32_t optr, int16_t timePrecision) {
  assert(pRight->nSQLOptr == TK_INTEGER || pRight->nSQLOptr == TK_FLOAT || pRight->nSQLOptr == TK_STRING ||
         pRight->nSQLOptr == TK_TIMESTAMP);

  int64_t val = 0;
  bool    parsed = false;
  if (pRight->val.nType == TSDB_DATA_TYPE_BINARY) {
    strdequote(pRight->val.pz);
    char* seg = strnchr(pRight->val.pz, '-', pRight->val.nLen, false);
    if (seg != NULL) {
      if (taosParseTime(pRight->val.pz, &val, pRight->val.nLen, TSDB_TIME_PRECISION_MICRO) == TSDB_CODE_SUCCESS) {
        parsed = true;
      } else {
        return TSDB_CODE_INVALID_SQL;
      }
    }
  } else if (pRight->nSQLOptr == TK_INTEGER && timePrecision == TSDB_TIME_PRECISION_MILLI) {
    /*
     * if the pRight->nSQLOptr == TK_INTEGER/TK_FLOAT, the value is adaptive, we
     * need the time precision of metermeta to transfer the value in MICROSECOND
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
  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    char* fieldName = tscFieldInfoGetField(pCmd, i)->name;
    for (int32_t j = 0; j < TSDB_COL_NAME_LEN && fieldName[j] != 0; ++j) {
      if (fieldName[j] == '(' || fieldName[j] == ')' || fieldName[j] == '*' || fieldName[j] == ',' ||
          fieldName[j] == '.' || fieldName[j] == '/' || fieldName[j] == '+' || fieldName[j] == '-' ||
          fieldName[j] == ' ') {
        fieldName[j] = '_';
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
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setFillPolicy(SSqlCmd* pCmd, SQuerySQL* pQuerySQL) {
  tVariantList*     pFillToken = pQuerySQL->fillType;
  tVariantListItem* pItem = &pFillToken->a[0];

  const int32_t START_INTERPO_COL_IDX = 1;
  const char* msg = "illegal value or data overflow";
  const char* msg1 = "value is expected";
  const char* msg2 = "invalid fill option";

  if (pItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
    setErrMsg(pCmd, msg2);
    return TSDB_CODE_INVALID_SQL;
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
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
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
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
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
    setErrMsg(pCmd, msg2);
    return TSDB_CODE_INVALID_SQL;
  }

  return TSDB_CODE_SUCCESS;
}

static void setDefaultOrderInfo(SSqlCmd* pCmd) {
  /* set default timestamp order information for all queries */
  pCmd->order.order = TSQL_SO_ASC;

  if (isTopBottomQuery(pCmd)) {
    pCmd->order.order = TSQL_SO_ASC;
    pCmd->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
  } else {
    pCmd->order.orderColId = -1;
  }

  /* for metric query, set default ascending order for group output */
  if (UTIL_METER_IS_METRIC(pCmd)) {
    pCmd->groupbyExpr.orderType = TSQL_SO_ASC;
  }
}

int32_t setOrderByClause(SSqlCmd* pCmd, SQuerySQL* pQuerySql, SSchema* pSchema, int32_t numOfCols) {
  const char* msg = "only support order by primary timestamp";
  const char* msg3 = "invalid column name";
  const char* msg5 = "only support order by primary timestamp and queried column";
  const char* msg6 = "only support order by primary timestamp and first tag in groupby clause";

  setDefaultOrderInfo(pCmd);

  if (pQuerySql->pSortOrder == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tVariantList* pSortorder = pQuerySql->pSortOrder;
  if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {
    if (pSortorder->nExpr > 1) {
      setErrMsg(pCmd, msg);
      return TSDB_CODE_INVALID_SQL;
    }
  } else {
    if (pSortorder->nExpr > 2) {
      setErrMsg(pCmd, msg6);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  // handle the first part of order by
  tVariant* pVar = &pSortorder->a[0].pVar;

  // e.g., order by 1 asc, return directly with out further check.
  if (pVar->nType >= TSDB_DATA_TYPE_TINYINT && pVar->nType <= TSDB_DATA_TYPE_BIGINT) {
    return TSDB_CODE_SUCCESS;
  }

  SSQLToken columnName = {pVar->nLen, pVar->nType, pVar->pz};

  if (UTIL_METER_IS_METRIC(pCmd)) {  // metric query
    SSchema* pTagSchema = tsGetTagSchema(pCmd->pMeterMeta);
    int32_t  columnIndex = getColumnIndexByName(&columnName, pTagSchema, pCmd->pMeterMeta->numOfTags);
    bool     orderByTags = false;
    bool     orderByTS = false;
    if (pCmd->groupbyExpr.tagIndex[0] == columnIndex) {
      if (columnIndex >= 0 || (strncasecmp(columnName.z, TSQL_TBNAME_L, 6) == 0)) {
        orderByTags = true;
      }
    }

    columnIndex = getColumnIndexByName(&columnName, pSchema, numOfCols);
    if (PRIMARYKEY_TIMESTAMP_COL_INDEX == columnIndex) {
      orderByTS = true;
    }

    if (!(orderByTags || orderByTS)) {
      setErrMsg(pCmd, msg6);
      return TSDB_CODE_INVALID_SQL;
    } else {
      assert(!(orderByTags && orderByTS));
    }

    if (pSortorder->nExpr == 1) {
      if (orderByTags) {
        pCmd->groupbyExpr.orderIdx = columnIndex;
        pCmd->groupbyExpr.orderType = pQuerySql->pSortOrder->a[0].sortOrder;
      } else {
        pCmd->order.order = pSortorder->a[0].sortOrder;
        pCmd->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }
    }

    if (pSortorder->nExpr == 2) {
      tVariant* pVar2 = &pSortorder->a[1].pVar;
      SSQLToken cname = {pVar2->nLen, pVar2->nType, pVar2->pz};
      columnIndex = getColumnIndexByName(&cname, pSchema, numOfCols);
      if (columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        setErrMsg(pCmd, msg5);
        return TSDB_CODE_INVALID_SQL;
      } else {
        pCmd->order.order = pSortorder->a[1].sortOrder;
        pCmd->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }
    }

  } else {  // meter query
    int32_t columnIndex = getColumnIndexByName(&columnName, pSchema, numOfCols);
    if (columnIndex <= -1) {
      setErrMsg(pCmd, msg3);
      return TSDB_CODE_INVALID_SQL;
    }

    if (columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX && !isTopBottomQuery(pCmd)) {
      setErrMsg(pCmd, msg5);
      return TSDB_CODE_INVALID_SQL;
    }

    if (isTopBottomQuery(pCmd) && pCmd->nAggTimeInterval >= 0) {
      /* order of top/bottom query in interval is not valid  */
      SSqlExpr* pExpr = tscSqlExprGet(pCmd, 0);
      assert(pExpr->sqlFuncId == TSDB_FUNC_TS);

      pExpr = tscSqlExprGet(pCmd, 1);
      if (pExpr->colInfo.colIdx != columnIndex && columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        setErrMsg(pCmd, msg);
        return TSDB_CODE_INVALID_SQL;
      }

      pCmd->order.order = pQuerySql->pSortOrder->a[0].sortOrder;
      pCmd->order.orderColId = pSchema[columnIndex].colId;
      return TSDB_CODE_SUCCESS;
    }

    pCmd->order.order = pQuerySql->pSortOrder->a[0].sortOrder;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t setAlterTableInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  SSqlCmd* pCmd = &pSql->cmd;

  SAlterTableSQL* pAlterSQL = pInfo->pAlterInfo;
  pCmd->command = TSDB_SQL_ALTER_TABLE;

  if (tscValidateName(&(pAlterSQL->name)) != TSDB_CODE_SUCCESS) {
  	const char* msg = "invalid table name";
    setErrMsg(pCmd, msg);
    return TSDB_CODE_INVALID_SQL;
  }

  if (setMeterID(pSql, &(pAlterSQL->name)) != TSDB_CODE_SUCCESS) {
    const char* msg = "table name too long";
    setErrMsg(pCmd, msg);
    return TSDB_CODE_INVALID_SQL;
  }

  int32_t ret = tscGetMeterMeta(pSql, pCmd->name);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  SMeterMeta* pMeterMeta = pCmd->pMeterMeta;
  SSchema*    pSchema = tsGetSchema(pMeterMeta);

  if (pInfo->sqlType == ALTER_TABLE_TAGS_ADD || pInfo->sqlType == ALTER_TABLE_TAGS_DROP ||
      pInfo->sqlType == ALTER_TABLE_TAGS_CHG) {
    if (UTIL_METER_IS_NOMRAL_METER(pCmd)) {
      const char* msg = "manipulation of tag available for metric";
      setErrMsg(pCmd, msg);
      return TSDB_CODE_INVALID_SQL;
    }
  } else if ((pInfo->sqlType == ALTER_TABLE_TAGS_SET) && (UTIL_METER_IS_METRIC(pCmd))) {
    const char* msg = "set tag value only available for table";
    setErrMsg(pCmd, msg);
    return TSDB_CODE_INVALID_SQL;
  } else if ((pInfo->sqlType == ALTER_TABLE_ADD_COLUMN || pInfo->sqlType == ALTER_TABLE_DROP_COLUMN) &&
             UTIL_METER_IS_CREATE_FROM_METRIC(pCmd)) {
    const char* msg = "column can only be modified by metric";
    setErrMsg(pCmd, msg);
    return TSDB_CODE_INVALID_SQL;
  }

  if (pInfo->sqlType == ALTER_TABLE_TAGS_ADD) {
    pCmd->count = TSDB_ALTER_TABLE_ADD_TAG_COLUMN;

    tFieldList* pFieldList = pAlterSQL->pAddColumns;
    if (pFieldList->nField > 1) {
      const char* msg = "only support add one tag";
      setErrMsg(pCmd, msg);
      return TSDB_CODE_INVALID_SQL;
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
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
    }

    // numOfTags == 1
    if (pAlterSQL->varList->nExpr > 1) {
      setErrMsg(pCmd, msg2);
      return TSDB_CODE_INVALID_SQL;
    }

    tVariantListItem* pItem = &pAlterSQL->varList->a[0];
    if (pItem->pVar.nLen > TSDB_COL_NAME_LEN) {
      setErrMsg(pCmd, msg3);
      return TSDB_CODE_INVALID_SQL;
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
      setErrMsg(pCmd, msg4);
      return TSDB_CODE_INVALID_SQL;
    } else if (idx == 0) {
      setErrMsg(pCmd, msg5);
      return TSDB_CODE_INVALID_SQL;
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
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
    }

    if (pSrcItem->pVar.nType != TSDB_DATA_TYPE_BINARY || pDstItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
      setErrMsg(pCmd, msg2);
      return TSDB_CODE_INVALID_SQL;
    }

    bool srcFound = false;
    bool dstFound = false;
    for (int32_t i = 0; i < pMeterMeta->numOfTags; ++i) {
      int32_t tagIdx = i + pMeterMeta->numOfColumns;
      char*   tagName = pSchema[tagIdx].name;

      size_t nameLen = strlen(tagName);
      if ((!srcFound) && (strncasecmp(tagName, pSrcItem->pVar.pz, nameLen) == 0 && (pSrcItem->pVar.nLen == nameLen))) {
        srcFound = true;
      }

      //todo extract method
      if ((!dstFound) && (strncasecmp(tagName, pDstItem->pVar.pz, nameLen) == 0 && (pDstItem->pVar.nLen == nameLen))) {
        dstFound = true;
      }
    }

    if ((!srcFound) || dstFound) {
      const char* msg = "invalid tag name";
      setErrMsg(pCmd, msg);
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
    pCmd->count = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;

    // Note: update can only be applied to meter not metric.
    // the following is handle display tags value for meters created according to metric
    char* pTagValue = tsGetTagsValue(pCmd->pMeterMeta);

    tVariantList* pVarList = pAlterSQL->varList;
    tVariant*     pTagName = &pVarList->a[0].pVar;

    if (pTagName->nLen > TSDB_COL_NAME_LEN) {
      const char* msg = "tag name too long";
      setErrMsg(pCmd, msg);
      return TSDB_CODE_INVALID_SQL;
    }

    int32_t  tagsIndex = -1;
    SSchema* pTagsSchema = tsGetTagSchema(pCmd->pMeterMeta);
    for (int32_t i = 0; i < pCmd->pMeterMeta->numOfTags; ++i) {
      if (strcmp(pTagName->pz, pTagsSchema[i].name) == 0 && strlen(pTagsSchema[i].name) == pTagName->nLen) {
        tagsIndex = i;
        tVariantDump(&pVarList->a[1].pVar, pCmd->payload, pTagsSchema[i].type);
        break;
      }

      pTagValue += pTagsSchema[i].bytes;
    }

    if (tagsIndex == -1) {
      const char* msg = "invalid tag name";
      setErrMsg(pCmd, msg);
      return TSDB_CODE_INVALID_SQL;
    }

    // validate the length of binary
    if (pTagsSchema[tagsIndex].type == TSDB_DATA_TYPE_BINARY &&
        pVarList->a[1].pVar.nLen > pTagsSchema[tagsIndex].bytes) {
      const char* msg = "tag value too long";
      setErrMsg(pCmd, msg);
      return TSDB_CODE_INVALID_SQL;
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
      setErrMsg(pCmd, msg);
      return TSDB_CODE_INVALID_SQL;
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
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
    }

    if (pAlterSQL->varList->nExpr > 1) {
      setErrMsg(pCmd, msg2);
      return TSDB_CODE_INVALID_SQL;
    }

    tVariantListItem* pItem = &pAlterSQL->varList->a[0];
    if (pItem->pVar.nLen > TSDB_COL_NAME_LEN) {
      setErrMsg(pCmd, msg3);
      return TSDB_CODE_INVALID_SQL;
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
      setErrMsg(pCmd, msg4);
      return TSDB_CODE_INVALID_SQL;
    } else if (idx == 0) {
      setErrMsg(pCmd, msg5);
      return TSDB_CODE_INVALID_SQL;
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
    setErrMsg(pCmd, msg0);
    return TSDB_CODE_INVALID_SQL;
  }

  for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
    int32_t functId = tscSqlExprGet(pCmd, i)->sqlFuncId;
    if (!IS_STREAM_QUERY_VALID(aAggs[functId].nStatus)) {
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateFunctionsInIntervalOrGroupbyQuery(SSqlCmd* pCmd) {
  bool isProjectionFunction = false;
  const char* msg = "column projection is not compatible with interval";

  // multi-output set/ todo refactor
  for (int32_t k = 0; k < pCmd->fieldsInfo.numOfOutputCols; ++k) {
    SSqlExpr* pExpr = tscSqlExprGet(pCmd, k);
    if (pExpr->sqlFuncId == TSDB_FUNC_PRJ || pExpr->sqlFuncId == TSDB_FUNC_TAGPRJ ||
        pExpr->sqlFuncId == TSDB_FUNC_DIFF || pExpr->sqlFuncId == TSDB_FUNC_ARITHM) {
      isProjectionFunction = true;
    }
  }
  if (pCmd->metricQuery == 0 || isProjectionFunction == true) {
    setErrMsg(pCmd, msg);
  }

  return isProjectionFunction == true ? TSDB_CODE_INVALID_SQL : TSDB_CODE_SUCCESS;
}

typedef struct SDNodeDynConfOption {
  char*   name;
  int32_t len;
} SDNodeDynConfOption;

int32_t validateDNodeConfig(tDCLSQL* pOptions) {
  if (pOptions->nTokens < 2 || pOptions->nTokens > 3) {
    return TSDB_CODE_INVALID_SQL;
  }

  SDNodeDynConfOption DNODE_DYNAMIC_CFG_OPTIONS[13] = {
      {"resetLog", 8},      {"resetQueryCache", 15}, {"dDebugFlag", 10},       {"taosDebugFlag", 13},
      {"tmrDebugFlag", 12}, {"cDebugFlag", 10},      {"uDebugFlag", 10},       {"mDebugFlag", 10},
      {"sdbDebugFlag", 12}, {"httpDebugFlag", 13},   {"monitorDebugFlag", 16}, {"qDebugflag", 10},
      {"debugFlag", 9}};

  SSQLToken* pOptionToken = &pOptions->a[1];

  if (pOptions->nTokens == 2) {
    // reset log and reset query cache does not need value
    for (int32_t i = 0; i < 2; ++i) {
      SDNodeDynConfOption* pOption = &DNODE_DYNAMIC_CFG_OPTIONS[i];
      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  } else {
    SSQLToken* pValToken = &pOptions->a[2];

    int32_t val = strtol(pValToken->z, NULL, 10);
    if (val < 131 || val > 199) {
      /* options value is out of valid range */
      return TSDB_CODE_INVALID_SQL;
    }

    for (int32_t i = 2; i < tListLen(DNODE_DYNAMIC_CFG_OPTIONS); ++i) {
      SDNodeDynConfOption* pOption = &DNODE_DYNAMIC_CFG_OPTIONS[i];

      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        /* options is valid */
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

  SSQLToken token = {
      .z = name,
  };
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

int32_t setLimitOffsetValueInfo(SSqlObj* pSql, SQuerySQL* pQuerySql) {
  SSqlCmd* pCmd = &pSql->cmd;
  bool     isMetric = UTIL_METER_IS_METRIC(pCmd);

  const char* msg0 = "soffset can not be less than 0";
  const char* msg1 = "offset can not be less than 0";
  const char* msg2 = "slimit/soffset only available for STable query";
  const char* msg3 = "function not supported on table";

  // handle the limit offset value, validate the limit
  pCmd->limit = pQuerySql->limit;
  pCmd->glimit = pQuerySql->glimit;

  if (isMetric) {
    bool    queryOnTags = false;
    int32_t ret = tscQueryOnlyMetricTags(pCmd, &queryOnTags);
    if (ret != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    if (queryOnTags == true) {  // local handle the metric tag query
      pCmd->command = TSDB_SQL_RETRIEVE_TAGS;
    }

    if (pCmd->glimit.limit == 0 || pCmd->limit.limit == 0) {
      tscTrace("%p limit 0, no output result", pSql);
      pCmd->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      return TSDB_CODE_SUCCESS;
    }

    if (pCmd->glimit.offset < 0) {
      setErrMsg(pCmd, msg0);
      return TSDB_CODE_INVALID_SQL;
    }

    /*
     * get the distribution of all meters among available vnodes that satisfy query condition from mnode ,
     * then launching multiple async-queries on referenced vnodes, which is the first-stage query operation]
     */
    int32_t code = tscGetMetricMeta(pSql, pCmd->name);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    /*
     * Query results are empty. Therefore, the result is filled with 0 if count function is employed in selection clause.
     *
     * The fill of empty result is required only when interval clause is absent.
     */
    SMetricMeta* pMetricMeta = pCmd->pMetricMeta;
    if (pCmd->pMeterMeta == NULL || pMetricMeta == NULL || pMetricMeta->numOfVnodes == 0 ||
        pMetricMeta->numOfMeters == 0) {
      tscTrace("%p no table in metricmeta, no output result", pSql);
      pCmd->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    }

    // keep original limitation value in globalLimit
    pCmd->globalLimit = pCmd->limit.limit;
  } else {
    if (pCmd->glimit.limit != -1 || pCmd->glimit.offset != 0) {
      setErrMsg(pCmd, msg2);
      return TSDB_CODE_INVALID_SQL;
    }

    for (int32_t i = 0; i < pCmd->fieldsInfo.numOfOutputCols; ++i) {
      SSqlExpr* pExpr = tscSqlExprGet(pCmd, i);
      if (pExpr->colInfo.colIdx == -1) {
        setErrMsg(pCmd, msg3);
        return TSDB_CODE_INVALID_SQL;
      }
    }

    if (pCmd->limit.limit == 0) {
      tscTrace("%p limit 0, no output result", pSql);
      pCmd->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    }

    if (pCmd->limit.offset < 0) {
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void setCreateDBOption(SCreateDbMsg* pMsg, SCreateDBInfo* pCreateDb) {
  pMsg->precision = TSDB_TIME_PRECISION_MILLI;  // millisecond by default

  pMsg->daysToKeep = htonl(-1);
  pMsg->daysToKeep1 = htonl(-1);
  pMsg->daysToKeep2 = htonl(-1);

  pMsg->blocksPerMeter = (pCreateDb->numOfBlocksPerTable == 0) ? htons(-1) : htons(pCreateDb->numOfBlocksPerTable);
  pMsg->compression = (pCreateDb->compressionLevel == 0) ? -1 : pCreateDb->compressionLevel;

  pMsg->commitLog = (pCreateDb->commitLog == 0) ? -1 : pCreateDb->commitLog;
  pMsg->commitTime = (pCreateDb->commitTime == 0) ? htonl(-1) : htonl(pCreateDb->commitTime);
  pMsg->maxSessions = (pCreateDb->tablesPerVnode == 0) ? htonl(-1) : htonl(pCreateDb->tablesPerVnode);
  pMsg->cacheNumOfBlocks.fraction = (pCreateDb->numOfAvgCacheBlocks == 0) ? -1 : pCreateDb->numOfAvgCacheBlocks;
  pMsg->cacheBlockSize = (pCreateDb->cacheBlockSize == 0) ? htonl(-1) : htonl(pCreateDb->cacheBlockSize);
  pMsg->rowsInFileBlock = (pCreateDb->rowPerFileBlock == 0) ? htonl(-1) : htonl(pCreateDb->rowPerFileBlock);
  pMsg->daysPerFile = (pCreateDb->daysPerFile == 0) ? htonl(-1) : htonl(pCreateDb->daysPerFile);
  pMsg->replications = (pCreateDb->replica == 0) ? -1 : pCreateDb->replica;
}

int32_t parseCreateDBOptions(SCreateDBInfo* pCreateDbSql, SSqlCmd* pCmd) {
  const char* msg0 = "invalid number of options";
  const char* msg1 = "invalid time precision";

  SCreateDbMsg *pMsg = (SCreateDbMsg *) (pCmd->payload + tsRpcHeadSize + sizeof(SMgmtHead));
  setCreateDBOption(pMsg, pCreateDbSql);

  if (pCreateDbSql->keep != NULL) {
    switch (pCreateDbSql->keep->nExpr) {
      case 1:
        pMsg->daysToKeep = htonl(pCreateDbSql->keep->a[0].pVar.i64Key);
        break;
      case 2: {
        pMsg->daysToKeep = htonl(pCreateDbSql->keep->a[0].pVar.i64Key);
        pMsg->daysToKeep1 = htonl(pCreateDbSql->keep->a[1].pVar.i64Key);
        break;
      }
      case 3: {
        pMsg->daysToKeep = htonl(pCreateDbSql->keep->a[0].pVar.i64Key);
        pMsg->daysToKeep1 = htonl(pCreateDbSql->keep->a[1].pVar.i64Key);
        pMsg->daysToKeep2 = htonl(pCreateDbSql->keep->a[2].pVar.i64Key);
        break;
      }
      default: {
        setErrMsg(pCmd, msg0);
        return TSDB_CODE_INVALID_SQL;
      }
    }
  }

  SSQLToken *pToken = &pCreateDbSql->precision;
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
      setErrMsg(pCmd, msg1);
      return TSDB_CODE_INVALID_SQL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

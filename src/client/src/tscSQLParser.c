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

#include <qSqlparser.h>
#include "os.h"
#include "qPlan.h"
#include "qSqlparser.h"
#include "qTableMeta.h"
#include "qUtil.h"
#include "taos.h"
#include "taosmsg.h"
#include "tcompare.h"
#include "texpr.h"
#include "tname.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tstrbuild.h"
#include "ttoken.h"
#include "ttokendef.h"
#include "qScript.h"
#include "ttype.h"

#define DEFAULT_PRIMARY_TIMESTAMP_COL_NAME "_c0"

#define TSWINDOW_IS_EQUAL(t1, t2) (((t1).skey == (t2).skey) && ((t1).ekey == (t2).ekey))

// -1 is tbname column index, so here use the -2 as the initial value
#define COLUMN_INDEX_INITIAL_VAL (-2)
#define COLUMN_INDEX_INITIALIZER \
  { COLUMN_INDEX_INITIAL_VAL, COLUMN_INDEX_INITIAL_VAL }
#define COLUMN_INDEX_VALIDE(tsc_index) (((tsc_index).tableIndex >= 0) && ((tsc_index).columnIndex >= TSDB_TBNAME_COLUMN_INDEX))
#define TBNAME_LIST_SEP ","

typedef struct SColumnList {  // todo refactor
  int32_t      num;
  SColumnIndex ids[TSDB_MAX_COLUMNS];
} SColumnList;

typedef struct SConvertFunc {
  int32_t originFuncId;
  int32_t execFuncId;
} SConvertFunc;

static SExprInfo* doAddProjectCol(SQueryInfo* pQueryInfo, int32_t colIndex, int32_t tableIndex, int32_t colId);

static int32_t setShowInfo(SSqlObj* pSql, SSqlInfo* pInfo);
static char*   getAccountId(SSqlObj* pSql);

static int  convertTimestampStrToInt64(tVariant *pVar, int32_t precision);
static bool serializeExprListToVariant(SArray* pList, tVariant **dst, int16_t colType, uint8_t precision);

static bool has(SArray* pFieldList, int32_t startIdx, const char* name);
static int32_t getDelimiterIndex(SStrToken* pTableName);
static bool validateTableColumnInfo(SArray* pFieldList, SSqlCmd* pCmd);
static bool validateTagParams(SArray* pTagsList, SArray* pFieldList, SSqlCmd* pCmd);

static int32_t setObjFullName(char* fullName, const char* account, SStrToken* pDB, SStrToken* tableName, int32_t* len);
static void getColumnName(tSqlExprItem* pItem, char* resultFieldName, char* rawName, int32_t nameLength);

static int32_t addExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t colIndex, tSqlExprItem* pItem,
    bool finalResult, SUdfInfo* pUdfInfo);
static int32_t insertResultField(SQueryInfo* pQueryInfo, int32_t outputIndex, SColumnList* pColList, int16_t bytes,
                          int8_t type, char* fieldName, SExprInfo* pSqlExpr);

static uint8_t convertRelationalOperator(SStrToken *pToken);

static int32_t validateSelectNodeList(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SArray* pSelNodeList, bool isSTable, bool joinQuery, bool timeWindowQuery);

static bool validateIpAddress(const char* ip, size_t size);
static bool hasUnsupportFunctionsForSTableQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static bool functionCompatibleCheck(SQueryInfo* pQueryInfo, bool joinQuery, bool twQuery);

static int32_t validateGroupbyNode(SQueryInfo* pQueryInfo, SArray* pList, SSqlCmd* pCmd);

static int32_t validateIntervalNode(SSqlObj* pSql, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode);
static int32_t parseIntervalOffset(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SStrToken* offsetToken);
static int32_t parseSlidingClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SStrToken* pSliding);
static int32_t validateStateWindowNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode, bool isStable);

static int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExprItem* pItem, bool outerQuery);

static int32_t validateWhereNode(SQueryInfo* pQueryInfo, tSqlExpr** pExpr, SSqlObj* pSql, bool joinQuery);
static int32_t validateFillNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode);
static int32_t validateOrderbyNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode, SSchema* pSchema);

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
static int32_t setCompactVnodeInfo(SSqlObj* pSql, struct SSqlInfo* pInfo);

static int32_t validateOneTag(SSqlCmd* pCmd, TAOS_FIELD* pTagField);
static bool hasTimestampForPointInterpQuery(SQueryInfo* pQueryInfo);
static bool hasNormalColumnFilter(SQueryInfo* pQueryInfo);

static int32_t validateLimitNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode, SSqlObj* pSql);
static int32_t parseCreateDBOptions(SSqlCmd* pCmd, SCreateDbInfo* pCreateDbSql);
static int32_t getColumnIndexByName(const SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex, char* msg);
static int32_t getTableIndexByName(SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex);

static int32_t getTableIndexImpl(SStrToken* pTableToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex);
static int32_t doFunctionsCompatibleCheck(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, char* msg);
static int32_t doLocalQueryProcess(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode);
static int32_t tscCheckCreateDbParams(SSqlCmd* pCmd, SCreateDbMsg* pCreate);

static SColumnList createColumnList(int32_t num, int16_t tableIndex, int32_t columnIndex);

static int32_t doCheckForCreateTable(SSqlObj* pSql, int32_t subClauseIndex, SSqlInfo* pInfo);
static int32_t doCheckForCreateFromStable(SSqlObj* pSql, SSqlInfo* pInfo);
static int32_t doCheckForStream(SSqlObj* pSql, SSqlInfo* pInfo);
static int32_t validateSqlNode(SSqlObj* pSql, SSqlNode* pSqlNode, SQueryInfo* pQueryInfo);

static int32_t exprTreeFromSqlExpr(SSqlCmd* pCmd, tExprNode **pExpr, const tSqlExpr* pSqlExpr, SQueryInfo* pQueryInfo, SArray* pCols, uint64_t *uid);
static bool    validateDebugFlag(int32_t v);
static int32_t checkQueryRangeForFill(SSqlCmd* pCmd, SQueryInfo* pQueryInfo);
static int32_t loadAllTableMeta(SSqlObj* pSql, struct SSqlInfo* pInfo);

static bool isTimeWindowQuery(SQueryInfo* pQueryInfo) {
  return pQueryInfo->interval.interval > 0 || pQueryInfo->sessionWindow.gap > 0;
}


int16_t getNewResColId(SSqlCmd* pCmd) {
  return pCmd->resColumnId--;
}

// serialize expr in exprlist to binary
// format  "type | size | value"
bool serializeExprListToVariant(SArray* pList, tVariant **dst, int16_t colType, uint8_t precision) {
  bool ret = false;
  if (!pList || pList->size <= 0 || colType < 0) {
    return ret;
  }

  tSqlExpr* item = ((tSqlExprItem*)(taosArrayGet(pList, 0)))->pNode;
  int32_t firstVarType = item->value.nType;

  SBufferWriter bw = tbufInitWriter( NULL, false);
  tbufEnsureCapacity(&bw, 512);
  if (colType == TSDB_DATA_TYPE_TIMESTAMP) {
    tbufWriteUint32(&bw, TSDB_DATA_TYPE_BIGINT);
  } else {
    tbufWriteUint32(&bw, colType);
  }

  tbufWriteInt32(&bw, (int32_t)(pList->size));

  for (int32_t i = 0; i < (int32_t)pList->size; i++) {
    tSqlExpr* pSub = ((tSqlExprItem*)(taosArrayGet(pList, i)))->pNode;
    tVariant* var  = &pSub->value;

    // check all the exprToken type in expr list same or not
    if (firstVarType != var->nType) {
      break;
    }
    if ((colType == TSDB_DATA_TYPE_BOOL || IS_SIGNED_NUMERIC_TYPE(colType))) {
      if (var->nType != TSDB_DATA_TYPE_BOOL && !IS_SIGNED_NUMERIC_TYPE(var->nType)) {
        break;
      }
      tbufWriteInt64(&bw, var->i64);
    } else if (IS_UNSIGNED_NUMERIC_TYPE(colType)) {
      if (IS_SIGNED_NUMERIC_TYPE(var->nType) || IS_UNSIGNED_NUMERIC_TYPE(var->nType)) {
        tbufWriteUint64(&bw, var->u64);
      } else {
        break;
      }
    } else if (colType == TSDB_DATA_TYPE_DOUBLE || colType == TSDB_DATA_TYPE_FLOAT) {
      if (IS_SIGNED_NUMERIC_TYPE(var->nType) || IS_UNSIGNED_NUMERIC_TYPE(var->nType)) {
        tbufWriteDouble(&bw, (double)(var->i64));
      } else if (var->nType == TSDB_DATA_TYPE_DOUBLE || var->nType == TSDB_DATA_TYPE_FLOAT){
        tbufWriteDouble(&bw, var->dKey);
      } else {
        break;
      }
    } else if (colType == TSDB_DATA_TYPE_BINARY) {
      if (var->nType != TSDB_DATA_TYPE_BINARY) {
        break;
      }
      tbufWriteBinary(&bw, var->pz, var->nLen);
    } else if (colType == TSDB_DATA_TYPE_NCHAR) {
      if (var->nType != TSDB_DATA_TYPE_BINARY) {
        break;
      }
      char   *buf = (char *)calloc(1, (var->nLen + 1)*TSDB_NCHAR_SIZE);
      if (tVariantDump(var, buf, colType, false) != TSDB_CODE_SUCCESS) {
        free(buf);
        break;
      }
      tbufWriteBinary(&bw, buf, twcslen((wchar_t *)buf) * TSDB_NCHAR_SIZE);
      free(buf);
    } else if (colType == TSDB_DATA_TYPE_TIMESTAMP) {
      if (var->nType == TSDB_DATA_TYPE_BINARY) {
         if (convertTimestampStrToInt64(var, precision) < 0) {
           break;
         }
         tbufWriteInt64(&bw, var->i64);
       } else if (var->nType == TSDB_DATA_TYPE_BIGINT) {
         tbufWriteInt64(&bw, var->i64);
       } else {
         break;
       }
    } else {
      break;
    }
    if (i == (int32_t)(pList->size - 1)) { ret = true;}
  }
  if (ret == true) {
    if ((*dst = calloc(1, sizeof(tVariant))) != NULL) {
      tVariantCreateFromBinary(*dst, tbufGetData(&bw, false), tbufTell(&bw), TSDB_DATA_TYPE_BINARY);
    } else {
      ret = false;
    }
  }
  tbufCloseWriter(&bw);
  return ret;
}


static uint8_t convertRelationalOperator(SStrToken *pToken) {
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
    case TK_IN:
      return TSDB_RELATION_IN;
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
static int32_t invalidOperationMsg(char* dstBuffer, const char* errMsg) {
  return tscInvalidOperationMsg(dstBuffer, errMsg, NULL);
}

static int convertTimestampStrToInt64(tVariant *pVar, int32_t precision) {
  int64_t     tsc_time = 0;
  strdequote(pVar->pz);

  char*           seg = strnchr(pVar->pz, '-', pVar->nLen, false);
  if (seg != NULL) {
    if (taosParseTime(pVar->pz, &tsc_time, pVar->nLen, precision, tsDaylight) != TSDB_CODE_SUCCESS) {
      return -1;
    }
  } else {
    if (tVariantDump(pVar, (char*)&tsc_time, TSDB_DATA_TYPE_BIGINT, true)) {
      return -1;
    }
  }
  tVariantDestroy(pVar);
  tVariantCreateFromBinary(pVar, (char*)&tsc_time, 0, TSDB_DATA_TYPE_BIGINT);
  return 0;
}
static int setColumnFilterInfoForTimestamp(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tVariant* pVar) {
  const char* msg = "invalid timestamp";

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  if (convertTimestampStrToInt64(pVar, tinfo.precision) < 0) {
   return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t handlePassword(SSqlCmd* pCmd, SStrToken* pPwd) {
  const char* msg1 = "password can not be empty";
  const char* msg2 = "name or password too long";
  const char* msg3 = "password needs single quote marks enclosed";

  if (pPwd->type != TK_STRING) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  strdequote(pPwd->z);
  pPwd->n = (uint32_t)strtrim(pPwd->z);  // trim space before and after passwords

  if (pPwd->n <= 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pPwd->n >= TSDB_KEY_LEN) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

// validate the out put field type for "UNION ALL" subclause
static int32_t normalizeVarDataTypeLength(SSqlCmd* pCmd) {
  const char* msg1 = "columns in select clause not identical";
  const char* msg2 = "too many select clause siblings, at most 100 allowed";

  int32_t siblings = 0;
  int32_t diffSize = 0;

  // if there is only one element, the limit of clause is the limit of global result.
  SQueryInfo* pQueryInfo1 = pCmd->pQueryInfo;
  SQueryInfo* pSibling = pQueryInfo1->sibling;

  // pQueryInfo1 itself
  ++siblings;

  while(pSibling != NULL) {
    int32_t ret = tscFieldInfoCompare(&pQueryInfo1->fieldsInfo, &pSibling->fieldsInfo, &diffSize);
    if (ret != 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    if (++siblings > 100) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    pSibling = pSibling->sibling;
  }

  if (diffSize) {
    pQueryInfo1 = pCmd->pQueryInfo;
    pSibling = pQueryInfo1->sibling;

    while(pSibling->sibling != NULL) {
      tscFieldInfoSetSize(&pQueryInfo1->fieldsInfo, &pSibling->fieldsInfo);
      pSibling = pSibling->sibling;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t readFromFile(char *name, uint32_t *len, void **buf) {
  struct stat fileStat;
  if (stat(name, &fileStat) < 0) {
    tscError("stat file %s failed, error:%s", name, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  *len = fileStat.st_size;

  if (*len <= 0) {
    tscError("file %s is empty", name);
    return TSDB_CODE_TSC_FILE_EMPTY;
  }

  *buf = calloc(1, *len);
  if (*buf == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int fd = open(name, O_RDONLY | O_BINARY);
  if (fd < 0) {
    tscError("open file %s failed, error:%s", name, strerror(errno));
    tfree(*buf);
    return TAOS_SYSTEM_ERROR(errno);
  }

  int64_t s = taosRead(fd, *buf, *len);
  if (s != *len) {
    tscError("read file %s failed, error:%s", name, strerror(errno));
    close(fd);
    tfree(*buf);
    return TSDB_CODE_TSC_APP_ERROR;
  }
  close(fd);
  return TSDB_CODE_SUCCESS;
}


int32_t handleUserDefinedFunc(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  const char *msg1 = "function name is too long";
  const char *msg2 = "path is too long";
  const char *msg3 = "invalid outputtype";
  const char *msg4 = "invalid script";
  const char *msg5 = "invalid dyn lib";
  SSqlCmd *pCmd = &pSql->cmd;

  switch (pInfo->type) {
    case TSDB_SQL_CREATE_FUNCTION: {
      SCreateFuncInfo *createInfo = &pInfo->pMiscInfo->funcOpt;
      uint32_t len = 0;
      void *buf = NULL;

      if (createInfo->output.type == (uint8_t)-1 || createInfo->output.bytes < 0) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      createInfo->name.z[createInfo->name.n] = 0;
      // funcname's naming rule is same to column 
      if (validateColumnName(createInfo->name.z) != TSDB_CODE_SUCCESS) {
        return  invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      strdequote(createInfo->name.z);

      if (strlen(createInfo->name.z) >= TSDB_FUNC_NAME_LEN) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      createInfo->path.z[createInfo->path.n] = 0;

      strdequote(createInfo->path.z);

      if (strlen(createInfo->path.z) >= PATH_MAX) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      int32_t ret = readFromFile(createInfo->path.z, &len, &buf);
      if (ret) {
        return ret;
      }
      //validate *.lua or .so
      int32_t pathLen = (int32_t)strlen(createInfo->path.z);
      if ((pathLen > 4) && (0 == strncmp(createInfo->path.z + pathLen - 4, ".lua", 4)) && !isValidScript(buf, len)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
      } else if (pathLen > 3 && (0 == strncmp(createInfo->path.z + pathLen - 3, ".so", 3))) {
        void *handle = taosLoadDll(createInfo->path.z);
        taosCloseDll(handle);
        if (handle == NULL) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
        }
      }

      //TODO CHECK CODE
      if (len + sizeof(SCreateFuncMsg) > pSql->cmd.allocSize) {
        ret = tscAllocPayload(&pSql->cmd, len + sizeof(SCreateFuncMsg));
        if (ret) {
          tfree(buf);
          return ret;
        }
      }

      SCreateFuncMsg *pMsg = (SCreateFuncMsg *)pSql->cmd.payload;

      strcpy(pMsg->name, createInfo->name.z);
      strcpy(pMsg->path, createInfo->path.z);

      pMsg->funcType = htonl(createInfo->type);
      pMsg->bufSize = htonl(createInfo->bufSize);

      pMsg->outputType = createInfo->output.type;
      pMsg->outputLen = htons(createInfo->output.bytes);

      pMsg->codeLen = htonl(len);
      memcpy(pMsg->code, buf, len);
      tfree(buf);

      break;
    }
    case TSDB_SQL_DROP_FUNCTION: {
      SStrToken* t0 = taosArrayGet(pInfo->pMiscInfo->a, 0);

      SDropFuncMsg *pMsg = (SDropFuncMsg *)pSql->cmd.payload;

      t0->z[t0->n] = 0;

      strdequote(t0->z);

      if (strlen(t0->z) >= TSDB_FUNC_NAME_LEN) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      strcpy(pMsg->name, t0->z);

      break;
    }
    default:
      return TSDB_CODE_TSC_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscValidateSqlInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
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

  SQueryInfo* pQueryInfo = tscGetQueryInfoS(pCmd);
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

      SStrToken* pzName = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if ((pInfo->type != TSDB_SQL_DROP_DNODE) && (tscValidateName(pzName) != TSDB_CODE_SUCCESS)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      if (pInfo->type == TSDB_SQL_DROP_DB) {
        assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
        code = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pzName);
        if (code != TSDB_CODE_SUCCESS) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
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
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        strncpy(pCmd->payload, pzName->z, pzName->n);
      }

      break;
    }

    case TSDB_SQL_USE_DB: {
      const char* msg = "invalid db name";
      SStrToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pToken);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
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
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      char buf[TSDB_DB_NAME_LEN] = {0};
      SStrToken token = taosTokenDup(&pCreateDB->dbname, buf, tListLen(buf));

      if (tscValidateName(&token) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), &token);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      if (parseCreateDBOptions(pCmd, pCreateDB) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      break;
    }

    case TSDB_SQL_CREATE_DNODE: {
      const char* msg = "invalid host name (ip address)";

      if (taosArrayGetSize(pInfo->pMiscInfo->a) > 1) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
      }

      SStrToken* id = taosArrayGet(pInfo->pMiscInfo->a, 0);
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

      SStrToken* pName = &pInfo->pMiscInfo->user.user;
      SStrToken* pPwd = &pInfo->pMiscInfo->user.passwd;

      if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (pName->n >= TSDB_USER_LEN) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SCreateAcctInfo* pAcctOpt = &pInfo->pMiscInfo->acctOpt;
      if (pAcctOpt->stat.n > 0) {
        if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
        } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
        } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
        } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
        } else {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
        }
      }

      break;
    }

    case TSDB_SQL_DESCRIBE_TABLE: {
      const char* msg1 = "invalid table name";

      SStrToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
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

      SStrToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      code = tscSetTableFullName(&pTableMetaInfo->name, pToken, pSql);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      return tscGetTableMeta(pSql, pTableMetaInfo);
    }
    case TSDB_SQL_SHOW_CREATE_DATABASE: {
      const char* msg1 = "invalid database name";

      SStrToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (pToken->n > TSDB_DB_NAME_LEN) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
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
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      char* pMsg = pCmd->payload;

      SCfgDnodeMsg* pCfg = (SCfgDnodeMsg*)pMsg;

      SStrToken* t0 = taosArrayGet(pMiscInfo->a, 0);
      SStrToken* t1 = taosArrayGet(pMiscInfo->a, 1);

      t0->n = strdequote(t0->z);
      strncpy(pCfg->ep, t0->z, t0->n);

      if (validateEp(pCfg->ep) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
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
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
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

          SStrToken* pPrivilege = &pUser->privilege;

          if (strncasecmp(pPrivilege->z, "super", 5) == 0 && pPrivilege->n == 5) {
            pCmd->count = 1;
          } else if (strncasecmp(pPrivilege->z, "read", 4) == 0 && pPrivilege->n == 4) {
            pCmd->count = 2;
          } else if (strncasecmp(pPrivilege->z, "write", 5) == 0 && pPrivilege->n == 5) {
            pCmd->count = 3;
          } else {
            return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
          }
        } else {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
        }
      }

      break;
    }

    case TSDB_SQL_CFG_LOCAL: {
      SMiscInfo  *pMiscInfo = pInfo->pMiscInfo;
      const char *msg = "invalid configure options or values";

      // validate the parameter names and options
      if (validateLocalConfig(pMiscInfo) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
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
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
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

      // set the command/global limit parameters from the first not empty subclause to the sqlcmd object
      SQueryInfo* queryInfo = pCmd->pQueryInfo;
      int16_t command = queryInfo->command;
      while (command == TSDB_SQL_RETRIEVE_EMPTY_RESULT && queryInfo->sibling != NULL) {
        queryInfo = queryInfo->sibling;
        command = queryInfo->command;
      }

      pCmd->active = queryInfo;
      pCmd->command = command;

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
      SStrToken* pzName = taosArrayGet(pInfo->pMiscInfo->a, 0);

      assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
      code = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pzName);
      if (code != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }
      break;
    }
    case TSDB_SQL_COMPACT_VNODE:{
      const char* msg = "invalid compact";
      if (setCompactVnodeInfo(pSql, pInfo) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
      }
      break;
    }
    default:
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "not support sql expression");
  }

  if (tscBuildMsg[pCmd->command] != NULL) {
    return tscBuildMsg[pCmd->command](pSql, pInfo);
  } else {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "not support sql expression");
  }
}

/*
 * if the top/bottom exists, only tags columns, tbname column, and primary timestamp column
 * are available.
 */
static bool isTopBottomQuery(SQueryInfo* pQueryInfo) {
  size_t size = tscNumOfExprs(pQueryInfo);
  
  for (int32_t i = 0; i < size; ++i) {
    int32_t functionId = tscExprGet(pQueryInfo, i)->base.functionId;

    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      return true;
    }
  }

  return false;
}

// need to add timestamp column in result set, if it is a time window query
static int32_t addPrimaryTsColumnForTimeWindowQuery(SQueryInfo* pQueryInfo, SSqlCmd* pCmd) {
  uint64_t uid = tscExprGet(pQueryInfo, 0)->base.uid;

  int32_t  tableIndex = COLUMN_INDEX_INITIAL_VAL;
  STableMetaInfo* pTableMetaInfo = NULL;
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    if (pTableMetaInfo->pTableMeta->id.uid == uid) {
      tableIndex = i;
      break;
    }
  }

  if (tableIndex == COLUMN_INDEX_INITIAL_VAL) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  SSchema s = {.bytes = TSDB_KEYSIZE, .type = TSDB_DATA_TYPE_TIMESTAMP, .colId = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  if (pTableMetaInfo) {
    tstrncpy(s.name, pTableMetaInfo->pTableMeta->schema[PRIMARYKEY_TIMESTAMP_COL_INDEX].name, sizeof(s.name));
  } else {
    tstrncpy(s.name, aAggs[TSDB_FUNC_TS].name, sizeof(s.name));
  }

  SColumnIndex tsc_index = {tableIndex, PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscAddFuncInSelectClause(pQueryInfo, 0, TSDB_FUNC_TS, &tsc_index, &s, TSDB_COL_NORMAL, getNewResColId(pCmd));
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
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    return TSDB_CODE_SUCCESS;
  }

  /*
   * invalid sql:
   * select count(tbname)/count(tag1)/count(tag2) from super_table_name [interval(1d)|session(ts, 1d)];
   */
  size_t size = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId == TSDB_FUNC_COUNT && TSDB_COL_IS_TAG(pExpr->base.colInfo.flag)) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  /*
   * invalid sql:
   * select tbname, tags_fields from super_table_name [interval(1s)|session(ts,1s)]
   */
  if (tscQueryTags(pQueryInfo) && isTimeWindowQuery(pQueryInfo)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  return addPrimaryTsColumnForTimeWindowQuery(pQueryInfo, pCmd);
}

int32_t validateIntervalNode(SSqlObj* pSql, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode) {
  const char* msg1 = "sliding cannot be used without interval";
  const char* msg2 = "interval cannot be less than 1 us";
  const char* msg3 = "interval value is too small";
  const char* msg4 = "only point interpolation query requires keyword EVERY";

  SSqlCmd* pCmd = &pSql->cmd;

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);

  if (!TPARSER_HAS_TOKEN(pSqlNode->interval.interval)) {
    if (TPARSER_HAS_TOKEN(pSqlNode->sliding)) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    return TSDB_CODE_SUCCESS;
  }

  // orderby column not set yet, set it to be the primary timestamp column
  if (pQueryInfo->order.orderColId == INT32_MIN) {
    pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
  }

  // interval is not null
  SStrToken *t = &pSqlNode->interval.interval;
  if (parseNatualDuration(t->z, t->n, &pQueryInfo->interval.interval,
                          &pQueryInfo->interval.intervalUnit, tinfo.precision) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (pQueryInfo->interval.interval <= 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  if (pQueryInfo->interval.intervalUnit != 'n' && pQueryInfo->interval.intervalUnit != 'y') {
    // interval cannot be less than 10 milliseconds
    if (convertTimePrecision(pQueryInfo->interval.interval, tinfo.precision, TSDB_TIME_PRECISION_MICRO) < tsMinIntervalTime) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  }

  if (parseIntervalOffset(pCmd, pQueryInfo, &pSqlNode->interval.offset) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (parseSlidingClause(pCmd, pQueryInfo, &pSqlNode->sliding) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  bool interpQuery = tscIsPointInterpQuery(pQueryInfo);
  if ((pSqlNode->interval.token == TK_EVERY && (!interpQuery)) || (pSqlNode->interval.token == TK_INTERVAL && interpQuery)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  // The following part is used to check for the invalid query expression.
  return checkInvalidExprForTimeWindow(pCmd, pQueryInfo);
}
static int32_t validateStateWindowNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode, bool isStable) {

  const char* msg1 = "invalid column name";
  const char* msg2 = "invalid column type";
  const char* msg3 = "not support state_window with group by ";
  const char* msg4 = "function not support for super table query";
  const char* msg5 = "not support state_window on tag column";

  SStrToken *col = &(pSqlNode->windowstateVal.col) ;
  if (col->z == NULL || col->n <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }
  if (pQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }
  pQueryInfo->groupbyExpr.numOfGroupCols = 1;

  //TODO(dengyihao): check tag column
  if (isStable) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(col, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) !=  TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
  int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
  if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  } else if (tsc_index.columnIndex >= numOfCols) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
  }

  SGroupbyExpr* pGroupExpr = &pQueryInfo->groupbyExpr;
  if (pGroupExpr->columnInfo == NULL) {
    pGroupExpr->columnInfo = taosArrayInit(4, sizeof(SColIndex));
  }

  SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, tsc_index.columnIndex);
  if (pSchema->type == TSDB_DATA_TYPE_TIMESTAMP || pSchema->type == TSDB_DATA_TYPE_FLOAT
      || pSchema->type == TSDB_DATA_TYPE_DOUBLE || pSchema->type == TSDB_DATA_TYPE_NCHAR
      || pSchema->type == TSDB_DATA_TYPE_BINARY) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  tscColumnListInsert(pQueryInfo->colList, tsc_index.columnIndex, pTableMeta->id.uid, pSchema);
  SColIndex colIndex = { .colIndex = tsc_index.columnIndex, .flag = TSDB_COL_NORMAL, .colId = pSchema->colId };
  taosArrayPush(pGroupExpr->columnInfo, &colIndex);
  pQueryInfo->groupbyExpr.orderType = TSDB_ORDER_ASC;
  pQueryInfo->stateWindow = true;
  return TSDB_CODE_SUCCESS;
}

int32_t validateSessionNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode * pSqlNode) {
  const char* msg1 = "gap should be fixed time window";
  const char* msg2 = "only one type time window allowed";
  const char* msg3 = "invalid column name";
  const char* msg4 = "invalid time window";

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  // no session window
  if (!TPARSER_HAS_TOKEN(pSqlNode->sessionVal.gap)) {
    return TSDB_CODE_SUCCESS;
  }

  SStrToken* col = &pSqlNode->sessionVal.col;
  SStrToken* gap = &pSqlNode->sessionVal.gap;

  char timeUnit = 0;
  if (parseNatualDuration(gap->z, gap->n, &pQueryInfo->sessionWindow.gap, &timeUnit, tinfo.precision) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  if (timeUnit == 'y' || timeUnit == 'n') {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pQueryInfo->sessionWindow.gap != 0 && pQueryInfo->interval.interval != 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }
  if (pQueryInfo->sessionWindow.gap == 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(col, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }
  if (tsc_index.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
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

  if (parseNatualDuration(t->z, t->n, &pQueryInfo->interval.offset,
                          &pQueryInfo->interval.offsetUnit, tinfo.precision) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (pQueryInfo->interval.offset < 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pQueryInfo->interval.offsetUnit != 'n' && pQueryInfo->interval.offsetUnit != 'y') {
    if (pQueryInfo->interval.intervalUnit != 'n' && pQueryInfo->interval.intervalUnit != 'y') {
      if (pQueryInfo->interval.offset >= pQueryInfo->interval.interval) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
    }
  } else if (pQueryInfo->interval.offsetUnit == pQueryInfo->interval.intervalUnit) {
    if (pQueryInfo->interval.offset >= pQueryInfo->interval.interval) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  } else if (pQueryInfo->interval.intervalUnit == 'n' && pQueryInfo->interval.offsetUnit == 'y') {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  } else if (pQueryInfo->interval.intervalUnit == 'y' && pQueryInfo->interval.offsetUnit == 'n') {
    if (pQueryInfo->interval.interval * 12 <= pQueryInfo->interval.offset) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
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

  SInterval* pInterval = &pQueryInfo->interval;
  if (pSliding->n == 0) {
    pInterval->slidingUnit = pInterval->intervalUnit;
    pInterval->sliding     = pInterval->interval;
    return TSDB_CODE_SUCCESS;
  }

  if (pInterval->intervalUnit == 'n' || pInterval->intervalUnit == 'y') {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  parseAbsoluteDuration(pSliding->z, pSliding->n, &pInterval->sliding, &pInterval->slidingUnit, tinfo.precision);

  if (pInterval->sliding < convertTimePrecision(tsMinSlidingTime, TSDB_TIME_PRECISION_MILLI, tinfo.precision)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }

  if (pInterval->sliding > pInterval->interval) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if ((pInterval->interval != 0) && (pInterval->interval/pInterval->sliding > INTERVAL_SLIDING_FACTOR)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscSetTableFullName(SName* pName, SStrToken* pTableName, SSqlObj* pSql) {
  const char* msg1 = "name too long";
  const char* msg2 = "acctId too long";
  const char* msg3 = "no acctId";
  const char* msg4 = "db name too long";
  const char* msg5 = "table name too long";
  const char* msg6 = "table name empty";

  SSqlCmd* pCmd = &pSql->cmd;
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  idx  = getDelimiterIndex(pTableName);
  if (idx != -1) { // db has been specified in sql string so we ignore current db path
    char* acctId = getAccountId(pSql);
    if (acctId == NULL || strlen(acctId) <= 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    code = tNameSetAcctId(pName, acctId);
    if (code != 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
    if (idx >= TSDB_DB_NAME_LEN) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
    }

    if (pTableName->n - 1 - idx >= TSDB_TABLE_NAME_LEN) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
    }

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, pTableName->n);

    code = tNameFromString(pName, name, T_NAME_DB|T_NAME_TABLE);
    if (code != 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  } else {  // get current DB name first, and then set it into path
    char* t = cloneCurrentDBName(pSql);
    if (strlen(t) == 0) {
      tfree(t);
      return TSDB_CODE_TSC_DB_NOT_SELECTED;
    }

    code = tNameFromString(pName, t, T_NAME_ACCT | T_NAME_DB);
    if (code != 0) {
      tfree(t);
      return TSDB_CODE_TSC_DB_NOT_SELECTED;
    }

    tfree(t);

    if (pTableName->n >= TSDB_TABLE_NAME_LEN) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    } else if(pTableName->n == 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
    }

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, pTableName->n);

    code = tNameFromString(pName, name, T_NAME_TABLE);
    if (code != 0) {
      code = invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
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
  const char* msg7 = "too many columns";

  // number of fields no less than 2
  size_t numOfCols = taosArrayGetSize(pFieldList);
  if (numOfCols <= 1 ) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
    return false;
  } else if (numOfCols > TSDB_MAX_COLUMNS) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
    return false;
  }

  // first column must be timestamp
  TAOS_FIELD* pField = taosArrayGet(pFieldList, 0);
  if (pField->type != TSDB_DATA_TYPE_TIMESTAMP) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    pField = taosArrayGet(pFieldList, i);
    if (!isValidDataType(pField->type)) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
      return false;
    }

    if (pField->bytes == 0) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
      return false;
    }

    if ((pField->type == TSDB_DATA_TYPE_BINARY && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_BINARY_LEN)) ||
        (pField->type == TSDB_DATA_TYPE_NCHAR && (pField->bytes <= 0 || pField->bytes > TSDB_MAX_NCHAR_LEN))) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
      return false;
    }

    if (validateColumnName(pField->name) != TSDB_CODE_SUCCESS) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      return false;
    }

    // field name must be unique
    if (has(pFieldList, i + 1, pField->name) == true) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return false;
    }

    nLen += pField->bytes;
  }

  // max row length must be less than TSDB_MAX_BYTES_PER_ROW
  if (nLen > TSDB_MAX_BYTES_PER_ROW) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    return false;
  }

  return true;
}


static bool validateTagParams(SArray* pTagsList, SArray* pFieldList, SSqlCmd* pCmd) {
  assert(pTagsList != NULL);

  const char* msg1 = "invalid number of tag columns";
  const char* msg2 = "tag length too long";
  const char* msg3 = "duplicated column names";
  //const char* msg4 = "timestamp not allowed in tags";
  const char* msg5 = "invalid data type in tags";
  const char* msg6 = "invalid tag name";
  const char* msg7 = "invalid binary/nchar tag length";

  // number of fields at least 1
  size_t numOfTags = taosArrayGetSize(pTagsList);
  if (numOfTags < 1 || numOfTags > TSDB_MAX_TAGS) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  for (int32_t i = 0; i < numOfTags; ++i) {
    TAOS_FIELD* p = taosArrayGet(pTagsList, i);
    if (!isValidDataType(p->type)) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
      return false;
    }

    if ((p->type == TSDB_DATA_TYPE_BINARY && p->bytes <= 0) ||
        (p->type == TSDB_DATA_TYPE_NCHAR && p->bytes <= 0)) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
      return false;
    }

    if (validateColumnName(p->name) != TSDB_CODE_SUCCESS) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      return false;
    }

    if (has(pTagsList, i + 1, p->name) == true) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return false;
    }
  }

  int32_t nLen = 0;
  for (int32_t i = 0; i < numOfTags; ++i) {
    TAOS_FIELD* p = taosArrayGet(pTagsList, i);
    if (p->bytes == 0) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
      return false;
    }

    nLen += p->bytes;
  }

  // max tag row length must be less than TSDB_MAX_TAGS_LEN
  if (nLen > TSDB_MAX_TAGS_LEN) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    return false;
  }

  // field name must be unique
  for (int32_t i = 0; i < numOfTags; ++i) {
    TAOS_FIELD* p = taosArrayGet(pTagsList, i);

    if (has(pFieldList, 0, p->name) == true) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return false;
    }
  }

  return true;
}

/*
 * tags name /column name is truncated in sql.y
 */
int32_t validateOneTag(SSqlCmd* pCmd, TAOS_FIELD* pTagField) {
  const char* msg3 = "tag length too long";
  const char* msg4 = "invalid tag name";
  const char* msg5 = "invalid binary/nchar tag length";
  const char* msg6 = "invalid data type in tags";
  const char* msg7 = "too many columns";

  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

  int32_t numOfTags = tscGetNumOfTags(pTableMeta);
  int32_t numOfCols = tscGetNumOfColumns(pTableMeta);

  // no more max columns
  if (numOfTags + numOfCols >= TSDB_MAX_COLUMNS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
  }

  // no more than 6 tags
  if (numOfTags == TSDB_MAX_TAGS) {
    char msg[128] = {0};
    sprintf(msg, "tags no more than %d", TSDB_MAX_TAGS);

    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  // no timestamp allowable
  //if (pTagField->type == TSDB_DATA_TYPE_TIMESTAMP) {
  //  invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  //  return false;
  //}

  if ((pTagField->type < TSDB_DATA_TYPE_BOOL) || (pTagField->type > TSDB_DATA_TYPE_UBIGINT)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
  }

  SSchema* pTagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
  int32_t  nLen = 0;

  for (int32_t i = 0; i < numOfTags; ++i) {
    nLen += pTagSchema[i].bytes;
  }

  // length less than TSDB_MAX_TASG_LEN
  if (nLen + pTagField->bytes > TSDB_MAX_TAGS_LEN) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  // tags name can not be a keyword
  if (validateColumnName(pTagField->name) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  // binary(val), val can not be equalled to or less than 0
  if ((pTagField->type == TSDB_DATA_TYPE_BINARY || pTagField->type == TSDB_DATA_TYPE_NCHAR) && pTagField->bytes <= 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
  }

  // field name must be unique
  SSchema* pSchema = tscGetTableSchema(pTableMeta);

  for (int32_t i = 0; i < numOfTags + numOfCols; ++i) {
    if (strncasecmp(pTagField->name, pSchema[i].name, sizeof(pTagField->name) - 1) == 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "duplicated column names");
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateOneColumn(SSqlCmd* pCmd, TAOS_FIELD* pColField) {
  const char* msg1 = "too many columns";
  const char* msg3 = "column length too long";
  const char* msg4 = "invalid data type";
  const char* msg5 = "invalid column name";
  const char* msg6 = "invalid column length";

//  assert(pCmd->numOfClause == 1);
  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;
  
  int32_t numOfTags = tscGetNumOfTags(pTableMeta);
  int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
  
  // no more max columns
  if (numOfCols >= TSDB_MAX_COLUMNS || numOfTags + numOfCols >= TSDB_MAX_COLUMNS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pColField->type < TSDB_DATA_TYPE_BOOL || pColField->type > TSDB_DATA_TYPE_UBIGINT) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  if (validateColumnName(pColField->name) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
  }

  SSchema* pSchema = tscGetTableSchema(pTableMeta);
  int32_t  nLen = 0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    nLen += pSchema[i].bytes;
  }

  if (pColField->bytes <= 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
  }

  // length less than TSDB_MAX_BYTES_PER_ROW
  if (nLen + pColField->bytes > TSDB_MAX_BYTES_PER_ROW) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  // field name must be unique
  for (int32_t i = 0; i < numOfTags + numOfCols; ++i) {
    if (strncasecmp(pColField->name, pSchema[i].name, sizeof(pColField->name) - 1) == 0) {
      //return tscErrorMsgWithCode(TSDB_CODE_TSC_DUP_COL_NAMES, tscGetErrorMsgPayload(pCmd), pColField->name, NULL);
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "duplicated column names");
    }
  }

  return TSDB_CODE_SUCCESS;
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

/* length limitation, strstr cannot be applied */
static int32_t getDelimiterIndex(SStrToken* pTableName) {
  for (uint32_t i = 0; i < pTableName->n; ++i) {
    if (pTableName->z[i] == TS_PATH_DELIMITER[0]) {
      return i;
    }
  }
  return -1;
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
      return TSDB_CODE_TSC_INVALID_OPERATION;
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
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else {  // pDB == NULL, the db prefix name is specified in tableName
      /* the length limitation includes tablename + dbname + sep */
      if (tableName->n >= TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
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

  return (totalLen < TSDB_TABLE_FNAME_LEN) ? TSDB_CODE_SUCCESS : TSDB_CODE_TSC_INVALID_OPERATION;
}

void tscInsertPrimaryTsSourceColumn(SQueryInfo* pQueryInfo, uint64_t tableUid) {
  SSchema s = {.type = TSDB_DATA_TYPE_TIMESTAMP, .bytes = TSDB_KEYSIZE, .colId = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  tscColumnListInsert(pQueryInfo->colList, PRIMARYKEY_TIMESTAMP_COL_INDEX, tableUid, &s);
}

static int32_t handleArithmeticExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t exprIndex, tSqlExprItem* pItem) {
  const char* msg1 = "invalid column name, illegal column type, or columns in arithmetic expression from two tables";
  const char* msg2 = "invalid arithmetic expression in select clause";
  const char* msg3 = "tag columns can not be used in arithmetic expression";
  const char* msg4 = "columns from different table mixed up in arithmetic expression";

  SColumnList columnList = {0};
  int32_t     arithmeticType = NON_ARITHMEIC_EXPR;

  if (validateArithmeticSQLExpr(pCmd, pItem->pNode, pQueryInfo, &columnList, &arithmeticType) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  int32_t tableIndex = columnList.ids[0].tableIndex;
  if (arithmeticType == NORMAL_ARITHMETIC) {
    pQueryInfo->type |= TSDB_QUERY_TYPE_PROJECTION_QUERY;

    // all columns in arithmetic expression must belong to the same table
    for (int32_t f = 1; f < columnList.num; ++f) {
      if (columnList.ids[f].tableIndex != tableIndex) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
      }
    }

    // expr string is set as the parameter of function
    SColumnIndex tsc_index = {.tableIndex = tableIndex};

    SExprInfo* pExpr = tscExprAppend(pQueryInfo, TSDB_FUNC_ARITHM, &tsc_index, TSDB_DATA_TYPE_DOUBLE, sizeof(double),
                                       getNewResColId(pCmd), sizeof(double), false);

    char* name = (pItem->aliasName != NULL)? pItem->aliasName:pItem->pNode->exprToken.z;
    size_t len = MIN(sizeof(pExpr->base.aliasName), pItem->pNode->exprToken.n + 1);
    tstrncpy(pExpr->base.aliasName, name, len);

    tExprNode* pNode = NULL;
    SArray* colList = taosArrayInit(10, sizeof(SColIndex));

    int32_t ret = exprTreeFromSqlExpr(pCmd, &pNode, pItem->pNode, pQueryInfo, colList, NULL);
    if (ret != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(colList);
      tExprTreeDestroy(pNode, NULL);
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    // check for if there is a tag in the arithmetic express
    size_t numOfNode = taosArrayGetSize(colList);
    for(int32_t k = 0; k < numOfNode; ++k) {
      SColIndex* pIndex = taosArrayGet(colList, k);
      if (TSDB_COL_IS_TAG(pIndex->flag)) {
        tExprTreeDestroy(pNode, NULL);
        taosArrayDestroy(colList);

        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
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
    tscExprAddParams(&pExpr->base, c, TSDB_DATA_TYPE_BINARY, (int32_t)len);
    insertResultField(pQueryInfo, exprIndex, &columnList, sizeof(double), TSDB_DATA_TYPE_DOUBLE, pExpr->base.aliasName, pExpr);

    // add ts column
    tscInsertPrimaryTsSourceColumn(pQueryInfo, pExpr->base.uid);

    tbufCloseWriter(&bw);
    taosArrayDestroy(colList);
    tExprTreeDestroy(pNode, NULL);
  } else {
    columnList.num = 0;
    columnList.ids[0] = (SColumnIndex) {0, 0};

    char rawName[TSDB_COL_NAME_LEN] = {0};
    char aliasName[TSDB_COL_NAME_LEN] = {0};
    getColumnName(pItem, aliasName, rawName, TSDB_COL_NAME_LEN);

    insertResultField(pQueryInfo, exprIndex, &columnList, sizeof(double), TSDB_DATA_TYPE_DOUBLE, aliasName, NULL);

    int32_t slot = tscNumOfFields(pQueryInfo) - 1;
    SInternalField* pInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, slot);
    assert(pInfo->pExpr == NULL);

    SExprInfo* pExprInfo = calloc(1, sizeof(SExprInfo));

    // arithmetic expression always return result in the format of double float
    pExprInfo->base.resBytes   = sizeof(double);
    pExprInfo->base.interBytes = 0;
    pExprInfo->base.resType    = TSDB_DATA_TYPE_DOUBLE;

    pExprInfo->base.functionId = TSDB_FUNC_ARITHM;
    pExprInfo->base.numOfParams = 1;
    pExprInfo->base.resColId = getNewResColId(pCmd);
    strncpy(pExprInfo->base.aliasName, aliasName, tListLen(pExprInfo->base.aliasName));
    strncpy(pExprInfo->base.token, rawName, tListLen(pExprInfo->base.token));

    int32_t ret = exprTreeFromSqlExpr(pCmd, &pExprInfo->pExpr, pItem->pNode, pQueryInfo, NULL, &(pExprInfo->base.uid));
    if (ret != TSDB_CODE_SUCCESS) {
      tExprTreeDestroy(pExprInfo->pExpr, NULL);
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "invalid expression in select clause");
    }

    pInfo->pExpr = pExprInfo;

    SBufferWriter bw = tbufInitWriter(NULL, false);

    TRY(0) {
      exprTreeToBinary(&bw, pInfo->pExpr->pExpr);
    } CATCH(code) {
      tbufCloseWriter(&bw);
      UNUSED(code);
      // TODO: other error handling
    } END_TRY

    SSqlExpr* pSqlExpr = &pInfo->pExpr->base;
    pSqlExpr->param[0].nLen = (int16_t) tbufTell(&bw);
    pSqlExpr->param[0].pz   = tbufGetData(&bw, true);
    pSqlExpr->param[0].nType = TSDB_DATA_TYPE_BINARY;

//    tbufCloseWriter(&bw); // TODO there is a memory leak
  }

  return TSDB_CODE_SUCCESS;
}

static void addProjectQueryCol(SQueryInfo* pQueryInfo, int32_t startPos, SColumnIndex* pIndex, tSqlExprItem* pItem, int32_t colId) {
  SExprInfo* pExpr = doAddProjectCol(pQueryInfo, pIndex->columnIndex, pIndex->tableIndex, colId);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pIndex->tableIndex);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

  SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, pIndex->columnIndex);

  char* colName = (pItem->aliasName == NULL) ? pSchema->name : pItem->aliasName;
  tstrncpy(pExpr->base.aliasName, colName, sizeof(pExpr->base.aliasName));

  SColumnList ids = {0};
  ids.num = 1;
  ids.ids[0] = *pIndex;

  if (pIndex->columnIndex == TSDB_TBNAME_COLUMN_INDEX || pIndex->columnIndex == TSDB_UD_COLUMN_INDEX ||
      pIndex->columnIndex >= tscGetNumOfColumns(pTableMeta)) {
    ids.num = 0;
  }

  insertResultField(pQueryInfo, startPos, &ids, pExpr->base.resBytes, (int8_t)pExpr->base.resType, pExpr->base.aliasName, pExpr);
}

static void addPrimaryTsColIntoResult(SQueryInfo* pQueryInfo, SSqlCmd* pCmd) {
  // primary timestamp column has been added already
  size_t size = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId == TSDB_FUNC_PRJ && pExpr->base.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return;
    }
  }


  // set the constant column value always attached to first table.
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, PRIMARYKEY_TIMESTAMP_COL_INDEX);

  // add the timestamp column into the output columns
  SColumnIndex tsc_index = {0};  // primary timestamp column info
  int32_t numOfCols = (int32_t)tscNumOfExprs(pQueryInfo);
  tscAddFuncInSelectClause(pQueryInfo, numOfCols, TSDB_FUNC_PRJ, &tsc_index, pSchema, TSDB_COL_NORMAL, getNewResColId(pCmd));

  SInternalField* pSupInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, numOfCols);
  pSupInfo->visible = false;

  pQueryInfo->type |= TSDB_QUERY_TYPE_PROJECTION_QUERY;
}

static bool hasNoneUserDefineExpr(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = taosArrayGetSize(pQueryInfo->exprList);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, i);

    if (TSDB_COL_IS_UD_COL(pExpr->colInfo.flag)) {
      continue;
    }

    return true;
  }

  return false;
}

void genUdfList(SArray* pUdfInfo, tSqlExpr *pNode) {
  if (pNode == NULL) {
    return;
  }

  if (pNode->type == SQL_NODE_EXPR) {
    genUdfList(pUdfInfo, pNode->pLeft);
    genUdfList(pUdfInfo, pNode->pRight);
    return;
  }

  if (pNode->type == SQL_NODE_SQLFUNCTION) {
    pNode->functionId = isValidFunction(pNode->Expr.operand.z, pNode->Expr.operand.n);
    if (pNode->functionId < 0) { // extract all possible user defined function
      struct SUdfInfo info = {0};
      info.name = strndup(pNode->Expr.operand.z, pNode->Expr.operand.n);
      int32_t functionId = (int32_t)taosArrayGetSize(pUdfInfo) * (-1) - 1;
      info.functionId = functionId;

      taosArrayPush(pUdfInfo, &info);
    }
  }
}

/*
static int32_t checkForUdf(SSqlObj* pSql, SQueryInfo* pQueryInfo, SArray* pSelection) {
  if (pQueryInfo->pUdfInfo != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  pQueryInfo->pUdfInfo = taosArrayInit(4, sizeof(struct SUdfInfo));

  size_t nExpr = taosArrayGetSize(pSelection);

  for (int32_t i = 0; i < nExpr; ++i) {
    tSqlExprItem* pItem = taosArrayGet(pSelection, i);

    int32_t type = pItem->pNode->type;
    if (type == SQL_NODE_EXPR || type == SQL_NODE_SQLFUNCTION) {
      genUdfList(pQueryInfo->pUdfInfo, pItem->pNode);
    }
  }

  if (taosArrayGetSize(pQueryInfo->pUdfInfo) > 0) {
    return tscGetUdfFromNode(pSql, pQueryInfo);
  } else {
    return TSDB_CODE_SUCCESS;
  }
}
*/

static SUdfInfo* isValidUdf(SArray* pUdfInfo, const char* name, int32_t len) {
  if(pUdfInfo == NULL){
    tscError("udfinfo is null");
    return NULL;
  }
  size_t t = taosArrayGetSize(pUdfInfo);
  for(int32_t i = 0; i < t; ++i) {
    SUdfInfo* pUdf = taosArrayGet(pUdfInfo, i);
    if (strlen(pUdf->name) == len && strncasecmp(pUdf->name, name, len) == 0) {
      return pUdf;
    }
  }

  return NULL;
}

int32_t validateSelectNodeList(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SArray* pSelNodeList, bool joinQuery,
                               bool timeWindowQuery, bool outerQuery) {
  assert(pSelNodeList != NULL && pCmd != NULL);

  const char* msg1 = "too many items in selection clause";
  const char* msg2 = "functions or others can not be mixed up";
  const char* msg3 = "not support query expression";
  const char* msg4 = "not support distinct mixed with proj/agg func";
  const char* msg5 = "invalid function name";
  const char* msg6 = "not support distinct mixed with join"; 
  const char* msg7 = "not support distinct mixed with groupby";
  const char* msg8 = "not support distinct in nest query";
  const char* msg9 = "invalid alias name";

  // too many result columns not support order by in query
  if (taosArrayGetSize(pSelNodeList) > TSDB_MAX_COLUMNS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }

  
  bool hasDistinct = false;
  bool hasAgg      = false; 
  size_t numOfExpr = taosArrayGetSize(pSelNodeList);
  int32_t distIdx = -1; 
  for (int32_t i = 0; i < numOfExpr; ++i) {
    int32_t outputIndex = (int32_t)tscNumOfExprs(pQueryInfo);
    tSqlExprItem* pItem = taosArrayGet(pSelNodeList, i);
     
    if (hasDistinct == false) {
       hasDistinct = (pItem->distinct == true); 
       distIdx     =  hasDistinct ? i : -1;
    }
    if(pItem->aliasName != NULL && validateColumnName(pItem->aliasName) != TSDB_CODE_SUCCESS){
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg9);
    }

    int32_t type = pItem->pNode->type;
    if (type == SQL_NODE_SQLFUNCTION) {
      hasAgg = true; 
      if (hasDistinct)  break;

      pItem->pNode->functionId = isValidFunction(pItem->pNode->Expr.operand.z, pItem->pNode->Expr.operand.n);

      if (pItem->pNode->functionId == TSDB_FUNC_BLKINFO && taosArrayGetSize(pQueryInfo->pUpstream) > 0) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      SUdfInfo* pUdfInfo = NULL;
      if (pItem->pNode->functionId < 0) {
        pUdfInfo = isValidUdf(pQueryInfo->pUdfInfo, pItem->pNode->Expr.operand.z, pItem->pNode->Expr.operand.n);
        if (pUdfInfo == NULL) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
        }

        pItem->pNode->functionId = pUdfInfo->functionId;
      }

      // sql function in selection clause, append sql function info in pSqlCmd structure sequentially
      if (addExprAndResultField(pCmd, pQueryInfo, outputIndex, pItem, true, pUdfInfo) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else if (type == SQL_NODE_TABLE_COLUMN || type == SQL_NODE_VALUE) {
      // use the dynamic array list to decide if the function is valid or not
      // select table_name1.field_name1, table_name2.field_name2  from table_name1, table_name2
      if (addProjectionExprAndResultField(pCmd, pQueryInfo, pItem, outerQuery) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else if (type == SQL_NODE_EXPR) {
      int32_t code = handleArithmeticExpr(pCmd, pQueryInfo, i, pItem);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    if (pQueryInfo->fieldsInfo.numOfOutput > TSDB_MAX_COLUMNS) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  //TODO(dengyihao), refactor as function     
  //handle distinct func mixed with other func 
  if (hasDistinct == true) {
    if (distIdx != 0 || hasAgg) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
    } 
    if (joinQuery) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
    }
    if (pQueryInfo->groupbyExpr.numOfGroupCols  != 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
    }
    if (pQueryInfo->pDownstream != NULL) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg8);
     }
    
    pQueryInfo->distinct = true;
  }
  
  
  // there is only one user-defined column in the final result field, add the timestamp column.
  size_t numOfSrcCols = taosArrayGetSize(pQueryInfo->colList);
  if ((numOfSrcCols <= 0 || !hasNoneUserDefineExpr(pQueryInfo)) && !tscQueryTags(pQueryInfo) && !tscQueryBlockInfo(pQueryInfo)) {
    addPrimaryTsColIntoResult(pQueryInfo, pCmd);
  }

  if (!functionCompatibleCheck(pQueryInfo, joinQuery, timeWindowQuery)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t insertResultField(SQueryInfo* pQueryInfo, int32_t outputIndex, SColumnList* pColList, int16_t bytes,
                          int8_t type, char* fieldName, SExprInfo* pSqlExpr) {
  for (int32_t i = 0; i < pColList->num; ++i) {
    int32_t tableIndex = pColList->ids[i].tableIndex;
    STableMeta* pTableMeta = pQueryInfo->pTableMetaInfo[tableIndex]->pTableMeta;

    int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
    if (pColList->ids[i].columnIndex >= numOfCols) {
      continue;
    }

    uint64_t uid = pTableMeta->id.uid;
    SSchema* pSchema = tscGetTableSchema(pTableMeta);
    tscColumnListInsert(pQueryInfo->colList, pColList->ids[i].columnIndex, uid, &pSchema[pColList->ids[i].columnIndex]);
  }
  
  TAOS_FIELD f = tscCreateField(type, fieldName, bytes);
  SInternalField* pInfo = tscFieldInfoInsert(&pQueryInfo->fieldsInfo, outputIndex, &f);
  pInfo->pExpr = pSqlExpr;
  
  return TSDB_CODE_SUCCESS;
}

SExprInfo* doAddProjectCol(SQueryInfo* pQueryInfo, int32_t colIndex, int32_t tableIndex, int32_t colId) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;
  int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
  
  SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, colIndex);

  int16_t functionId = (int16_t)((colIndex >= numOfCols) ? TSDB_FUNC_TAGPRJ : TSDB_FUNC_PRJ);
  SColumnIndex tsc_index = {.tableIndex = tableIndex,};
  
  if (functionId == TSDB_FUNC_TAGPRJ) {
    tsc_index.columnIndex = colIndex - tscGetNumOfColumns(pTableMeta);
    tscColumnListInsert(pTableMetaInfo->tagColList, tsc_index.columnIndex, pTableMeta->id.uid, pSchema);
  } else {
    tsc_index.columnIndex = colIndex;
  }

  return tscExprAppend(pQueryInfo, functionId, &tsc_index, pSchema->type, pSchema->bytes, colId, 0,
                          (functionId == TSDB_FUNC_TAGPRJ));
}

SExprInfo* tscAddFuncInSelectClause(SQueryInfo* pQueryInfo, int32_t outputColIndex, int16_t functionId,
                                  SColumnIndex* pIndex, SSchema* pColSchema, int16_t flag, int16_t colId) {
  SExprInfo* pExpr = tscExprInsert(pQueryInfo, outputColIndex, functionId, pIndex, pColSchema->type,
                                     pColSchema->bytes, colId, 0, TSDB_COL_IS_TAG(flag));
  tstrncpy(pExpr->base.aliasName, pColSchema->name, sizeof(pExpr->base.aliasName));
  tstrncpy(pExpr->base.token, pColSchema->name, sizeof(pExpr->base.token));

  SColumnList ids = createColumnList(1, pIndex->tableIndex, pIndex->columnIndex);
  if (TSDB_COL_IS_TAG(flag)) {
    ids.num = 0;
  }

  insertResultField(pQueryInfo, outputColIndex, &ids, pColSchema->bytes, pColSchema->type, pColSchema->name, pExpr);

  pExpr->base.colInfo.flag = flag;
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pIndex->tableIndex);
  
  if (TSDB_COL_IS_TAG(flag)) {
    tscColumnListInsert(pTableMetaInfo->tagColList, pIndex->columnIndex, pTableMetaInfo->pTableMeta->id.uid, pColSchema);
  }

  return pExpr;
}

static int32_t doAddProjectionExprAndResultFields(SQueryInfo* pQueryInfo, SColumnIndex* pIndex, int32_t startPos, SSqlCmd* pCmd) {
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
    SExprInfo* pExpr = doAddProjectCol(pQueryInfo, j, pIndex->tableIndex, getNewResColId(pCmd));
    tstrncpy(pExpr->base.aliasName, pSchema[j].name, sizeof(pExpr->base.aliasName));

    pIndex->columnIndex = j;
    SColumnList ids = {0};
    ids.ids[0] = *pIndex;
    ids.num = 1;

    insertResultField(pQueryInfo, startPos + j, &ids, pSchema[j].bytes, pSchema[j].type, pSchema[j].name, pExpr);
  }

  return numOfTotalColumns;
}

int32_t addProjectionExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExprItem* pItem, bool outerQuery) {
  const char* msg1 = "tag for normal table query is not allowed";
  const char* msg2 = "invalid column name";
  const char* msg3 = "tbname not allowed in outer query";

  int32_t startPos = (int32_t)tscNumOfExprs(pQueryInfo);
  int32_t tokenId = pItem->pNode->tokenId;

  if (tokenId == TK_ALL) {  // project on all fields
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_PROJECTION_QUERY);

    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
    if (getTableIndexByName(&pItem->pNode->columnName, pQueryInfo, &tsc_index) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    // all meters columns are required
    if (tsc_index.tableIndex == COLUMN_INDEX_INITIAL_VAL) {  // all table columns are required.
      for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
        tsc_index.tableIndex = i;
        int32_t inc = doAddProjectionExprAndResultFields(pQueryInfo, &tsc_index, startPos, pCmd);
        startPos += inc;
      }
    } else {
      doAddProjectionExprAndResultFields(pQueryInfo, &tsc_index, startPos, pCmd);
    }

    // add the primary timestamp column even though it is not required by user
    STableMeta* pTableMeta = pQueryInfo->pTableMetaInfo[tsc_index.tableIndex]->pTableMeta;
    if (pTableMeta->tableType != TSDB_TEMP_TABLE) {
      tscInsertPrimaryTsSourceColumn(pQueryInfo, pTableMeta->id.uid);
    }
  } else if (tokenId == TK_STRING || tokenId == TK_INTEGER || tokenId == TK_FLOAT) {  // simple column projection query
    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;

    // user-specified constant value as a new result column
    tsc_index.columnIndex = (pQueryInfo->udColumnId--);
    tsc_index.tableIndex = 0;

    SSchema colSchema = tGetUserSpecifiedColumnSchema(&pItem->pNode->value, &pItem->pNode->exprToken, pItem->aliasName);
    SExprInfo* pExpr = tscAddFuncInSelectClause(pQueryInfo, startPos, TSDB_FUNC_PRJ, &tsc_index, &colSchema, TSDB_COL_UDC,
                                                getNewResColId(pCmd));

    // NOTE: the first parameter is reserved for the tag column id during join query process.
    pExpr->base.numOfParams = 2;
    tVariantAssign(&pExpr->base.param[1], &pItem->pNode->value);
  } else if (tokenId == TK_ID) {
    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;

    if (getColumnIndexByName(&pItem->pNode->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      if (outerQuery) {
        STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
        int32_t         numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);

        bool existed = false;
        SSchema* pSchema = pTableMetaInfo->pTableMeta->schema;
        for (int32_t i = 0; i < numOfCols; ++i) {
          if (strncasecmp(pSchema[i].name, TSQL_TBNAME_L, tListLen(pSchema[i].name)) == 0) {
            existed = true;
            tsc_index.columnIndex = i;
            break;
          }
        }

        if (!existed) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        SSchema colSchema = pSchema[tsc_index.columnIndex];
        char    name[TSDB_COL_NAME_LEN] = {0};
        getColumnName(pItem, name, colSchema.name, sizeof(colSchema.name) - 1);

        tstrncpy(colSchema.name, name, TSDB_COL_NAME_LEN);
        /*SExprInfo* pExpr = */ tscAddFuncInSelectClause(pQueryInfo, startPos, TSDB_FUNC_PRJ, &tsc_index, &colSchema,
                                                         TSDB_COL_NORMAL, getNewResColId(pCmd));
      } else {
        SSchema colSchema = *tGetTbnameColumnSchema();
        char    name[TSDB_COL_NAME_LEN] = {0};
        getColumnName(pItem, name, colSchema.name, sizeof(colSchema.name) - 1);

        tstrncpy(colSchema.name, name, TSDB_COL_NAME_LEN);
        /*SExprInfo* pExpr = */ tscAddFuncInSelectClause(pQueryInfo, startPos, TSDB_FUNC_TAGPRJ, &tsc_index, &colSchema,
                                                         TSDB_COL_TAG, getNewResColId(pCmd));
      }
    } else {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
      STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

      if (tsc_index.columnIndex >= tscGetNumOfColumns(pTableMeta) && UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      addProjectQueryCol(pQueryInfo, startPos, &tsc_index, pItem, getNewResColId(pCmd));
      pQueryInfo->type |= TSDB_QUERY_TYPE_PROJECTION_QUERY;
    }

    // add the primary timestamp column even though it is not required by user
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
    if (!UTIL_TABLE_IS_TMP_TABLE(pTableMetaInfo)) {
      tscInsertPrimaryTsSourceColumn(pQueryInfo, pTableMetaInfo->pTableMeta->id.uid);
    }
  } else {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setExprInfoForFunctions(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSchema* pSchema, SConvertFunc cvtFunc,
                                       const char* name, int32_t resColIdx, SColumnIndex* pColIndex, bool finalResult,
                                       SUdfInfo* pUdfInfo) {
  const char* msg1 = "not support column types";

  int32_t f = cvtFunc.execFuncId;
  if (f == TSDB_FUNC_SPREAD) {
    int32_t t1 = pSchema->type;
    if (IS_VAR_DATA_TYPE(t1) || t1 == TSDB_DATA_TYPE_BOOL) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      return -1;
    }
  }

  int16_t resType = 0;
  int16_t resBytes = 0;
  int32_t interBufSize = 0;

  getResultDataInfo(pSchema->type, pSchema->bytes, f, 0, &resType, &resBytes, &interBufSize, 0, false, pUdfInfo);
  SExprInfo* pExpr = tscExprAppend(pQueryInfo, f, pColIndex, resType, resBytes, getNewResColId(pCmd), interBufSize, false);
  tstrncpy(pExpr->base.aliasName, name, tListLen(pExpr->base.aliasName));

  if (cvtFunc.originFuncId == TSDB_FUNC_LAST_ROW && cvtFunc.originFuncId != f) {
    pExpr->base.colInfo.flag |= TSDB_COL_NULL;
  }

  // set reverse order scan data blocks for last query
  if (f == TSDB_FUNC_LAST) {
    pExpr->base.numOfParams = 1;
    pExpr->base.param[0].i64 = TSDB_ORDER_DESC;
    pExpr->base.param[0].nType = TSDB_DATA_TYPE_INT;
  }
  
  // for all queries, the timestamp column needs to be loaded
  SSchema s = {.colId = PRIMARYKEY_TIMESTAMP_COL_INDEX, .bytes = TSDB_KEYSIZE, .type = TSDB_DATA_TYPE_TIMESTAMP,};
  tscColumnListInsert(pQueryInfo->colList, PRIMARYKEY_TIMESTAMP_COL_INDEX, pExpr->base.uid, &s);

  // if it is not in the final result, do not add it
  SColumnList ids = createColumnList(1, pColIndex->tableIndex, pColIndex->columnIndex);
  if (finalResult) {
    insertResultField(pQueryInfo, resColIdx, &ids, resBytes, (int8_t)resType, pExpr->base.aliasName, pExpr);
  } else {
    tscColumnListInsert(pQueryInfo->colList, ids.ids[0].columnIndex, pExpr->base.uid, pSchema);
  }

  return TSDB_CODE_SUCCESS;
}

void setResultColName(char* name, tSqlExprItem* pItem, int32_t functionId, SStrToken* pToken, bool multiCols) {
  if (pItem->aliasName != NULL) {
    tstrncpy(name, pItem->aliasName, TSDB_COL_NAME_LEN);
  } else if (multiCols) {
    char tsc_index[TSDB_COL_NAME_LEN] = {0};
    int32_t len = MIN(pToken->n + 1, TSDB_COL_NAME_LEN);
    tstrncpy(tsc_index, pToken->z, len);

    if (tsKeepOriginalColumnName) { // keep the original column name
      tstrncpy(name, tsc_index, TSDB_COL_NAME_LEN);
    } else {
      int32_t size = TSDB_COL_NAME_LEN + tListLen(aAggs[functionId].name) + 2 + 1;
      char tmp[TSDB_COL_NAME_LEN + tListLen(aAggs[functionId].name) + 2 + 1] = {0};
      snprintf(tmp, size, "%s(%s)", aAggs[functionId].name, tsc_index);

      tstrncpy(name, tmp, TSDB_COL_NAME_LEN);
    }
  } else  { // use the user-input result column name
    int32_t len = MIN(pItem->pNode->exprToken.n + 1, TSDB_COL_NAME_LEN);
    tstrncpy(name, pItem->pNode->exprToken.z, len);
  }
}

static void updateLastScanOrderIfNeeded(SQueryInfo* pQueryInfo) {
  if (pQueryInfo->sessionWindow.gap > 0 ||
      pQueryInfo->stateWindow ||
      taosArrayGetSize(pQueryInfo->pUpstream) > 0 ||
      tscGroupbyColumn(pQueryInfo)) {
    size_t numOfExpr = tscNumOfExprs(pQueryInfo);
    for (int32_t i = 0; i < numOfExpr; ++i) {
      SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
      if (pExpr->base.functionId != TSDB_FUNC_LAST && pExpr->base.functionId != TSDB_FUNC_LAST_DST) {
        continue;
      }

      pExpr->base.numOfParams = 1;
      pExpr->base.param->i64 = TSDB_ORDER_ASC;
      pExpr->base.param->nType = TSDB_DATA_TYPE_INT;
    }
  }
}

static UNUSED_FUNC void updateFunctionInterBuf(SQueryInfo* pQueryInfo, bool superTable, SUdfInfo* pUdfInfo) {
  size_t numOfExpr = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExpr; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);

    int32_t param = (int32_t)pExpr->base.param[0].i64;
    getResultDataInfo(pExpr->base.colType, pExpr->base.colBytes, pExpr->base.functionId, param, &pExpr->base.resType, &pExpr->base.resBytes,
                      &pExpr->base.interBytes, 0, superTable, pUdfInfo);
  }
}

int32_t addExprAndResultField(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t colIndex, tSqlExprItem* pItem, bool finalResult,
    SUdfInfo* pUdfInfo) {
  STableMetaInfo* pTableMetaInfo = NULL;
  int32_t functionId = pItem->pNode->functionId;

  const char* msg1 = "not support column types";
  const char* msg2 = "invalid parameters";
  const char* msg3 = "illegal column name";
  const char* msg4 = "invalid table name";
  const char* msg5 = "parameter is out of range [0, 100]";
  const char* msg6 = "functions applied to tags are not allowed";
  const char* msg7 = "normal table can not apply this function";
  const char* msg8 = "multi-columns selection does not support alias column name";
  const char* msg9 = "diff/derivative can no be applied to unsigned numeric type";
  const char* msg10 = "derivative duration should be greater than 1 Second";
  const char* msg11 = "third parameter in derivative should be 0 or 1";
  const char* msg12 = "parameter is out of range [1, 100]";
  const char* msg13 = "parameter list required";

  switch (functionId) {
    case TSDB_FUNC_COUNT: {
      /* more than one parameter for count() function */
      if (pItem->pNode->Expr.paramList != NULL && taosArrayGetSize(pItem->pNode->Expr.paramList) != 1) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SExprInfo* pExpr = NULL;
      SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;

      if (pItem->pNode->Expr.paramList != NULL) {
        tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->Expr.paramList, 0);
        SStrToken* pToken = &pParamElem->pNode->columnName;
        int16_t tokenId = pParamElem->pNode->tokenId;
        if ((pToken->z == NULL || pToken->n == 0) && (TK_INTEGER != tokenId)) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        // select count(table.*), select count(1), count(2)
        if (tokenId == TK_ALL || tokenId == TK_INTEGER) {
          // check if the table name is valid or not
          SStrToken tmpToken = pParamElem->pNode->columnName;

          if (getTableIndexByName(&tmpToken, pQueryInfo, &tsc_index) != TSDB_CODE_SUCCESS) {
            return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
          }

          tsc_index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
          int32_t size = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
          pExpr = tscExprAppend(pQueryInfo, functionId, &tsc_index, TSDB_DATA_TYPE_BIGINT, size, getNewResColId(pCmd), size, false);
        } else {
          // count the number of table created according to the super table
          if (getColumnIndexByName(pToken, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
            return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }

          pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);

          // count tag is equalled to count(tbname)
          bool isTag = false;
          if (tsc_index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta) || tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
            tsc_index.columnIndex = TSDB_TBNAME_COLUMN_INDEX;
            isTag = true;
          }

          int32_t size = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
          pExpr = tscExprAppend(pQueryInfo, functionId, &tsc_index, TSDB_DATA_TYPE_BIGINT, size, getNewResColId(pCmd), size, isTag);
        }
      } else {  // count(*) is equalled to count(primary_timestamp_key)
        tsc_index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
        int32_t size = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
        pExpr = tscExprAppend(pQueryInfo, functionId, &tsc_index, TSDB_DATA_TYPE_BIGINT, size, getNewResColId(pCmd), size, false);
      }

      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);

      memset(pExpr->base.aliasName, 0, tListLen(pExpr->base.aliasName));
      getColumnName(pItem, pExpr->base.aliasName, pExpr->base.token,sizeof(pExpr->base.aliasName) - 1);
      
      SColumnList list = createColumnList(1, tsc_index.tableIndex, tsc_index.columnIndex);
      if (finalResult) {
        int32_t numOfOutput = tscNumOfFields(pQueryInfo);
        insertResultField(pQueryInfo, numOfOutput, &list, sizeof(int64_t), TSDB_DATA_TYPE_BIGINT, pExpr->base.aliasName, pExpr);
      } else {
        for (int32_t i = 0; i < list.num; ++i) {
          SSchema* ps = tscGetTableSchema(pTableMetaInfo->pTableMeta);
          tscColumnListInsert(pQueryInfo->colList, list.ids[i].columnIndex, pTableMetaInfo->pTableMeta->id.uid,
              &ps[list.ids[i].columnIndex]);
        }
      }

      // the time stamp may be always needed
      if (tsc_index.tableIndex < tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
        tscInsertPrimaryTsSourceColumn(pQueryInfo, pTableMetaInfo->pTableMeta->id.uid);
      }

      return TSDB_CODE_SUCCESS;
    }

    case TSDB_FUNC_SUM:
    case TSDB_FUNC_AVG:
    case TSDB_FUNC_RATE:
    case TSDB_FUNC_IRATE:
    case TSDB_FUNC_TWA:
    case TSDB_FUNC_MIN:
    case TSDB_FUNC_MAX:
    case TSDB_FUNC_DIFF:
    case TSDB_FUNC_DERIVATIVE:
    case TSDB_FUNC_STDDEV:
    case TSDB_FUNC_LEASTSQR: {
      // 1. valid the number of parameters
      int32_t numOfParams = (pItem->pNode->Expr.paramList == NULL)? 0: (int32_t) taosArrayGetSize(pItem->pNode->Expr.paramList);

      // no parameters or more than one parameter for function
      if (pItem->pNode->Expr.paramList == NULL ||
          (functionId != TSDB_FUNC_LEASTSQR && functionId != TSDB_FUNC_DERIVATIVE && numOfParams != 1) ||
          ((functionId == TSDB_FUNC_LEASTSQR || functionId == TSDB_FUNC_DERIVATIVE) && numOfParams != 3)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->Expr.paramList, 0);
      if (pParamElem->pNode->tokenId != TK_ALL && pParamElem->pNode->tokenId != TK_ID) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
      if ((getColumnIndexByName(&pParamElem->pNode->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
      STableComInfo info = tscGetTableInfo(pTableMetaInfo->pTableMeta);

      // functions can not be applied to tags
      if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX || (tsc_index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta))) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      // 2. check if sql function can be applied on this column data type
      SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, tsc_index.columnIndex);

      if (!IS_NUMERIC_TYPE(pSchema->type)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      } else if (IS_UNSIGNED_NUMERIC_TYPE(pSchema->type) && (functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_DERIVATIVE)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg9);
      }

      int16_t resultType = 0;
      int16_t resultSize = 0;
      int32_t intermediateResSize = 0;

      if (getResultDataInfo(pSchema->type, pSchema->bytes, functionId, 0, &resultType, &resultSize,
                            &intermediateResSize, 0, false, NULL) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      // set the first column ts for diff query
      if (functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_DERIVATIVE) {
        SColumnIndex indexTS = {.tableIndex = tsc_index.tableIndex, .columnIndex = 0};
        SExprInfo*   pExpr = tscExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &indexTS, TSDB_DATA_TYPE_TIMESTAMP,
                                         TSDB_KEYSIZE, getNewResColId(pCmd), TSDB_KEYSIZE, false);
        tstrncpy(pExpr->base.aliasName, aAggs[TSDB_FUNC_TS_DUMMY].name, sizeof(pExpr->base.aliasName));

        SColumnList ids = createColumnList(1, 0, 0);
        insertResultField(pQueryInfo, colIndex, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP, aAggs[TSDB_FUNC_TS_DUMMY].name, pExpr);
      }

      SExprInfo* pExpr = tscExprAppend(pQueryInfo, functionId, &tsc_index, resultType, resultSize, getNewResColId(pCmd), intermediateResSize, false);

      if (functionId == TSDB_FUNC_LEASTSQR) { // set the leastsquares parameters
        char val[8] = {0};
        if (tVariantDump(&pParamElem[1].pNode->value, val, TSDB_DATA_TYPE_DOUBLE, true) < 0) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }

        tscExprAddParams(&pExpr->base, val, TSDB_DATA_TYPE_DOUBLE, DOUBLE_BYTES);

        memset(val, 0, tListLen(val));
        if (tVariantDump(&pParamElem[2].pNode->value, val, TSDB_DATA_TYPE_DOUBLE, true) < 0) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }

        tscExprAddParams(&pExpr->base, val, TSDB_DATA_TYPE_DOUBLE, DOUBLE_BYTES);
      } else if (functionId == TSDB_FUNC_IRATE) {
        int64_t prec = info.precision;
        tscExprAddParams(&pExpr->base, (char*)&prec, TSDB_DATA_TYPE_BIGINT, LONG_BYTES);
      } else if (functionId == TSDB_FUNC_DERIVATIVE) {
        char val[8] = {0};

        int64_t tickPerSec = 0;
        if (tVariantDump(&pParamElem[1].pNode->value, (char*) &tickPerSec, TSDB_DATA_TYPE_BIGINT, true) < 0) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }

        if (info.precision == TSDB_TIME_PRECISION_MILLI) {
          tickPerSec /= TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_MICRO);
        } else if (info.precision == TSDB_TIME_PRECISION_MICRO) {
          tickPerSec /= TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_MILLI);
	      }

        if (tickPerSec <= 0 || tickPerSec < TSDB_TICK_PER_SECOND(info.precision)) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg10);
        }

        tscExprAddParams(&pExpr->base, (char*) &tickPerSec, TSDB_DATA_TYPE_BIGINT, LONG_BYTES);
        memset(val, 0, tListLen(val));

        if (tVariantDump(&pParamElem[2].pNode->value, val, TSDB_DATA_TYPE_BIGINT, true) < 0) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }

        int64_t v = *(int64_t*) val;
        if (v != 0 && v != 1) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg11);
        }

        tscExprAddParams(&pExpr->base, val, TSDB_DATA_TYPE_BIGINT, LONG_BYTES);
      }

      SColumnList ids = createColumnList(1, tsc_index.tableIndex, tsc_index.columnIndex);

      memset(pExpr->base.aliasName, 0, tListLen(pExpr->base.aliasName));
      getColumnName(pItem, pExpr->base.aliasName, pExpr->base.token,sizeof(pExpr->base.aliasName) - 1);

      if (finalResult) {
        int32_t numOfOutput = tscNumOfFields(pQueryInfo);
        insertResultField(pQueryInfo, numOfOutput, &ids, pExpr->base.resBytes, (int32_t)pExpr->base.resType,
                          pExpr->base.aliasName, pExpr);
      } else {
        assert(ids.num == 1);
        tscColumnListInsert(pQueryInfo->colList, ids.ids[0].columnIndex, pExpr->base.uid, pSchema);
      }
      tscInsertPrimaryTsSourceColumn(pQueryInfo, pExpr->base.uid);
        
      return TSDB_CODE_SUCCESS;
    }

    case TSDB_FUNC_FIRST:
    case TSDB_FUNC_LAST:
    case TSDB_FUNC_SPREAD:
    case TSDB_FUNC_LAST_ROW:
    case TSDB_FUNC_INTERP: {
      bool requireAllFields = (pItem->pNode->Expr.paramList == NULL);

      // NOTE: has time range condition or normal column filter condition, the last_row query will be transferred to last query
      SConvertFunc cvtFunc = {.originFuncId = functionId, .execFuncId = functionId};
      if (functionId == TSDB_FUNC_LAST_ROW && ((!TSWINDOW_IS_EQUAL(pQueryInfo->window, TSWINDOW_INITIALIZER)) ||
                                               (hasNormalColumnFilter(pQueryInfo)) ||
                                               taosArrayGetSize(pQueryInfo->pUpstream)>0)) {
        cvtFunc.execFuncId = TSDB_FUNC_LAST;
      }

      if (!requireAllFields) {
        if (taosArrayGetSize(pItem->pNode->Expr.paramList) < 1) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
        }

        if (taosArrayGetSize(pItem->pNode->Expr.paramList) > 1 && (pItem->aliasName != NULL && strlen(pItem->aliasName) > 0)) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg8);
        }

        /* in first/last function, multiple columns can be add to resultset */
        for (int32_t i = 0; i < taosArrayGetSize(pItem->pNode->Expr.paramList); ++i) {
          tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->Expr.paramList, i);
          if (pParamElem->pNode->tokenId != TK_ALL && pParamElem->pNode->tokenId != TK_ID) {
            return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }

          SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;

          if (pParamElem->pNode->tokenId == TK_ALL) { // select table.*
            SStrToken tmpToken = pParamElem->pNode->columnName;

            if (getTableIndexByName(&tmpToken, pQueryInfo, &tsc_index) != TSDB_CODE_SUCCESS) {
              return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
            }

            pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
            SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

            char name[TSDB_COL_NAME_LEN] = {0};
            for (int32_t j = 0; j < tscGetNumOfColumns(pTableMetaInfo->pTableMeta); ++j) {
              tsc_index.columnIndex = j;
              SStrToken t = {.z = pSchema[j].name, .n = (uint32_t)strnlen(pSchema[j].name, TSDB_COL_NAME_LEN)};
              setResultColName(name, pItem, cvtFunc.originFuncId, &t, true);

              if (setExprInfoForFunctions(pCmd, pQueryInfo, &pSchema[j], cvtFunc, name, colIndex++, &tsc_index,
                  finalResult, pUdfInfo) != 0) {
                return TSDB_CODE_TSC_INVALID_OPERATION;
              }
            }

          } else {
            if (getColumnIndexByName(&pParamElem->pNode->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
              return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
            }

            pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);

            // functions can not be applied to tags
            if ((tsc_index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) || (tsc_index.columnIndex < 0)) {
              return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
            }

            char name[TSDB_COL_NAME_LEN] = {0};
            SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, tsc_index.columnIndex);

            bool multiColOutput = taosArrayGetSize(pItem->pNode->Expr.paramList) > 1;
            setResultColName(name, pItem, cvtFunc.originFuncId, &pParamElem->pNode->columnName, multiColOutput);

            if (setExprInfoForFunctions(pCmd, pQueryInfo, pSchema, cvtFunc, name, colIndex++, &tsc_index, finalResult, pUdfInfo) != 0) {
              return TSDB_CODE_TSC_INVALID_OPERATION;
            }
          }
        }

      } else {  // select * from xxx
        int32_t numOfFields = 0;

        // multicolumn selection does not support alias name
        if (pItem->aliasName != NULL && strlen(pItem->aliasName) > 0) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg8);
        }

        for (int32_t j = 0; j < pQueryInfo->numOfTables; ++j) {
          pTableMetaInfo = tscGetMetaInfo(pQueryInfo, j);
          SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

          for (int32_t i = 0; i < tscGetNumOfColumns(pTableMetaInfo->pTableMeta); ++i) {
            SColumnIndex tsc_index = {.tableIndex = j, .columnIndex = i};

            char name[TSDB_COL_NAME_LEN] = {0};
            SStrToken t = {.z = pSchema[i].name, .n = (uint32_t)strnlen(pSchema[i].name, TSDB_COL_NAME_LEN)};
            setResultColName(name, pItem, cvtFunc.originFuncId, &t, true);

            if (setExprInfoForFunctions(pCmd, pQueryInfo, &pSchema[tsc_index.columnIndex], cvtFunc, name, colIndex, &tsc_index,
                finalResult, pUdfInfo) != 0) {
              return TSDB_CODE_TSC_INVALID_OPERATION;
            }
            colIndex++;
          }

          numOfFields += tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
        }
      }
      return TSDB_CODE_SUCCESS;
    }

    case TSDB_FUNC_TOP:
    case TSDB_FUNC_BOTTOM:
    case TSDB_FUNC_PERCT:
    case TSDB_FUNC_APERCT: {
      // 1. valid the number of parameters
      if (pItem->pNode->Expr.paramList == NULL || taosArrayGetSize(pItem->pNode->Expr.paramList) != 2) {
        /* no parameters or more than one parameter for function */
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->Expr.paramList, 0);
      if (pParamElem->pNode->tokenId != TK_ID) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
      
      SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByName(&pParamElem->pNode->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }
      
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
      SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, tsc_index.columnIndex);

      // functions can not be applied to tags
      if (tsc_index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      // 2. valid the column type
      if (!IS_NUMERIC_TYPE(pSchema->type)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      // 3. valid the parameters
      if (pParamElem[1].pNode->tokenId == TK_ID) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tVariant* pVariant = &pParamElem[1].pNode->value;

      int16_t  resultType = pSchema->type;
      int16_t  resultSize = pSchema->bytes;
      int32_t  interResult = 0;

      char val[8] = {0};

      SExprInfo* pExpr = NULL;
      if (functionId == TSDB_FUNC_PERCT || functionId == TSDB_FUNC_APERCT) {
        tVariantDump(pVariant, val, TSDB_DATA_TYPE_DOUBLE, true);

        double dp = GET_DOUBLE_VAL(val);
        if (dp < 0 || dp > TOP_BOTTOM_QUERY_LIMIT) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
        }

        getResultDataInfo(pSchema->type, pSchema->bytes, functionId, 0, &resultType, &resultSize, &interResult, 0, false,
            pUdfInfo);

        /*
         * sql function transformation
         * for dp = 0, it is actually min,
         * for dp = 100, it is max,
         */
        tscInsertPrimaryTsSourceColumn(pQueryInfo, pTableMetaInfo->pTableMeta->id.uid);
        colIndex += 1;  // the first column is ts

        pExpr = tscExprAppend(pQueryInfo, functionId, &tsc_index, resultType, resultSize, getNewResColId(pCmd), interResult, false);
        tscExprAddParams(&pExpr->base, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
      } else {
        tVariantDump(pVariant, val, TSDB_DATA_TYPE_BIGINT, true);

        int64_t nTop = GET_INT32_VAL(val);
        if (nTop <= 0 || nTop > 100) {  // todo use macro
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg12);
        }

        // todo REFACTOR
        // set the first column ts for top/bottom query
        SColumnIndex index1 = {tsc_index.tableIndex, PRIMARYKEY_TIMESTAMP_COL_INDEX};
        pExpr = tscExprAppend(pQueryInfo, TSDB_FUNC_TS, &index1, TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE, getNewResColId(pCmd),
                                 0, false);
        tstrncpy(pExpr->base.aliasName, aAggs[TSDB_FUNC_TS].name, sizeof(pExpr->base.aliasName));

        const int32_t TS_COLUMN_INDEX = PRIMARYKEY_TIMESTAMP_COL_INDEX;
        SColumnList   ids = createColumnList(1, tsc_index.tableIndex, TS_COLUMN_INDEX);
        insertResultField(pQueryInfo, colIndex, &ids, TSDB_KEYSIZE, TSDB_DATA_TYPE_TIMESTAMP,
                          aAggs[TSDB_FUNC_TS].name, pExpr);

        colIndex += 1;  // the first column is ts

        pExpr = tscExprAppend(pQueryInfo, functionId, &tsc_index, resultType, resultSize, getNewResColId(pCmd), resultSize, false);
        tscExprAddParams(&pExpr->base, val, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
      }
  
      memset(pExpr->base.aliasName, 0, tListLen(pExpr->base.aliasName));
      getColumnName(pItem, pExpr->base.aliasName, pExpr->base.token,sizeof(pExpr->base.aliasName) - 1);

      // todo refactor: tscColumnListInsert part
      SColumnList ids = createColumnList(1, tsc_index.tableIndex, tsc_index.columnIndex);

      if (finalResult) {
        insertResultField(pQueryInfo, colIndex, &ids, resultSize, (int8_t)resultType, pExpr->base.aliasName, pExpr);
      } else {
        assert(ids.num == 1);
        tscColumnListInsert(pQueryInfo->colList, ids.ids[0].columnIndex, pExpr->base.uid, pSchema);
      }

      return TSDB_CODE_SUCCESS;
    }
    
    case TSDB_FUNC_TID_TAG: {
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
      if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
      }
    
      // no parameters or more than one parameter for function
      if (pItem->pNode->Expr.paramList == NULL || taosArrayGetSize(pItem->pNode->Expr.paramList) != 1) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
      
      tSqlExprItem* pParamItem = taosArrayGet(pItem->pNode->Expr.paramList, 0);
      tSqlExpr* pParam = pParamItem->pNode;

      SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByName(&pParam->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }
    
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
      SSchema* pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
  
      // functions can not be applied to normal columns
      int32_t numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
      if (tsc_index.columnIndex < numOfCols && tsc_index.columnIndex != TSDB_TBNAME_COLUMN_INDEX) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }
    
      if (tsc_index.columnIndex > 0) {
        tsc_index.columnIndex -= numOfCols;
      }
      
      // 2. valid the column type
      int16_t colType = 0;
      if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        colType = TSDB_DATA_TYPE_BINARY;
      } else {
        colType = pSchema[tsc_index.columnIndex].type;
      }
      
      if (colType == TSDB_DATA_TYPE_BOOL) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      tscColumnListInsert(pTableMetaInfo->tagColList, tsc_index.columnIndex, pTableMetaInfo->pTableMeta->id.uid,
                          &pSchema[tsc_index.columnIndex]);
      SSchema* pTagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);

      SSchema s = {0};
      if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        s = *tGetTbnameColumnSchema();
      } else {
        s = pTagSchema[tsc_index.columnIndex];
      }
      
      int16_t bytes = 0;
      int16_t type  = 0;
      int32_t inter = 0;

      int32_t ret = getResultDataInfo(s.type, s.bytes, TSDB_FUNC_TID_TAG, 0, &type, &bytes, &inter, 0, 0, NULL);
      assert(ret == TSDB_CODE_SUCCESS);
      
      s.type = (uint8_t)type;
      s.bytes = bytes;

      TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);
      tscAddFuncInSelectClause(pQueryInfo, 0, TSDB_FUNC_TID_TAG, &tsc_index, &s, TSDB_COL_TAG, getNewResColId(pCmd));
      
      return TSDB_CODE_SUCCESS;
    }

    case TSDB_FUNC_BLKINFO: {
      // no parameters or more than one parameter for function
      if (pItem->pNode->Expr.paramList != NULL && taosArrayGetSize(pItem->pNode->Expr.paramList) != 0) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SColumnIndex tsc_index = {.tableIndex = 0, .columnIndex = 0,};
      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);

      int32_t inter   = 0;
      int16_t resType = 0;
      int16_t bytes   = 0;

      getResultDataInfo(TSDB_DATA_TYPE_INT, 4, TSDB_FUNC_BLKINFO, 0, &resType, &bytes, &inter, 0, 0, NULL);

      SSchema s = {.name = "block_dist", .type = TSDB_DATA_TYPE_BINARY, .bytes = bytes};

      SExprInfo* pExpr =
          tscExprInsert(pQueryInfo, 0, TSDB_FUNC_BLKINFO, &tsc_index, resType, bytes, getNewResColId(pCmd), bytes, 0);
      tstrncpy(pExpr->base.aliasName, s.name, sizeof(pExpr->base.aliasName));

      SColumnList ids = createColumnList(1, tsc_index.tableIndex, tsc_index.columnIndex);
      insertResultField(pQueryInfo, 0, &ids, bytes, s.type, s.name, pExpr);

      pExpr->base.numOfParams = 1;
      pExpr->base.param[0].i64 = pTableMetaInfo->pTableMeta->tableInfo.rowSize;
      pExpr->base.param[0].nType = TSDB_DATA_TYPE_BIGINT;

      return TSDB_CODE_SUCCESS;
    }

    default: {
      pUdfInfo = isValidUdf(pQueryInfo->pUdfInfo, pItem->pNode->Expr.operand.z, pItem->pNode->Expr.operand.n);
      if (pUdfInfo == NULL) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg9);
      }

      if (pItem->pNode->Expr.paramList == NULL || taosArrayGetSize(pItem->pNode->Expr.paramList) <= 0) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg13);        
      }
      
      tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->Expr.paramList, 0);;
      if (pParamElem->pNode->tokenId != TK_ID) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByName(&pParamElem->pNode->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);

      // functions can not be applied to tags
      if (tsc_index.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      int32_t inter   = 0;
      int16_t resType = 0;
      int16_t bytes   = 0;
      getResultDataInfo(TSDB_DATA_TYPE_INT, 4, functionId, 0, &resType, &bytes, &inter, 0, false, pUdfInfo);

      SExprInfo* pExpr = tscExprAppend(pQueryInfo, functionId, &tsc_index, resType, bytes, getNewResColId(pCmd), inter, false);

      memset(pExpr->base.aliasName, 0, tListLen(pExpr->base.aliasName));
      getColumnName(pItem, pExpr->base.aliasName, pExpr->base.token, sizeof(pExpr->base.aliasName) - 1);

      SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, tsc_index.columnIndex);

      uint64_t uid = pTableMetaInfo->pTableMeta->id.uid;
      SColumnList ids = createColumnList(1, tsc_index.tableIndex, tsc_index.columnIndex);
      if (finalResult) {
        insertResultField(pQueryInfo, colIndex, &ids, pUdfInfo->resBytes, pUdfInfo->resType, pExpr->base.aliasName, pExpr);
      } else {
        for (int32_t i = 0; i < ids.num; ++i) {
          tscColumnListInsert(pQueryInfo->colList, tsc_index.columnIndex, uid, pSchema);
        }
      }
      tscInsertPrimaryTsSourceColumn(pQueryInfo, pTableMetaInfo->pTableMeta->id.uid);
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_TSC_INVALID_OPERATION;
}

// todo refactor
static SColumnList createColumnList(int32_t num, int16_t tableIndex, int32_t columnIndex) {
  assert(num == 1 && tableIndex >= 0);

  SColumnList columnList = {0};
  columnList.num = num;

  int32_t tsc_index = num - 1;
  columnList.ids[tsc_index].tableIndex = tableIndex;
  columnList.ids[tsc_index].columnIndex = columnIndex;

  return columnList;
}

void getColumnName(tSqlExprItem* pItem, char* resultFieldName, char* rawName, int32_t nameLength) {
  int32_t len = ((int32_t)pItem->pNode->exprToken.n < nameLength) ? (int32_t)pItem->pNode->exprToken.n : nameLength;
  strncpy(rawName, pItem->pNode->exprToken.z, len);

  if (pItem->aliasName != NULL) {
    int32_t aliasNameLen = (int32_t) strlen(pItem->aliasName);
    len = (aliasNameLen < nameLength)? aliasNameLen:nameLength;
    strncpy(resultFieldName, pItem->aliasName, len);
  } else {
    strncpy(resultFieldName, rawName, len);
  }
}

static bool isTablenameToken(SStrToken* token) {
  SStrToken tmpToken = *token;
  SStrToken tableToken = {0};

  extractTableNameFromToken(&tmpToken, &tableToken);
  return (tmpToken.n == strlen(TSQL_TBNAME_L) && strncasecmp(TSQL_TBNAME_L, tmpToken.z, tmpToken.n) == 0);
}

static int16_t doGetColumnIndex(SQueryInfo* pQueryInfo, int32_t tsc_index, SStrToken* pToken) {
  STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, tsc_index)->pTableMeta;

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

int32_t doGetColumnIndexByName(SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex, char* msg) {
  const char* msg0 = "ambiguous column name";
  const char* msg1 = "invalid column name";

  if (pToken->n == 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (isTablenameToken(pToken)) {
    pIndex->columnIndex = TSDB_TBNAME_COLUMN_INDEX;
  } else if (strlen(DEFAULT_PRIMARY_TIMESTAMP_COL_NAME) == pToken->n &&
            strncasecmp(pToken->z, DEFAULT_PRIMARY_TIMESTAMP_COL_NAME, pToken->n) == 0) {
    pIndex->columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX; // just make runtime happy, need fix java test case InsertSpecialCharacterJniTest
  } else {
    // not specify the table name, try to locate the table tsc_index by column name
    if (pIndex->tableIndex == COLUMN_INDEX_INITIAL_VAL) {
      for (int16_t i = 0; i < pQueryInfo->numOfTables; ++i) {
        int16_t colIndex = doGetColumnIndex(pQueryInfo, i, pToken);

        if (colIndex != COLUMN_INDEX_INITIAL_VAL) {
          if (pIndex->columnIndex != COLUMN_INDEX_INITIAL_VAL) {
            return invalidOperationMsg(msg, msg0);
          } else {
            pIndex->tableIndex = i;
            pIndex->columnIndex = colIndex;
          }
        }
      }
    } else {  // table tsc_index is valid, get the column tsc_index
      int16_t colIndex = doGetColumnIndex(pQueryInfo, pIndex->tableIndex, pToken);
      if (colIndex != COLUMN_INDEX_INITIAL_VAL) {
        pIndex->columnIndex = colIndex;
      }
    }

    if (pIndex->columnIndex == COLUMN_INDEX_INITIAL_VAL) {
      return invalidOperationMsg(msg, msg1);
    }
  }

  if (COLUMN_INDEX_VALIDE(*pIndex)) {
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_TSC_INVALID_OPERATION;
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
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getTableIndexByName(SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex) {
  SStrToken tableToken = {0};
  extractTableNameFromToken(pToken, &tableToken);

  if (getTableIndexImpl(&tableToken, pQueryInfo, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getColumnIndexByName(const SStrToken* pToken, SQueryInfo* pQueryInfo, SColumnIndex* pIndex, char* msg) {
  if (pQueryInfo->pTableMetaInfo == NULL || pQueryInfo->numOfTables == 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  SStrToken tmpToken = *pToken;

  if (getTableIndexByName(&tmpToken, pQueryInfo, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return doGetColumnIndexByName(&tmpToken, pQueryInfo, pIndex, msg);
}

int32_t setShowInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  SSqlCmd*        pCmd = &pSql->cmd;
  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);

  pCmd->command = TSDB_SQL_SHOW;

  const char* msg1 = "invalid name";
  const char* msg2 = "wildcard string should be less than %d characters";
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
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      if (pDbPrefixToken->n <= 0) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      if (tscValidateName(pDbPrefixToken) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pDbPrefixToken);
      if (ret != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }
    }

    // show table/stable like 'xxxx', set the like pattern for show tables
    SStrToken* pPattern = &pShowInfo->pattern;
    if (pPattern->type != 0) {
      pPattern->n = strdequote(pPattern->z);

      if (pPattern->n <= 0) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      if (pPattern->n > tsMaxWildCardsLen){
        char tmp[64] = {0};
        sprintf(tmp, msg2, tsMaxWildCardsLen);
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), tmp);
      }
    }
  } else if (showType == TSDB_MGMT_TABLE_VNODES) {
    if (pShowInfo->prefix.type == 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "No specified ip of dnode");
    }

    // show vnodes may be ip addr of dnode in payload
    SStrToken* pDnodeIp = &pShowInfo->prefix;
    if (pDnodeIp->n >= TSDB_IPv4ADDR_LEN) {  // ip addr is too long
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    if (!validateIpAddress(pDnodeIp->z, pDnodeIp->n)) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
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
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  strncpy(pCmd->payload, idStr->z, idStr->n);

  const char delim = ':';
  char* connIdStr = strtok(idStr->z, &delim);
  char* queryIdStr = strtok(NULL, &delim);

  int32_t connId = (int32_t)strtol(connIdStr, NULL, 10);
  if (connId <= 0) {
    memset(pCmd->payload, 0, strlen(pCmd->payload));
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (killType == TSDB_SQL_KILL_CONNECTION) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t queryId = (int32_t)strtol(queryIdStr, NULL, 10);
  if (queryId <= 0) {
    memset(pCmd->payload, 0, strlen(pCmd->payload));
    if (killType == TSDB_SQL_KILL_QUERY) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    } else {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }
  }
  
  return TSDB_CODE_SUCCESS;
}
static int32_t setCompactVnodeInfo(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  SSqlCmd* pCmd = &pSql->cmd;
  pCmd->command = pInfo->type;

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
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  assert(tscGetNumOfTags(pTableMetaInfo->pTableMeta) >= 0);

  int16_t bytes = 0;
  int16_t type = 0;
  int32_t interBytes = 0;
  
  size_t size = tscNumOfExprs(pQueryInfo);
  for (int32_t k = 0; k < size; ++k) {
    SExprInfo*   pExpr = tscExprGet(pQueryInfo, k);
    int16_t functionId = aAggs[pExpr->base.functionId].stableFuncId;

    int32_t colIndex = pExpr->base.colInfo.colIndex;
    SSchema* pSrcSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, colIndex);
    
    if ((functionId >= TSDB_FUNC_SUM && functionId <= TSDB_FUNC_TWA) ||
        (functionId >= TSDB_FUNC_FIRST_DST && functionId <= TSDB_FUNC_STDDEV_DST) ||
        (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_IRATE)) {
      if (getResultDataInfo(pSrcSchema->type, pSrcSchema->bytes, functionId, (int32_t)pExpr->base.param[0].i64, &type, &bytes,
                            &interBytes, 0, true, NULL) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      tscExprUpdate(pQueryInfo, k, functionId, pExpr->base.colInfo.colIndex, TSDB_DATA_TYPE_BINARY, bytes);
      // todo refactor
      pExpr->base.interBytes = interBytes;
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
  
  size_t size = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo*   pExpr = tscExprGet(pQueryInfo, i);
    SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, pExpr->base.colInfo.colIndex);
    
    // the final result size and type in the same as query on single table.
    // so here, set the flag to be false;
    int32_t inter = 0;
    
    int32_t functionId = pExpr->base.functionId;
    if (functionId < 0) {
      continue;
    }

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

    getResultDataInfo(pSchema->type, pSchema->bytes, functionId, 0, &pExpr->base.resType, &pExpr->base.resBytes, &inter,
                      0, false, NULL);
  }
}

bool hasUnsupportFunctionsForSTableQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg1 = "TWA/Diff/Derivative/Irate are not allowed to apply to super table directly";
  const char* msg2 = "TWA/Diff/Derivative/Irate only support group by tbname for super table query";
  const char* msg3 = "functions not support for super table query";

  // filter sql function not supported by metric query yet.
  size_t size = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    int32_t functionId = tscExprGet(pQueryInfo, i)->base.functionId;
    if (functionId < 0) {
      continue;
    }

    if ((aAggs[functionId].status & TSDB_FUNCSTATE_STABLE) == 0) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      return true;
    }
  }

  if (tscIsTWAQuery(pQueryInfo) || tscIsDiffDerivQuery(pQueryInfo) || tscIsIrateQuery(pQueryInfo)) {
    if (pQueryInfo->groupbyExpr.numOfGroupCols == 0) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      return true;
    }

    SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, 0);
    if (pColIndex->colIndex != TSDB_TBNAME_COLUMN_INDEX) {
      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      return true;
    }
  } else if (tscIsSessionWindowQuery(pQueryInfo)) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    return true;
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

bool groupbyTbname(SQueryInfo* pQueryInfo) {
  if (pQueryInfo->groupbyExpr.columnInfo == NULL ||
    taosArrayGetSize(pQueryInfo->groupbyExpr.columnInfo) == 0) {
    return false;
  }

  size_t s = taosArrayGetSize(pQueryInfo->groupbyExpr.columnInfo);
  for (int32_t i = 0; i < s; i++) {
    SColIndex* colIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, i);
    if (colIndex->colIndex == TSDB_TBNAME_COLUMN_INDEX) {
      return true;
    }
  }

  return false;
}





static bool functionCompatibleCheck(SQueryInfo* pQueryInfo, bool joinQuery, bool twQuery) {
  int32_t startIdx = 0;
  int32_t aggUdf = 0;
  int32_t scalarUdf = 0;
  int32_t prjNum = 0;
  int32_t aggNum = 0;
  int32_t countTbname = 0;

  size_t numOfExpr = tscNumOfExprs(pQueryInfo);
  assert(numOfExpr > 0);

  int32_t factor = INT32_MAX;

  // diff function cannot be executed with other function
  // arithmetic function can be executed with other arithmetic functions
  size_t size = tscNumOfExprs(pQueryInfo);
  
  for (int32_t i = startIdx; i < size; ++i) {
    SExprInfo* pExpr1 = tscExprGet(pQueryInfo, i);

    int16_t functionId = pExpr1->base.functionId;
    if (functionId < 0) {
       SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * functionId - 1);
       pUdfInfo->funcType == TSDB_UDF_TYPE_AGGREGATE ? ++aggUdf : ++scalarUdf;

       continue;
    }

    if (functionId == TSDB_FUNC_TAGPRJ || functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY) {
      ++prjNum;

      continue;
    }

    if (functionId == TSDB_FUNC_PRJ) {
      ++prjNum;
    }

    if (functionId == TSDB_FUNC_PRJ && (pExpr1->base.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX || TSDB_COL_IS_UD_COL(pExpr1->base.colInfo.flag))) {
      continue;
    }

    if (factor == INT32_MAX) {
      factor = functionCompatList[functionId];
    } else {
      if (functionCompatList[functionId] != factor) {
        return false;
      } else {
        if (factor == -1) { // two functions with the same -1 flag
          return false;
        }
      }
    }

    if (functionId == TSDB_FUNC_LAST_ROW && (joinQuery || twQuery || !groupbyTagsOrNull(pQueryInfo))) {
      return false;
    }

    if (functionId == TSDB_FUNC_COUNT && (pExpr1->base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX || TSDB_COL_IS_TAG(pExpr1->base.colInfo.flag))) {
      ++countTbname;
    }
  }

  aggNum = (int32_t)size - prjNum - aggUdf - scalarUdf - countTbname;

  assert(aggNum >= 0);

  if (aggUdf > 0 && (prjNum > 0 || aggNum > 0 || scalarUdf > 0)) {
    return false;
  }

  if (scalarUdf > 0 && aggNum > 0) {
    return false;
  }

  if (countTbname && (prjNum > 0 || aggNum > 0 || scalarUdf > 0 || aggUdf > 0)) {
    return false;
  }

  return true;
}

int32_t validateGroupbyNode(SQueryInfo* pQueryInfo, SArray* pList, SSqlCmd* pCmd) {
  const char* msg1 = "too many columns in group by clause";
  const char* msg2 = "invalid column name in group by clause";
  const char* msg3 = "columns from one table allowed as group by columns";
  const char* msg4 = "join query does not support group by";
  const char* msg5 = "not allowed column type for group by";
  const char* msg6 = "tags not allowed for table query";
  const char* msg7 = "not support group by expression";
  const char* msg8 = "normal column can only locate at the end of group by clause";

  // todo : handle two tables situation
  STableMetaInfo* pTableMetaInfo = NULL;
  if (pList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pQueryInfo->numOfTables > 1) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  SGroupbyExpr* pGroupExpr = &pQueryInfo->groupbyExpr;
  if (pGroupExpr->columnInfo == NULL) {
    pGroupExpr->columnInfo = taosArrayInit(4, sizeof(SColIndex));
  }

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }

  if (pGroupExpr->columnInfo == NULL || pQueryInfo->colList == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pGroupExpr->numOfGroupCols = (int16_t)taosArrayGetSize(pList);
  if (pGroupExpr->numOfGroupCols > TSDB_MAX_TAGS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  SSchema *pSchema       = NULL;
  int32_t tableIndex     = COLUMN_INDEX_INITIAL_VAL;
  int32_t numOfGroupCols = 0;

  size_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    tVariantListItem * pItem = taosArrayGet(pList, i);
    tVariant* pVar = &pItem->pVar;

    SStrToken token = {pVar->nLen, pVar->nType, pVar->pz};

    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(&token, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    if (tableIndex == COLUMN_INDEX_INITIAL_VAL) {
      tableIndex = tsc_index.tableIndex;
    } else if (tableIndex != tsc_index.tableIndex) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;

    if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      pSchema = tGetTbnameColumnSchema();
    } else {
      pSchema = tscGetTableColumnSchema(pTableMeta, tsc_index.columnIndex);
    }

    int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
    bool groupTag = (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX || tsc_index.columnIndex >= numOfCols);

    if (groupTag) {
      if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      int32_t relIndex = tsc_index.columnIndex;
      if (tsc_index.columnIndex != TSDB_TBNAME_COLUMN_INDEX) {
        relIndex -= numOfCols;
      }

      SColIndex colIndex = { .colIndex = relIndex, .flag = TSDB_COL_TAG, .colId = pSchema->colId, };
      strncpy(colIndex.name, pSchema->name, tListLen(colIndex.name));
      taosArrayPush(pGroupExpr->columnInfo, &colIndex);
      
      tsc_index.columnIndex = relIndex;
      tscColumnListInsert(pTableMetaInfo->tagColList, tsc_index.columnIndex, pTableMeta->id.uid, pSchema);
    } else {
      // check if the column type is valid, here only support the bool/tinyint/smallint/bigint group by
      if (pSchema->type == TSDB_DATA_TYPE_FLOAT || pSchema->type == TSDB_DATA_TYPE_DOUBLE) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      tscColumnListInsert(pQueryInfo->colList, tsc_index.columnIndex, pTableMeta->id.uid, pSchema);
      
      SColIndex colIndex = { .colIndex = tsc_index.columnIndex, .flag = TSDB_COL_NORMAL, .colId = pSchema->colId };
      strncpy(colIndex.name, pSchema->name, tListLen(colIndex.name));

      taosArrayPush(pGroupExpr->columnInfo, &colIndex);
      pQueryInfo->groupbyExpr.orderType = TSDB_ORDER_ASC;
      numOfGroupCols++;
    }
  }

  // 1. only one normal column allowed in the group by clause
  // 2. the normal column in the group by clause can only located in the end position
  if (numOfGroupCols > 1) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
  }

  for(int32_t i = 0; i < num; ++i) {
    SColIndex* pIndex = taosArrayGet(pGroupExpr->columnInfo, i);
    if (TSDB_COL_IS_NORMAL_COL(pIndex->flag) && i != num - 1) {
     return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg8);
    }
  }

  pQueryInfo->groupbyExpr.tableIndex = tableIndex;
  return TSDB_CODE_SUCCESS;
}


static SColumnFilterInfo* addColumnFilterInfo(SColumnFilterList* filterList) {
  int32_t size = (filterList->numOfFilters) + 1;

  char* tmp = (char*) realloc((void*)(filterList->filterInfo), sizeof(SColumnFilterInfo) * (size));
  if (tmp != NULL) {
    filterList->filterInfo = (SColumnFilterInfo*)tmp;
  } else {
    return NULL;
  }

  filterList->numOfFilters = size;

  SColumnFilterInfo* pColFilterInfo = &(filterList->filterInfo[size - 1]);
  memset(pColFilterInfo, 0, sizeof(SColumnFilterInfo));

  return pColFilterInfo;
}

static int32_t doExtractColumnFilterInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, int32_t timePrecision, SColumnFilterInfo* pColumnFilter,
                                         int16_t colType, tSqlExpr* pExpr) {
  const char* msg = "not supported filter condition";

  tSqlExpr *pRight = pExpr->pRight;

  if (colType >= TSDB_DATA_TYPE_TINYINT && colType <= TSDB_DATA_TYPE_BIGINT) {
    colType = TSDB_DATA_TYPE_BIGINT;
  } else if (colType == TSDB_DATA_TYPE_FLOAT || colType == TSDB_DATA_TYPE_DOUBLE) {
    colType = TSDB_DATA_TYPE_DOUBLE;
  } else if ((colType == TSDB_DATA_TYPE_TIMESTAMP) && (TSDB_DATA_TYPE_BINARY == pRight->value.nType)) {
    int retVal = setColumnFilterInfoForTimestamp(pCmd, pQueryInfo, &pRight->value);
    if (TSDB_CODE_SUCCESS != retVal) {
      return retVal;
    }
  } else if ((colType == TSDB_DATA_TYPE_TIMESTAMP) && (TSDB_DATA_TYPE_BIGINT == pRight->value.nType)) {
    if (pRight->flags & (1 << EXPR_FLAG_NS_TIMESTAMP)) {
      pRight->value.i64 =
          convertTimePrecision(pRight->value.i64, TSDB_TIME_PRECISION_NANO, timePrecision);
      pRight->flags &= ~(1 << EXPR_FLAG_NS_TIMESTAMP);
    }
  }

  int32_t retVal = TSDB_CODE_SUCCESS;

  int32_t bufLen = 0;
  if (IS_NUMERIC_TYPE(pRight->value.nType)) {
    bufLen = 60;
  } else {
    /*
     * make memory sanitizer happy;
     */
    if (pRight->value.nLen == 0) {
      bufLen = pRight->value.nLen + 2;
    } else {
      bufLen = pRight->value.nLen + 1;
    }
  }

  if (pExpr->tokenId == TK_LE || pExpr->tokenId == TK_LT) {
    retVal = tVariantDump(&pRight->value, (char*)&pColumnFilter->upperBndd, colType, false);

  // TK_GT,TK_GE,TK_EQ,TK_NE are based on the pColumn->lowerBndd
  } else if (pExpr->tokenId == TK_IN) {
    tVariant *pVal; 
    if (pRight->tokenId != TK_SET || !serializeExprListToVariant(pRight->Expr.paramList, &pVal, colType, timePrecision)) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
    }
    pColumnFilter->pz  = (int64_t)calloc(1, pVal->nLen + 1);
    pColumnFilter->len = pVal->nLen;
    pColumnFilter->filterstr = 1;
    memcpy((char *)(pColumnFilter->pz), (char *)(pVal->pz), pVal->nLen);

    tVariantDestroy(pVal);
    free(pVal);

  } else if (colType == TSDB_DATA_TYPE_BINARY) {
    pColumnFilter->pz = (int64_t)calloc(1, bufLen * TSDB_NCHAR_SIZE);
    pColumnFilter->len = pRight->value.nLen;
    retVal = tVariantDump(&pRight->value, (char*)pColumnFilter->pz, colType, false);

  } else if (colType == TSDB_DATA_TYPE_NCHAR) {
    // pRight->value.nLen + 1 is larger than the actual nchar string length
    pColumnFilter->pz = (int64_t)calloc(1, bufLen * TSDB_NCHAR_SIZE);
    retVal = tVariantDump(&pRight->value, (char*)pColumnFilter->pz, colType, false);
    size_t len = twcslen((wchar_t*)pColumnFilter->pz);
    pColumnFilter->len = len * TSDB_NCHAR_SIZE;

  } else {
    retVal = tVariantDump(&pRight->value, (char*)&pColumnFilter->lowerBndd, colType, false);
  }

  if (retVal != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
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
    case TK_IN:
      pColumnFilter->lowerRelOptr = TSDB_RELATION_IN;
      break;
    default:
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
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
  SArray* pList = pExpr->Expr.paramList;

  int32_t size = (int32_t) taosArrayGetSize(pList);
  if (size <= 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
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
      return TSDB_CODE_TSC_INVALID_OPERATION;
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
  const char* msg4 = "primary key not support this operator";

  SColumn* pColumn = tscColumnListInsert(pQueryInfo->colList, pIndex->columnIndex, pTableMeta->id.uid, pSchema);
  SColumnFilterInfo* pColFilter = NULL;

  /*
   * in case of TK_AND filter condition, we first find the corresponding column and build the query condition together
   * the already existed condition.
   */
  if (sqlOptr == TK_AND) {
    // this is a new filter condition on this column
    if (pColumn->info.flist.numOfFilters == 0) {
      pColFilter = addColumnFilterInfo(&pColumn->info.flist);
    } else {  // update the existed column filter information, find the filter info here
      pColFilter = &pColumn->info.flist.filterInfo[0];
    }

    if (pColFilter == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  } else if (sqlOptr == TK_OR) {
    // TODO fixme: failed to invalid the filter expression: "col1 = 1 OR col2 = 2"
    pColFilter = addColumnFilterInfo(&pColumn->info.flist);
    if (pColFilter == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  } else {  // error;
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  pColFilter->filterstr =
      ((pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) ? 1 : 0);

  if (pColFilter->filterstr) {
    if (pExpr->tokenId != TK_EQ
      && pExpr->tokenId != TK_NE
      && pExpr->tokenId != TK_ISNULL
      && pExpr->tokenId != TK_NOTNULL
      && pExpr->tokenId != TK_LIKE
      && pExpr->tokenId != TK_IN) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  } else {
    if (pExpr->tokenId == TK_LIKE) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
    
    if (pSchema->type == TSDB_DATA_TYPE_BOOL) {
      int32_t t = pExpr->tokenId;
      if (t != TK_EQ && t != TK_NE && t != TK_NOTNULL && t != TK_ISNULL && t != TK_IN) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }
    }
  }

  pColumn->columnIndex = pIndex->columnIndex;
  pColumn->tableUid = pTableMeta->id.uid;
  if (pColumn->columnIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX && pExpr->tokenId == TK_IN) {
     return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  STableComInfo tinfo = tscGetTableInfo(pTableMeta);
  return doExtractColumnFilterInfo(pCmd, pQueryInfo, tinfo.precision, pColFilter, pSchema->type, pExpr);
}

static int32_t getTablenameCond(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr* pTableCond, SStringBuilder* sb) {
  const char* msg0 = "invalid table name list";
  const char* msg1 = "not string following like";

  if (pTableCond == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSqlExpr* pLeft = pTableCond->pLeft;
  tSqlExpr* pRight = pTableCond->pRight;

  if (!isTablenameToken(&pLeft->columnName)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  int32_t ret = TSDB_CODE_SUCCESS;

  if (pTableCond->tokenId == TK_IN) {
    ret = tablenameListToString(pRight, sb);
  } else if (pTableCond->tokenId == TK_LIKE) {
    if (pRight->tokenId != TK_STRING) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
    
    ret = tablenameCondToString(pRight, sb);
  }

  if (ret != TSDB_CODE_SUCCESS) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }

  return ret;
}

static int32_t getColumnQueryCondInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr* pExpr, int32_t relOptr) {
  if (pExpr == NULL) {
    pQueryInfo->onlyHasTagCond &= true;
    return TSDB_CODE_SUCCESS;
  }
  pQueryInfo->onlyHasTagCond &= false;

  if (!tSqlExprIsParentOfLeaf(pExpr)) {  // internal node
    int32_t ret = getColumnQueryCondInfo(pCmd, pQueryInfo, pExpr->pLeft, pExpr->tokenId);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    return getColumnQueryCondInfo(pCmd, pQueryInfo, pExpr->pRight, pExpr->tokenId);
  } else {  // handle leaf node
    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(&pExpr->pLeft->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    return extractColumnFilterInfo(pCmd, pQueryInfo, &tsc_index, pExpr, relOptr);
  }
}

static int32_t checkAndSetJoinCondInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr* pExpr) {
  int32_t code = 0;
  const char* msg1 = "timestamp required for join tables";
  const char* msg2 = "only support one join tag for each table";
  const char* msg3 = "type of join columns must be identical";
  const char* msg4 = "invalid column name in join condition";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (!tSqlExprIsParentOfLeaf(pExpr)) {
    code = checkAndSetJoinCondInfo(pCmd, pQueryInfo, pExpr->pLeft);
    if (code) {
      return code;
    }

    return checkAndSetJoinCondInfo(pCmd, pQueryInfo, pExpr->pRight);
  }

  SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(&pExpr->pLeft->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
  SSchema* pTagSchema1 = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, tsc_index.columnIndex);

  assert(tsc_index.tableIndex >= 0 && tsc_index.tableIndex < TSDB_MAX_JOIN_TABLE_NUM);

  SJoinNode **leftNode = &pQueryInfo->tagCond.joinInfo.joinTables[tsc_index.tableIndex];
  if (*leftNode == NULL) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  (*leftNode)->uid = pTableMetaInfo->pTableMeta->id.uid;
  (*leftNode)->tagColId = pTagSchema1->colId;

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;

    tsc_index.columnIndex = tsc_index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
    if (tscColumnExists(pTableMetaInfo->tagColList, pTagSchema1->colId, pTableMetaInfo->pTableMeta->id.uid) < 0) {
      tscColumnListInsert(pTableMetaInfo->tagColList, tsc_index.columnIndex, pTableMeta->id.uid, pTagSchema1);
      atomic_add_fetch_32(&pTableMetaInfo->joinTagNum, 1);

      if (pTableMetaInfo->joinTagNum > 1) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
    }
  }

  int16_t leftIdx = tsc_index.tableIndex;

  tsc_index = (SColumnIndex)COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(&pExpr->pRight->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
  SSchema* pTagSchema2 = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, tsc_index.columnIndex);

  assert(tsc_index.tableIndex >= 0 && tsc_index.tableIndex < TSDB_MAX_JOIN_TABLE_NUM);

  SJoinNode **rightNode = &pQueryInfo->tagCond.joinInfo.joinTables[tsc_index.tableIndex];
  if (*rightNode == NULL) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  (*rightNode)->uid = pTableMetaInfo->pTableMeta->id.uid;
  (*rightNode)->tagColId = pTagSchema2->colId;

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    tsc_index.columnIndex = tsc_index.columnIndex - tscGetNumOfColumns(pTableMeta);
    if (tscColumnExists(pTableMetaInfo->tagColList, pTagSchema2->colId, pTableMeta->id.uid) < 0) {

      tscColumnListInsert(pTableMetaInfo->tagColList, tsc_index.columnIndex, pTableMeta->id.uid, pTagSchema2);
      atomic_add_fetch_32(&pTableMetaInfo->joinTagNum, 1);
      
      if (pTableMetaInfo->joinTagNum > 1) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }
    }
  }

  int16_t rightIdx = tsc_index.tableIndex;

  if (pTagSchema1->type != pTagSchema2->type) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  if ((*leftNode)->tagJoin == NULL) {
    (*leftNode)->tagJoin = taosArrayInit(2, sizeof(int16_t));
  }

  if ((*rightNode)->tagJoin == NULL) {
    (*rightNode)->tagJoin = taosArrayInit(2, sizeof(int16_t));
  }

  taosArrayPush((*leftNode)->tagJoin, &rightIdx);
  taosArrayPush((*rightNode)->tagJoin, &leftIdx);

  pQueryInfo->tagCond.joinInfo.hasJoin = true;

  return TSDB_CODE_SUCCESS;

}

static int32_t getJoinCondInfo(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr* pExpr) {
  if (pExpr == NULL) {
    pQueryInfo->onlyHasTagCond &= true;
    return TSDB_CODE_SUCCESS;
  }

  return checkAndSetJoinCondInfo(pCmd, pQueryInfo, pExpr);
}

static int32_t validateSQLExpr(SSqlCmd* pCmd, tSqlExpr* pExpr, SQueryInfo* pQueryInfo, SColumnList* pList,
                               int32_t* type, uint64_t* uid) {
  if (pExpr->type == SQL_NODE_TABLE_COLUMN) {
    if (*type == NON_ARITHMEIC_EXPR) {
      *type = NORMAL_ARITHMETIC;
    } else if (*type == AGG_ARIGHTMEIC) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(&pExpr->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // if column is timestamp, bool, binary, nchar, not support arithmetic, so return invalid sql
    STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex)->pTableMeta;
    SSchema*    pSchema = tscGetTableSchema(pTableMeta) + tsc_index.columnIndex;
    
    if ((pSchema->type == TSDB_DATA_TYPE_TIMESTAMP) || (pSchema->type == TSDB_DATA_TYPE_BOOL) ||
        (pSchema->type == TSDB_DATA_TYPE_BINARY) || (pSchema->type == TSDB_DATA_TYPE_NCHAR)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    pList->ids[pList->num++] = tsc_index;
  } else if ((pExpr->tokenId == TK_FLOAT && (isnan(pExpr->value.dKey) || isinf(pExpr->value.dKey))) ||
             pExpr->tokenId == TK_NULL) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  } else if (pExpr->type == SQL_NODE_SQLFUNCTION) {
    if (*type == NON_ARITHMEIC_EXPR) {
      *type = AGG_ARIGHTMEIC;
    } else if (*type == NORMAL_ARITHMETIC) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    int32_t outputIndex = (int32_t)tscNumOfExprs(pQueryInfo);
  
    tSqlExprItem item = {.pNode = pExpr, .aliasName = NULL};
  
    // sql function list in selection clause.
    // Append the sqlExpr into exprList of pQueryInfo structure sequentially
    pExpr->functionId = isValidFunction(pExpr->Expr.operand.z, pExpr->Expr.operand.n);
    if (pExpr->functionId < 0) {
      SUdfInfo* pUdfInfo = NULL;
      pUdfInfo = isValidUdf(pQueryInfo->pUdfInfo, pExpr->Expr.operand.z, pExpr->Expr.operand.n);
      if (pUdfInfo == NULL) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "invalid function name");
      }
    }

    if (addExprAndResultField(pCmd, pQueryInfo, outputIndex, &item, false, NULL) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // It is invalid in case of more than one sqlExpr, such as first(ts, k) - last(ts, k)
    int32_t inc = (int32_t) tscNumOfExprs(pQueryInfo) - outputIndex;
    if (inc > 1) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // Not supported data type in arithmetic expression
    uint64_t id = -1;
    for(int32_t i = 0; i < inc; ++i) {
      SExprInfo* p1 = tscExprGet(pQueryInfo, i + outputIndex);

      int16_t t = p1->base.resType;
      if (t == TSDB_DATA_TYPE_BINARY || t == TSDB_DATA_TYPE_NCHAR || t == TSDB_DATA_TYPE_BOOL || t == TSDB_DATA_TYPE_TIMESTAMP) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (i == 0) {
        id = p1->base.uid;
        continue;
      }

      if (id != p1->base.uid) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
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
      return TSDB_CODE_TSC_INVALID_OPERATION;
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

  if (pLeft->tokenId >= TK_BOOL && pLeft->tokenId <= TK_BINARY && (optr == TK_NOTNULL || optr == TK_ISNULL)) {
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
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    return false;
  }

  SColumnIndex rightIndex = COLUMN_INDEX_INITIALIZER;

  if (getColumnIndexByName(&pRight->columnName, pQueryInfo, &rightIndex, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    return false;
  }

  // todo extract function
  STableMetaInfo* pLeftMeterMeta = tscGetMetaInfo(pQueryInfo, pLeftIndex->tableIndex);
  SSchema*        pLeftSchema = tscGetTableSchema(pLeftMeterMeta->pTableMeta);
  int16_t         leftType = pLeftSchema[pLeftIndex->columnIndex].type;

  tscColumnListInsert(pQueryInfo->colList, pLeftIndex->columnIndex, pLeftMeterMeta->pTableMeta->id.uid, &pLeftSchema[pLeftIndex->columnIndex]);

  STableMetaInfo* pRightMeterMeta = tscGetMetaInfo(pQueryInfo, rightIndex.tableIndex);
  SSchema*        pRightSchema = tscGetTableSchema(pRightMeterMeta->pTableMeta);
  int16_t         rightType = pRightSchema[rightIndex.columnIndex].type;

  tscColumnListInsert(pQueryInfo->colList, rightIndex.columnIndex, pRightMeterMeta->pTableMeta->id.uid, &pRightSchema[rightIndex.columnIndex]);

  if (leftType != rightType) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    return false;
  } else if (pLeftIndex->tableIndex == rightIndex.tableIndex) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
    return false;
  }

  // table to table/ super table to super table are allowed
  if (UTIL_TABLE_IS_SUPER_TABLE(pLeftMeterMeta) != UTIL_TABLE_IS_SUPER_TABLE(pRightMeterMeta)) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
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
      return invalidOperationMsg(msgBuf, msg);
    }

    *parent = tSqlExprCreate((*parent), pExpr, parentOptr);
  } else {
    *parent = pExpr;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateNullExpr(tSqlExpr* pExpr, char* msgBuf) {
  const char* msg = "only support is [not] null";

  tSqlExpr* pRight = pExpr->pRight;
  if (pRight->tokenId == TK_NULL && (!(pExpr->tokenId == TK_ISNULL || pExpr->tokenId == TK_NOTNULL))) {
    return invalidOperationMsg(msgBuf, msg);
  }

  return TSDB_CODE_SUCCESS;
}

// check for like expression
static int32_t validateLikeExpr(tSqlExpr* pExpr, STableMeta* pTableMeta, int32_t tsc_index, char* msgBuf) {
  const char* msg1 = "wildcard string should be less than %d characters";
  const char* msg2 = "illegal column name";

  tSqlExpr* pLeft  = pExpr->pLeft;
  tSqlExpr* pRight = pExpr->pRight;

  if (pExpr->tokenId == TK_LIKE) {
    if (pRight->value.nLen > tsMaxWildCardsLen) {
      char tmp[64] = {0};
      sprintf(tmp, msg1, tsMaxWildCardsLen);
      return invalidOperationMsg(msgBuf, tmp);
    }

    SSchema* pSchema = tscGetTableSchema(pTableMeta);
    if ((!isTablenameToken(&pLeft->columnName)) && !IS_VAR_DATA_TYPE(pSchema[tsc_index].type)) {
      return invalidOperationMsg(msgBuf, msg2);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t handleExprInQueryCond(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr** pExpr, SCondExpr* pCondExpr,
                                     int32_t* type, int32_t parentOptr, int32_t* tbIdx, bool joinQuery) {
  const char* msg1 = "table query cannot use tags filter";
  const char* msg2 = "illegal column name";
  const char* msg3 = "only one query time range allowed";
  const char* msg4 = "too many join tables";
  const char* msg5 = "not support ordinary column join";
  const char* msg6 = "only one query condition on tbname allowed";
  const char* msg7 = "only in/like allowed in filter table name";
  const char* msg8 = "illegal condition expression";

  tSqlExpr* pLeft  = (*pExpr)->pLeft;
  tSqlExpr* pRight = (*pExpr)->pRight;

  int32_t ret = TSDB_CODE_SUCCESS;

  SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
  if (getColumnIndexByName(&pLeft->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  *tbIdx = tsc_index.tableIndex;

  assert(tSqlExprIsParentOfLeaf(*pExpr));

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
  STableMeta*     pTableMeta = pTableMetaInfo->pTableMeta;

  // validate the null expression
  int32_t code = validateNullExpr(*pExpr, tscGetErrorMsgPayload(pCmd));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // validate the like expression
  code = validateLikeExpr(*pExpr, pTableMeta, tsc_index.columnIndex, tscGetErrorMsgPayload(pCmd));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, tsc_index.columnIndex);
  if (pSchema->type == TSDB_DATA_TYPE_TIMESTAMP && tsc_index.columnIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {  // query on time range
    if (!validateJoinExprNode(pCmd, pQueryInfo, *pExpr, &tsc_index)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // set join query condition
    if (pRight->tokenId == TK_ID) {  // no need to keep the timestamp join condition
      TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_QUERY);
      pCondExpr->tsJoin = true;

      assert(tsc_index.tableIndex >= 0 && tsc_index.tableIndex < TSDB_MAX_JOIN_TABLE_NUM);
      SJoinNode **leftNode = &pQueryInfo->tagCond.joinInfo.joinTables[tsc_index.tableIndex];
      if (*leftNode == NULL) {
        *leftNode = calloc(1, sizeof(SJoinNode));
        if (*leftNode == NULL) {
          return TSDB_CODE_TSC_OUT_OF_MEMORY;
        }
      }

      int16_t leftIdx = tsc_index.tableIndex;

      if (getColumnIndexByName(&pRight->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      if (tsc_index.tableIndex < 0 || tsc_index.tableIndex >= TSDB_MAX_JOIN_TABLE_NUM) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
      }

      SJoinNode **rightNode = &pQueryInfo->tagCond.joinInfo.joinTables[tsc_index.tableIndex];
      if (*rightNode == NULL) {
        *rightNode = calloc(1, sizeof(SJoinNode));
        if (*rightNode == NULL) {
          return TSDB_CODE_TSC_OUT_OF_MEMORY;
        }
      }

      int16_t rightIdx = tsc_index.tableIndex;

      if ((*leftNode)->tsJoin == NULL) {
        (*leftNode)->tsJoin = taosArrayInit(2, sizeof(int16_t));
      }

      if ((*rightNode)->tsJoin == NULL) {
        (*rightNode)->tsJoin = taosArrayInit(2, sizeof(int16_t));
      }

      taosArrayPush((*leftNode)->tsJoin, &rightIdx);
      taosArrayPush((*rightNode)->tsJoin, &leftIdx);

      /*
       * To release expression, e.g., m1.ts = m2.ts,
       * since this expression is used to set the join query type
       */
      tSqlExprDestroy(*pExpr);
    } else {
      ret = setExprToCond(&pCondExpr->pTimewindow, *pExpr, msg3, parentOptr, pCmd->payload);
    }

    *pExpr = NULL;  // remove this expression
    *type = TSQL_EXPR_TS;
  } else if (tsc_index.columnIndex >= tscGetNumOfColumns(pTableMeta) || tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    // query on tags, check for tag query condition
    if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    // in case of in operator, keep it in a seprate attribute
    if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      if (!validTableNameOptr(*pExpr)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
      }
  
      if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (pCondExpr->pTableCond == NULL) {
        pCondExpr->pTableCond = *pExpr;
        pCondExpr->relType = parentOptr;
        pCondExpr->tableCondIndex = tsc_index.tableIndex;
      } else {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
      }

      *type = TSQL_EXPR_TBNAME;
      *pExpr = NULL;
    } else {
      if (pRight != NULL && pRight->tokenId == TK_ID) {  // join on tag columns for stable query
        if (!validateJoinExprNode(pCmd, pQueryInfo, *pExpr, &tsc_index)) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }

        pQueryInfo->type |= TSDB_QUERY_TYPE_JOIN_QUERY;
        ret = setExprToCond(&pCondExpr->pJoinExpr, *pExpr, NULL, parentOptr, pCmd->payload);
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

    if (pRight->tokenId == TK_ID) {
      if (joinQuery) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5); // other column cannot be served as the join column
      } else {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg8);
      }
    }

    ret = setExprToCond(&pCondExpr->pColumnCond, *pExpr, NULL, parentOptr, pCmd->payload);
    *pExpr = NULL;  // remove it from expr tree
  }

  return ret;
}

int32_t getQueryCondExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, tSqlExpr** pExpr, SCondExpr* pCondExpr,
                        int32_t* type, int32_t parentOptr, int32_t *tbIdx, bool joinQuery) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* msg1 = "query condition between columns/tables must use 'AND'";

  if ((*pExpr)->flags & (1 << EXPR_FLAG_TS_ERROR)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  tSqlExpr* pLeft = (*pExpr)->pLeft;
  tSqlExpr* pRight = (*pExpr)->pRight;

  if (!isValidExpr(pLeft, pRight, (*pExpr)->tokenId)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  int32_t leftType = -1;
  int32_t rightType = -1;
  int32_t leftTbIdx = 0;
  int32_t rightTbIdx = 0;

  if (!tSqlExprIsParentOfLeaf(*pExpr)) {
    int32_t ret = getQueryCondExpr(pCmd, pQueryInfo, &(*pExpr)->pLeft, pCondExpr, &leftType, (*pExpr)->tokenId, &leftTbIdx, joinQuery);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    ret = getQueryCondExpr(pCmd, pQueryInfo, &(*pExpr)->pRight, pCondExpr, &rightType, (*pExpr)->tokenId, &rightTbIdx, joinQuery);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    /*
     *  if left child and right child do not belong to the same group, the sub
     *  expression is not valid for parent node, it must be TK_AND operator.
     */
    if (leftType != rightType) {
      if ((*pExpr)->tokenId == TK_OR && (leftType + rightType != TSQL_EXPR_TBNAME + TSQL_EXPR_TAG)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }
    }

    if (((leftTbIdx != rightTbIdx) || (leftTbIdx == -1 || rightTbIdx == -1)) && ((*pExpr)->tokenId == TK_OR)) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    *type = rightType;

    *tbIdx = (leftTbIdx == rightTbIdx) ? leftTbIdx : -1;

    return TSDB_CODE_SUCCESS;
  }

  exchangeExpr(*pExpr);

  if (pLeft->tokenId == TK_ID && pRight->tokenId == TK_TIMESTAMP && (pRight->flags & (1 << EXPR_FLAG_TIMESTAMP_VAR))) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if ((pLeft->flags & (1 << EXPR_FLAG_TS_ERROR)) || (pRight->flags & (1 << EXPR_FLAG_TS_ERROR))) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return handleExprInQueryCond(pCmd, pQueryInfo, pExpr, pCondExpr, type, parentOptr, tbIdx, joinQuery);
}

static void doExtractExprForSTable(SSqlCmd* pCmd, tSqlExpr** pExpr, SQueryInfo* pQueryInfo, tSqlExpr** pOut, int32_t tableIndex) {
  if (tSqlExprIsParentOfLeaf(*pExpr)) {
    tSqlExpr* pLeft = (*pExpr)->pLeft;

    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(&pLeft->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return;
    }

    if (tsc_index.tableIndex != tableIndex) {
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

      invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
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

    for (int32_t j = 0; j < pCol->info.flist.numOfFilters; ++j) {
      SColumnFilterInfo* pColFilter = &pCol->info.flist.filterInfo[j];
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
  int32_t code = 0;

  if (pExpr == NULL) {
    pQueryInfo->onlyHasTagCond &= true;
    return TSDB_CODE_SUCCESS;
  }
  pQueryInfo->onlyHasTagCond &= false;
  

  if (!tSqlExprIsParentOfLeaf(pExpr)) {
    if (pExpr->tokenId == TK_OR) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    code = getTimeRangeFromExpr(pCmd, pQueryInfo, pExpr->pLeft);
    if (code) {
      return code;
    }

    return getTimeRangeFromExpr(pCmd, pQueryInfo, pExpr->pRight);
  } else {
    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(&pExpr->pLeft->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
    STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
    
    tSqlExpr* pRight = pExpr->pRight;

    STimeWindow win = {.skey = INT64_MIN, .ekey = INT64_MAX};
    if (getTimeRange(&win, pRight, pExpr->tokenId, tinfo.precision) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg0);
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
  const char* msg4 = "only ts column join allowed";

  if (!QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
    if (pQueryInfo->numOfTables == 1) {
      pQueryInfo->onlyHasTagCond &= true;
      return TSDB_CODE_SUCCESS;
    } else {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }
  }
  pQueryInfo->onlyHasTagCond &= false;

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {  // for stable join, tag columns
                                                   // must be present for join
    if (pCondExpr->pJoinExpr == NULL) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  } else if ((!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) && pCondExpr->pJoinExpr) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  if (!pCondExpr->tsJoin) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

static void cleanQueryExpr(SCondExpr* pCondExpr) {
  if (pCondExpr->pTableCond) {
    tSqlExprDestroy(pCondExpr->pTableCond);
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

/*
static void doAddJoinTagsColumnsIntoTagList(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SCondExpr* pCondExpr) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (QUERY_IS_JOIN_QUERY(pQueryInfo->type) && UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;

    if (getColumnIndexByName(pCmd, &pCondExpr->pJoinExpr->pLeft->ColName, pQueryInfo, &tsc_index) != TSDB_CODE_SUCCESS) {
      tscError("%p: invalid column name (left)", pQueryInfo);
    }

    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
    tsc_index.columnIndex = tsc_index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);

    SSchema* pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
    tscColumnListInsert(pTableMetaInfo->tagColList, &tsc_index, &pSchema[tsc_index.columnIndex]);
  
    if (getColumnIndexByName(pCmd, &pCondExpr->pJoinExpr->pRight->ColName, pQueryInfo, &tsc_index) != TSDB_CODE_SUCCESS) {
      tscError("%p: invalid column name (right)", pQueryInfo);
    }

    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
    tsc_index.columnIndex = tsc_index.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);

    pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
    tscColumnListInsert(pTableMetaInfo->tagColList, &tsc_index, &pSchema[tsc_index.columnIndex]);
  }
}
*/

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
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
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

    int32_t bufLen = 0;
    if (IS_NUMERIC_TYPE(vVariant->nType)) {
      bufLen = 60;  // The maximum length of string that a number is converted to.
    } else {
      bufLen = vVariant->nLen + 1;
    }

    if (schemaType == TSDB_DATA_TYPE_BINARY) {
      char *tmp = calloc(1, bufLen * TSDB_NCHAR_SIZE);
      retVal = tVariantDump(vVariant, tmp, schemaType, false);
      free(tmp);
    } else if (schemaType == TSDB_DATA_TYPE_NCHAR) {
      // pRight->value.nLen + 1 is larger than the actual nchar string length
      char *tmp = calloc(1, bufLen * TSDB_NCHAR_SIZE);
      retVal = tVariantDump(vVariant, tmp, schemaType, false);
      free(tmp);
    } else {
      double tmp;
      retVal = tVariantDump(vVariant, (char*)&tmp, schemaType, false);
    }
    
    if (retVal != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  } while (0);

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
    
    // add to required table column list
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    int64_t uid = pTableMetaInfo->pTableMeta->id.uid;
    int32_t numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
    
    size_t num = taosArrayGetSize(colList);
    for(int32_t j = 0; j < num; ++j) {
      SColIndex* pIndex = taosArrayGet(colList, j);
      SColumnIndex tsc_index = {.tableIndex = i, .columnIndex = pIndex->colIndex - numOfCols};

      SSchema* s = tscGetTableSchema(pTableMetaInfo->pTableMeta);
      tscColumnListInsert(pTableMetaInfo->tagColList, tsc_index.columnIndex, pTableMetaInfo->pTableMeta->id.uid,
                          &s[pIndex->colIndex]);
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
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "filter on tag not supported for normal table");
    }

    if (ret) {
      break;
    }
  }

  pCondExpr->pTagCond = NULL;
  return ret;
}

int32_t validateJoinNodes(SQueryInfo* pQueryInfo, SSqlObj* pSql) {
  const char* msg1 = "timestamp required for join tables";
  const char* msg2 = "tag required for join stables";

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    SJoinNode *node = pQueryInfo->tagCond.joinInfo.joinTables[i];

    if (node == NULL || node->tsJoin == NULL || taosArrayGetSize(node->tsJoin) <= 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(&pSql->cmd), msg1);
    }
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
      SJoinNode *node = pQueryInfo->tagCond.joinInfo.joinTables[i];

      if (node == NULL || node->tagJoin == NULL || taosArrayGetSize(node->tagJoin) <= 0) {
        return invalidOperationMsg(tscGetErrorMsgPayload(&pSql->cmd), msg2);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}


void mergeJoinNodesImpl(int8_t* r, int8_t* p, int16_t* tidx, SJoinNode** nodes, int32_t type) {
  SJoinNode *node = nodes[*tidx];
  SArray* arr = (type == 0) ? node->tsJoin : node->tagJoin;
  size_t size = taosArrayGetSize(arr);

  p[*tidx] = 1;

  for (int32_t j = 0; j < size; j++) {
    int16_t* idx = taosArrayGet(arr, j);
    r[*idx] = 1;
    if (p[*idx] == 0) {
      mergeJoinNodesImpl(r, p, idx, nodes, type);
    }
  }
}

int32_t mergeJoinNodes(SQueryInfo* pQueryInfo, SSqlObj* pSql) {
  const char* msg1 = "not all join tables have same timestamp";
  const char* msg2 = "not all join tables have same tag";

  int8_t r[TSDB_MAX_JOIN_TABLE_NUM] = {0};
  int8_t p[TSDB_MAX_JOIN_TABLE_NUM] = {0};

  for (int16_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    mergeJoinNodesImpl(r, p, &i, pQueryInfo->tagCond.joinInfo.joinTables, 0);

    taosArrayClear(pQueryInfo->tagCond.joinInfo.joinTables[i]->tsJoin);

    for (int32_t j = 0; j < TSDB_MAX_JOIN_TABLE_NUM; ++j) {
      if (r[j]) {
        taosArrayPush(pQueryInfo->tagCond.joinInfo.joinTables[i]->tsJoin, &j);
      }
    }

    memset(r, 0, sizeof(r));
    memset(p, 0, sizeof(p));
  }

  if (taosArrayGetSize(pQueryInfo->tagCond.joinInfo.joinTables[0]->tsJoin) != pQueryInfo->numOfTables) {
    return invalidOperationMsg(tscGetErrorMsgPayload(&pSql->cmd), msg1);
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    for (int16_t i = 0; i < pQueryInfo->numOfTables; ++i) {
      mergeJoinNodesImpl(r, p, &i, pQueryInfo->tagCond.joinInfo.joinTables, 1);

      taosArrayClear(pQueryInfo->tagCond.joinInfo.joinTables[i]->tagJoin);

      for (int32_t j = 0; j < TSDB_MAX_JOIN_TABLE_NUM; ++j) {
        if (r[j]) {
          taosArrayPush(pQueryInfo->tagCond.joinInfo.joinTables[i]->tagJoin, &j);
        }
      }

      memset(r, 0, sizeof(r));
      memset(p, 0, sizeof(p));
    }

    if (taosArrayGetSize(pQueryInfo->tagCond.joinInfo.joinTables[0]->tagJoin) != pQueryInfo->numOfTables) {
      return invalidOperationMsg(tscGetErrorMsgPayload(&pSql->cmd), msg2);
    }

  }

  return TSDB_CODE_SUCCESS;
}


int32_t validateWhereNode(SQueryInfo* pQueryInfo, tSqlExpr** pExpr, SSqlObj* pSql, bool joinQuery) {
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
    return invalidOperationMsg(tscGetErrorMsgPayload(&pSql->cmd), msg1);
  }

  int32_t type = 0;
  int32_t tbIdx = 0;
  if ((ret = getQueryCondExpr(&pSql->cmd, pQueryInfo, pExpr, &condExpr, &type, (*pExpr)->tokenId, &tbIdx, joinQuery)) != TSDB_CODE_SUCCESS) {
    goto PARSE_WHERE_EXIT;
  }

  tSqlExprCompact(pExpr);

  // after expression compact, the expression tree is only include tag query condition
  condExpr.pTagCond = (*pExpr);

  // 1. check if it is a join query
  if ((ret = validateJoinExpr(&pSql->cmd, pQueryInfo, &condExpr)) != TSDB_CODE_SUCCESS) {
    goto PARSE_WHERE_EXIT;
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
    goto PARSE_WHERE_EXIT;
  }

  // 5. other column query condition
  if ((ret = getColumnQueryCondInfo(&pSql->cmd, pQueryInfo, condExpr.pColumnCond, TK_AND)) != TSDB_CODE_SUCCESS) {
    goto PARSE_WHERE_EXIT;
  }

  if (taosArrayGetSize(pQueryInfo->pUpstream) > 0 ) {
    if ((ret = getColumnQueryCondInfo(&pSql->cmd, pQueryInfo, condExpr.pTimewindow, TK_AND)) != TSDB_CODE_SUCCESS) {
      goto PARSE_WHERE_EXIT;
    }
  }

  // 6. join condition
  if ((ret = getJoinCondInfo(&pSql->cmd, pQueryInfo, condExpr.pJoinExpr)) != TSDB_CODE_SUCCESS) {
    goto PARSE_WHERE_EXIT;
  }

  // 7. query condition for table name
  pQueryInfo->tagCond.relType = (condExpr.relType == TK_AND) ? TSDB_RELATION_AND : TSDB_RELATION_OR;

  ret = setTableCondForSTableQuery(&pSql->cmd, pQueryInfo, getAccountId(pSql), condExpr.pTableCond, condExpr.tableCondIndex, &sb);
  taosStringBuilderDestroy(&sb);
  if (ret) {
    goto PARSE_WHERE_EXIT;
  }

  if (!validateFilterExpr(pQueryInfo)) {
    ret = invalidOperationMsg(tscGetErrorMsgPayload(&pSql->cmd), msg2);
    goto PARSE_WHERE_EXIT;
  }

  //doAddJoinTagsColumnsIntoTagList(&pSql->cmd, pQueryInfo, &condExpr);
  if (condExpr.tsJoin) {
    ret = validateJoinNodes(pQueryInfo, pSql);
    if (ret) {
      goto PARSE_WHERE_EXIT;
    }

    ret = mergeJoinNodes(pQueryInfo, pSql);
    if (ret) {
      goto PARSE_WHERE_EXIT;
    }
  }

PARSE_WHERE_EXIT:

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
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  int64_t val = 0;
  bool    parsed = false;
  if (pRight->value.nType == TSDB_DATA_TYPE_BINARY) {
    pRight->value.nLen = strdequote(pRight->value.pz);

    char* seg = strnchr(pRight->value.pz, '-', pRight->value.nLen, false);
    if (seg != NULL) {
      if (taosParseTime(pRight->value.pz, &val, pRight->value.nLen, timePrecision, tsDaylight) == TSDB_CODE_SUCCESS) {
        parsed = true;
      } else {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else {
      SStrToken token = {.z = pRight->value.pz, .n = pRight->value.nLen, .type = TK_ID};
      int32_t   len = tGetToken(pRight->value.pz, &token.type);

      if ((token.type != TK_INTEGER && token.type != TK_FLOAT) || len != pRight->value.nLen) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    }
  }

  if (!parsed) {
    /*
     * failed to parse timestamp in regular formation, try next
     * it may be a epoch time in string format
     */
    if (pRight->flags & (1 << EXPR_FLAG_NS_TIMESTAMP)) {
      pRight->value.i64 = convertTimePrecision(pRight->value.i64, TSDB_TIME_PRECISION_NANO, timePrecision);
      pRight->flags &= ~(1 << EXPR_FLAG_NS_TIMESTAMP);
    }

    tVariantDump(&pRight->value, (char*)&val, TSDB_DATA_TYPE_BIGINT, true);
  }

  if (optr == TK_LE) {
    win->ekey = val;
  } else if (optr == TK_LT) {
    win->ekey = val - 1;
  } else if (optr == TK_GT) {
    win->skey = val + 1;
  } else if (optr == TK_GE) {
    win->skey = val;
  } else if (optr == TK_EQ) {
    win->ekey = win->skey = val;
  } else if (optr == TK_NE) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
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
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateFillNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode) {
  SArray* pFillToken = pSqlNode->fillType;
  if (pSqlNode->fillType == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tVariantListItem* pItem = taosArrayGet(pFillToken, 0);

  const int32_t START_INTERPO_COL_IDX = 1;

  const char* msg1 = "value is expected";
  const char* msg2 = "invalid fill option";
  const char* msg3 = "top/bottom not support fill";
  const char* msg4 = "illegal value or data overflow";
  const char* msg5 = "fill only available for interval query";
  const char* msg6 = "not supported function now";
  const char* msg7 = "join query not supported fill operation";

  if ((!isTimeWindowQuery(pQueryInfo)) && (!tscIsPointInterpQuery(pQueryInfo))) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
  }
  if(QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
  }

  /*
   * fill options are set at the end position, when all columns are set properly
   * the columns may be increased due to group by operation
   */
  if (checkQueryRangeForFill(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }


  if (pItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }
  
  size_t numOfFields = tscNumOfFields(pQueryInfo);
  
  if (pQueryInfo->fillVal == NULL) {
    pQueryInfo->fillVal      = calloc(numOfFields, sizeof(int64_t));
    pQueryInfo->numOfFillVal = (int32_t)numOfFields;
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
    if (tscIsPointInterpQuery(pQueryInfo) && pQueryInfo->order.order == TSDB_ORDER_DESC) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
    }
  } else if (strncasecmp(pItem->pVar.pz, "next", 4) == 0 && pItem->pVar.nLen == 4) {
    pQueryInfo->fillType = TSDB_FILL_NEXT;
  } else if (strncasecmp(pItem->pVar.pz, "linear", 6) == 0 && pItem->pVar.nLen == 6) {
    pQueryInfo->fillType = TSDB_FILL_LINEAR;
  } else if (strncasecmp(pItem->pVar.pz, "value", 5) == 0 && pItem->pVar.nLen == 5) {
    pQueryInfo->fillType = TSDB_FILL_SET_VALUE;

    size_t num = taosArrayGetSize(pFillToken);
    if (num == 1) {  // no actual value, return with error code
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
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
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
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
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  size_t numOfExprs = tscNumOfExprs(pQueryInfo);
  for(int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId == TSDB_FUNC_TOP || pExpr->base.functionId == TSDB_FUNC_BOTTOM) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
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
  } else { // in case of select tbname from super_table, the default order column can not be the primary ts column
    pQueryInfo->order.orderColId = INT32_MIN;  // todo define a macro
  }

  /* for super table query, set default ascending order for group output */
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    pQueryInfo->groupbyExpr.orderType = TSDB_ORDER_ASC;
  }

  if (pQueryInfo->distinct) {
    pQueryInfo->order.order = TSDB_ORDER_ASC;
    pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
  }
}

int32_t validateOrderbyNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode, SSchema* pSchema) {
  const char* msg0 = "only one column allowed in orderby";
  const char* msg1 = "invalid column name in orderby clause";
  const char* msg2 = "too many order by columns";
  const char* msg3 = "only primary timestamp/tbname/first tag in groupby clause allowed";
  const char* msg4 = "only tag in groupby clause allowed in order clause";
  const char* msg5 = "only primary timestamp/column in top/bottom function allowed as order column";
  const char* msg6 = "only primary timestamp allowed as the second order column";
  const char* msg7 = "only primary timestamp/column in groupby clause allowed as order column";
  const char* msg8 = "only column in groupby clause allowed as order column";
  const char* msg9 = "orderby column must projected in subquery";
  const char* msg10 = "not support distinct mixed with order by";
  const char* msg11 = "not support order with udf";
  const char* msg12 = "order by tags not supported with diff/derivative/csum/mavg";

  setDefaultOrderInfo(pQueryInfo);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (pSqlNode->pSortOrder == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  char* pMsgBuf = tscGetErrorMsgPayload(pCmd);
  SArray* pSortOrder = pSqlNode->pSortOrder;

  /*
   * for table query, there is only one or none order option is allowed, which is the
   * ts or values(top/bottom) order is supported.
   *
   * for super table query, the order option must be less than 3 and the second must be ts.
   *
   * order by has 5 situations
   * 1. from stable group by tag1 order by tag1 [ts]
   * 2. from stable group by tbname order by tbname [ts]
   * 3. from stable/table group by column1 order by column1
   * 4. from stable/table order by ts
   * 5. select stable/table top(column2,1) ... order by column2
   */
  size_t size = taosArrayGetSize(pSortOrder);
  if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    if (size > 1) {
      return invalidOperationMsg(pMsgBuf, msg0);
    }
  } else {
    if (size > 2) {
      return invalidOperationMsg(pMsgBuf, msg2);
    }
  }
  if (size > 0 && pQueryInfo->distinct) {
    return invalidOperationMsg(pMsgBuf, msg10);
  }

  // handle the first part of order by
  tVariant* pVar = taosArrayGet(pSortOrder, 0);

  if (pVar->nType != TSDB_DATA_TYPE_BINARY){
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  SStrToken    columnName = {pVar->nLen, pVar->nType, pVar->pz};
  SColumnIndex indexColumn = COLUMN_INDEX_INITIALIZER;
  bool udf = false;

  if (pQueryInfo->pUdfInfo && taosArrayGetSize(pQueryInfo->pUdfInfo) > 0) {
    int32_t usize = (int32_t)taosArrayGetSize(pQueryInfo->pUdfInfo);

    for (int32_t i = 0; i < usize; ++i) {
      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, i);
      if (pUdfInfo->funcType == TSDB_UDF_TYPE_SCALAR) {
        udf = true;
        break;
      }
    }
  }

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {  // super table query
    if (getColumnIndexByName(&columnName, pQueryInfo, &indexColumn, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(pMsgBuf, msg1);
    }

    bool orderByTags = false;
    bool orderByTS = false;
    bool orderByGroupbyCol = false;

    if (indexColumn.columnIndex >= tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {    // order by tag1
      int32_t relTagIndex = indexColumn.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);

      // it is a tag column
      if (pQueryInfo->groupbyExpr.columnInfo == NULL) {
        return invalidOperationMsg(pMsgBuf, msg4);
      }
      SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, 0);
      if (relTagIndex == pColIndex->colIndex) {
        orderByTags = true;
      }
    } else if (indexColumn.columnIndex == TSDB_TBNAME_COLUMN_INDEX) { // order by tbname
      // it is a tag column
      if (pQueryInfo->groupbyExpr.columnInfo == NULL) {
        return invalidOperationMsg(pMsgBuf, msg4);
      }
      SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, 0);
      if (TSDB_TBNAME_COLUMN_INDEX == pColIndex->colIndex) {
        orderByTags = true;
      }
    }else if (indexColumn.columnIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {     // order by ts
      orderByTS = true;
    }else{    // order by normal column
      SArray *columnInfo = pQueryInfo->groupbyExpr.columnInfo;
      if (columnInfo != NULL && taosArrayGetSize(columnInfo) > 0) {
        SColIndex* pColIndex = taosArrayGet(columnInfo, 0);
        if (pColIndex->colIndex == indexColumn.columnIndex) {
          orderByGroupbyCol = true;
        }
      }
    }

    if (!(orderByTags || orderByTS || orderByGroupbyCol) && !isTopBottomQuery(pQueryInfo)) {
      return invalidOperationMsg(pMsgBuf, msg3);
    }

    size_t s = taosArrayGetSize(pSortOrder);
    if (s == 1) {
      if (orderByTags) {
        if (tscIsDiffDerivQuery(pQueryInfo)) {
          return invalidOperationMsg(pMsgBuf, msg12);
        }
        pQueryInfo->groupbyExpr.orderIndex = indexColumn.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);

        tVariantListItem* p1 = taosArrayGet(pSqlNode->pSortOrder, 0);
        pQueryInfo->groupbyExpr.orderType = p1->sortOrder;
      } else if (orderByGroupbyCol) {
        tVariantListItem* p1 = taosArrayGet(pSqlNode->pSortOrder, 0);

        pQueryInfo->groupbyExpr.orderType = p1->sortOrder;
        pQueryInfo->order.orderColId = pSchema[indexColumn.columnIndex].colId;
        if (udf) {
          return invalidOperationMsg(pMsgBuf, msg11);
        }
      } else if (isTopBottomQuery(pQueryInfo)) {
        /* order of top/bottom query in interval is not valid  */

        int32_t pos = tscGetTopBotQueryExprIndex(pQueryInfo);
        assert(pos > 0);
        SExprInfo* pExpr = tscExprGet(pQueryInfo, pos - 1);
        assert(pExpr->base.functionId == TSDB_FUNC_TS);

        pExpr = tscExprGet(pQueryInfo, pos);

        if (pExpr->base.colInfo.colIndex != indexColumn.columnIndex && indexColumn.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
          return invalidOperationMsg(pMsgBuf, msg5);
        }

        tVariantListItem* p1 = taosArrayGet(pSqlNode->pSortOrder, 0);
        pQueryInfo->order.order = p1->sortOrder;
        pQueryInfo->order.orderColId = pSchema[indexColumn.columnIndex].colId;
        return TSDB_CODE_SUCCESS;
      } else {
        tVariantListItem* p1 = taosArrayGet(pSqlNode->pSortOrder, 0);

        if (udf) {
          return invalidOperationMsg(pMsgBuf, msg11);
        }

        pQueryInfo->order.order = p1->sortOrder;
        pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;

        // orderby ts query on super table
        if (tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
          bool found = false;
          for (int32_t i = 0; i < tscNumOfExprs(pQueryInfo); ++i) {
            SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
            if (pExpr->base.functionId == TSDB_FUNC_PRJ && pExpr->base.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
              found = true;
              break;
            }
          }
          if (!found && pQueryInfo->pDownstream) {
            return invalidOperationMsg(pMsgBuf, msg9);
          }
          addPrimaryTsColIntoResult(pQueryInfo, pCmd);
        }
      }
    } else {
      tVariantListItem *pItem = taosArrayGet(pSqlNode->pSortOrder, 0);
      if (orderByTags) {
        pQueryInfo->groupbyExpr.orderIndex = indexColumn.columnIndex - tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
        pQueryInfo->groupbyExpr.orderType = pItem->sortOrder;
      } else if (orderByGroupbyCol){
        pQueryInfo->order.order = pItem->sortOrder;
        pQueryInfo->order.orderColId = indexColumn.columnIndex;
        if (udf) {
          return invalidOperationMsg(pMsgBuf, msg11);
        }
      } else {
        pQueryInfo->order.order = pItem->sortOrder;
        pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
        if (udf) {
          return invalidOperationMsg(pMsgBuf, msg11);
        }
      }

      pItem = taosArrayGet(pSqlNode->pSortOrder, 1);
      tVariant* pVar2 = &pItem->pVar;
      SStrToken cname = {pVar2->nLen, pVar2->nType, pVar2->pz};
      if (getColumnIndexByName(&cname, pQueryInfo, &indexColumn, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
        return invalidOperationMsg(pMsgBuf, msg1);
      }

      if (indexColumn.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        return invalidOperationMsg(pMsgBuf, msg6);
      } else {
        tVariantListItem* p1 = taosArrayGet(pSortOrder, 1);
        pQueryInfo->order.order = p1->sortOrder;
        pQueryInfo->order.orderColId = PRIMARYKEY_TIMESTAMP_COL_INDEX;
      }
    }

  } else if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo) || UTIL_TABLE_IS_CHILD_TABLE(pTableMetaInfo)) { // check order by clause for normal table & temp table
    if (getColumnIndexByName(&columnName, pQueryInfo, &indexColumn, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(pMsgBuf, msg1);
    }

    if (indexColumn.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX && !isTopBottomQuery(pQueryInfo)) {
      bool validOrder = false;
      SArray *columnInfo = pQueryInfo->groupbyExpr.columnInfo;
      if (columnInfo != NULL && taosArrayGetSize(columnInfo) > 0) {
        SColIndex* pColIndex = taosArrayGet(columnInfo, 0);
        validOrder = (pColIndex->colIndex == indexColumn.columnIndex);
      }

      if (!validOrder) {
        return invalidOperationMsg(pMsgBuf, msg7);
      }

      if (udf) {
        return invalidOperationMsg(pMsgBuf, msg11);
      }

      if (udf) {
        return invalidOperationMsg(pMsgBuf, msg11);
      }

      tVariantListItem* p1 = taosArrayGet(pSqlNode->pSortOrder, 0);
      pQueryInfo->groupbyExpr.orderIndex = pSchema[indexColumn.columnIndex].colId;
      pQueryInfo->groupbyExpr.orderType = p1->sortOrder;
    }

    if (isTopBottomQuery(pQueryInfo)) {
      SArray *columnInfo = pQueryInfo->groupbyExpr.columnInfo;
      if (columnInfo != NULL && taosArrayGetSize(columnInfo) > 0) {
        SColIndex* pColIndex = taosArrayGet(columnInfo, 0);

        if (pColIndex->colIndex != indexColumn.columnIndex) {
          return invalidOperationMsg(pMsgBuf, msg8);
        }
      } else {
        int32_t pos = tscGetTopBotQueryExprIndex(pQueryInfo);
        assert(pos > 0);
        SExprInfo* pExpr = tscExprGet(pQueryInfo, pos - 1);
        assert(pExpr->base.functionId == TSDB_FUNC_TS);

        pExpr = tscExprGet(pQueryInfo, pos);

        if (pExpr->base.colInfo.colIndex != indexColumn.columnIndex && indexColumn.columnIndex != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
          return invalidOperationMsg(pMsgBuf, msg5);
        }
      }

      tVariantListItem* pItem = taosArrayGet(pSqlNode->pSortOrder, 0);
      pQueryInfo->order.order = pItem->sortOrder;

      pQueryInfo->order.orderColId = pSchema[indexColumn.columnIndex].colId;
      return TSDB_CODE_SUCCESS;
    }

    if (udf) {
      return invalidOperationMsg(pMsgBuf, msg11);
    }

    tVariantListItem* pItem = taosArrayGet(pSqlNode->pSortOrder, 0);
    pQueryInfo->order.order = pItem->sortOrder;
    pQueryInfo->order.orderColId = pSchema[indexColumn.columnIndex].colId;
  } else {
    // handle the temp table order by clause. You can order by any single column in case of the temp table, created by
    // inner subquery.
    assert(UTIL_TABLE_IS_TMP_TABLE(pTableMetaInfo) && taosArrayGetSize(pSqlNode->pSortOrder) == 1);

    if (getColumnIndexByName(&columnName, pQueryInfo, &indexColumn, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(pMsgBuf, msg1);
    }

    if (udf) {
      return invalidOperationMsg(pMsgBuf, msg11);
    }

    tVariantListItem* pItem = taosArrayGet(pSqlNode->pSortOrder, 0);
    pQueryInfo->order.order = pItem->sortOrder;
    pQueryInfo->order.orderColId = pSchema[indexColumn.columnIndex].colId;
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
  const char* msg21 = "only binary/nchar column length could be modified";
  const char* msg23 = "only column length coulbe be modified";
  const char* msg24 = "invalid binary/nchar column length";

  int32_t code = TSDB_CODE_SUCCESS;

  SSqlCmd*        pCmd = &pSql->cmd;
  SAlterTableInfo* pAlterSQL = pInfo->pAlterInfo;
  SQueryInfo*     pQueryInfo = tscGetQueryInfo(pCmd);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, DEFAULT_TABLE_INDEX);

  if (tscValidateName(&(pAlterSQL->name)) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  code = tscSetTableFullName(&pTableMetaInfo->name, &(pAlterSQL->name), pSql);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tscGetTableMeta(pSql, pTableMetaInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  char* pMsg = tscGetErrorMsgPayload(pCmd);
  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;

  if (pAlterSQL->tableType == TSDB_SUPER_TABLE && !(UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo))) {
    return invalidOperationMsg(pMsg, msg20);
  }

  if (pAlterSQL->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN || pAlterSQL->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN ||
      pAlterSQL->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN || pAlterSQL->type == TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN) {
    if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      return invalidOperationMsg(pMsg, msg3);
    }
  } else if ((pAlterSQL->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) && (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo))) {
    return invalidOperationMsg(pMsg, msg4);
  } else if ((pAlterSQL->type == TSDB_ALTER_TABLE_ADD_COLUMN || pAlterSQL->type == TSDB_ALTER_TABLE_DROP_COLUMN || pAlterSQL->type == TSDB_ALTER_TABLE_CHANGE_COLUMN) &&
             UTIL_TABLE_IS_CHILD_TABLE(pTableMetaInfo)) {
    return invalidOperationMsg(pMsg, msg6);
  }

  if (pAlterSQL->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
    SArray* pFieldList = pAlterSQL->pAddColumns;
    if (taosArrayGetSize(pFieldList) > 1) {
      return invalidOperationMsg(pMsg, msg5);
    }

    TAOS_FIELD* p = taosArrayGet(pFieldList, 0);
    int32_t ret = validateOneTag(pCmd, p);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, p);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
    if (tscGetNumOfTags(pTableMeta) == 1) {
      return invalidOperationMsg(pMsg, msg7);
    }

    // numOfTags == 1
    if (taosArrayGetSize(pAlterSQL->varList) > 1) {
      return invalidOperationMsg(pMsg, msg8);
    }

    tVariantListItem* pItem = taosArrayGet(pAlterSQL->varList, 0);
    if (pItem->pVar.nLen >= TSDB_COL_NAME_LEN) {
      return invalidOperationMsg(pMsg, msg9);
    }

    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
    SStrToken    name = {.z = pItem->pVar.pz, .n = pItem->pVar.nLen, .type = TK_STRING};

    if (getColumnIndexByName(&name, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    int32_t numOfCols = tscGetNumOfColumns(pTableMeta);
    if (tsc_index.columnIndex < numOfCols) {
      return invalidOperationMsg(pMsg, msg10);
    } else if (tsc_index.columnIndex == numOfCols) {
      return invalidOperationMsg(pMsg, msg11);
    }

    char name1[128] = {0};
    strncpy(name1, pItem->pVar.pz, pItem->pVar.nLen);
  
    TAOS_FIELD f = tscCreateField(TSDB_DATA_TYPE_INT, name1, tDataTypes[TSDB_DATA_TYPE_INT].bytes);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    SArray* pVarList = pAlterSQL->varList;
    if (taosArrayGetSize(pVarList) > 2) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    tVariantListItem* pSrcItem = taosArrayGet(pAlterSQL->varList, 0);
    tVariantListItem* pDstItem = taosArrayGet(pAlterSQL->varList, 1);

    if (pSrcItem->pVar.nLen >= TSDB_COL_NAME_LEN || pDstItem->pVar.nLen >= TSDB_COL_NAME_LEN) {
      return invalidOperationMsg(pMsg, msg9);
    }

    if (pSrcItem->pVar.nType != TSDB_DATA_TYPE_BINARY || pDstItem->pVar.nType != TSDB_DATA_TYPE_BINARY) {
      return invalidOperationMsg(pMsg, msg10);
    }

    SColumnIndex srcIndex = COLUMN_INDEX_INITIALIZER;
    SColumnIndex destIndex = COLUMN_INDEX_INITIALIZER;

    SStrToken srcToken = {.z = pSrcItem->pVar.pz, .n = pSrcItem->pVar.nLen, .type = TK_STRING};
    if (getColumnIndexByName(&srcToken, pQueryInfo, &srcIndex, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(pMsg, msg17);
    }

    SStrToken destToken = {.z = pDstItem->pVar.pz, .n = pDstItem->pVar.nLen, .type = TK_STRING};
    if (getColumnIndexByName(&destToken, pQueryInfo, &destIndex, tscGetErrorMsgPayload(pCmd)) == TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(pMsg, msg19);
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
    if (getColumnIndexByName(&name, pQueryInfo, &columnIndex, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (columnIndex.columnIndex < tscGetNumOfColumns(pTableMeta)) {
      return invalidOperationMsg(pMsg, msg12);
    }

    tVariantListItem* pItem = taosArrayGet(pVarList, 1);
    SSchema* pTagsSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, columnIndex.columnIndex);

    if (IS_VAR_DATA_TYPE(pTagsSchema->type) && (pItem->pVar.nLen > pTagsSchema->bytes * TSDB_NCHAR_SIZE)) {
      return invalidOperationMsg(pMsg, msg14);
    }
    pAlterSQL->tagData.data = calloc(1, pTagsSchema->bytes * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);

    if (tVariantDump(&pItem->pVar, pAlterSQL->tagData.data, pTagsSchema->type, true) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(pMsg, msg13);
    }
    
    pAlterSQL->tagData.dataLen = pTagsSchema->bytes;

    // validate the length of binary
    if ((pTagsSchema->type == TSDB_DATA_TYPE_BINARY || pTagsSchema->type == TSDB_DATA_TYPE_NCHAR) &&
        varDataTLen(pAlterSQL->tagData.data) > pTagsSchema->bytes) {
      return invalidOperationMsg(pMsg, msg14);
    }

    int32_t schemaLen = sizeof(STColumn) * numOfTags;
    int32_t size = sizeof(SUpdateTableTagValMsg) + pTagsSchema->bytes + schemaLen + TSDB_EXTRA_PAYLOAD_SIZE;

    if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
      tscError("0x%"PRIx64" failed to malloc for alter table pMsg", pSql->self);
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

    // the schema is located after the pMsg body, then followed by true tag value
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

    // copy the tag value to pMsg body
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
      const char* msgx = "only support add one column";
      return invalidOperationMsg(pMsg, msgx);
    }

    TAOS_FIELD* p = taosArrayGet(pFieldList, 0);
    int32_t ret = validateOneColumn(pCmd, p);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, p);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    if (tscGetNumOfColumns(pTableMeta) == TSDB_MIN_COLUMNS) {  //
      return invalidOperationMsg(pMsg, msg15);
    }

    size_t size = taosArrayGetSize(pAlterSQL->varList);
    if (size > 1) {
      return invalidOperationMsg(pMsg, msg16);
    }

    tVariantListItem* pItem = taosArrayGet(pAlterSQL->varList, 0);

    SColumnIndex columnIndex = COLUMN_INDEX_INITIALIZER;
    SStrToken    name = {.type = TK_STRING, .z = pItem->pVar.pz, .n = pItem->pVar.nLen};
    if (getColumnIndexByName(&name, pQueryInfo, &columnIndex, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(pMsg, msg17);
    }

    if (columnIndex.columnIndex == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return invalidOperationMsg(pMsg, msg18);
    }

    char name1[TSDB_COL_NAME_LEN] = {0};
    tstrncpy(name1, pItem->pVar.pz, sizeof(name1));
    TAOS_FIELD f = tscCreateField(TSDB_DATA_TYPE_INT, name1, tDataTypes[TSDB_DATA_TYPE_INT].bytes);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  } else if (pAlterSQL->type == TSDB_ALTER_TABLE_CHANGE_COLUMN) {
    if (taosArrayGetSize(pAlterSQL->pAddColumns) >= 2) {
      return invalidOperationMsg(pMsg, msg16);
    }


    TAOS_FIELD* pItem = taosArrayGet(pAlterSQL->pAddColumns, 0);
    if (pItem->type != TSDB_DATA_TYPE_BINARY && pItem->type != TSDB_DATA_TYPE_NCHAR) {
      return invalidOperationMsg(pMsg, msg21);
    }

    SColumnIndex columnIndex = COLUMN_INDEX_INITIALIZER;
    SStrToken    name = {.type = TK_STRING, .z = pItem->name, .n = (uint32_t)strlen(pItem->name)};
    if (getColumnIndexByName(&name, pQueryInfo, &columnIndex, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(pMsg, msg17);
    }

    SSchema* pColSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, columnIndex.columnIndex);

    if (pColSchema->type != TSDB_DATA_TYPE_BINARY && pColSchema->type != TSDB_DATA_TYPE_NCHAR) {
      return invalidOperationMsg(pMsg, msg21);
    }

    if (pItem->type != pColSchema->type) {
      return invalidOperationMsg(pMsg, msg23);
    }

    if ((pItem->type == TSDB_DATA_TYPE_BINARY && (pItem->bytes <= 0 || pItem->bytes > TSDB_MAX_BINARY_LEN)) ||
        (pItem->type == TSDB_DATA_TYPE_NCHAR && (pItem->bytes <= 0 || pItem->bytes > TSDB_MAX_NCHAR_LEN))) {
      return invalidOperationMsg(pMsg, msg24);
    }

    if (pItem->bytes <= pColSchema->bytes) {
      return tscErrorMsgWithCode(TSDB_CODE_TSC_INVALID_COLUMN_LENGTH, pMsg, pItem->name, NULL);
    }

    SSchema* pSchema = (SSchema*) pTableMetaInfo->pTableMeta->schema;
    int16_t numOfColumns = pTableMetaInfo->pTableMeta->tableInfo.numOfColumns;
    int16_t i;
    uint32_t nLen = 0;
    for (i = 0; i < numOfColumns; ++i) {
      nLen += (i != columnIndex.columnIndex) ? pSchema[i].bytes : pItem->bytes;
    }
    if (nLen >= TSDB_MAX_BYTES_PER_ROW) {
      return invalidOperationMsg(pMsg, msg24);
    }
    TAOS_FIELD f = tscCreateField(pColSchema->type, name.z, pItem->bytes);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  }else if (pAlterSQL->type == TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN) {
    if (taosArrayGetSize(pAlterSQL->pAddColumns) >= 2) {
      return invalidOperationMsg(pMsg, msg16);
    }

    TAOS_FIELD* pItem = taosArrayGet(pAlterSQL->pAddColumns, 0);
    if (pItem->type != TSDB_DATA_TYPE_BINARY && pItem->type != TSDB_DATA_TYPE_NCHAR) {
      return invalidOperationMsg(pMsg, msg21);
    }

    SColumnIndex columnIndex = COLUMN_INDEX_INITIALIZER;
    SStrToken    name = {.type = TK_STRING, .z = pItem->name, .n = (uint32_t)strlen(pItem->name)};
    if (getColumnIndexByName(&name, pQueryInfo, &columnIndex, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(pMsg, msg17);
    }

    SSchema* pColSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, columnIndex.columnIndex);

    if (columnIndex.columnIndex < tscGetNumOfColumns(pTableMetaInfo->pTableMeta)) {
      return invalidOperationMsg(pMsg, msg10);
    }

    if (pColSchema->type != TSDB_DATA_TYPE_BINARY && pColSchema->type != TSDB_DATA_TYPE_NCHAR) {
      return invalidOperationMsg(pMsg, msg21);
    }

    if (pItem->type != pColSchema->type) {
      return invalidOperationMsg(pMsg, msg23);
    }

    if ((pItem->type == TSDB_DATA_TYPE_BINARY && (pItem->bytes <= 0 || pItem->bytes > TSDB_MAX_BINARY_LEN)) ||
        (pItem->type == TSDB_DATA_TYPE_NCHAR && (pItem->bytes <= 0 || pItem->bytes > TSDB_MAX_NCHAR_LEN))) {
      return invalidOperationMsg(pMsg, msg24);
    }

    if (pItem->bytes <= pColSchema->bytes) {
      return tscErrorMsgWithCode(TSDB_CODE_TSC_INVALID_TAG_LENGTH, pMsg, pItem->name, NULL);
    }

    SSchema* pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
    int16_t numOfTags = tscGetNumOfTags(pTableMetaInfo->pTableMeta);
    int32_t numOfCols = tscGetNumOfColumns(pTableMetaInfo->pTableMeta);
    int32_t tagIndex = columnIndex.columnIndex - numOfCols;
    assert(tagIndex>=0);
    uint32_t nLen = 0;
    for (int i = 0; i < numOfTags; ++i) {
      nLen += (i != tagIndex) ? pSchema[i].bytes : pItem->bytes;
    }
    if (nLen >= TSDB_MAX_TAGS_LEN) {
      return invalidOperationMsg(pMsg, msg24);
    }

    TAOS_FIELD f = tscCreateField(pColSchema->type, name.z, pItem->bytes);
    tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateSqlFunctionInStreamSql(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg0 = "sample interval can not be less than 10ms.";
  const char* msg1 = "functions not allowed in select clause";
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  if (pQueryInfo->interval.interval != 0 &&
      convertTimePrecision(pQueryInfo->interval.interval, tinfo.precision, TSDB_TIME_PRECISION_MILLI)< 10 &&
     pQueryInfo->interval.intervalUnit != 'n' &&
     pQueryInfo->interval.intervalUnit != 'y') {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }
  
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  for (int32_t i = 0; i < size; ++i) {
    int32_t functId = tscExprGet(pQueryInfo, i)->base.functionId;
    if (!IS_STREAM_QUERY_VALID(aAggs[functId].status)) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateFunctionsInIntervalOrGroupbyQuery(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  bool        isProjectionFunction = false;
  const char* msg1 = "functions not compatible with interval";

  // multi-output set/ todo refactor
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  
  for (int32_t k = 0; k < size; ++k) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, k);

    if (pExpr->base.functionId < 0) {
      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * pExpr->base.functionId - 1);
      if (pUdfInfo->funcType == TSDB_UDF_TYPE_SCALAR) {
        isProjectionFunction = true;
        break;
      } else {
        continue;
      }
    }

    // projection query on primary timestamp, the selectivity function needs to be present.
    if (pExpr->base.functionId == TSDB_FUNC_PRJ && pExpr->base.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      bool hasSelectivity = false;
      for (int32_t j = 0; j < size; ++j) {
        SExprInfo* pEx = tscExprGet(pQueryInfo, j);
        if ((aAggs[pEx->base.functionId].status & TSDB_FUNCSTATE_SELECTIVITY) == TSDB_FUNCSTATE_SELECTIVITY) {
          hasSelectivity = true;
          break;
        }
      }

      if (hasSelectivity) {
        continue;
      }
    }

    int32_t f = pExpr->base.functionId;
    if ((f == TSDB_FUNC_PRJ && pExpr->base.numOfParams == 0) || f == TSDB_FUNC_DIFF || f == TSDB_FUNC_ARITHM || f == TSDB_FUNC_DERIVATIVE) {
      isProjectionFunction = true;
      break;
    }
  }

  if (isProjectionFunction) {
    invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  return isProjectionFunction == true ? TSDB_CODE_TSC_INVALID_OPERATION : TSDB_CODE_SUCCESS;
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
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  } else {
    uint16_t port = atoi(pos + 1);
    if (0 == port) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateDNodeConfig(SMiscInfo* pOptions) {
  int32_t numOfToken = (int32_t) taosArrayGetSize(pOptions->a);

  if (numOfToken < 2 || numOfToken > 3) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  const int tokenLogEnd = 2;
  const int tokenBalance = 2;
  const int tokenMonitor = 3;
  const int tokenDebugFlag = 4;
  const int tokenDebugFlagEnd = 20;
  const int tokenOfflineInterval = 21;
  const SDNodeDynConfOption cfgOptions[] = {
      {"resetLog", 8},    {"resetQueryCache", 15},  {"balance", 7},     {"monitor", 7},
      {"debugFlag", 9},   {"monDebugFlag", 12},     {"vDebugFlag", 10}, {"mDebugFlag", 10},
      {"cDebugFlag", 10}, {"httpDebugFlag", 13},    {"qDebugflag", 10}, {"sdbDebugFlag", 12},
      {"uDebugFlag", 10}, {"tsdbDebugFlag", 13},    {"sDebugflag", 10}, {"rpcDebugFlag", 12},
      {"dDebugFlag", 10}, {"mqttDebugFlag", 13},    {"wDebugFlag", 10}, {"tmrDebugFlag", 12},
      {"cqDebugFlag", 11},
      {"offlineInterval", 15},
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
      return TSDB_CODE_TSC_INVALID_OPERATION;  // options value is invalid
    }
    return TSDB_CODE_SUCCESS;
  } else if ((strncasecmp(cfgOptions[tokenMonitor].name, pOptionToken->z, pOptionToken->n) == 0) &&
             (cfgOptions[tokenMonitor].len == pOptionToken->n)) {
    SStrToken* pValToken = taosArrayGet(pOptions->a, 2);
    int32_t    val = strtol(pValToken->z, NULL, 10);
    if (val != 0 && val != 1) {
      return TSDB_CODE_TSC_INVALID_OPERATION;  // options value is invalid
    }
    return TSDB_CODE_SUCCESS;
  } else if ((strncasecmp(cfgOptions[tokenOfflineInterval].name, pOptionToken->z, pOptionToken->n) == 0) &&
             (cfgOptions[tokenOfflineInterval].len == pOptionToken->n)) {
    SStrToken* pValToken = taosArrayGet(pOptions->a, 2);
    int32_t    val = strtol(pValToken->z, NULL, 10);
    if (val < 1 || val > 600) {
      return TSDB_CODE_TSC_INVALID_OPERATION;  // options value is invalid
    }
    return TSDB_CODE_SUCCESS;
  } else {
    SStrToken* pValToken = taosArrayGet(pOptions->a, 2);

    int32_t val = strtol(pValToken->z, NULL, 10);
    if (val < 0 || val > 256) {
      /* options value is out of valid range */
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    for (int32_t i = tokenDebugFlag; i < tokenDebugFlagEnd; ++i) {
      const SDNodeDynConfOption* pOption = &cfgOptions[i];

      // options is valid
      if ((strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0) && (pOption->len == pOptionToken->n)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  return TSDB_CODE_TSC_INVALID_OPERATION;
}

int32_t validateLocalConfig(SMiscInfo* pOptions) {
  int32_t numOfToken = (int32_t) taosArrayGetSize(pOptions->a);
  if (numOfToken < 1 || numOfToken > 2) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
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
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    for (int32_t i = 1; i < tListLen(LOCAL_DYNAMIC_CFG_OPTIONS); ++i) {
      SDNodeDynConfOption* pOption = &LOCAL_DYNAMIC_CFG_OPTIONS[i];
      if ((pOption->len == pOptionToken->n)
              && (strncasecmp(pOption->name, pOptionToken->z, pOptionToken->n) == 0)) {
        return TSDB_CODE_SUCCESS;
      }
    }
  }
  return TSDB_CODE_TSC_INVALID_OPERATION;
}

int32_t validateColumnName(char* name) {
  bool ret = taosIsKeyWordToken(name, (int32_t)strlen(name));
  if (ret) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  SStrToken token = {.z = name};
  token.n = tGetToken(name, &token.type);

  if (token.type != TK_STRING && token.type != TK_ID) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (token.type == TK_STRING) {
    strdequote(token.z);
    strntolower(token.z, token.z, token.n);
    token.n = (uint32_t)strtrim(token.z);

    int32_t k = tGetToken(token.z, &token.type);
    if (k != token.n) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    return validateColumnName(token.z);
  } else {
    if (isNumber(&token)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  }

  return TSDB_CODE_SUCCESS;
}

bool hasTimestampForPointInterpQuery(SQueryInfo* pQueryInfo) {
  if (!tscIsPointInterpQuery(pQueryInfo)) {
    return true;
  }

  if (pQueryInfo->window.skey == INT64_MIN || pQueryInfo->window.ekey == INT64_MAX) {
    return false;
  }

  return !(pQueryInfo->window.skey != pQueryInfo->window.ekey && pQueryInfo->interval.interval == 0);
}

int32_t validateLimitNode(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode, SSqlObj* pSql) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  const char* msg0 = "soffset/offset can not be less than 0";
  const char* msg1 = "slimit/soffset only available for STable query";
  const char* msg2 = "slimit/soffset can not apply to projection query";

  // handle the limit offset value, validate the limit
  pQueryInfo->limit = pSqlNode->limit;
  pQueryInfo->clauseLimit = pQueryInfo->limit.limit;
  pQueryInfo->slimit = pSqlNode->slimit;
  
  tscDebug("0x%"PRIx64" limit:%" PRId64 ", offset:%" PRId64 " slimit:%" PRId64 ", soffset:%" PRId64, pSql->self,
      pQueryInfo->limit.limit, pQueryInfo->limit.offset, pQueryInfo->slimit.limit, pQueryInfo->slimit.offset);
  
  if (pQueryInfo->slimit.offset < 0 || pQueryInfo->limit.offset < 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg0);
  }

  if (pQueryInfo->limit.limit == 0) {
    tscDebug("0x%"PRIx64" limit 0, no output result", pSql->self);
    pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
    return TSDB_CODE_SUCCESS;
  }

  // todo refactor
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    if (tscIsProjectionQueryOnSTable(pQueryInfo, 0)) {
      if (pQueryInfo->slimit.limit > 0 || pQueryInfo->slimit.offset > 0) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      // for projection query on super table, all queries are subqueries
      if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) &&
          !TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_QUERY)) {
        pQueryInfo->type |= TSDB_QUERY_TYPE_SUBQUERY;
      }
    }

    if (pQueryInfo->slimit.limit == 0) {
      tscDebug("0x%"PRIx64" slimit 0, no output result", pSql->self);
      pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      return TSDB_CODE_SUCCESS;
    }

    /*
     * Get the distribution of all tables among all available virtual nodes that are qualified for the query condition
     * and created according to this super table from management node.
     * And then launching multiple async-queries against all qualified virtual nodes, during the first-stage
     * query operation.
     */
//    assert(allVgroupInfoRetrieved(pQueryInfo));

    // No tables included. No results generated. Query results are empty.
    if (pTableMetaInfo->vgroupList->numOfVgroups == 0) {
      tscDebug("0x%"PRIx64" no table in super table, no output result", pSql->self);
      pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      return TSDB_CODE_SUCCESS;
    }

    // keep original limitation value in globalLimit
    pQueryInfo->clauseLimit = pQueryInfo->limit.limit;
    pQueryInfo->prjOffset   = pQueryInfo->limit.offset;
    pQueryInfo->vgroupLimit = -1;

    if (tscOrderedProjectionQueryOnSTable(pQueryInfo, 0)) {
      /*
       * The offset value should be removed during retrieve data from virtual node, since the
       * global order are done at the client side, so the offset is applied at the client side.
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
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t setKeepOption(SSqlCmd* pCmd, SCreateDbMsg* pMsg, SCreateDbInfo* pCreateDb) {
  const char* msg1 = "invalid number of keep options";
  const char* msg2 = "invalid keep value";
  const char* msg3 = "invalid keep value, should be keep0 <= keep1 <= keep2";

  pMsg->daysToKeep0 = htonl(-1);
  pMsg->daysToKeep1 = htonl(-1);
  pMsg->daysToKeep2 = htonl(-1);

  SArray* pKeep = pCreateDb->keep;
  if (pKeep != NULL) {
    size_t s = taosArrayGetSize(pKeep);
#ifdef _STORAGE
    if (s >= 4 ||s <= 0) {
#else
    if (s != 1) {
#endif
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    tVariantListItem* p0 = taosArrayGet(pKeep, 0);
    tVariantListItem* p1 = (s > 1) ? taosArrayGet(pKeep, 1) : p0;
    tVariantListItem* p2 = (s > 2) ? taosArrayGet(pKeep, 2) : p1;

    if ((int32_t)p0->pVar.i64 <= 0 || (int32_t)p1->pVar.i64 <= 0 || (int32_t)p2->pVar.i64 <= 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
    if (!(((int32_t)p0->pVar.i64 <= (int32_t)p1->pVar.i64) && ((int32_t)p1->pVar.i64 <= (int32_t)p2->pVar.i64))) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    pMsg->daysToKeep0 = htonl((int32_t)p0->pVar.i64);
    pMsg->daysToKeep1 = htonl((int32_t)p1->pVar.i64);
    pMsg->daysToKeep2 = htonl((int32_t)p2->pVar.i64);

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
    } else if (strncmp(pToken->z, TSDB_TIME_PRECISION_NANO_STR, pToken->n) == 0 &&
               strlen(TSDB_TIME_PRECISION_NANO_STR) == pToken->n) {
      pMsg->precision = TSDB_TIME_PRECISION_NANO;
    } else {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
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
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (setTimePrecision(pCmd, pMsg, pCreateDbSql) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (tscCheckCreateDbParams(pCmd, pMsg) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

void addGroupInfoForSubquery(SSqlObj* pParentObj, SSqlObj* pSql, int32_t subClauseIndex, int32_t tableIndex) {
  SQueryInfo* pParentQueryInfo = tscGetQueryInfo(&pParentObj->cmd);

  if (pParentQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    SQueryInfo* pQueryInfo = tscGetQueryInfo(&pSql->cmd);
    SExprInfo* pExpr = NULL;

    size_t size = taosArrayGetSize(pQueryInfo->exprList);
    if (size > 0) {
      pExpr = tscExprGet(pQueryInfo, (int32_t)size - 1);
    }

    if (pExpr == NULL || pExpr->base.functionId != TSDB_FUNC_TAG) {
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pParentQueryInfo, tableIndex);

      uint64_t uid = pTableMetaInfo->pTableMeta->id.uid;
      int16_t colId = tscGetJoinTagColIdByUid(&pQueryInfo->tagCond, uid);

      SSchema* pTagSchema = tscGetColumnSchemaById(pTableMetaInfo->pTableMeta, colId);
      int16_t colIndex = tscGetTagColIndexById(pTableMetaInfo->pTableMeta, colId);
      SColumnIndex tsc_index = {.tableIndex = 0, .columnIndex = colIndex};

      char*   name = pTagSchema->name;
      int16_t type = pTagSchema->type;
      int16_t bytes = pTagSchema->bytes;

      pExpr = tscExprAppend(pQueryInfo, TSDB_FUNC_TAG, &tsc_index, type, bytes, getNewResColId(&pSql->cmd), bytes, true);
      pExpr->base.colInfo.flag = TSDB_COL_TAG;

      // NOTE: tag column does not add to source column list
      SColumnList ids = {0};
      insertResultField(pQueryInfo, (int32_t)size, &ids, bytes, (int8_t)type, name, pExpr);

      int32_t relIndex = tsc_index.columnIndex;

      pExpr->base.colInfo.colIndex = relIndex;
      SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, 0);
      pColIndex->colIndex = relIndex;

      tscColumnListInsert(pTableMetaInfo->tagColList, relIndex, uid, pTagSchema);
    }
  }
}

// limit the output to be 1 for each state value
static void doLimitOutputNormalColOfGroupby(SExprInfo* pExpr) {
  int32_t outputRow = 1;
  tVariantCreateFromBinary(&pExpr->base.param[0], (char*)&outputRow, sizeof(int32_t), TSDB_DATA_TYPE_INT);
  pExpr->base.numOfParams = 1;
}

void doAddGroupColumnForSubquery(SQueryInfo* pQueryInfo, int32_t tagIndex, SSqlCmd* pCmd) {
  SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, tagIndex);
  size_t size = tscNumOfExprs(pQueryInfo);

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  SSchema*     pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, pColIndex->colIndex);
  SColumnIndex colIndex = {.tableIndex = 0, .columnIndex = pColIndex->colIndex};

  SExprInfo* pExprInfo = tscAddFuncInSelectClause(pQueryInfo, (int32_t)size, TSDB_FUNC_PRJ, &colIndex, pSchema,
      TSDB_COL_NORMAL, getNewResColId(pCmd));

  strncpy(pExprInfo->base.token, pExprInfo->base.colInfo.name, tListLen(pExprInfo->base.token));

  int32_t numOfFields = tscNumOfFields(pQueryInfo);
  SInternalField* pInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, numOfFields - 1);

  doLimitOutputNormalColOfGroupby(pInfo->pExpr);
  pInfo->visible = false;
}

static void doUpdateSqlFunctionForTagPrj(SQueryInfo* pQueryInfo) {
  int32_t tagLength = 0;
  size_t size = taosArrayGetSize(pQueryInfo->exprList);

//todo is 0??
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  bool isSTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);

  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId == TSDB_FUNC_TAGPRJ || pExpr->base.functionId == TSDB_FUNC_TAG) {
      pExpr->base.functionId = TSDB_FUNC_TAG_DUMMY;
      tagLength += pExpr->base.resBytes;
    } else if (pExpr->base.functionId == TSDB_FUNC_PRJ && pExpr->base.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      pExpr->base.functionId = TSDB_FUNC_TS_DUMMY;
      tagLength += pExpr->base.resBytes;
    }
  }

  SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId < 0) {
      continue;
    }

    if ((pExpr->base.functionId != TSDB_FUNC_TAG_DUMMY && pExpr->base.functionId != TSDB_FUNC_TS_DUMMY) &&
       !(pExpr->base.functionId == TSDB_FUNC_PRJ && TSDB_COL_IS_UD_COL(pExpr->base.colInfo.flag))) {
      SSchema* pColSchema = &pSchema[pExpr->base.colInfo.colIndex];
      getResultDataInfo(pColSchema->type, pColSchema->bytes, pExpr->base.functionId, (int32_t)pExpr->base.param[0].i64, &pExpr->base.resType,
                        &pExpr->base.resBytes, &pExpr->base.interBytes, tagLength, isSTable, NULL);
    }
  }
}

static int32_t doUpdateSqlFunctionForColPrj(SQueryInfo* pQueryInfo) {
  size_t size = taosArrayGetSize(pQueryInfo->exprList);
  
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);

    if (pExpr->base.functionId == TSDB_FUNC_PRJ && (!TSDB_COL_IS_UD_COL(pExpr->base.colInfo.flag) && (pExpr->base.colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX))) {
      bool qualifiedCol = false;
      for (int32_t j = 0; j < pQueryInfo->groupbyExpr.numOfGroupCols; ++j) {
        SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, j);
  
        if (pExpr->base.colInfo.colId == pColIndex->colId) {
          qualifiedCol = true;
          doLimitOutputNormalColOfGroupby(pExpr);
          pExpr->base.numOfParams = 1;
          break;
        }
      }

      // it is not a tag column/tbname column/user-defined column, return error
      if (!qualifiedCol) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool tagColumnInGroupby(SGroupbyExpr* pGroupbyExpr, int16_t columnId) {
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
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId == TSDB_FUNC_PRJ) {
      hasColumnPrj = true;
    } else if (pExpr->base.functionId == TSDB_FUNC_TAGPRJ) {
      hasTagPrj = true;
    }
  }

  return (hasTagPrj) && (hasColumnPrj == false);
}

// check if all the tags prj columns belongs to the group by columns
static bool allTagPrjInGroupby(SQueryInfo* pQueryInfo) {
  bool allInGroupby = true;

  size_t size = tscNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId != TSDB_FUNC_TAGPRJ) {
      continue;
    }

    if (!tagColumnInGroupby(&pQueryInfo->groupbyExpr, pExpr->base.colInfo.colId)) {
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
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    if (pExpr->base.functionId == TSDB_FUNC_TAGPRJ) {
      pExpr->base.functionId = TSDB_FUNC_TAG;
    }
  }
}

/*
 * check for selectivity function + tags column function both exist.
 * 1. tagprj functions are not compatible with aggregated function when missing "group by" clause
 * 2. if selectivity function and tagprj function both exist, there should be only
 *    one selectivity function exists.
 */
static int32_t checkUpdateTagPrjFunctions(SQueryInfo* pQueryInfo, char* msg) {
  const char* msg1 = "only one selectivity function allowed in presence of tags function";
  const char* msg2 = "aggregation function should not be mixed up with projection";

  bool    tagTsColExists = false;
  int16_t numOfSelectivity = 0;
  int16_t numOfAggregation = 0;

  size_t numOfExprs = taosArrayGetSize(pQueryInfo->exprList);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = taosArrayGetP(pQueryInfo->exprList, i);
    if (pExpr->base.functionId == TSDB_FUNC_TAGPRJ ||
        (pExpr->base.functionId == TSDB_FUNC_PRJ && pExpr->base.colInfo.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX)) {
      tagTsColExists = true;  // selectivity + ts/tag column
      break;
    }
  }

  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = taosArrayGetP(pQueryInfo->exprList, i);
  
    int16_t functionId = pExpr->base.functionId;
    if (functionId == TSDB_FUNC_TAGPRJ || functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_TS ||
        functionId == TSDB_FUNC_ARITHM || functionId == TSDB_FUNC_TS_DUMMY) {
      continue;
    }

    if (functionId < 0) {
      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * functionId - 1);
      if (pUdfInfo->funcType == TSDB_UDF_TYPE_AGGREGATE) {
        ++numOfAggregation;
      }

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
      return invalidOperationMsg(msg, msg1);
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
        SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
        int16_t functionId = pExpr->base.functionId;
        if (functionId == TSDB_FUNC_TAGPRJ || (aAggs[functionId].status & TSDB_FUNCSTATE_SELECTIVITY) == 0) {
          continue;
        }

        if ((functionId == TSDB_FUNC_LAST_ROW) ||
             (functionId == TSDB_FUNC_LAST_DST && (pExpr->base.colInfo.flag & TSDB_COL_NULL) != 0)) {
          // do nothing
        } else {
          return invalidOperationMsg(msg, msg1);
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
        return invalidOperationMsg(msg, msg2);
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
  const char* msg1 = "interval not allowed in group by normal column";

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);

  SSchema* tagSchema = NULL;
  if (!UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
    tagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
  }

  SSchema tmp = {.type = 0, .name = "", .colId = 0, .bytes = 0};
  SSchema* s = &tmp;

  for (int32_t i = 0; i < pQueryInfo->groupbyExpr.numOfGroupCols; ++i) {
    SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, i);
    int16_t colIndex = pColIndex->colIndex;

    if (colIndex == TSDB_TBNAME_COLUMN_INDEX) {
      s = tGetTbnameColumnSchema();
    } else {
      if (TSDB_COL_IS_TAG(pColIndex->flag)) {
        if(tagSchema){
          s = &tagSchema[colIndex];
        }
      } else {
        s = &pSchema[colIndex];
      }
    }
    
    if (TSDB_COL_IS_TAG(pColIndex->flag)) {

      int32_t f = TSDB_FUNC_TAG;
      if (tscIsDiffDerivQuery(pQueryInfo)) {
        f = TSDB_FUNC_TAGPRJ;
      }

      int32_t pos = tscGetFirstInvisibleFieldPos(pQueryInfo);      

      SColumnIndex tsc_index = {.tableIndex = pQueryInfo->groupbyExpr.tableIndex, .columnIndex = colIndex};
      SExprInfo*   pExpr = tscExprInsert(pQueryInfo, pos, f, &tsc_index, s->type, s->bytes, getNewResColId(pCmd), s->bytes, true);

      memset(pExpr->base.aliasName, 0, sizeof(pExpr->base.aliasName));
      tstrncpy(pExpr->base.aliasName, s->name, sizeof(pExpr->base.aliasName));
      tstrncpy(pExpr->base.token, s->name, sizeof(pExpr->base.aliasName));

      pExpr->base.colInfo.flag = TSDB_COL_TAG;

      // NOTE: tag column does not add to source column list
      SColumnList ids = createColumnList(1, 0, pColIndex->colIndex);
      insertResultField(pQueryInfo, pos, &ids, s->bytes, (int8_t)s->type, s->name, pExpr);
    } else {
      // if this query is "group by" normal column, time window query is not allowed
      if (isTimeWindowQuery(pQueryInfo)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      size_t size = tscNumOfExprs(pQueryInfo);

      bool hasGroupColumn = false;
      for (int32_t j = 0; j < size; ++j) {
        SExprInfo* pExpr = tscExprGet(pQueryInfo, j);
        if ((pExpr->base.functionId == TSDB_FUNC_PRJ) && pExpr->base.colInfo.colId == pColIndex->colId) {
          hasGroupColumn = true;
          break;
        }
      }

      //if the group by column does not required by user, add an invisible column into the final result set.
      if (!hasGroupColumn) {
        doAddGroupColumnForSubquery(pQueryInfo, i, pCmd);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doTagFunctionCheck(SQueryInfo* pQueryInfo) {
  bool tagProjection = false;
  bool tableCounting = false;

  int32_t numOfCols = (int32_t) tscNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
    int32_t functionId = pExpr->base.functionId;

    if (functionId == TSDB_FUNC_TAGPRJ) {
      tagProjection = true;
      continue;
    }

    if (functionId == TSDB_FUNC_COUNT) {
      assert(pExpr->base.colInfo.colId == TSDB_TBNAME_COLUMN_INDEX);
      tableCounting = true;
    }
  }

  return (tableCounting && tagProjection)? -1:0;
}

int32_t doFunctionsCompatibleCheck(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, char* msg) {
  const char* msg1 = "functions/columns not allowed in group by query";
  const char* msg2 = "projection query on columns not allowed";
  const char* msg3 = "group by/session/state_window not allowed on projection query";
  const char* msg4 = "retrieve tags not compatible with group by or interval query";
  const char* msg5 = "functions can not be mixed up";
  const char* msg6 = "TWA/Diff/Derivative/Irate only support group by tbname";

  // only retrieve tags, group by is not supportted
  if (tscQueryTags(pQueryInfo)) {
    if (doTagFunctionCheck(pQueryInfo) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(msg, msg5);
    }

    if (pQueryInfo->groupbyExpr.numOfGroupCols > 0 || isTimeWindowQuery(pQueryInfo)) {
      return invalidOperationMsg(msg, msg4);
    } else {
      return TSDB_CODE_SUCCESS;
    }
  }
  if (tscIsProjectionQuery(pQueryInfo) && tscIsSessionWindowQuery(pQueryInfo)) {
    return invalidOperationMsg(msg, msg3);
  }

  if (pQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    // check if all the tags prj columns belongs to the group by columns
    if (onlyTagPrjFunction(pQueryInfo) && allTagPrjInGroupby(pQueryInfo)) {
      // It is a groupby aggregate query, the tag project function is not suitable for this case.
      updateTagPrjFunction(pQueryInfo);

      return doAddGroupbyColumnsOnDemand(pCmd, pQueryInfo);
    }

    // check all query functions in selection clause, multi-output functions are not allowed
    size_t size = tscNumOfExprs(pQueryInfo);
    for (int32_t i = 0; i < size; ++i) {
      SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
      int32_t   f = pExpr->base.functionId;

      /*
       * group by normal columns.
       * Check if the column projection is identical to the group by column or not
       */
      if (f == TSDB_FUNC_PRJ && pExpr->base.colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
        bool qualified = false;
        for (int32_t j = 0; j < pQueryInfo->groupbyExpr.numOfGroupCols; ++j) {
          SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, j);
          if (pColIndex->colId == pExpr->base.colInfo.colId) {
            qualified = true;
            break;
          }
        }

        if (!qualified) {
          return invalidOperationMsg(msg, msg2);
        }
      }

      if (f < 0) {
        SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * f - 1);
        if (pUdfInfo->funcType == TSDB_UDF_TYPE_SCALAR) {
          return invalidOperationMsg(msg, msg1);
        }
        
        continue;
      }

      if ((!pQueryInfo->stateWindow) && (f == TSDB_FUNC_DIFF || f == TSDB_FUNC_DERIVATIVE || f == TSDB_FUNC_TWA || f == TSDB_FUNC_IRATE)) {
        for (int32_t j = 0; j < pQueryInfo->groupbyExpr.numOfGroupCols; ++j) {
          SColIndex* pColIndex = taosArrayGet(pQueryInfo->groupbyExpr.columnInfo, j);
          if (j == 0) {
            if (pColIndex->colIndex != TSDB_TBNAME_COLUMN_INDEX) {
              return invalidOperationMsg(msg, msg6);
            }
          } else if (!TSDB_COL_IS_TAG(pColIndex->flag)) {
            return invalidOperationMsg(msg, msg6);
          }
        }
      }

      if (IS_MULTIOUTPUT(aAggs[f].status) && f != TSDB_FUNC_TOP && f != TSDB_FUNC_BOTTOM && f != TSDB_FUNC_DIFF &&
          f != TSDB_FUNC_DERIVATIVE && f != TSDB_FUNC_TAGPRJ && f != TSDB_FUNC_PRJ) {
        return invalidOperationMsg(msg, msg1);
      }

      if (f == TSDB_FUNC_COUNT && pExpr->base.colInfo.colIndex == TSDB_TBNAME_COLUMN_INDEX) {
        return invalidOperationMsg(msg, msg1);
      }
    }

    if (checkUpdateTagPrjFunctions(pQueryInfo, msg) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (doAddGroupbyColumnsOnDemand(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // projection query on super table does not compatible with "group by" syntax
    if (tscIsProjectionQuery(pQueryInfo) && !(tscIsDiffDerivQuery(pQueryInfo))) {
      return invalidOperationMsg(msg, msg3);
    }

    return TSDB_CODE_SUCCESS;
  } else {
    return checkUpdateTagPrjFunctions(pQueryInfo, msg);
  }
}


int32_t validateFunctionFromUpstream(SQueryInfo* pQueryInfo, char* msg) {
  const char* msg1 = "TWA/Diff/Derivative/Irate are not allowed to apply to super table without group by tbname";

  int32_t numOfExprs = (int32_t)tscNumOfExprs(pQueryInfo);
  size_t upNum = taosArrayGetSize(pQueryInfo->pUpstream);
  
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);
  
    int32_t f = pExpr->base.functionId;
    if (f == TSDB_FUNC_DERIVATIVE || f == TSDB_FUNC_TWA || f == TSDB_FUNC_IRATE || f == TSDB_FUNC_DIFF) {
      for (int32_t j = 0; j < upNum; ++j) {
        SQueryInfo* pUp = taosArrayGetP(pQueryInfo->pUpstream, j);
        STableMetaInfo  *pTableMetaInfo = tscGetMetaInfo(pUp, 0);
        bool isSTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);
        if ((!isSTable) || groupbyTbname(pUp) || pUp->interval.interval > 0) {
          return TSDB_CODE_SUCCESS;
        }
      }
    
      return invalidOperationMsg(msg, msg1);
    }
  }

  return TSDB_CODE_SUCCESS;
}


int32_t doLocalQueryProcess(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode) {
  const char* msg1 = "only one expression allowed";
  const char* msg2 = "invalid expression in select clause";
  const char* msg3 = "invalid function";

  SArray* pExprList = pSqlNode->pSelNodeList;
  size_t size = taosArrayGetSize(pExprList);
  if (size != 1) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  bool server_status = false;
  tSqlExprItem* pExprItem = taosArrayGet(pExprList, 0);
  tSqlExpr* pExpr = pExprItem->pNode;
  if (pExpr->Expr.operand.z == NULL) {
    //handle 'select 1'
    if (pExpr->exprToken.n == 1 && 0 == strncasecmp(pExpr->exprToken.z, "1", 1)) {
      server_status = true; 
    } else {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    } 
  } 
  // TODO redefine the function
   SDNodeDynConfOption functionsInfo[5] = {{"database()", 10},
                                            {"server_version()", 16},
                                            {"server_status()", 15},
                                            {"client_version()", 16},
                                            {"current_user()", 14}};

  int32_t tsc_index = -1;
  if (server_status == true) {
    tsc_index = 2;
  } else {
    for (int32_t i = 0; i < tListLen(functionsInfo); ++i) {
      if (strncasecmp(functionsInfo[i].name, pExpr->exprToken.z, functionsInfo[i].len) == 0 &&
          functionsInfo[i].len == pExpr->exprToken.n) {
        tsc_index = i;
        break;
      }
    }
  }

  switch (tsc_index) {
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
    default: { return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3); }
  }
  
  SColumnIndex ind = {0};
  SExprInfo* pExpr1 = tscExprAppend(pQueryInfo, TSDB_FUNC_TAG_DUMMY, &ind, TSDB_DATA_TYPE_INT,
                                      tDataTypes[TSDB_DATA_TYPE_INT].bytes, getNewResColId(pCmd), tDataTypes[TSDB_DATA_TYPE_INT].bytes, false);

  tSqlExprItem* item = taosArrayGet(pExprList, 0);
  const char* name = (item->aliasName != NULL)? item->aliasName:functionsInfo[tsc_index].name;
  tstrncpy(pExpr1->base.aliasName, name, tListLen(pExpr1->base.aliasName));
  
  return TSDB_CODE_SUCCESS;
}

// can only perform the parameters based on the macro definitation
int32_t tscCheckCreateDbParams(SSqlCmd* pCmd, SCreateDbMsg* pCreate) {
  char msg[512] = {0};

  if (pCreate->walLevel != -1 && (pCreate->walLevel < TSDB_MIN_WAL_LEVEL || pCreate->walLevel > TSDB_MAX_WAL_LEVEL)) {
    snprintf(msg, tListLen(msg), "invalid db option walLevel: %d, only 1-2 allowed", pCreate->walLevel);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (pCreate->replications != -1 &&
      (pCreate->replications < TSDB_MIN_DB_REPLICA_OPTION || pCreate->replications > TSDB_MAX_DB_REPLICA_OPTION)) {
    snprintf(msg, tListLen(msg), "invalid db option replications: %d valid range: [%d, %d]", pCreate->replications,
             TSDB_MIN_DB_REPLICA_OPTION, TSDB_MAX_DB_REPLICA_OPTION);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  int32_t blocks = ntohl(pCreate->totalBlocks);
  if (blocks != -1 && (blocks < TSDB_MIN_TOTAL_BLOCKS || blocks > TSDB_MAX_TOTAL_BLOCKS)) {
    snprintf(msg, tListLen(msg), "invalid db option totalBlocks: %d valid range: [%d, %d]", blocks,
             TSDB_MIN_TOTAL_BLOCKS, TSDB_MAX_TOTAL_BLOCKS);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (pCreate->quorum != -1 &&
      (pCreate->quorum < TSDB_MIN_DB_QUORUM_OPTION || pCreate->quorum > TSDB_MAX_DB_QUORUM_OPTION)) {
    snprintf(msg, tListLen(msg), "invalid db option quorum: %d valid range: [%d, %d]", pCreate->quorum,
             TSDB_MIN_DB_QUORUM_OPTION, TSDB_MAX_DB_QUORUM_OPTION);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  int32_t val = htonl(pCreate->daysPerFile);
  if (val != -1 && (val < TSDB_MIN_DAYS_PER_FILE || val > TSDB_MAX_DAYS_PER_FILE)) {
    snprintf(msg, tListLen(msg), "invalid db option daysPerFile: %d valid range: [%d, %d]", val,
             TSDB_MIN_DAYS_PER_FILE, TSDB_MAX_DAYS_PER_FILE);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  val = htonl(pCreate->cacheBlockSize);
  if (val != -1 && (val < TSDB_MIN_CACHE_BLOCK_SIZE || val > TSDB_MAX_CACHE_BLOCK_SIZE)) {
    snprintf(msg, tListLen(msg), "invalid db option cacheBlockSize: %d valid range: [%d, %d]", val,
             TSDB_MIN_CACHE_BLOCK_SIZE, TSDB_MAX_CACHE_BLOCK_SIZE);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  val = htonl(pCreate->maxTables);
  if (val != -1 && (val < TSDB_MIN_TABLES || val > TSDB_MAX_TABLES)) {
    snprintf(msg, tListLen(msg), "invalid db option maxSessions: %d valid range: [%d, %d]", val,
             TSDB_MIN_TABLES, TSDB_MAX_TABLES);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (pCreate->precision != TSDB_TIME_PRECISION_MILLI && pCreate->precision != TSDB_TIME_PRECISION_MICRO &&
      pCreate->precision != TSDB_TIME_PRECISION_NANO) {
    snprintf(msg, tListLen(msg), "invalid db option timePrecision: %d valid value: [%d, %d, %d]", pCreate->precision,
             TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  val = htonl(pCreate->commitTime);
  if (val != -1 && (val < TSDB_MIN_COMMIT_TIME || val > TSDB_MAX_COMMIT_TIME)) {
    snprintf(msg, tListLen(msg), "invalid db option commitTime: %d valid range: [%d, %d]", val,
             TSDB_MIN_COMMIT_TIME, TSDB_MAX_COMMIT_TIME);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  val = htonl(pCreate->fsyncPeriod);
  if (val != -1 && (val < TSDB_MIN_FSYNC_PERIOD || val > TSDB_MAX_FSYNC_PERIOD)) {
    snprintf(msg, tListLen(msg), "invalid db option fsyncPeriod: %d valid range: [%d, %d]", val,
             TSDB_MIN_FSYNC_PERIOD, TSDB_MAX_FSYNC_PERIOD);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  if (pCreate->compression != -1 &&
      (pCreate->compression < TSDB_MIN_COMP_LEVEL || pCreate->compression > TSDB_MAX_COMP_LEVEL)) {
    snprintf(msg, tListLen(msg), "invalid db option compression: %d valid range: [%d, %d]", pCreate->compression,
             TSDB_MIN_COMP_LEVEL, TSDB_MAX_COMP_LEVEL);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }

  val = (int16_t)htons(pCreate->partitions);
  if (val != -1 &&
      (val < TSDB_MIN_DB_PARTITON_OPTION || val > TSDB_MAX_DB_PARTITON_OPTION)) {
    snprintf(msg, tListLen(msg), "invalid topic option partition: %d valid range: [%d, %d]", val,
             TSDB_MIN_DB_PARTITON_OPTION, TSDB_MAX_DB_PARTITON_OPTION);
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg);
  }


  return TSDB_CODE_SUCCESS;
}

// for debug purpose
void tscPrintSelNodeList(SSqlObj* pSql, int32_t subClauseIndex) {
  SQueryInfo* pQueryInfo = tscGetQueryInfo(&pSql->cmd);

  int32_t size = (int32_t)tscNumOfExprs(pQueryInfo);
  if (size == 0) {
    return;
  }

  int32_t totalBufSize = 1024;

  char    str[1024+1] = {0};
  int32_t offset = 0;

  offset += sprintf(str, "num:%d [", size);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, i);

    char    tmpBuf[1024] = {0};
    int32_t tmpLen = 0;
    char   *name = NULL;

    if (pExpr->base.functionId < 0) {
      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, -1 * pExpr->base.functionId - 1);
      name = pUdfInfo->name;
    } else {
      name = aAggs[pExpr->base.functionId].name;
    }

    tmpLen =
        sprintf(tmpBuf, "%s(uid:%" PRIu64 ", %d)", name, pExpr->base.uid, pExpr->base.colInfo.colId);

    if (tmpLen + offset >= totalBufSize - 1) break;


    offset += sprintf(str + offset, "%s", tmpBuf);

    if (i < size - 1) {
      str[offset++] = ',';
    }
  }

  assert(offset < totalBufSize);
  str[offset] = ']';
  assert(offset < totalBufSize);
  tscDebug("0x%"PRIx64" select clause:%s", pSql->self, str);
}

int32_t doCheckForCreateTable(SSqlObj* pSql, int32_t subClauseIndex, SSqlInfo* pInfo) {
  const char* msg1 = "invalid table name";

  SSqlCmd*        pCmd = &pSql->cmd;
  SQueryInfo*     pQueryInfo = tscGetQueryInfo(pCmd);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;

  SArray* pFieldList = pCreateTable->colInfo.pColumns;
  SArray* pTagList = pCreateTable->colInfo.pTagColumns;

  assert(pFieldList != NULL);

  // if sql specifies db, use it, otherwise use default db
  SStrToken* pzTableName = &(pCreateTable->name);

  if (tscValidateName(pzTableName) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  int32_t code = tscSetTableFullName(&pTableMetaInfo->name, pzTableName, pSql);
  if(code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (!validateTableColumnInfo(pFieldList, pCmd) ||
      (pTagList != NULL && !validateTagParams(pTagList, pFieldList, pCmd))) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
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
  const char* msg6 = "create table only from super table is allowed";

  SSqlCmd* pCmd = &pSql->cmd;

  SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;
  SQueryInfo*      pQueryInfo = tscGetQueryInfo(pCmd);

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
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    int32_t code = tscSetTableFullName(&pStableMetaInfo->name, pToken, pSql);
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

    if (!UTIL_TABLE_IS_SUPER_TABLE(pStableMetaInfo)) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
    }

    size_t valSize = taosArrayGetSize(pValList);

    // too long tag values will return invalid sql, not be truncated automatically
    SSchema  *pTagSchema = tscGetTableTagSchema(pStableMetaInfo->pTableMeta);
    STableComInfo tinfo = tscGetTableInfo(pStableMetaInfo->pTableMeta);
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
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      if (schemaSize < valSize) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
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

            char tagVal[TSDB_MAX_TAGS_LEN] = {0};
            if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
              if (pItem->pVar.nLen > pSchema->bytes) {
                tdDestroyKVRowBuilder(&kvRowBuilder);
                return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
              }
            } else if (pSchema->type == TSDB_DATA_TYPE_TIMESTAMP) {
              if (pItem->pVar.nType == TSDB_DATA_TYPE_BINARY) {
                ret = convertTimestampStrToInt64(&(pItem->pVar), tinfo.precision);
                if (ret != TSDB_CODE_SUCCESS) {
                  return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
                }
              } else if (pItem->pVar.nType == TSDB_DATA_TYPE_TIMESTAMP) {
                pItem->pVar.i64 = convertTimePrecision(pItem->pVar.i64, TSDB_TIME_PRECISION_NANO, tinfo.precision);
              }
            }

            ret = tVariantDump(&(pItem->pVar), tagVal, pSchema->type, true);

            // check again after the convert since it may be converted from binary to nchar.
            if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
              int16_t len = varDataTLen(tagVal);
              if (len > pSchema->bytes) {
                tdDestroyKVRowBuilder(&kvRowBuilder);
                return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
              }
            }

            if (ret != TSDB_CODE_SUCCESS) {
              tdDestroyKVRowBuilder(&kvRowBuilder);
              return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
            }

            tdAddColToKVRow(&kvRowBuilder, pSchema->colId, pSchema->type, tagVal);

            findColumnIndex = true;
            break;
          }
        }

        if (!findColumnIndex) {
          tdDestroyKVRowBuilder(&kvRowBuilder);
          return tscInvalidOperationMsg(pCmd->payload, "invalid tag name", sToken->z);
        }
      }
    } else {
      if (schemaSize != valSize) {
        tdDestroyKVRowBuilder(&kvRowBuilder);
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
      }

      for (int32_t i = 0; i < valSize; ++i) {
        SSchema*          pSchema = &pTagSchema[i];
        tVariantListItem* pItem = taosArrayGet(pValList, i);

        char tagVal[TSDB_MAX_TAGS_LEN];
        if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
          if (pItem->pVar.nLen > pSchema->bytes) {
            tdDestroyKVRowBuilder(&kvRowBuilder);
            return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }
        } else if (pSchema->type == TSDB_DATA_TYPE_TIMESTAMP) {
          if (pItem->pVar.nType == TSDB_DATA_TYPE_BINARY) {
            ret = convertTimestampStrToInt64(&(pItem->pVar), tinfo.precision);
            if (ret != TSDB_CODE_SUCCESS) {
              return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
            }
          } else if (pItem->pVar.nType == TSDB_DATA_TYPE_TIMESTAMP) {
            pItem->pVar.i64 = convertTimePrecision(pItem->pVar.i64, TSDB_TIME_PRECISION_NANO, tinfo.precision);
          }
        }


        ret = tVariantDump(&(pItem->pVar), tagVal, pSchema->type, true);

        // check again after the convert since it may be converted from binary to nchar.
        if (pSchema->type == TSDB_DATA_TYPE_BINARY || pSchema->type == TSDB_DATA_TYPE_NCHAR) {
          int16_t len = varDataTLen(tagVal);
          if (len > pSchema->bytes) {
            tdDestroyKVRowBuilder(&kvRowBuilder);
            return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
          }
        }

        if (ret != TSDB_CODE_SUCCESS) {
          tdDestroyKVRowBuilder(&kvRowBuilder);
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
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
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, TABLE_INDEX);
    ret = tscSetTableFullName(&pTableMetaInfo->name, &pCreateTableInfo->name, pSql);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    pCreateTableInfo->fullname = calloc(1, tNameLen(&pTableMetaInfo->name) + 1);
    ret = tNameExtractFullName(&pTableMetaInfo->name, pCreateTableInfo->fullname);
    if (ret != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
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
  const char* msg8 = "the first column should be primary timestamp column";

  SSqlCmd*    pCmd = &pSql->cmd;
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  assert(pQueryInfo->numOfTables == 1);

  SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;
  STableMetaInfo*  pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  // if sql specifies db, use it, otherwise use default db
  SStrToken* pName = &(pCreateTable->name);
  SSqlNode* pSqlNode = pCreateTable->pSelect;

  if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }
  
  // check to valid and create to name
  if(pInfo->pCreateTableInfo->to.n > 0) {
    if (tscValidateName(&pInfo->pCreateTableInfo->to) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }
    int32_t code = tscSetTableFullName(&pInfo->pCreateTableInfo->toSName, &pInfo->pCreateTableInfo->to, pSql);
    if(code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }  
  

  SRelationInfo* pFromInfo = pInfo->pCreateTableInfo->pSelect->from;
  if (pFromInfo == NULL || taosArrayGetSize(pFromInfo->list) == 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
  }

  SRelElementPair* p1 = taosArrayGet(pFromInfo->list, 0);
  SStrToken srcToken = {.z = p1->tableName.z, .n = p1->tableName.n, .type = TK_STRING};
  if (tscValidateName(&srcToken) != TSDB_CODE_SUCCESS) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  int32_t code = tscSetTableFullName(&pTableMetaInfo->name, &srcToken, pSql);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tscGetTableMeta(pSql, pTableMetaInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (validateSelectNodeList(&pSql->cmd, pQueryInfo, pSqlNode->pSelNodeList, false, false, false) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  int32_t joinQuery = (pSqlNode->from != NULL && taosArrayGetSize(pSqlNode->from->list) > 1);

  if (pSqlNode->pWhere != NULL) {  // query condition in stream computing
    if (validateWhereNode(pQueryInfo, &pSqlNode->pWhere, pSql, joinQuery) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  }

  // set interval value
  if (validateIntervalNode(pSql, pQueryInfo, pSqlNode) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (isTimeWindowQuery(pQueryInfo) && (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  // project query primary column must be timestamp type
  if (tscIsProjectionQuery(pQueryInfo)) {
    SExprInfo* pExpr = tscExprGet(pQueryInfo, 0);
    if (pExpr->base.colInfo.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg8);
    }
  } else {
    if (pQueryInfo->interval.interval == 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
    }
  }

  // set the created table[stream] name
  code = tscSetTableFullName(&pTableMetaInfo->name, pName, pSql);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pSqlNode->sqlstr.n > TSDB_MAX_SAVED_SQL_LEN) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
  }

  if (tsRewriteFieldNameIfNecessary(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  pCmd->numOfCols = pQueryInfo->fieldsInfo.numOfOutput;

  if (validateSqlFunctionInStreamSql(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  /*
   * check if fill operation is available, the fill operation is parsed and executed during query execution,
   * not here.
   */
  if (pSqlNode->fillType != NULL) {
    if (pQueryInfo->interval.interval == 0) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    tVariantListItem* pItem = taosArrayGet(pSqlNode->fillType, 0);
    if (pItem->pVar.nType == TSDB_DATA_TYPE_BINARY) {
      if (!((strncmp(pItem->pVar.pz, "none", 4) == 0 && pItem->pVar.nLen == 4) ||
            (strncmp(pItem->pVar.pz, "null", 4) == 0 && pItem->pVar.nLen == 4))) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
      }
    }
  }

  // set the number of stream table columns
  pCmd->numOfCols = pQueryInfo->fieldsInfo.numOfOutput;
  return TSDB_CODE_SUCCESS;
}

int32_t checkQueryRangeForFill(SSqlCmd* pCmd, SQueryInfo* pQueryInfo) {
  const char* msg3 = "start(end) time of query range required or time range too large";

  if (pQueryInfo->interval.interval == 0) {
    return TSDB_CODE_SUCCESS;
  }

    bool initialWindows = TSWINDOW_IS_EQUAL(pQueryInfo->window, TSWINDOW_INITIALIZER);
    if (initialWindows) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
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
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
    }

    return TSDB_CODE_SUCCESS;
}

// TODO normalize the function expression and compare it
int32_t tscGetExprFilters(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SArray* pSelectNodeList, tSqlExpr* pSqlExpr, SExprInfo** pExpr) {
  const char* msg1 = "invalid sql expression in having";

  *pExpr = NULL;
  size_t nx = tscNumOfExprs(pQueryInfo);

  // parameters is needed for functions
  if (pSqlExpr->Expr.paramList == NULL && pSqlExpr->functionId != TSDB_FUNC_COUNT) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  tSqlExprItem *pParam  = NULL;
  SSchema      schema = {0};

  if (pSqlExpr->Expr.paramList != NULL) {
    pParam = taosArrayGet(pSqlExpr->Expr.paramList, 0);
    SStrToken* pToken = &pParam->pNode->columnName;

    SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
    getColumnIndexByName(pToken, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd));
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
    schema = *tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, tsc_index.columnIndex);
  } else {
    schema = (SSchema) {.colId = PRIMARYKEY_TIMESTAMP_COL_INDEX, .type = TSDB_DATA_TYPE_TIMESTAMP, .bytes = TSDB_KEYSIZE};
  }

  for(int32_t i = 0; i < nx; ++i) {
    SExprInfo* pExprInfo = tscExprGet(pQueryInfo, i);
    if (pExprInfo->base.functionId == pSqlExpr->functionId && pExprInfo->base.colInfo.colId == schema.colId) {
      ++pQueryInfo->havingFieldNum;
      *pExpr = pExprInfo;
      return TSDB_CODE_SUCCESS;
    }
  }

//  size_t num = taosArrayGetSize(pSelectNodeList);
//  for(int32_t i = 0; i < num; ++i) {
//    tSqlExprItem* pItem = taosArrayGet(pSelectNodeList, i);
//
//    if (tSqlExprCompare(pItem->pNode, pSqlExpr) == 0) { // exists, not added it,
//
//      SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
//      int32_t functionId = pSqlExpr->functionId;
//      if (pSqlExpr->Expr.paramList == NULL) {
//        tsc_index.columnIndex = 0;
//        tsc_index.tableIndex  = 0;
//      } else {
//        tSqlExprItem* pParamElem = taosArrayGet(pSqlExpr->Expr.paramList, 0);
//        SStrToken* pToken = &pParamElem->pNode->columnName;
//        getColumnIndexByName(pToken, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd));
//      }
//
//      size_t numOfNodeInSel = tscNumOfExprs(pQueryInfo);
//      for(int32_t k = 0; k < numOfNodeInSel; ++k) {
//        SExprInfo* pExpr1 = tscExprGet(pQueryInfo, k);
//
//        if (pExpr1->base.functionId != functionId) {
//          continue;
//        }
//
//        if (pExpr1->base.colInfo.colIndex != tsc_index.columnIndex) {
//          continue;
//        }
//
//        ++pQueryInfo->havingFieldNum;
//        *pExpr = pExpr1;
//        break;
//      }
//
//      assert(*pExpr != NULL);
//      return TSDB_CODE_SUCCESS;
//    }
//  }

  tSqlExprItem item = {.pNode = pSqlExpr, .aliasName = NULL, .distinct = false};

  int32_t outputIndex = (int32_t)tscNumOfExprs(pQueryInfo);

  // ADD TRUE FOR TEST
  if (addExprAndResultField(pCmd, pQueryInfo, outputIndex, &item, true, NULL) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  ++pQueryInfo->havingFieldNum;

  size_t n = tscNumOfExprs(pQueryInfo);
  *pExpr = tscExprGet(pQueryInfo, (int32_t)n - 1);

  SInternalField* pField = taosArrayGetLast(pQueryInfo->fieldsInfo.internalField);
  pField->visible = false;

  return TSDB_CODE_SUCCESS;
}

static int32_t handleExprInHavingClause(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SArray* pSelectNodeList, tSqlExpr* pExpr, int32_t sqlOptr) {
  const char* msg1 = "non binary column not support like operator";
  const char* msg2 = "invalid operator for binary column in having clause";
  const char* msg3 = "invalid operator for bool column in having clause";

  SColumnFilterInfo* pColFilter = NULL;
  // TODO refactor: validate the expression
  /*
   * in case of TK_AND filter condition, we first find the corresponding column and build the query condition together
   * the already existed condition.
   */
  SExprInfo *expr = NULL;
  if (sqlOptr == TK_AND) {
    int32_t ret = tscGetExprFilters(pCmd, pQueryInfo, pSelectNodeList, pExpr->pLeft, &expr);
    if (ret) {
      return ret;
    }

    // this is a new filter condition on this column
    if (expr->base.flist.numOfFilters == 0) {
      pColFilter = addColumnFilterInfo(&expr->base.flist);
    } else {  // update the existed column filter information, find the filter info here
      pColFilter = &expr->base.flist.filterInfo[0];
    }

    if (pColFilter == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  } else if (sqlOptr == TK_OR) {
    int32_t ret = tscGetExprFilters(pCmd, pQueryInfo, pSelectNodeList, pExpr->pLeft, &expr);
    if (ret) {
      return ret;
    }

    // TODO fixme: failed to invalid the filter expression: "col1 = 1 OR col2 = 2"
    // TODO refactor
    pColFilter = addColumnFilterInfo(&expr->base.flist);
    if (pColFilter == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  } else {  // error;
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  pColFilter->filterstr =
      ((expr->base.resType == TSDB_DATA_TYPE_BINARY || expr->base.resType == TSDB_DATA_TYPE_NCHAR) ? 1 : 0);

  if (pColFilter->filterstr) {
    if (pExpr->tokenId != TK_EQ
        && pExpr->tokenId != TK_NE
        && pExpr->tokenId != TK_ISNULL
        && pExpr->tokenId != TK_NOTNULL
        && pExpr->tokenId != TK_LIKE
        ) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }
  } else {
    if (pExpr->tokenId == TK_LIKE) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    if (expr->base.resType == TSDB_DATA_TYPE_BOOL) {
      if (pExpr->tokenId != TK_EQ && pExpr->tokenId != TK_NE) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }
    }
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;

  int32_t ret = doExtractColumnFilterInfo(pCmd, pQueryInfo, pTableMeta->tableInfo.precision, pColFilter,
                                          expr->base.resType, pExpr);
  if (ret) {
    return ret;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getHavingExpr(SSqlCmd* pCmd, SQueryInfo* pQueryInfo, SArray* pSelectNodeList, tSqlExpr* pExpr, int32_t parentOptr) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* msg1 = "invalid having clause";

  tSqlExpr* pLeft = pExpr->pLeft;
  tSqlExpr* pRight = pExpr->pRight;

  if (pExpr->tokenId == TK_AND || pExpr->tokenId == TK_OR) {
    int32_t ret = getHavingExpr(pCmd, pQueryInfo, pSelectNodeList, pExpr->pLeft, pExpr->tokenId);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    return getHavingExpr(pCmd, pQueryInfo, pSelectNodeList, pExpr->pRight, pExpr->tokenId);
  }

  if (pLeft == NULL || pRight == NULL) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pLeft->type == pRight->type) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  exchangeExpr(pExpr);

  pLeft  = pExpr->pLeft;
  pRight = pExpr->pRight;
  if (pLeft->type != SQL_NODE_SQLFUNCTION) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pRight->type != SQL_NODE_VALUE) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pExpr->tokenId >= TK_BITAND) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pLeft->Expr.paramList) {
    size_t size = taosArrayGetSize(pLeft->Expr.paramList);
    for (int32_t i = 0; i < size; i++) {
      tSqlExprItem* pParamItem = taosArrayGet(pLeft->Expr.paramList, i);

      tSqlExpr* pExpr1 = pParamItem->pNode;
      if (pExpr1->tokenId != TK_ALL &&
          pExpr1->tokenId != TK_ID &&
          pExpr1->tokenId != TK_STRING &&
          pExpr1->tokenId != TK_INTEGER &&
          pExpr1->tokenId != TK_FLOAT) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (pExpr1->tokenId == TK_ID && (pExpr1->columnName.z == NULL && pExpr1->columnName.n == 0)) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
      }

      if (pExpr1->tokenId == TK_ID) {
        SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
        if ((getColumnIndexByName(&pExpr1->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd)) != TSDB_CODE_SUCCESS)) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
        }

        STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex);
        STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;

        if (tsc_index.columnIndex <= 0 ||
            tsc_index.columnIndex >= tscGetNumOfColumns(pTableMeta)) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
        }
      }
    }
  }

  pLeft->functionId = isValidFunction(pLeft->Expr.operand.z, pLeft->Expr.operand.n);
  if (pLeft->functionId < 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  return handleExprInHavingClause(pCmd, pQueryInfo, pSelectNodeList, pExpr, parentOptr);
}

int32_t validateHavingClause(SQueryInfo* pQueryInfo, tSqlExpr* pExpr, SSqlCmd* pCmd, SArray* pSelectNodeList,
                             int32_t joinQuery, int32_t timeWindowQuery) {
  const char* msg1 = "having only works with group by";
  const char* msg2 = "functions or others can not be mixed up";
  const char* msg3 = "invalid expression in having clause";

  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pQueryInfo->groupbyExpr.numOfGroupCols <= 0) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
  }

  if (pExpr->pLeft == NULL || pExpr->pRight == NULL) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
  }

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }

  int32_t ret = 0;

  if ((ret = getHavingExpr(pCmd, pQueryInfo, pSelectNodeList, pExpr, TK_AND)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  //REDO function check
  if (!functionCompatibleCheck(pQueryInfo, joinQuery, timeWindowQuery)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getTableNameFromSqlNode(SSqlNode* pSqlNode, SArray* tableNameList, char* msgBuf, SSqlObj* pSql) {
  const char* msg1 = "invalid table name";

  int32_t numOfTables = (int32_t) taosArrayGetSize(pSqlNode->from->list);
  assert(pSqlNode->from->type == SQL_NODE_FROM_TABLELIST);

  for(int32_t j = 0; j < numOfTables; ++j) {
    SRelElementPair* item = taosArrayGet(pSqlNode->from->list, j);

    SStrToken* t = &item->tableName;
    if (t->type == TK_INTEGER || t->type == TK_FLOAT) {
      return invalidOperationMsg(msgBuf, msg1);
    }

    tscDequoteAndTrimToken(t);
    if (tscValidateName(t) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(msgBuf, msg1);
    }

    SName name = {0};
    int32_t code = tscSetTableFullName(&name, t, pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    taosArrayPush(tableNameList, &name);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getTableNameFromSubquery(SSqlNode* pSqlNode, SArray* tableNameList, char* msgBuf, SSqlObj* pSql) {
  int32_t numOfSub = (int32_t) taosArrayGetSize(pSqlNode->from->list);

  for(int32_t j = 0; j < numOfSub; ++j) {
    SRelElementPair* sub = taosArrayGet(pSqlNode->from->list, j);

    int32_t num = (int32_t)taosArrayGetSize(sub->pSubquery);
    for (int32_t i = 0; i < num; ++i) {
      SSqlNode* p = taosArrayGetP(sub->pSubquery, i);
      if (p->from == NULL) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      
      if (p->from->type == SQL_NODE_FROM_TABLELIST) {
        int32_t code = getTableNameFromSqlNode(p, tableNameList, msgBuf, pSql);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else {
        getTableNameFromSubquery(p, tableNameList, msgBuf, pSql);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

void tscTableMetaCallBack(void *param, TAOS_RES *res, int code);
static void freeElem(void* p) {
  tfree(*(char**)p);
}

int32_t tnameComparFn(const void* p1, const void* p2) {
  SName* pn1 = (SName*)p1;
  SName* pn2 = (SName*)p2;

  int32_t ret = strncmp(pn1->acctId, pn2->acctId, tListLen(pn1->acctId));
  if (ret != 0) {
    return ret > 0? 1:-1;
  } else {
    ret = strncmp(pn1->dbname, pn2->dbname, tListLen(pn1->dbname));
    if (ret != 0) {
      return ret > 0? 1:-1;
    } else {
      ret = strncmp(pn1->tname, pn2->tname, tListLen(pn1->tname));
      if (ret != 0) {
        return ret > 0? 1:-1;
      } else {
        return 0;
      }
    }
  }
}

int32_t loadAllTableMeta(SSqlObj* pSql, struct SSqlInfo* pInfo) {
  SSqlCmd* pCmd = &pSql->cmd;

  // the table meta has already been loaded from local buffer or mnode already
  if (pCmd->pTableMetaMap != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  SArray* tableNameList = NULL;
  SArray* pVgroupList   = NULL;
  SArray* plist         = NULL;
  STableMeta* pTableMeta = NULL;
  size_t    tableMetaCapacity = 0;
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);

  pCmd->pTableMetaMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);

  tableNameList = taosArrayInit(4, sizeof(SName));
  size_t size = taosArrayGetSize(pInfo->list);
  for (int32_t i = 0; i < size; ++i) {
    SSqlNode* pSqlNode = taosArrayGetP(pInfo->list, i);
    if (pSqlNode->from == NULL) {
      goto _end;
    }

    // load the table meta in the from clause
    if (pSqlNode->from->type == SQL_NODE_FROM_TABLELIST) {
      code = getTableNameFromSqlNode(pSqlNode, tableNameList, tscGetErrorMsgPayload(pCmd), pSql);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }
    } else {
      code = getTableNameFromSubquery(pSqlNode, tableNameList, tscGetErrorMsgPayload(pCmd), pSql);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }
    }
  }

  char     name[TSDB_TABLE_FNAME_LEN] = {0};

  //if (!pSql->pBuf) {
  //  if (NULL == (pSql->pBuf = tcalloc(1, 80 * TSDB_MAX_COLUMNS))) {
  //    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
  //    goto _end;
  //  }
  //}

  plist = taosArrayInit(4, POINTER_BYTES);
  pVgroupList = taosArrayInit(4, POINTER_BYTES);

  taosArraySort(tableNameList, tnameComparFn);
  taosArrayRemoveDuplicate(tableNameList, tnameComparFn, NULL);

  size_t numOfTables = taosArrayGetSize(tableNameList);
  for (int32_t i = 0; i < numOfTables; ++i) {
    SName* pname = taosArrayGet(tableNameList, i);
    tNameExtractFullName(pname, name);

    size_t len = strlen(name);
      
    if (NULL == taosHashGetCloneExt(tscTableMetaMap, name, len, NULL,  (void **)&pTableMeta, &tableMetaCapacity)){
      tfree(pTableMeta);
      tableMetaCapacity = 0;
    }

    if (pTableMeta && pTableMeta->id.uid > 0) {
      tscDebug("0x%"PRIx64" retrieve table meta %s from local buf", pSql->self, name);

      // avoid mem leak, may should update pTableMeta
      void* pVgroupIdList = NULL;
      if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
        code = tscCreateTableMetaFromSTableMeta((STableMeta **)(&pTableMeta), name, &tableMetaCapacity);

        // create the child table meta from super table failed, try load it from mnode
        if (code != TSDB_CODE_SUCCESS) {
          char* t = strdup(name);
          taosArrayPush(plist, &t);
          continue;
        }
      } else if (pTableMeta->tableType == TSDB_SUPER_TABLE) {
        // the vgroup list of super table is not kept in local buffer, so here need retrieve it from the mnode each time
        tscDebug("0x%"PRIx64" try to acquire cached super table %s vgroup id list", pSql->self, name);
        void* pv = taosCacheAcquireByKey(tscVgroupListBuf, name, len);
        if (pv == NULL) {
          char* t = strdup(name);
          taosArrayPush(pVgroupList, &t);
          tscDebug("0x%"PRIx64" failed to retrieve stable %s vgroup id list in cache, try fetch from mnode", pSql->self, name);
        } else {
          tFilePage* pdata = (tFilePage*) pv;
          pVgroupIdList = taosArrayInit((size_t) pdata->num, sizeof(int32_t));
          if (pVgroupIdList == NULL) {
            return TSDB_CODE_TSC_OUT_OF_MEMORY;
          }

          taosArrayAddBatch(pVgroupIdList, pdata->data, (int32_t) pdata->num);
          taosCacheRelease(tscVgroupListBuf, &pv, false);
        }
      }

      if (taosHashGet(pCmd->pTableMetaMap, name, len) == NULL) {
        STableMeta* pMeta = tscTableMetaDup(pTableMeta);
        STableMetaVgroupInfo tvi = { .pTableMeta = pMeta,  .vgroupIdList = pVgroupIdList};
        taosHashPut(pCmd->pTableMetaMap, name, len, &tvi, sizeof(STableMetaVgroupInfo));
      }
    } else {
      // Add to the retrieve table meta array list.
      // If the tableMeta is missing, the cached vgroup list for the corresponding super table will be ignored.
      tscDebug("0x%"PRIx64" failed to retrieve table meta %s from local buf", pSql->self, name);

      char* t = strdup(name);
      taosArrayPush(plist, &t);
    }
  }

  size_t funcSize = 0;
  if (pInfo->funcs) {
    funcSize = taosArrayGetSize(pInfo->funcs);
  }

  if (funcSize > 0) {
    for (size_t i = 0; i < funcSize; ++i) {
      SStrToken* t = taosArrayGet(pInfo->funcs, i);
      if (NULL == t) {
        continue;
      }

      if (t->n >= TSDB_FUNC_NAME_LEN) {
        code = tscSQLSyntaxErrMsg(tscGetErrorMsgPayload(pCmd), "too long function name", t->z);
        if (code != TSDB_CODE_SUCCESS) {
          goto _end;
        }
      }

      int32_t functionId = isValidFunction(t->z, t->n);
      if (functionId < 0) {
        struct SUdfInfo info = {0};
        info.name = strndup(t->z, t->n);
        info.keep = true;
        if (pQueryInfo->pUdfInfo == NULL) {
          pQueryInfo->pUdfInfo = taosArrayInit(4, sizeof(struct SUdfInfo));
        } else if (taosArrayGetSize(pQueryInfo->pUdfInfo) > 0) {
          int32_t usize = (int32_t)taosArrayGetSize(pQueryInfo->pUdfInfo);
          int32_t exist = 0;
          
          for (int32_t j = 0; j < usize; ++j) {
            SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, j);
            int32_t len = (int32_t)strlen(pUdfInfo->name);
            if (len == t->n && strncasecmp(info.name, pUdfInfo->name, t->n) == 0) {
              exist = 1;
              break;
            }
          }

          if (exist) {
            continue;
          }
        }

        info.functionId = (int32_t)taosArrayGetSize(pQueryInfo->pUdfInfo) * (-1) - 1;;
        taosArrayPush(pQueryInfo->pUdfInfo, &info);
        if (taosArrayGetSize(pQueryInfo->pUdfInfo) > 1) {
          code = tscInvalidOperationMsg(pCmd->payload, "only one udf allowed", NULL);
          goto _end;
        }        
      }
    }
  }

  // load the table meta for a given table name list
  if (taosArrayGetSize(plist) > 0 || taosArrayGetSize(pVgroupList) > 0 || (pQueryInfo->pUdfInfo && taosArrayGetSize(pQueryInfo->pUdfInfo) > 0)) {
    code = getMultiTableMetaFromMnode(pSql, plist, pVgroupList, pQueryInfo->pUdfInfo, tscTableMetaCallBack, true);
  }

_end:
  if (plist != NULL) {
    taosArrayDestroyEx(plist, freeElem);
  }

  if (pVgroupList != NULL) {
    taosArrayDestroyEx(pVgroupList, freeElem);
  }

  if (tableNameList != NULL) {
    taosArrayDestroy(tableNameList);
  }

  tfree(pTableMeta);

  return code;
}

static int32_t doLoadAllTableMeta(SSqlObj* pSql, SQueryInfo* pQueryInfo, SSqlNode* pSqlNode, int32_t numOfTables) {
  const char* msg1 = "invalid table name";
  const char* msg2 = "invalid table alias name";
  const char* msg3 = "alias name too long";
  const char* msg4 = "self join not allowed";

  int32_t code = TSDB_CODE_SUCCESS;
  SSqlCmd* pCmd = &pSql->cmd;

  if (numOfTables > taosHashGetSize(pCmd->pTableMetaMap)) {
    return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
  }

  for (int32_t i = 0; i < numOfTables; ++i) {
    if (pQueryInfo->numOfTables <= i) {  // more than one table
      tscAddEmptyMetaInfo(pQueryInfo);
    }

    SRelElementPair *item = taosArrayGet(pSqlNode->from->list, i);
    SStrToken       *oriName = &item->tableName;

    if (oriName->type == TK_INTEGER || oriName->type == TK_FLOAT) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    tscDequoteAndTrimToken(oriName);
    if (tscValidateName(oriName) != TSDB_CODE_SUCCESS) {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    code = tscSetTableFullName(&pTableMetaInfo->name, oriName, pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    SStrToken* aliasName = &item->aliasName;
    if (TPARSER_HAS_TOKEN(*aliasName)) {
      if (aliasName->type == TK_INTEGER || aliasName->type == TK_FLOAT) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
      }

      tscDequoteAndTrimToken(aliasName);
      if (tscValidateName(aliasName) != TSDB_CODE_SUCCESS || aliasName->n >= TSDB_TABLE_NAME_LEN) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
      }

      strncpy(pTableMetaInfo->aliasName, aliasName->z, aliasName->n);
    } else {
      strncpy(pTableMetaInfo->aliasName, tNameGetTableName(&pTableMetaInfo->name), tListLen(pTableMetaInfo->aliasName));
    }

    char fname[TSDB_TABLE_FNAME_LEN] = {0};
    tNameExtractFullName(&pTableMetaInfo->name, fname);
    STableMetaVgroupInfo* p = taosHashGet(pCmd->pTableMetaMap, fname, strnlen(fname, TSDB_TABLE_FNAME_LEN));

    pTableMetaInfo->pTableMeta        = tscTableMetaDup(p->pTableMeta);
    pTableMetaInfo->tableMetaCapacity = tscGetTableMetaSize(pTableMetaInfo->pTableMeta);
    assert(pTableMetaInfo->pTableMeta != NULL);

    if (p->vgroupIdList != NULL) {
      size_t s = taosArrayGetSize(p->vgroupIdList);

      size_t vgroupsz = sizeof(SVgroupInfo) * s + sizeof(SVgroupsInfo);
      pTableMetaInfo->vgroupList = calloc(1, vgroupsz);
      if (pTableMetaInfo->vgroupList == NULL) {
        return TSDB_CODE_TSC_OUT_OF_MEMORY;
      }

      pTableMetaInfo->vgroupList->numOfVgroups = (int32_t) s;
      for(int32_t j = 0; j < s; ++j) {
        int32_t* id = taosArrayGet(p->vgroupIdList, j);

        // check if current buffer contains the vgroup info. If not, add it
        SNewVgroupInfo existVgroupInfo = {.inUse = -1,};
        taosHashGetClone(tscVgroupMap, id, sizeof(*id), NULL, &existVgroupInfo);

        assert(existVgroupInfo.inUse >= 0);
        SVgroupInfo *pVgroup = &pTableMetaInfo->vgroupList->vgroups[j];

        pVgroup->numOfEps = existVgroupInfo.numOfEps;
        pVgroup->vgId     = existVgroupInfo.vgId;
        memcpy(&pVgroup->epAddr, &existVgroupInfo.ep, sizeof(pVgroup->epAddr));
      }
    }
  }

  return code;
}

static STableMeta* extractTempTableMetaFromSubquery(SQueryInfo* pUpstream) {
  STableMetaInfo* pUpstreamTableMetaInfo = tscGetMetaInfo(pUpstream, 0);

  int32_t     numOfColumns = pUpstream->fieldsInfo.numOfOutput;
  STableMeta *meta = calloc(1, sizeof(STableMeta) + sizeof(SSchema) * numOfColumns);
  meta->tableType = TSDB_TEMP_TABLE;

  STableComInfo *info = &meta->tableInfo;
  info->numOfColumns = numOfColumns;
  info->precision    = pUpstreamTableMetaInfo->pTableMeta->tableInfo.precision;
  info->numOfTags    = 0;

  int32_t n = 0;
  for(int32_t i = 0; i < numOfColumns; ++i) {
    SInternalField* pField = tscFieldInfoGetInternalField(&pUpstream->fieldsInfo, i);
    if (!pField->visible) {
      continue;
    }

    meta->schema[n].bytes = pField->field.bytes;
    meta->schema[n].type  = pField->field.type;

    SExprInfo* pExpr = pField->pExpr;
    meta->schema[n].colId = pExpr->base.resColId;
    tstrncpy(meta->schema[n].name, pField->pExpr->base.aliasName, TSDB_COL_NAME_LEN);
    info->rowSize += meta->schema[n].bytes;

    n += 1;
  }
  info->numOfColumns = n; 
  return meta;
}

static int32_t doValidateSubquery(SSqlNode* pSqlNode, int32_t tsc_index, SSqlObj* pSql, SQueryInfo* pQueryInfo, char* msgBuf) {
  SRelElementPair* subInfo = taosArrayGet(pSqlNode->from->list, tsc_index);

  // union all is not support currently
  SSqlNode* p = taosArrayGetP(subInfo->pSubquery, 0);
  if (taosArrayGetSize(subInfo->pSubquery) >= 2) {
    return invalidOperationMsg(msgBuf, "not support union in subquery");
  }

  SQueryInfo* pSub = calloc(1, sizeof(SQueryInfo));
  tscInitQueryInfo(pSub);

  SArray *pUdfInfo = NULL;
  if (pQueryInfo->pUdfInfo) {
    pUdfInfo = taosArrayDup(pQueryInfo->pUdfInfo);
  }

  pSub->pUdfInfo = pUdfInfo;
  pSub->udfCopy = true;

  pSub->pDownstream = pQueryInfo;
  taosArrayPush(pQueryInfo->pUpstream, &pSub);
  int32_t code = validateSqlNode(pSql, p, pSub);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // create dummy table meta info
  STableMetaInfo* pTableMetaInfo1 = calloc(1, sizeof(STableMetaInfo));
  if (pTableMetaInfo1 == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pTableMetaInfo1->pTableMeta        = extractTempTableMetaFromSubquery(pSub);
  pTableMetaInfo1->tableMetaCapacity = tscGetTableMetaSize(pTableMetaInfo1->pTableMeta);

  if (subInfo->aliasName.n > 0) {
    if (subInfo->aliasName.n >= TSDB_TABLE_FNAME_LEN) {
      tfree(pTableMetaInfo1);
      return invalidOperationMsg(msgBuf, "subquery alias name too long");
    }

    tstrncpy(pTableMetaInfo1->aliasName, subInfo->aliasName.z, subInfo->aliasName.n + 1);
  }

  // NOTE: order mix up in subquery not support yet.
  pQueryInfo->order = pSub->order;

  STableMetaInfo** tmp = realloc(pQueryInfo->pTableMetaInfo, (pQueryInfo->numOfTables + 1) * POINTER_BYTES);
  if (tmp == NULL) {
    tfree(pTableMetaInfo1);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pQueryInfo->pTableMetaInfo = tmp;

  pQueryInfo->pTableMetaInfo[pQueryInfo->numOfTables] = pTableMetaInfo1;
  pQueryInfo->numOfTables += 1;

  // all columns are added into the table column list
  STableMeta* pMeta = pTableMetaInfo1->pTableMeta;
  int32_t startOffset = (int32_t) taosArrayGetSize(pQueryInfo->colList);

  for(int32_t i = 0; i < pMeta->tableInfo.numOfColumns; ++i) {
    tscColumnListInsert(pQueryInfo->colList, i + startOffset, pMeta->id.uid, &pMeta->schema[i]);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateSqlNode(SSqlObj* pSql, SSqlNode* pSqlNode, SQueryInfo* pQueryInfo) {
  assert(pSqlNode != NULL && (pSqlNode->from == NULL || taosArrayGetSize(pSqlNode->from->list) > 0));

  const char* msg1 = "point interpolation query needs timestamp";
  const char* msg2 = "too many tables in from clause";
  const char* msg3 = "start(end) time of query range required or time range too large";
  const char* msg4 = "interval query not supported, since the result of sub query not include valid timestamp column";
  const char* msg5 = "only tag query not compatible with normal column filter";
  const char* msg6 = "not support stddev/percentile/interp in the outer query yet";
  const char* msg7 = "derivative/twa/rate/irate/diff requires timestamp column exists in subquery";
  const char* msg8 = "condition missing for join query";
  const char* msg9 = "not support 3 level select";

  int32_t  code = TSDB_CODE_SUCCESS;
  SSqlCmd* pCmd = &pSql->cmd;

  STableMetaInfo  *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (pTableMetaInfo == NULL) {
    pTableMetaInfo = tscAddEmptyMetaInfo(pQueryInfo);
  }

  /*
   * handle the sql expression without from subclause
   * select server_status();
   * select server_version();
   * select client_version();
   * select current_database();
   */
  if (pSqlNode->from == NULL) {
    assert(pSqlNode->fillType == NULL && pSqlNode->pGroupby == NULL && pSqlNode->pWhere == NULL &&
           pSqlNode->pSortOrder == NULL);
    return doLocalQueryProcess(pCmd, pQueryInfo, pSqlNode);
  }

  if (pSqlNode->from->type == SQL_NODE_FROM_SUBQUERY) {
    clearAllTableMetaInfo(pQueryInfo, false);
    pQueryInfo->numOfTables = 0;

    // parse the subquery in the first place
    int32_t numOfSub = (int32_t)taosArrayGetSize(pSqlNode->from->list);
    for (int32_t i = 0; i < numOfSub; ++i) {
      // check if there is 3 level select
      SRelElementPair* subInfo = taosArrayGet(pSqlNode->from->list, i);
      SSqlNode* p = taosArrayGetP(subInfo->pSubquery, 0);
      if (p->from->type == SQL_NODE_FROM_SUBQUERY) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg9);
      }

      code = doValidateSubquery(pSqlNode, i, pSql, pQueryInfo, tscGetErrorMsgPayload(pCmd));
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    int32_t timeWindowQuery =
        (TPARSER_HAS_TOKEN(pSqlNode->interval.interval) || TPARSER_HAS_TOKEN(pSqlNode->sessionVal.gap));
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TABLE_QUERY);

    int32_t joinQuery = (pSqlNode->from != NULL && taosArrayGetSize(pSqlNode->from->list) > 1);

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
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg6);
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
        if (f == TSDB_FUNC_DERIVATIVE || f == TSDB_FUNC_TWA || f == TSDB_FUNC_IRATE || 
            f == TSDB_FUNC_RATE       || f == TSDB_FUNC_DIFF) {
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg7);
        }
      }
    }

    // validate the query filter condition info
    if (pSqlNode->pWhere != NULL) {
      if (validateWhereNode(pQueryInfo, &pSqlNode->pWhere, pSql, joinQuery) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else {
      if (pQueryInfo->numOfTables > 1) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg8);
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
          return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg4);
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

    if ((code = validateFunctionFromUpstream(pQueryInfo, tscGetErrorMsgPayload(pCmd))) != TSDB_CODE_SUCCESS) {
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
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg2);
    }

    // set all query tables, which are maybe more than one.
    code = doLoadAllTableMeta(pSql, pQueryInfo, pSqlNode, (int32_t) numOfTables);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    bool isSTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);

    int32_t type = isSTable? TSDB_QUERY_TYPE_STABLE_QUERY:TSDB_QUERY_TYPE_TABLE_QUERY;
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, type);

    int32_t joinQuery = (pSqlNode->from != NULL && taosArrayGetSize(pSqlNode->from->list) > 1);

    // parse the group by clause in the first place
    if (validateGroupbyNode(pQueryInfo, pSqlNode->pGroupby, pCmd) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
    pQueryInfo->onlyHasTagCond = true;
    // set where info
    if (pSqlNode->pWhere != NULL) {
      if (validateWhereNode(pQueryInfo, &pSqlNode->pWhere, pSql, joinQuery) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      pSqlNode->pWhere = NULL;
    } else {
      if (taosArrayGetSize(pSqlNode->from->list) > 1) { // Cross join not allowed yet
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "cross join not supported yet");
      }
    }

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
        int32_t numOfCols = (int32_t)taosArrayGetSize(pQueryInfo->colList);
        for (int32_t i = 0; i < numOfCols; ++i) {
          SColumn* pCols = taosArrayGetP(pQueryInfo->colList, i);
          if (pCols->info.flist.numOfFilters > 0) {
            return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg5);
          }
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
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg1);
    }

    // in case of join query, time range is required.
    if (QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
      uint64_t timeRange = (uint64_t)pQueryInfo->window.ekey - pQueryInfo->window.skey;
      if (timeRange == 0 && pQueryInfo->window.skey == 0) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), msg3);
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
    pQueryInfo->groupbyTag = tscGroupbyTag(pQueryInfo);
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

#if 0
  SQueryNode* p = qCreateQueryPlan(pQueryInfo);
  char* s = queryPlanToString(p);
  printf("%s\n", s);
  tfree(s);
  qDestroyQueryPlan(p);
#endif

  return TSDB_CODE_SUCCESS;  // Does not build query message here
}

int32_t exprTreeFromSqlExpr(SSqlCmd* pCmd, tExprNode **pExpr, const tSqlExpr* pSqlExpr, SQueryInfo* pQueryInfo, SArray* pCols, uint64_t *uid) {
  tExprNode* pLeft = NULL;
  tExprNode* pRight= NULL;
  SColumnIndex tsc_index = COLUMN_INDEX_INITIALIZER;
  
  if (pSqlExpr->pLeft != NULL) {
    int32_t ret = exprTreeFromSqlExpr(pCmd, &pLeft, pSqlExpr->pLeft, pQueryInfo, pCols, uid);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }
  
  if (pSqlExpr->pRight != NULL) {
    int32_t ret = exprTreeFromSqlExpr(pCmd, &pRight, pSqlExpr->pRight, pQueryInfo, pCols, uid);
    if (ret != TSDB_CODE_SUCCESS) {
      tExprTreeDestroy(pLeft, NULL);
      return ret;
    }
  }

  if (pSqlExpr->pLeft == NULL && pSqlExpr->pRight == NULL && pSqlExpr->tokenId == 0) {
    *pExpr = calloc(1, sizeof(tExprNode));
    return TSDB_CODE_SUCCESS;
  }
  
  if (pSqlExpr->pLeft == NULL) {  // it is the leaf node
    assert(pSqlExpr->pRight == NULL);

    if (pSqlExpr->type == SQL_NODE_VALUE) {
      int32_t ret = TSDB_CODE_SUCCESS;
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_VALUE;
      (*pExpr)->pVal = calloc(1, sizeof(tVariant));
      tVariantAssign((*pExpr)->pVal, &pSqlExpr->value);

      STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, pQueryInfo->curTableIdx)->pTableMeta;
      if (pCols != NULL) {
        size_t colSize = taosArrayGetSize(pCols);

        if (colSize > 0) {
          SColIndex* idx = taosArrayGet(pCols, colSize - 1);
          SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, idx->colIndex);
          // convert time by precision
          if (pSchema != NULL && TSDB_DATA_TYPE_TIMESTAMP == pSchema->type && TSDB_DATA_TYPE_BINARY == (*pExpr)->pVal->nType) {
            ret = setColumnFilterInfoForTimestamp(pCmd, pQueryInfo, (*pExpr)->pVal);
          }
        }
      }
      return ret;
    } else if (pSqlExpr->type == SQL_NODE_SQLFUNCTION) {
      // arithmetic expression on the results of aggregation functions
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_COL;
      (*pExpr)->pSchema = calloc(1, sizeof(SSchema));
      strncpy((*pExpr)->pSchema->name, pSqlExpr->exprToken.z, pSqlExpr->exprToken.n);
      
      // set the input column data byte and type.
      size_t size = taosArrayGetSize(pQueryInfo->exprList);
      
      for (int32_t i = 0; i < size; ++i) {
        SExprInfo* p1 = taosArrayGetP(pQueryInfo->exprList, i);
        
        if (strcmp((*pExpr)->pSchema->name, p1->base.aliasName) == 0) {
          (*pExpr)->pSchema->type  = (uint8_t)p1->base.resType;
          (*pExpr)->pSchema->bytes = p1->base.resBytes;
          (*pExpr)->pSchema->colId = p1->base.resColId;

          if (uid != NULL) {
            *uid = p1->base.uid;
          }

          break;
        }
      }
    } else if (pSqlExpr->type == SQL_NODE_TABLE_COLUMN) { // column name, normal column arithmetic expression
      int32_t ret = getColumnIndexByName(&pSqlExpr->columnName, pQueryInfo, &tsc_index, tscGetErrorMsgPayload(pCmd));
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      pQueryInfo->curTableIdx = tsc_index.tableIndex;
      STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, tsc_index.tableIndex)->pTableMeta;
      int32_t numOfColumns = tscGetNumOfColumns(pTableMeta);

      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_COL;
      (*pExpr)->pSchema = calloc(1, sizeof(SSchema));

      SSchema* pSchema = NULL;
      
      if (tsc_index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        pSchema = (*pExpr)->pSchema;
        strcpy(pSchema->name, TSQL_TBNAME_L);
        pSchema->type = TSDB_DATA_TYPE_BINARY;
        pSchema->colId = TSDB_TBNAME_COLUMN_INDEX;
        pSchema->bytes = -1;
      } else {
        pSchema = tscGetTableColumnSchema(pTableMeta, tsc_index.columnIndex);
        *(*pExpr)->pSchema = *pSchema;
      }
  
      if (pCols != NULL) {  // record the involved columns
        SColIndex colIndex = {0};
        tstrncpy(colIndex.name, pSchema->name, sizeof(colIndex.name));
        colIndex.colId = pSchema->colId;
        colIndex.colIndex = tsc_index.columnIndex;
        colIndex.flag = (tsc_index.columnIndex >= numOfColumns)? 1:0;

        taosArrayPush(pCols, &colIndex);
      }
      
      return TSDB_CODE_SUCCESS;
    } else if (pSqlExpr->tokenId == TK_SET) {
      int32_t colType = -1;
      STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, pQueryInfo->curTableIdx)->pTableMeta;
      if (pCols != NULL) {
        size_t colSize = taosArrayGetSize(pCols);

        if (colSize > 0) {
          SColIndex* idx = taosArrayGet(pCols, colSize - 1);
          if (idx->colIndex == TSDB_TBNAME_COLUMN_INDEX) {
            colType = TSDB_DATA_TYPE_BINARY;
          } else {
            SSchema* pSchema = tscGetTableColumnSchema(pTableMeta, idx->colIndex);
            if (pSchema != NULL) {
              colType = pSchema->type;
            }
          }
        }
      }
      tVariant *pVal;
      if (colType >= TSDB_DATA_TYPE_TINYINT && colType <= TSDB_DATA_TYPE_BIGINT) {
        colType = TSDB_DATA_TYPE_BIGINT;
      } else if (colType == TSDB_DATA_TYPE_FLOAT || colType == TSDB_DATA_TYPE_DOUBLE) {
        colType = TSDB_DATA_TYPE_DOUBLE;
      }
      STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pQueryInfo->curTableIdx);
      STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
      if (serializeExprListToVariant(pSqlExpr->Expr.paramList, &pVal, colType, tinfo.precision) == false) {
        return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "not support filter expression");
      }
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TSQL_NODE_VALUE;
      (*pExpr)->pVal = pVal;
    } else {
      return invalidOperationMsg(tscGetErrorMsgPayload(pCmd), "not support filter expression");
    }
    
  } else {
    *pExpr = (tExprNode *)calloc(1, sizeof(tExprNode));
    (*pExpr)->nodeType = TSQL_NODE_EXPR;
    
    (*pExpr)->_node.hasPK = false;
    (*pExpr)->_node.pLeft = pLeft;
    (*pExpr)->_node.pRight = pRight;
    
    SStrToken t = {.type = pSqlExpr->tokenId};
    (*pExpr)->_node.optr = convertRelationalOperator(&t);
    
    assert((*pExpr)->_node.optr != 0);

    // check for dividing by 0
    if ((*pExpr)->_node.optr == TSDB_BINARY_OP_DIVIDE) {
      if (pRight->nodeType == TSQL_NODE_VALUE) {
        if (pRight->pVal->nType == TSDB_DATA_TYPE_INT && pRight->pVal->i64 == 0) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        } else if (pRight->pVal->nType == TSDB_DATA_TYPE_FLOAT && pRight->pVal->dKey == 0) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      }
    }

    // NOTE: binary|nchar data allows the >|< type filter
    if ((*pExpr)->_node.optr != TSDB_RELATION_EQUAL && (*pExpr)->_node.optr != TSDB_RELATION_NOT_EQUAL) {
      if (pRight != NULL && pRight->nodeType == TSQL_NODE_VALUE) {
        if (pRight->pVal->nType == TSDB_DATA_TYPE_BOOL && pLeft->pSchema->type == TSDB_DATA_TYPE_BOOL) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
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
    if (pCol->info.flist.numOfFilters > 0) {
      return true;
    }
  }

  return false;
}

#if 0
void normalizeSqlNode(SSqlNode* pSqlNode, const char* dbName) {
  assert(pSqlNode != NULL);

  if (pSqlNode->from->type == SQL_NODE_FROM_TABLELIST) {
//    SRelElementPair *item = taosArrayGet(pSqlNode->from->list, 0);
//    item->TableName.name;
  }

  //  1. pSqlNode->pSelNodeList
  if (pSqlNode->pSelNodeList != NULL && taosArrayGetSize(pSqlNode->pSelNodeList) > 0) {
    SArray* pSelNodeList = pSqlNode->pSelNodeList;
    size_t numOfExpr = taosArrayGetSize(pSelNodeList);
    for (int32_t i = 0; i < numOfExpr; ++i) {
      tSqlExprItem* pItem = taosArrayGet(pSelNodeList, i);
      int32_t type = pItem->pNode->type;
      if (type == SQL_NODE_VALUE || type == SQL_NODE_EXPR) {
        continue;
      }

      if (type == SQL_NODE_TABLE_COLUMN) {
      }
    }
  }

//  2. pSqlNode->pWhere
//  3. pSqlNode->pHaving
//  4. pSqlNode->pSortOrder
//  pSqlNode->from
}

#endif


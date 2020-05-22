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

#include "os.h"
#include "taosmsg.h"

#include "tcache.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "taosdef.h"
#include "tscLog.h"
#include "qextbuffer.h"
#include "tscSecondaryMerge.h"
#include "tschemautil.h"
#include "tname.h"

static void tscSetLocalQueryResult(SSqlObj *pSql, const char *val, const char *columnName, size_t valueLength);

static int32_t getToStringLength(const char *pData, int32_t length, int32_t type) {
  char buf[512] = {0};

  int32_t len = 0;
  int32_t MAX_BOOL_TYPE_LENGTH = 5;  // max(strlen("true"), strlen("false"));
  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
      return length;
    case TSDB_DATA_TYPE_NCHAR:
      return length;
    case TSDB_DATA_TYPE_DOUBLE: {
      double dv = 0;
      dv = GET_DOUBLE_VAL(pData);
      len = sprintf(buf, "%lf", dv);
      if (strncasecmp("nan", buf, 3) == 0) {
        len = 4;
      }
    } break;
    case TSDB_DATA_TYPE_FLOAT: {
      float fv = 0;
      fv = GET_FLOAT_VAL(pData);
      len = sprintf(buf, "%f", fv);
      if (strncasecmp("nan", buf, 3) == 0) {
        len = 4;
      }
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT:
      len = sprintf(buf, "%" PRId64, *(int64_t *)pData);
      break;
    case TSDB_DATA_TYPE_BOOL:
      len = MAX_BOOL_TYPE_LENGTH;
      break;
    default:
      len = sprintf(buf, "%d", *(int32_t *)pData);
      break;
  };
  return len;
}

/*
 * we need to convert all data into string, so we need to sprintf all kinds of
 * non-string data into string, and record its length to get the right
 * maximum length. The length may be less or greater than its original binary length:
 * For example:
 * length((short) 1) == 1, less than sizeof(short)
 * length((uint64_t) 123456789011) > 12, greater than sizsof(uint64_t)
 */
static int32_t tscMaxLengthOfTagsFields(SSqlObj *pSql) {
  STableMeta *pMeta = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0)->pTableMeta;

  if (pMeta->tableType == TSDB_SUPER_TABLE || pMeta->tableType == TSDB_NORMAL_TABLE ||
      pMeta->tableType == TSDB_STREAM_TABLE) {
    return 0;
  }

  char *   pTagValue = tsGetTagsValue(pMeta);
  SSchema *pTagsSchema = tscGetTableTagSchema(pMeta);

  int32_t len = getToStringLength(pTagValue, pTagsSchema[0].bytes, pTagsSchema[0].type);

  pTagValue += pTagsSchema[0].bytes;
  int32_t numOfTags = tscGetNumOfTags(pMeta);
  
  for (int32_t i = 1; i < numOfTags; ++i) {
    int32_t tLen = getToStringLength(pTagValue, pTagsSchema[i].bytes, pTagsSchema[i].type);
    if (len < tLen) {
      len = tLen;
    }

    pTagValue += pTagsSchema[i].bytes;
  }

  return len;
}

static int32_t tscSetValueToResObj(SSqlObj *pSql, int32_t rowLen) {
  SSqlRes *pRes = &pSql->res;

  // one column for each row
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableMeta *    pMeta = pTableMetaInfo->pTableMeta;

  /*
   * tagValueCnt is to denote the number of tags columns for meter, not metric. and is to show the column data.
   * for meter, which is created according to metric, the value of tagValueCnt is not 0, and the numOfTags must be 0.
   * for metric, the value of tagValueCnt must be 0, but the numOfTags is not 0
   */

  int32_t numOfRows = tscGetNumOfColumns(pMeta);
  int32_t totalNumOfRows = numOfRows + tscGetNumOfTags(pMeta);

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    numOfRows = numOfRows + tscGetNumOfTags(pMeta);
  }

  tscInitResObjForLocalQuery(pSql, totalNumOfRows, rowLen);
  SSchema *pSchema = tscGetTableSchema(pMeta);

  for (int32_t i = 0; i < numOfRows; ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 0);
    char* dst = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 0) * totalNumOfRows + pField->bytes * i;
    STR_WITH_MAXSIZE_TO_VARSTR(dst, pSchema[i].name, TSDB_COL_NAME_LEN);

    char *type = tDataTypeDesc[pSchema[i].type].aName;

    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 1);
    dst = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 1) * totalNumOfRows + pField->bytes * i;
    
    STR_TO_VARSTR(dst, type);
    
    int32_t bytes = pSchema[i].bytes;
    if (pSchema[i].type == TSDB_DATA_TYPE_BINARY || pSchema[i].type == TSDB_DATA_TYPE_NCHAR) {
      bytes -= VARSTR_HEADER_SIZE;
      
      if (pSchema[i].type == TSDB_DATA_TYPE_NCHAR) {
        bytes = bytes / TSDB_NCHAR_SIZE;
      }
    }

    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 2);
    *(int32_t *)(pRes->data + tscFieldInfoGetOffset(pQueryInfo, 2) * totalNumOfRows + pField->bytes * i) = bytes;

    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 3);
    if (i >= tscGetNumOfColumns(pMeta) && tscGetNumOfTags(pMeta) != 0) {
      char* output = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 3) * totalNumOfRows + pField->bytes * i;
      const char *src = "TAG";
      STR_WITH_SIZE_TO_VARSTR(output, src, strlen(src));
    }
  }

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    return 0;
  }

  // the following is handle display tags value for meters created according to metric
  char *pTagValue = tsGetTagsValue(pMeta);
  for (int32_t i = numOfRows; i < totalNumOfRows; ++i) {
    // field name
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 0);
    char* output = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 0) * totalNumOfRows + pField->bytes * i;
    STR_WITH_MAXSIZE_TO_VARSTR(output, pSchema[i].name, TSDB_COL_NAME_LEN);

    // type name
    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 1);
    char *type = tDataTypeDesc[pSchema[i].type].aName;
    
    output = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 1) * totalNumOfRows + pField->bytes * i;
    STR_WITH_MAXSIZE_TO_VARSTR(output, type, pField->bytes);

    // type length
    int32_t bytes = pSchema[i].bytes;
    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 2);
    if (pSchema[i].type == TSDB_DATA_TYPE_NCHAR) {
      bytes = bytes / TSDB_NCHAR_SIZE;
    }

    *(int32_t *)(pRes->data + tscFieldInfoGetOffset(pQueryInfo, 2) * totalNumOfRows + pField->bytes * i) = bytes;

    // tag value
    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 3);
    char *target = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 3) * totalNumOfRows + pField->bytes * i;
    const char *src = "TAG";
    STR_WITH_SIZE_TO_VARSTR(target, src, strlen(src));

    pTagValue += pSchema[i].bytes;
  }

  return 0;
}

static int32_t tscBuildTableSchemaResultFields(SSqlObj *pSql, int32_t numOfCols, int32_t typeColLength,
                                               int32_t noteColLength) {
  int32_t  rowLen = 0;
  SColumnIndex index = {0};
  
  pSql->cmd.numOfCols = numOfCols;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  pQueryInfo->order.order = TSDB_ORDER_ASC;

  TAOS_FIELD f = {.type = TSDB_DATA_TYPE_BINARY, .bytes = TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE};
  strncpy(f.name, "Field", TSDB_COL_NAME_LEN);
  
  SFieldSupInfo* pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_BINARY,
      TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE, TSDB_COL_NAME_LEN, false);
  
  rowLen += (TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE);

  f.bytes = typeColLength;
  f.type = TSDB_DATA_TYPE_BINARY;
  strncpy(f.name, "Type", TSDB_COL_NAME_LEN);
  
  pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_BINARY, typeColLength,
      typeColLength, false);
  
  rowLen += typeColLength;

  f.bytes = sizeof(int32_t);
  f.type = TSDB_DATA_TYPE_INT;
  strncpy(f.name, "Length", TSDB_COL_NAME_LEN);
  
  pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_INT, sizeof(int32_t),
      sizeof(int32_t), false);
  
  rowLen += sizeof(int32_t);

  f.bytes = noteColLength;
  f.type = TSDB_DATA_TYPE_BINARY;
  strncpy(f.name, "Note", TSDB_COL_NAME_LEN);
  
  pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_BINARY, noteColLength,
      noteColLength, false);
  
  rowLen += noteColLength;
  return rowLen;
}

static int32_t tscProcessDescribeTable(SSqlObj *pSql) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  assert(tscGetMetaInfo(pQueryInfo, 0)->pTableMeta != NULL);

  const int32_t NUM_OF_DESC_TABLE_COLUMNS = 4;
  const int32_t TYPE_COLUMN_LENGTH = 16;
  const int32_t NOTE_COLUMN_MIN_LENGTH = 8;

  int32_t noteFieldLen = tscMaxLengthOfTagsFields(pSql);
  if (noteFieldLen == 0) {
    noteFieldLen = NOTE_COLUMN_MIN_LENGTH;
  }

  int32_t rowLen = tscBuildTableSchemaResultFields(pSql, NUM_OF_DESC_TABLE_COLUMNS, TYPE_COLUMN_LENGTH, noteFieldLen);
  tscFieldInfoUpdateOffset(pQueryInfo);
  return tscSetValueToResObj(pSql, rowLen);
}

static void tscProcessCurrentUser(SSqlObj *pSql) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  tscSetLocalQueryResult(pSql, pSql->pTscObj->user, pExpr->aliasName, TSDB_USER_LEN);
}

static void tscProcessCurrentDB(SSqlObj *pSql) {
  char db[TSDB_DB_NAME_LEN + 1] = {0};
  extractDBName(pSql->pTscObj->db, db);
  
  // no use db is invoked before.
  if (strlen(db) == 0) {
    setNull(db, TSDB_DATA_TYPE_BINARY, TSDB_DB_NAME_LEN);
  }
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  tscSetLocalQueryResult(pSql, db, pExpr->aliasName, TSDB_DB_NAME_LEN);
}

static void tscProcessServerVer(SSqlObj *pSql) {
  const char* v = pSql->pTscObj->sversion;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  tscSetLocalQueryResult(pSql, v, pExpr->aliasName, tListLen(pSql->pTscObj->sversion));
}

static void tscProcessClientVer(SSqlObj *pSql) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  tscSetLocalQueryResult(pSql, version, pExpr->aliasName, strlen(version));
}

static void tscProcessServStatus(SSqlObj *pSql) {
  STscObj* pObj = pSql->pTscObj;
  
  if (pObj->pHb != NULL) {
    if (pObj->pHb->res.code == TSDB_CODE_NETWORK_UNAVAIL) {
      pSql->res.code = TSDB_CODE_NETWORK_UNAVAIL;
      return;
    }
  } else {
    if (pSql->res.code == TSDB_CODE_NETWORK_UNAVAIL) {
      return;
    }
  }
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  tscSetLocalQueryResult(pSql, "1", pExpr->aliasName, 2);
}

void tscSetLocalQueryResult(SSqlObj *pSql, const char *val, const char *columnName, size_t valueLength) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  pCmd->numOfCols = 1;
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  pQueryInfo->order.order = TSDB_ORDER_ASC;
  
  tscFieldInfoClear(&pQueryInfo->fieldsInfo);
  
  TAOS_FIELD f = tscCreateField(TSDB_DATA_TYPE_BINARY, columnName, valueLength);
  tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  
  tscInitResObjForLocalQuery(pSql, 1, valueLength);

  TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 0);
  SFieldSupInfo* pInfo = tscFieldInfoGetSupp(&pQueryInfo->fieldsInfo, 0);
  pInfo->pSqlExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  
  strncpy(pRes->data, val, pField->bytes);
}

int tscProcessLocalCmd(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;

  if (pCmd->command == TSDB_SQL_CFG_LOCAL) {
    pSql->res.code = (uint8_t)taosCfgDynamicOptions(pCmd->payload);
  } else if (pCmd->command == TSDB_SQL_DESCRIBE_TABLE) {
    pSql->res.code = (uint8_t)tscProcessDescribeTable(pSql);
  } else if (pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    /*
     * set the qhandle to be 1 in order to pass the qhandle check, and to call partial release function to
     * free allocated resources and remove the SqlObj from sql query linked list
     */
    pSql->res.qhandle = 0x1;
    pSql->res.numOfRows = 0;
  } else if (pCmd->command == TSDB_SQL_RESET_CACHE) {
    taosCacheEmpty(tscCacheHandle);
  } else if (pCmd->command == TSDB_SQL_SERV_VERSION) {
    tscProcessServerVer(pSql);
  } else if (pCmd->command == TSDB_SQL_CLI_VERSION) {
    tscProcessClientVer(pSql);
  } else if (pCmd->command == TSDB_SQL_CURRENT_USER) {
    tscProcessCurrentUser(pSql);
  } else if (pCmd->command == TSDB_SQL_CURRENT_DB) {
    tscProcessCurrentDB(pSql);
  } else if (pCmd->command == TSDB_SQL_SERV_STATUS) {
    tscProcessServStatus(pSql);
  } else {
    pSql->res.code = TSDB_CODE_INVALID_SQL;
    tscError("%p not support command:%d", pSql, pCmd->command);
  }

  // keep the code in local variable in order to avoid invalid read in case of async query
  int32_t code = pSql->res.code;

  if (pSql->fp != NULL) {  // callback function
    if (code == 0) {
      (*pSql->fp)(pSql->param, pSql, 0);
    } else {
      tscQueueAsyncRes(pSql);
    }
  }

  return code;
}

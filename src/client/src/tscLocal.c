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

#include "taosdef.h"
#include "tname.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "taos.h"
#include "tscSubquery.h"

#define STR_NOCASE_EQUAL(str1, len1, str2, len2) ((len1 == len2) && 0 == strncasecmp(str1, str2, len1)) 

typedef enum BuildType {
  SCREATE_BUILD_TABLE = 1, 
  SCREATE_BUILD_DB    = 2, 
} BuildType; 

typedef enum Stage {
  SCREATE_CALLBACK_QUERY    = 1,
  SCREATE_CALLBACK_RETRIEVE = 2,
} Stage;

// support 'show create table'   
typedef struct SCreateBuilder {
  char sTableName[TSDB_TABLE_FNAME_LEN];
  char buf[TSDB_TABLE_FNAME_LEN];
  SSqlObj *pParentSql; 
  SSqlObj *pInterSql;
  int32_t (*fp)(void *para, char* result);
  Stage callStage;
} SCreateBuilder;

static void tscSetLocalQueryResult(SSqlObj *pSql, const char *val, const char *columnName, int16_t type, size_t valueLength);

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
    STR_WITH_MAXSIZE_TO_VARSTR(dst, pSchema[i].name, pField->bytes);

    char *type = tDataTypeDesc[pSchema[i].type].aName;

    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 1);
    dst = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 1) * totalNumOfRows + pField->bytes * i;
    
    STR_WITH_MAXSIZE_TO_VARSTR(dst, type, pField->bytes);
    
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
      STR_WITH_MAXSIZE_TO_VARSTR(output, src, pField->bytes);
    }
  }

  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    return 0;
  }

  // the following is handle display tags for table created according to super table
  for (int32_t i = numOfRows; i < totalNumOfRows; ++i) {
    // field name
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 0);
    char* output = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 0) * totalNumOfRows + pField->bytes * i;
    STR_WITH_MAXSIZE_TO_VARSTR(output, pSchema[i].name, pField->bytes);

    // type name
    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 1);
    char *type = tDataTypeDesc[pSchema[i].type].aName;
    
    output = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 1) * totalNumOfRows + pField->bytes * i;
    STR_WITH_MAXSIZE_TO_VARSTR(output, type, pField->bytes);

    // type length
    int32_t bytes = pSchema[i].bytes;
    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 2);
    if (pSchema[i].type == TSDB_DATA_TYPE_BINARY || pSchema[i].type == TSDB_DATA_TYPE_NCHAR) {
      bytes -= VARSTR_HEADER_SIZE;
      
      if (pSchema[i].type == TSDB_DATA_TYPE_NCHAR) {
        bytes = bytes / TSDB_NCHAR_SIZE;
      }
    }

    *(int32_t *)(pRes->data + tscFieldInfoGetOffset(pQueryInfo, 2) * totalNumOfRows + pField->bytes * i) = bytes;

    // tag value
    pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 3);
    char *target = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 3) * totalNumOfRows + pField->bytes * i;
    const char *src = "TAG";
    STR_WITH_MAXSIZE_TO_VARSTR(target, src, pField->bytes);
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

  TAOS_FIELD f = {.type = TSDB_DATA_TYPE_BINARY, .bytes = (TSDB_COL_NAME_LEN - 1) + VARSTR_HEADER_SIZE};
  tstrncpy(f.name, "Field", sizeof(f.name));
  
  SInternalField* pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_BINARY,
      (TSDB_COL_NAME_LEN - 1) + VARSTR_HEADER_SIZE, -1000, (TSDB_COL_NAME_LEN - 1), false);
  
  rowLen += ((TSDB_COL_NAME_LEN - 1) + VARSTR_HEADER_SIZE);

  f.bytes = (int16_t)(typeColLength + VARSTR_HEADER_SIZE);
  f.type = TSDB_DATA_TYPE_BINARY;
  tstrncpy(f.name, "Type", sizeof(f.name));
  
  pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_BINARY, (int16_t)(typeColLength + VARSTR_HEADER_SIZE),
      -1000, typeColLength, false);
  
  rowLen += typeColLength + VARSTR_HEADER_SIZE;

  f.bytes = sizeof(int32_t);
  f.type = TSDB_DATA_TYPE_INT;
  tstrncpy(f.name, "Length", sizeof(f.name));
  
  pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_INT, sizeof(int32_t),
      -1000, sizeof(int32_t), false);
  
  rowLen += sizeof(int32_t);

  f.bytes = (int16_t)(noteColLength + VARSTR_HEADER_SIZE);
  f.type = TSDB_DATA_TYPE_BINARY;
  tstrncpy(f.name, "Note", sizeof(f.name));
  
  pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_BINARY, (int16_t)(noteColLength + VARSTR_HEADER_SIZE),
      -1000, noteColLength, false);
  
  rowLen += noteColLength + VARSTR_HEADER_SIZE;
  return rowLen;
}

static int32_t tscProcessDescribeTable(SSqlObj *pSql) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  
  assert(tscGetMetaInfo(pQueryInfo, 0)->pTableMeta != NULL);

  const int32_t NUM_OF_DESC_TABLE_COLUMNS = 4;
  const int32_t TYPE_COLUMN_LENGTH = 16;
  const int32_t NOTE_COLUMN_MIN_LENGTH = 8;

  int32_t noteFieldLen = NOTE_COLUMN_MIN_LENGTH;

  int32_t rowLen = tscBuildTableSchemaResultFields(pSql, NUM_OF_DESC_TABLE_COLUMNS, TYPE_COLUMN_LENGTH, noteFieldLen);
  tscFieldInfoUpdateOffset(pQueryInfo);
  return tscSetValueToResObj(pSql, rowLen);
}
static int32_t tscGetNthFieldResult(TAOS_ROW row, TAOS_FIELD* fields, int *lengths, int idx, char *result) {
  const char *val = (const char*)row[idx];
  if (val == NULL) {
    sprintf(result, "%s", TSDB_DATA_NULL_STR);
    return -1;
  } 
  uint8_t type = fields[idx].type;
  int32_t length = lengths[idx]; 

  switch (type) {
    case TSDB_DATA_TYPE_BOOL: 
      sprintf(result, "%s", ((((int32_t)(*((char *)val))) == 1) ? "true" : "false"));
      break;
    case TSDB_DATA_TYPE_TINYINT:
      sprintf(result, "%d", *((int8_t *)val));
      break;
    case TSDB_DATA_TYPE_SMALLINT: 
      sprintf(result, "%d", *((int16_t *)val));
      break;
    case TSDB_DATA_TYPE_INT:
      sprintf(result, "%d", *((int32_t *)val));
      break;
    case TSDB_DATA_TYPE_BIGINT: 
      sprintf(result, "%"PRId64, *((int64_t *)val)); 
      break;
    case TSDB_DATA_TYPE_FLOAT:
      sprintf(result, "%f", GET_FLOAT_VAL(val)); 
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      sprintf(result, "%f", GET_DOUBLE_VAL(val)); 
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_BINARY:
      memcpy(result, val, length); 
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      ///formatTimestamp(buf, *(int64_t*)val, TSDB_TIME_PRECISION_MICRO);
      //memcpy(result, val, strlen(buf));
      sprintf(result, "%"PRId64, *((int64_t *)val)); 
      break;
    default:
      break; 
  }
  return 0;
} 

void tscSCreateCallBack(void *param, TAOS_RES *tres, int code) {
  if (param == NULL || tres == NULL) {
    return;
  }  
  SCreateBuilder *builder = (SCreateBuilder *)(param);
  SSqlObj *pParentSql = builder->pParentSql;  
  SSqlObj *pSql = (SSqlObj *)tres; 

  SSqlRes *pRes = &pParentSql->res;
  pRes->code = taos_errno(pSql); 
  if (pRes->code != TSDB_CODE_SUCCESS) {
    taos_free_result(pSql);  
    free(builder);
    tscAsyncResultOnError(pParentSql);
    return;
  }

  if (builder->callStage == SCREATE_CALLBACK_QUERY) {
    taos_fetch_rows_a(tres, tscSCreateCallBack, param);    
    builder->callStage = SCREATE_CALLBACK_RETRIEVE;
  } else {
    char *result = calloc(1, TSDB_MAX_BINARY_LEN);
    pRes->code = builder->fp(builder, result);

    taos_free_result(pSql);  
    free(builder);
    free(result);

    if (pRes->code == TSDB_CODE_SUCCESS) {
      (*pParentSql->fp)(pParentSql->param, pParentSql, code);  
    } else {
      tscAsyncResultOnError(pParentSql);
    }
  }
}

TAOS_ROW tscFetchRow(void *param) {
  SCreateBuilder *builder = (SCreateBuilder *)param;
  if (builder == NULL) {
    return NULL;
  } 
  SSqlObj *pSql = builder->pInterSql;
  if (pSql == NULL || pSql->signature != pSql) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }
  
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;
  
  if (pRes->qhandle == 0 ||
      pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      pCmd->command == TSDB_SQL_INSERT) {
    return NULL;
  }

  // set the sql object owner
  tscSetSqlOwner(pSql);

  // current data set are exhausted, fetch more data from node
  if (pRes->row >= pRes->numOfRows && (pRes->completed != true || hasMoreVnodesToTry(pSql) || hasMoreClauseToTry(pSql)) &&
      (pCmd->command == TSDB_SQL_RETRIEVE ||
       pCmd->command == TSDB_SQL_RETRIEVE_LOCALMERGE ||
       pCmd->command == TSDB_SQL_TABLE_JOIN_RETRIEVE ||
       pCmd->command == TSDB_SQL_FETCH ||
       pCmd->command == TSDB_SQL_SHOW ||
       pCmd->command == TSDB_SQL_SHOW_CREATE_TABLE ||
       pCmd->command == TSDB_SQL_SHOW_CREATE_DATABASE ||
       pCmd->command == TSDB_SQL_SELECT ||
       pCmd->command == TSDB_SQL_DESCRIBE_TABLE ||
       pCmd->command == TSDB_SQL_SERV_STATUS ||
       pCmd->command == TSDB_SQL_CURRENT_DB ||
       pCmd->command == TSDB_SQL_SERV_VERSION ||
       pCmd->command == TSDB_SQL_CLI_VERSION ||
       pCmd->command == TSDB_SQL_CURRENT_USER )) {
    taos_fetch_rows_a(pSql, tscSCreateCallBack, param);
    return NULL;
  }

  void* data = doSetResultRowData(pSql);

  tscClearSqlOwner(pSql);
  return data;
}
static int32_t tscGetTableTagValue(SCreateBuilder *builder, char *result) {
  TAOS_ROW row = tscFetchRow(builder);
  SSqlObj* pSql = builder->pInterSql;

  if (row == NULL) {
   return TSDB_CODE_TSC_INVALID_TABLE_NAME;
  }

  int32_t* lengths = taos_fetch_lengths(pSql);  
  int num_fields = taos_num_fields(pSql);
  TAOS_FIELD *fields = taos_fetch_fields(pSql);

  char buf[TSDB_COL_NAME_LEN + 16]; 
  for (int i = 0; i < num_fields; i++) {
    memset(buf, 0, sizeof(buf));
    int32_t ret = tscGetNthFieldResult(row, fields, lengths, i, buf);

    if (i == 0) {
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "%s", "(");
    } 
    if ((fields[i].type == TSDB_DATA_TYPE_NCHAR 
        || fields[i].type == TSDB_DATA_TYPE_BINARY 
        || fields[i].type == TSDB_DATA_TYPE_TIMESTAMP) && 0 == ret) {
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "\"%s\",", buf);
    } else {
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "%s,", buf);
    }
    if (i == num_fields - 1) {
      sprintf(result + strlen(result) - 1, "%s", ")");
    }
  }  

  if (0 == strlen(result)) {
   return TSDB_CODE_TSC_INVALID_TABLE_NAME;
  }
  return TSDB_CODE_SUCCESS;
}


// build 'show create table/database' result fields 
static int32_t tscSCreateBuildResultFields(SSqlObj *pSql, BuildType type, const char *ddl) {
  int32_t  rowLen = 0;
  int16_t  ddlLen = (int16_t)strlen(ddl); 
  SColumnIndex index = {0};
  pSql->cmd.numOfCols = 2;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  pQueryInfo->order.order = TSDB_ORDER_ASC;

  TAOS_FIELD f; 
  if (type == SCREATE_BUILD_TABLE) {
    f.type  = TSDB_DATA_TYPE_BINARY;
    f.bytes = (TSDB_COL_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
    tstrncpy(f.name, "Table", sizeof(f.name));
  } else {
    f.type  = TSDB_DATA_TYPE_BINARY;
    f.bytes =  (TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
    tstrncpy(f.name, "Database", sizeof(f.name));
  } 

  SInternalField* pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_BINARY, f.bytes, -1000, f.bytes - VARSTR_HEADER_SIZE, false);

  rowLen += f.bytes; 

  f.bytes = (int16_t)(ddlLen + VARSTR_HEADER_SIZE);
  f.type = TSDB_DATA_TYPE_BINARY;
  if (type == SCREATE_BUILD_TABLE) {
    tstrncpy(f.name, "Create Table", sizeof(f.name));
  } else {
    tstrncpy(f.name, "Create Database", sizeof(f.name));
  }

  pInfo = tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);
  pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index, TSDB_DATA_TYPE_BINARY, 
      (int16_t)(ddlLen + VARSTR_HEADER_SIZE), -1000, ddlLen, false);

  rowLen += ddlLen + VARSTR_HEADER_SIZE;

  return rowLen;
}
static int32_t tscSCreateSetValueToResObj(SSqlObj *pSql, int32_t rowLen, const char *tableName, const char *ddl) {
  SSqlRes *pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  int32_t numOfRows = 1;
  if (strlen(ddl) == 0) {
    
  }
  tscInitResObjForLocalQuery(pSql, numOfRows, rowLen);

  TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 0);
  char* dst = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 0) * numOfRows;
  STR_WITH_MAXSIZE_TO_VARSTR(dst, tableName, pField->bytes);

  pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, 1);
  dst = pRes->data + tscFieldInfoGetOffset(pQueryInfo, 1) * numOfRows;
  STR_WITH_MAXSIZE_TO_VARSTR(dst, ddl, pField->bytes);
  return 0;
}
static int32_t tscSCreateBuildResult(SSqlObj *pSql, BuildType type, const char *str, const char *result) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0); 
  int32_t rowLen = tscSCreateBuildResultFields(pSql, type, result);

  tscFieldInfoUpdateOffset(pQueryInfo);
  return tscSCreateSetValueToResObj(pSql, rowLen, str, result);  
}
int32_t tscRebuildCreateTableStatement(void *param,char *result) {
  SCreateBuilder *builder = (SCreateBuilder *)param;
  int32_t code = TSDB_CODE_SUCCESS;

  char *buf = calloc(1,TSDB_MAX_BINARY_LEN);
  if (buf == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  code = tscGetTableTagValue(builder, buf);
  if (code == TSDB_CODE_SUCCESS) {
    snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "CREATE TABLE %s USING %s TAGS %s", builder->buf, builder->sTableName, buf);
    code = tscSCreateBuildResult(builder->pParentSql, SCREATE_BUILD_TABLE, builder->buf, result);    
  }  
  free(buf);
  return code;
}

static int32_t tscGetDBInfo(SCreateBuilder *builder, char *result) {
  TAOS_ROW row = tscFetchRow(builder);
  if (row == NULL) {
   return TSDB_CODE_TSC_DB_NOT_SELECTED;
  }
  const char *showColumns[] = {"REPLICA", "QUORUM", "DAYS", "KEEP", "BLOCKS", NULL};

  SSqlObj *pSql = builder->pInterSql;
  TAOS_FIELD *fields = taos_fetch_fields(pSql);
  int num_fields = taos_num_fields(pSql);

  char buf[TSDB_DB_NAME_LEN + 64] = {0}; 
  do {
    int32_t* lengths = taos_fetch_lengths(pSql);  
    int32_t ret = tscGetNthFieldResult(row, fields, lengths, 0, buf);
    if (0 == ret && STR_NOCASE_EQUAL(buf, strlen(buf), builder->buf, strlen(builder->buf))) {
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "CREATE DATABASE %s", buf);  
      for (int i = 1; i < num_fields; i++) {
        for (int j = 0; showColumns[j] != NULL; j++) {
          if (STR_NOCASE_EQUAL(fields[i].name, strlen(fields[i].name), showColumns[j], strlen(showColumns[j]))) {
            memset(buf, 0, sizeof(buf));
            ret = tscGetNthFieldResult(row, fields, lengths, i, buf);
            if (ret == 0) {
              snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), " %s %s", showColumns[j], buf); 
            }
          }
        }
      }
      break;
    } 
    
    row = tscFetchRow(builder);
  } while (row != NULL);

  if (0 == strlen(result)) {
   return TSDB_CODE_TSC_DB_NOT_SELECTED;
  }

  return TSDB_CODE_SUCCESS;
}
int32_t tscRebuildCreateDBStatement(void *param,char *result) {
  SCreateBuilder *builder = (SCreateBuilder *)param;
  int32_t code = TSDB_CODE_SUCCESS;

  char *buf = calloc(1, TSDB_MAX_BINARY_LEN);
  if (buf == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  code = tscGetDBInfo(param, buf);  
  if (code == TSDB_CODE_SUCCESS) {
    code = tscSCreateBuildResult(builder->pParentSql, SCREATE_BUILD_DB, builder->buf, buf);    
  }
  free(buf);
  return code;
}

static int32_t tscGetTableTagColumnName(SSqlObj *pSql, char **result) {
  char *buf = (char *)malloc(TSDB_MAX_BINARY_LEN);
  if (buf == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY; 
  }
  buf[0] = 0;

  STableMeta *pMeta = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0)->pTableMeta; 
  if (pMeta->tableType == TSDB_SUPER_TABLE || pMeta->tableType == TSDB_NORMAL_TABLE ||
      pMeta->tableType == TSDB_STREAM_TABLE) {
    free(buf);
    return TSDB_CODE_TSC_INVALID_VALUE;
  } 

  SSchema *pTagsSchema = tscGetTableTagSchema(pMeta);  
  int32_t numOfTags = tscGetNumOfTags(pMeta);
  for (int32_t i = 0; i < numOfTags; i++) {
    if (i != numOfTags - 1) {
      snprintf(buf + strlen(buf), TSDB_MAX_BINARY_LEN - strlen(buf), "%s,", pTagsSchema[i].name);  
    } else {
      snprintf(buf + strlen(buf), TSDB_MAX_BINARY_LEN - strlen(buf), "%s", pTagsSchema[i].name);
    }
  }   

  *result = buf;
  return TSDB_CODE_SUCCESS;
}  
static int32_t tscRebuildDDLForSubTable(SSqlObj *pSql, const char *tableName, char *ddl) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableMeta *    pMeta = pTableMetaInfo->pTableMeta;

  SSqlObj *pInterSql = (SSqlObj *)calloc(1, sizeof(SSqlObj)); 
  if (pInterSql == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }  

  SCreateBuilder *param = (SCreateBuilder *)malloc(sizeof(SCreateBuilder));    
  if (param == NULL) {
    free(pInterSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  char fullName[TSDB_TABLE_FNAME_LEN * 2] = {0};
  extractDBName(pTableMetaInfo->name, fullName);
  extractTableName(pMeta->sTableName, param->sTableName);
  snprintf(fullName + strlen(fullName), TSDB_TABLE_FNAME_LEN - strlen(fullName),  ".%s", param->sTableName);
  extractTableName(pTableMetaInfo->name, param->buf);

  param->pParentSql = pSql;
  param->pInterSql  = pInterSql;
  param->fp         = tscRebuildCreateTableStatement;
  param->callStage  = SCREATE_CALLBACK_QUERY;

  char *query = (char *)calloc(1, TSDB_MAX_BINARY_LEN); 
  if (query == NULL) {
    free(param);
    free(pInterSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  char *columns = NULL;
  int32_t code = tscGetTableTagColumnName(pSql, &columns) ;
  if (code != TSDB_CODE_SUCCESS) {
    free(param); 
    free(pInterSql);
    free(query);
    return code;
  }

  snprintf(query + strlen(query), TSDB_MAX_BINARY_LEN - strlen(query), "SELECT %s FROM %s WHERE TBNAME IN(\'%s\')", columns, fullName, param->buf);
  doAsyncQuery(pSql->pTscObj, pInterSql, tscSCreateCallBack, param, query, strlen(query));
  free(query);
  free(columns);

  return TSDB_CODE_TSC_ACTION_IN_PROGRESS; 
}
static int32_t tscRebuildDDLForNormalTable(SSqlObj *pSql, const char *tableName, char *ddl) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableMeta *    pMeta = pTableMetaInfo->pTableMeta;

  int32_t numOfRows = tscGetNumOfColumns(pMeta);
  SSchema *pSchema = tscGetTableSchema(pMeta);

  char *result = ddl;
  sprintf(result, "create table %s (", tableName);
  for (int32_t i = 0; i < numOfRows; ++i) {
    uint8_t type = pSchema[i].type;
    if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
      int32_t bytes = pSchema[i].bytes - VARSTR_HEADER_SIZE;
      if (type == TSDB_DATA_TYPE_NCHAR) {
        bytes =  bytes/TSDB_NCHAR_SIZE;
      }
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "%s %s(%d),", pSchema[i].name, tDataTypeDesc[pSchema[i].type].aName, bytes);
    } else {
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "%s %s,", pSchema[i].name, tDataTypeDesc[pSchema[i].type].aName); 
    }
  }
  sprintf(result + strlen(result) - 1, "%s", ")");

  return TSDB_CODE_SUCCESS;
}
static int32_t tscRebuildDDLForSuperTable(SSqlObj *pSql, const char *tableName, char *ddl) {
  char *result = ddl;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableMeta *    pMeta = pTableMetaInfo->pTableMeta;

  int32_t numOfRows = tscGetNumOfColumns(pMeta);
  int32_t totalRows = numOfRows + tscGetNumOfTags(pMeta);
  SSchema *pSchema = tscGetTableSchema(pMeta);

  sprintf(result, "create table %s (", tableName);
  for (int32_t i = 0; i < numOfRows; ++i) {
    uint8_t type = pSchema[i].type;
    if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
      int32_t bytes = pSchema[i].bytes - VARSTR_HEADER_SIZE;
      if (type == TSDB_DATA_TYPE_NCHAR) {
        bytes =  bytes/TSDB_NCHAR_SIZE;
      }
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result),"%s %s(%d),", pSchema[i].name,tDataTypeDesc[pSchema[i].type].aName, bytes);
    } else {
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "%s %s,", pSchema[i].name, tDataTypeDesc[type].aName); 
    }
  }
  snprintf(result + strlen(result) - 1, TSDB_MAX_BINARY_LEN - strlen(result), "%s %s", ")", "TAGS (");

  for (int32_t i = numOfRows; i < totalRows; i++) {
    uint8_t type = pSchema[i].type;
    if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
      int32_t bytes = pSchema[i].bytes - VARSTR_HEADER_SIZE;
      if (type == TSDB_DATA_TYPE_NCHAR) {
        bytes =  bytes/TSDB_NCHAR_SIZE;
      }
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "%s %s(%d),", pSchema[i].name,tDataTypeDesc[pSchema[i].type].aName, bytes);
    } else {
      snprintf(result + strlen(result), TSDB_MAX_BINARY_LEN - strlen(result), "%s %s,", pSchema[i].name, tDataTypeDesc[type].aName); 
    }
  }
  sprintf(result + strlen(result) - 1, "%s", ")");

  return TSDB_CODE_SUCCESS;
}

static int32_t tscProcessShowCreateTable(SSqlObj *pSql) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  assert(pTableMetaInfo->pTableMeta != NULL);

  char tableName[TSDB_TABLE_NAME_LEN] = {0};
  extractTableName(pTableMetaInfo->name, tableName);

  char *result = (char *)calloc(1, TSDB_MAX_BINARY_LEN);
  int32_t code = TSDB_CODE_SUCCESS;
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    code = tscRebuildDDLForSuperTable(pSql, tableName, result);
  } else if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
    code = tscRebuildDDLForNormalTable(pSql, tableName, result);
  } else if (UTIL_TABLE_IS_CHILD_TABLE(pTableMetaInfo)) {
    code = tscRebuildDDLForSubTable(pSql, tableName, result);
  } else {
    code = TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = tscSCreateBuildResult(pSql, SCREATE_BUILD_TABLE, tableName, result);
  } 
  free(result);
  return code;
}

static int32_t tscProcessShowCreateDatabase(SSqlObj *pSql) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  SSqlObj *pInterSql = (SSqlObj *)calloc(1, sizeof(SSqlObj)); 
  if (pInterSql == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }  

  SCreateBuilder *param = (SCreateBuilder *)malloc(sizeof(SCreateBuilder));    
  if (param == NULL) {
    free(pInterSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  extractTableName(pTableMetaInfo->name, param->buf);
  param->pParentSql = pSql;
  param->pInterSql  = pInterSql;
  param->fp         = tscRebuildCreateDBStatement;
  param->callStage  = SCREATE_CALLBACK_QUERY;
   
  const char *query = "show databases";
  doAsyncQuery(pSql->pTscObj, pInterSql, tscSCreateCallBack, param, query, strlen(query));
  return TSDB_CODE_TSC_ACTION_IN_PROGRESS;
}
static int32_t tscProcessCurrentUser(SSqlObj *pSql) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  pExpr->resBytes = TSDB_USER_LEN + TSDB_DATA_TYPE_BINARY;
  pExpr->resType = TSDB_DATA_TYPE_BINARY;

  char* vx = calloc(1, pExpr->resBytes);
  if (vx == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  size_t size = sizeof(pSql->pTscObj->user);
  STR_WITH_MAXSIZE_TO_VARSTR(vx, pSql->pTscObj->user, size);

  tscSetLocalQueryResult(pSql, vx, pExpr->aliasName, pExpr->resType, pExpr->resBytes);
  free(vx);

  return TSDB_CODE_SUCCESS;
}

static int32_t tscProcessCurrentDB(SSqlObj *pSql) {
  char db[TSDB_DB_NAME_LEN] = {0};
  extractDBName(pSql->pTscObj->db, db);

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);

  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  pExpr->resType = TSDB_DATA_TYPE_BINARY;

  size_t t = strlen(db);
  pExpr->resBytes = TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE;

  char* vx = calloc(1, pExpr->resBytes);
  if (vx == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  if (t == 0) {
    setVardataNull(vx, TSDB_DATA_TYPE_BINARY);
  } else {
    STR_WITH_SIZE_TO_VARSTR(vx, db, (VarDataLenT)t);
  }

  tscSetLocalQueryResult(pSql, vx, pExpr->aliasName, pExpr->resType, pExpr->resBytes);
  free(vx);

  return TSDB_CODE_SUCCESS;
}

static int32_t tscProcessServerVer(SSqlObj *pSql) {
  const char* v = pSql->pTscObj->sversion;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);

  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  pExpr->resType = TSDB_DATA_TYPE_BINARY;

  size_t t = strlen(v);
  pExpr->resBytes = (int16_t)(t + VARSTR_HEADER_SIZE);

  char* vx = calloc(1, pExpr->resBytes);
  if (vx == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;

  }

  STR_WITH_SIZE_TO_VARSTR(vx, v, (VarDataLenT)t);
  tscSetLocalQueryResult(pSql, vx, pExpr->aliasName, pExpr->resType, pExpr->resBytes);

  free(vx);
  return TSDB_CODE_SUCCESS;

}

static int32_t tscProcessClientVer(SSqlObj *pSql) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  pExpr->resType = TSDB_DATA_TYPE_BINARY;

  size_t t = strlen(version);
  pExpr->resBytes = (int16_t)(t + VARSTR_HEADER_SIZE);

  char* v = calloc(1, pExpr->resBytes);
  if (v == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;

  }

  STR_WITH_SIZE_TO_VARSTR(v, version, (VarDataLenT)t);
  tscSetLocalQueryResult(pSql, v, pExpr->aliasName, pExpr->resType, pExpr->resBytes);

  free(v);
  return TSDB_CODE_SUCCESS;

}

// TODO add test cases.
static int32_t checkForOnlineNode(SSqlObj* pSql) {
  int32_t* data = pSql->res.length;
  if (data == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t total  = data[0];
  int32_t online = data[1];
  return (online < total)? TSDB_CODE_RPC_NETWORK_UNAVAIL:TSDB_CODE_SUCCESS;
}

static int32_t tscProcessServStatus(SSqlObj *pSql) {
  STscObj* pObj = pSql->pTscObj;

  SSqlObj* pHb = (SSqlObj*)taosAcquireRef(tscObjRef, pObj->hbrid);
  if (pHb != NULL) {
    pSql->res.code = pHb->res.code;
    taosReleaseRef(tscObjRef, pObj->hbrid);
  }

  if (pSql->res.code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    return pSql->res.code;
  }

  pSql->res.code = checkForOnlineNode(pHb);
  if (pSql->res.code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    return pSql->res.code;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);

  int32_t val = 1;
  tscSetLocalQueryResult(pSql, (char*) &val, pExpr->aliasName, TSDB_DATA_TYPE_INT, sizeof(int32_t));
  return TSDB_CODE_SUCCESS;
}

void tscSetLocalQueryResult(SSqlObj *pSql, const char *val, const char *columnName, int16_t type, size_t valueLength) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  pCmd->numOfCols = 1;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  pQueryInfo->order.order = TSDB_ORDER_ASC;

  tscFieldInfoClear(&pQueryInfo->fieldsInfo);
  pQueryInfo->fieldsInfo.internalField = taosArrayInit(1, sizeof(SInternalField));

  TAOS_FIELD f = tscCreateField((int8_t)type, columnName, (int16_t)valueLength);
  tscFieldInfoAppend(&pQueryInfo->fieldsInfo, &f);

  tscInitResObjForLocalQuery(pSql, 1, (int32_t)valueLength);

  SInternalField* pInfo = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, 0);
  pInfo->pSqlExpr = taosArrayGetP(pQueryInfo->exprList, 0);

  memcpy(pRes->data, val, pInfo->field.bytes);
}

int tscProcessLocalCmd(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pCmd->command == TSDB_SQL_CFG_LOCAL) {
    pRes->code = (uint8_t)taosCfgDynamicOptions(pCmd->payload);
  } else if (pCmd->command == TSDB_SQL_DESCRIBE_TABLE) {
    pRes->code = (uint8_t)tscProcessDescribeTable(pSql);
  } else if (pCmd->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT) {
    /*
     * set the qhandle to be 1 in order to pass the qhandle check, and to call partial release function to
     * free allocated resources and remove the SqlObj from sql query linked list
     */
    pRes->qhandle = 0x1;
    pRes->numOfRows = 0;
  } else if (pCmd->command == TSDB_SQL_SHOW_CREATE_TABLE) {
    pRes->code = tscProcessShowCreateTable(pSql); 
  } else if (pCmd->command == TSDB_SQL_SHOW_CREATE_DATABASE) {
    pRes->code = tscProcessShowCreateDatabase(pSql); 
  } else if (pCmd->command == TSDB_SQL_RESET_CACHE) {
    taosHashEmpty(tscTableMetaInfo);
    pRes->code = TSDB_CODE_SUCCESS;
  } else if (pCmd->command == TSDB_SQL_SERV_VERSION) {
    pRes->code = tscProcessServerVer(pSql);
  } else if (pCmd->command == TSDB_SQL_CLI_VERSION) {
    pRes->code = tscProcessClientVer(pSql);
  } else if (pCmd->command == TSDB_SQL_CURRENT_USER) {
    pRes->code = tscProcessCurrentUser(pSql);
  } else if (pCmd->command == TSDB_SQL_CURRENT_DB) {
    pRes->code = tscProcessCurrentDB(pSql);
  } else if (pCmd->command == TSDB_SQL_SERV_STATUS) {
    pRes->code = tscProcessServStatus(pSql);
  } else {
    pRes->code = TSDB_CODE_TSC_INVALID_SQL;
    tscError("%p not support command:%d", pSql, pCmd->command);
  }

  // keep the code in local variable in order to avoid invalid read in case of async query

  int32_t code = pRes->code;
  if (code == TSDB_CODE_SUCCESS) {
    (*pSql->fp)(pSql->param, pSql, code);
  } else if (code == TSDB_CODE_TSC_ACTION_IN_PROGRESS){
  } else {
    tscAsyncResultOnError(pSql);
  }
  return code;
}

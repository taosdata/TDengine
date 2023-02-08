#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cJSON.h"
#include "catalog.h"
#include "clientInt.h"
#include "osSemaphore.h"
#include "osThread.h"
#include "query.h"
#include "taos.h"
#include "taoserror.h"
#include "tcommon.h"
#include "tdef.h"
#include "tglobal.h"
#include "tlog.h"
#include "tmsg.h"
#include "tname.h"
#include "ttime.h"
#include "ttypes.h"

//=================================================================================================

#define SPACE ' '
#define COMMA ','
#define EQUAL '='
#define QUOTE '"'
#define SLASH '\\'

#define JUMP_SPACE(sql, sqlEnd) \
  while (sql < sqlEnd) {        \
    if (*sql == SPACE)          \
      sql++;                    \
    else                        \
      break;                    \
  }
// comma ,
#define IS_SLASH_COMMA(sql) (*(sql) == COMMA && *((sql)-1) == SLASH)
#define IS_COMMA(sql)       (*(sql) == COMMA && *((sql)-1) != SLASH)
// space
#define IS_SLASH_SPACE(sql) (*(sql) == SPACE && *((sql)-1) == SLASH)
#define IS_SPACE(sql)       (*(sql) == SPACE && *((sql)-1) != SLASH)
// equal =
#define IS_SLASH_EQUAL(sql) (*(sql) == EQUAL && *((sql)-1) == SLASH)
#define IS_EQUAL(sql)       (*(sql) == EQUAL && *((sql)-1) != SLASH)
// quote "
#define IS_SLASH_QUOTE(sql) (*(sql) == QUOTE && *((sql)-1) == SLASH)
#define IS_QUOTE(sql)       (*(sql) == QUOTE && *((sql)-1) != SLASH)
// SLASH
#define IS_SLASH_SLASH(sql) (*(sql) == SLASH && *((sql)-1) == SLASH)

#define IS_SLASH_LETTER(sql) \
  (IS_SLASH_COMMA(sql) || IS_SLASH_SPACE(sql) || IS_SLASH_EQUAL(sql) || IS_SLASH_QUOTE(sql) || IS_SLASH_SLASH(sql))

#define MOVE_FORWARD_ONE(sql, len) (memmove((void *)((sql)-1), (sql), len))

#define PROCESS_SLASH(key, keyLen)           \
  for (int i = 1; i < keyLen; ++i) {         \
    if (IS_SLASH_LETTER(key + i)) {          \
      MOVE_FORWARD_ONE(key + i, keyLen - i); \
      i--;                                   \
      keyLen--;                              \
    }                                        \
  }

#define IS_INVALID_COL_LEN(len)   ((len) <= 0 || (len) >= TSDB_COL_NAME_LEN)
#define IS_INVALID_TABLE_LEN(len) ((len) <= 0 || (len) >= TSDB_TABLE_NAME_LEN)

#define OTD_JSON_SUB_FIELDS_NUM 2
#define OTD_JSON_FIELDS_NUM     4

#define TS        "_ts"
#define TS_LEN    3
#define VALUE     "_value"
#define VALUE_LEN 6

#define BINARY_ADD_LEN 2  // "binary"   2 means " "
#define NCHAR_ADD_LEN  3  // L"nchar"   3 means L" "

#define MAX_RETRY_TIMES 5
//=================================================================================================
typedef TSDB_SML_PROTOCOL_TYPE SMLProtocolType;

typedef enum {
  SCHEMA_ACTION_NULL,
  SCHEMA_ACTION_CREATE_STABLE,
  SCHEMA_ACTION_ADD_COLUMN,
  SCHEMA_ACTION_ADD_TAG,
  SCHEMA_ACTION_CHANGE_COLUMN_SIZE,
  SCHEMA_ACTION_CHANGE_TAG_SIZE,
} ESchemaAction;

typedef struct {
  const char *measure;
  const char *tags;
  const char *cols;
  const char *timestamp;

  int32_t measureLen;
  int32_t measureTagsLen;
  int32_t tagsLen;
  int32_t colsLen;
  int32_t timestampLen;
} SSmlLineInfo;

typedef struct {
  const char *sTableName;  // super table name
  int32_t     sTableNameLen;
  char        childTableName[TSDB_TABLE_NAME_LEN];
  uint64_t    uid;

  SArray *tags;

  // if info->formatData is true, elements are SArray<SSmlKv*>.
  // if info->formatData is false, elements are SHashObj<cols key string, SSmlKv*> for find by key quickly
  SArray *cols;
} SSmlTableInfo;

typedef struct {
  SArray   *tags;     // save the origin order to create table
  SHashObj *tagHash;  // elements are <key, index in tags>

  SArray   *cols;
  SHashObj *colHash;

  STableMeta *tableMeta;
} SSmlSTableMeta;

typedef struct {
  int32_t len;
  char   *buf;
} SSmlMsgBuf;

typedef struct {
  int32_t code;
  int32_t lineNum;

  int32_t numOfSTables;
  int32_t numOfCTables;
  int32_t numOfCreateSTables;
  int32_t numOfAlterColSTables;
  int32_t numOfAlterTagSTables;

  int64_t parseTime;
  int64_t schemaTime;
  int64_t insertBindTime;
  int64_t insertRpcTime;
  int64_t endTime;
} SSmlCostInfo;

typedef struct {
  int64_t id;

  SMLProtocolType protocol;
  int8_t          precision;
  bool            dataFormat;  // true means that the name and order of keys in each line are the same(only for influx protocol)
  bool            isRawLine;
  int32_t         ttl;

  SHashObj *childTables;
  SHashObj *superTables;
  SHashObj *pVgHash;
  void     *exec;

  STscObj     *taos;
  SCatalog    *pCatalog;
  SRequestObj *pRequest;
  SQuery      *pQuery;

  SSmlCostInfo cost;
  SSmlMsgBuf   msgBuf;
  SHashObj    *dumplicateKey;  // for dumplicate key
  SArray      *colsContainer;  // for cols parse, if dataFormat == false
  int32_t      uid;            // used for automatic create child table

  cJSON       *root;  // for parse json
} SSmlHandle;
//=================================================================================================

//=================================================================================================
static volatile int64_t linesSmlHandleId = 0;
static int64_t          smlGenId() {
           int64_t id;

           do {
             id = atomic_add_fetch_64(&linesSmlHandleId, 1);
  } while (id == 0);

           return id;
}

static inline bool smlDoubleToInt64OverFlow(double num) {
  if (num >= (double)INT64_MAX || num <= (double)INT64_MIN) return true;
  return false;
}

static inline bool smlCheckDuplicateKey(const char *key, int32_t keyLen, SHashObj *pHash) {
  void *val = taosHashGet(pHash, key, keyLen);
  if (val) {
    return true;
  }
  taosHashPut(pHash, key, keyLen, key, 1);
  return false;
}

static int32_t smlBuildInvalidDataMsg(SSmlMsgBuf *pBuf, const char *msg1, const char *msg2) {
  if (pBuf->buf) {
    memset(pBuf->buf, 0, pBuf->len);
    if (msg1) strncat(pBuf->buf, msg1, pBuf->len);
    int32_t left = pBuf->len - strlen(pBuf->buf);
    if (left > 2 && msg2) {
      strncat(pBuf->buf, ":", left - 1);
      strncat(pBuf->buf, msg2, left - 2);
    }
  }
  return TSDB_CODE_SML_INVALID_DATA;
}

static int32_t smlGenerateSchemaAction(SSchema *colField, SHashObj *colHash, SSmlKv *kv, bool isTag,
                                       ESchemaAction *action, SSmlHandle *info) {
  uint16_t *index = colHash ? (uint16_t *)taosHashGet(colHash, kv->key, kv->keyLen) : NULL;
  if (index) {
    if (colField[*index].type != kv->type) {
      uError("SML:0x%" PRIx64 " point type and db type mismatch. key: %s. point type: %d, db type: %d", info->id,
             kv->key, colField[*index].type, kv->type);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }

    if ((colField[*index].type == TSDB_DATA_TYPE_VARCHAR &&
         (colField[*index].bytes - VARSTR_HEADER_SIZE) < kv->length) ||
        (colField[*index].type == TSDB_DATA_TYPE_NCHAR &&
         ((colField[*index].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE < kv->length))) {
      if (isTag) {
        *action = SCHEMA_ACTION_CHANGE_TAG_SIZE;
      } else {
        *action = SCHEMA_ACTION_CHANGE_COLUMN_SIZE;
      }
    }
  } else {
    if (isTag) {
      *action = SCHEMA_ACTION_ADD_TAG;
    } else {
      *action = SCHEMA_ACTION_ADD_COLUMN;
    }
  }
  return 0;
}

static int32_t smlFindNearestPowerOf2(int32_t length, uint8_t type) {
  int32_t result = 1;
  while (result <= length) {
    result *= 2;
  }
  if (type == TSDB_DATA_TYPE_BINARY && result > TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) {
    result = TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE;
  } else if (type == TSDB_DATA_TYPE_NCHAR && result > (TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
    result = (TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
  }

  if (type == TSDB_DATA_TYPE_NCHAR) {
    result = result * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  } else if (type == TSDB_DATA_TYPE_BINARY) {
    result = result + VARSTR_HEADER_SIZE;
  }
  return result;
}

static int32_t smlProcessSchemaAction(SSmlHandle *info, SSchema *schemaField, SHashObj *schemaHash, SArray *cols,
                                      ESchemaAction *action, bool isTag) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int j = 0; j < taosArrayGetSize(cols); ++j) {
    if (j == 0 && !isTag) continue;
    SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, j);
    code = smlGenerateSchemaAction(schemaField, schemaHash, kv, isTag, action, info);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t smlCheckMeta(SSchema *schema, int32_t length, SArray *cols, bool isTag) {
  SHashObj *hashTmp = taosHashInit(length, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  int32_t   i = 0;
  for (; i < length; i++) {
    taosHashPut(hashTmp, schema[i].name, strlen(schema[i].name), &i, SHORT_BYTES);
  }

  if (isTag) {
    i = 0;
  } else {
    i = 1;
  }
  for (; i < taosArrayGetSize(cols); i++) {
    SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, i);
    if (taosHashGet(hashTmp, kv->key, kv->keyLen) == NULL) {
      taosHashCleanup(hashTmp);
      return -1;
    }
  }
  taosHashCleanup(hashTmp);
  return 0;
}

static int32_t getBytes(uint8_t type, int32_t length) {
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    return smlFindNearestPowerOf2(length, type);
  } else {
    return tDataTypes[type].bytes;
  }
}

static int32_t smlBuildFieldsList(SSmlHandle *info, SSchema *schemaField, SHashObj *schemaHash, SArray *cols,
                                  SArray *results, int32_t numOfCols, bool isTag) {
  for (int j = 0; j < taosArrayGetSize(cols); ++j) {
    SSmlKv       *kv = (SSmlKv *)taosArrayGetP(cols, j);
    ESchemaAction action = SCHEMA_ACTION_NULL;
    smlGenerateSchemaAction(schemaField, schemaHash, kv, isTag, &action, info);
    if (action == SCHEMA_ACTION_ADD_COLUMN || action == SCHEMA_ACTION_ADD_TAG) {
      SField field = {0};
      field.type = kv->type;
      field.bytes = getBytes(kv->type, kv->length);
      memcpy(field.name, kv->key, kv->keyLen);
      taosArrayPush(results, &field);
    } else if (action == SCHEMA_ACTION_CHANGE_COLUMN_SIZE || action == SCHEMA_ACTION_CHANGE_TAG_SIZE) {
      uint16_t *index = (uint16_t *)taosHashGet(schemaHash, kv->key, kv->keyLen);
      uint16_t  newIndex = *index;
      if (isTag) newIndex -= numOfCols;
      SField *field = (SField *)taosArrayGet(results, newIndex);
      field->bytes = getBytes(kv->type, kv->length);
    }
  }
  return TSDB_CODE_SUCCESS;
}

// static int32_t smlSendMetaMsg(SSmlHandle *info, SName *pName, SSmlSTableMeta *sTableData,
//                               int32_t colVer, int32_t tagVer, int8_t source, uint64_t suid){
static int32_t smlSendMetaMsg(SSmlHandle *info, SName *pName, SArray *pColumns, SArray *pTags, STableMeta *pTableMeta,
                              ESchemaAction action) {
  SRequestObj   *pRequest = NULL;
  SMCreateStbReq pReq = {0};
  int32_t        code = TSDB_CODE_SUCCESS;
  SCmdMsgInfo    pCmdMsg = {0};

  // put front for free
  pReq.numOfColumns = taosArrayGetSize(pColumns);
  pReq.pColumns = pColumns;
  pReq.numOfTags = taosArrayGetSize(pTags);
  pReq.pTags = pTags;

  code = buildRequest(info->taos->id, "", 0, NULL, false, &pRequest, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pRequest->syncQuery = true;
  if (!pRequest->pDb) {
    code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    goto end;
  }

  if (action == SCHEMA_ACTION_CREATE_STABLE) {
    pReq.colVer = 1;
    pReq.tagVer = 1;
    pReq.suid = 0;
    pReq.source = TD_REQ_FROM_APP;
  } else if (action == SCHEMA_ACTION_ADD_TAG || action == SCHEMA_ACTION_CHANGE_TAG_SIZE) {
    pReq.colVer = pTableMeta->sversion;
    pReq.tagVer = pTableMeta->tversion + 1;
    pReq.suid = pTableMeta->uid;
    pReq.source = TD_REQ_FROM_TAOX;
  } else if (action == SCHEMA_ACTION_ADD_COLUMN || action == SCHEMA_ACTION_CHANGE_COLUMN_SIZE) {
    pReq.colVer = pTableMeta->sversion + 1;
    pReq.tagVer = pTableMeta->tversion;
    pReq.suid = pTableMeta->uid;
    pReq.source = TD_REQ_FROM_TAOX;
  }

  if (pReq.numOfTags == 0) {
    pReq.numOfTags = 1;
    SField field = {0};
    field.type = TSDB_DATA_TYPE_NCHAR;
    field.bytes = 1;
    strcpy(field.name, tsSmlTagName);
    taosArrayPush(pReq.pTags, &field);
  }

  pReq.commentLen = -1;
  pReq.igExists = true;
  tNameExtractFullName(pName, pReq.name);

  pCmdMsg.epSet = getEpSet_s(&info->taos->pAppInfo->mgmtEp);
  pCmdMsg.msgType = TDMT_MND_CREATE_STB;
  pCmdMsg.msgLen = tSerializeSMCreateStbReq(NULL, 0, &pReq);
  pCmdMsg.pMsg = taosMemoryMalloc(pCmdMsg.msgLen);
  if (NULL == pCmdMsg.pMsg) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  tSerializeSMCreateStbReq(pCmdMsg.pMsg, pCmdMsg.msgLen, &pReq);

  SQuery pQuery;
  memset(&pQuery, 0, sizeof(pQuery));
  pQuery.execMode = QUERY_EXEC_MODE_RPC;
  pQuery.pCmdMsg = &pCmdMsg;
  pQuery.msgType = pQuery.pCmdMsg->msgType;
  pQuery.stableQuery = true;

  launchQueryImpl(pRequest, &pQuery, true, NULL);

  if (pRequest->code == TSDB_CODE_SUCCESS) {
    catalogRemoveTableMeta(info->pCatalog, pName);
  }
  code = pRequest->code;
  taosMemoryFree(pCmdMsg.pMsg);

end:
  destroyRequest(pRequest);
  tFreeSMCreateStbReq(&pReq);
  return code;
}

static int32_t smlModifyDBSchemas(SSmlHandle *info) {
  int32_t     code = 0;
  SHashObj   *hashTmp = NULL;
  STableMeta *pTableMeta = NULL;

  SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
  tstrncpy(pName.dbname, info->pRequest->pDb, sizeof(pName.dbname));

  SRequestConnInfo conn = {0};
  conn.pTrans = info->taos->pAppInfo->pTransporter;
  conn.requestId = info->pRequest->requestId;
  conn.requestObjRefId = info->pRequest->self;
  conn.mgmtEps = getEpSet_s(&info->taos->pAppInfo->mgmtEp);

  SSmlSTableMeta **tableMetaSml = (SSmlSTableMeta **)taosHashIterate(info->superTables, NULL);
  while (tableMetaSml) {
    SSmlSTableMeta *sTableData = *tableMetaSml;
    bool            needCheckMeta = false;  // for multi thread

    size_t superTableLen = 0;
    void  *superTable = taosHashGetKey(tableMetaSml, &superTableLen);
    memset(pName.tname, 0, TSDB_TABLE_NAME_LEN);
    memcpy(pName.tname, superTable, superTableLen);

    code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);

    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_MND_STB_NOT_EXIST) {
      SArray *pColumns = taosArrayInit(taosArrayGetSize(sTableData->cols), sizeof(SField));
      SArray *pTags = taosArrayInit(taosArrayGetSize(sTableData->tags), sizeof(SField));
      smlBuildFieldsList(info, NULL, NULL, sTableData->tags, pTags, 0, true);
      smlBuildFieldsList(info, NULL, NULL, sTableData->cols, pColumns, 0, false);

      code = smlSendMetaMsg(info, &pName, pColumns, pTags, NULL, SCHEMA_ACTION_CREATE_STABLE);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " smlSendMetaMsg failed. can not create %s", info->id, pName.tname);
        goto end;
      }
      info->cost.numOfCreateSTables++;
      taosMemoryFreeClear(pTableMeta);

      code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " catalogGetSTableMeta failed. super table name %s", info->id, pName.tname);
        goto end;
      }
    } else if (code == TSDB_CODE_SUCCESS) {
      hashTmp = taosHashInit(pTableMeta->tableInfo.numOfTags, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                             HASH_NO_LOCK);
      for (uint16_t i = pTableMeta->tableInfo.numOfColumns;
           i < pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags; i++) {
        taosHashPut(hashTmp, pTableMeta->schema[i].name, strlen(pTableMeta->schema[i].name), &i, SHORT_BYTES);
      }

      ESchemaAction action = SCHEMA_ACTION_NULL;
      code = smlProcessSchemaAction(info, pTableMeta->schema, hashTmp, sTableData->tags, &action, true);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
      if (action != SCHEMA_ACTION_NULL) {
        SArray *pColumns =
            taosArrayInit(taosArrayGetSize(sTableData->cols) + pTableMeta->tableInfo.numOfColumns, sizeof(SField));
        SArray *pTags =
            taosArrayInit(taosArrayGetSize(sTableData->tags) + pTableMeta->tableInfo.numOfTags, sizeof(SField));

        for (uint16_t i = 0; i < pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags; i++) {
          SField field = {0};
          field.type = pTableMeta->schema[i].type;
          field.bytes = pTableMeta->schema[i].bytes;
          strcpy(field.name, pTableMeta->schema[i].name);
          if (i < pTableMeta->tableInfo.numOfColumns) {
            taosArrayPush(pColumns, &field);
          } else {
            taosArrayPush(pTags, &field);
          }
        }
        smlBuildFieldsList(info, pTableMeta->schema, hashTmp, sTableData->tags, pTags,
                           pTableMeta->tableInfo.numOfColumns, true);

        code = smlSendMetaMsg(info, &pName, pColumns, pTags, pTableMeta, action);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " smlSendMetaMsg failed. can not create %s", info->id, pName.tname);
          goto end;
        }

        info->cost.numOfAlterTagSTables++;
        taosMemoryFreeClear(pTableMeta);
        code = catalogRefreshTableMeta(info->pCatalog, &conn, &pName, -1);
        if (code != TSDB_CODE_SUCCESS) {
          goto end;
        }
        code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);
        if (code != TSDB_CODE_SUCCESS) {
          goto end;
        }
      }

      taosHashClear(hashTmp);
      for (uint16_t i = 0; i < pTableMeta->tableInfo.numOfColumns; i++) {
        taosHashPut(hashTmp, pTableMeta->schema[i].name, strlen(pTableMeta->schema[i].name), &i, SHORT_BYTES);
      }
      action = SCHEMA_ACTION_NULL;
      code = smlProcessSchemaAction(info, pTableMeta->schema, hashTmp, sTableData->cols, &action, false);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
      if (action != SCHEMA_ACTION_NULL) {
        SArray *pColumns =
            taosArrayInit(taosArrayGetSize(sTableData->cols) + pTableMeta->tableInfo.numOfColumns, sizeof(SField));
        SArray *pTags =
            taosArrayInit(taosArrayGetSize(sTableData->tags) + pTableMeta->tableInfo.numOfTags, sizeof(SField));

        for (uint16_t i = 0; i < pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags; i++) {
          SField field = {0};
          field.type = pTableMeta->schema[i].type;
          field.bytes = pTableMeta->schema[i].bytes;
          strcpy(field.name, pTableMeta->schema[i].name);
          if (i < pTableMeta->tableInfo.numOfColumns) {
            taosArrayPush(pColumns, &field);
          } else {
            taosArrayPush(pTags, &field);
          }
        }

        smlBuildFieldsList(info, pTableMeta->schema, hashTmp, sTableData->cols, pColumns,
                           pTableMeta->tableInfo.numOfColumns, false);

        code = smlSendMetaMsg(info, &pName, pColumns, pTags, pTableMeta, action);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " smlSendMetaMsg failed. can not create %s", info->id, pName.tname);
          goto end;
        }

        info->cost.numOfAlterColSTables++;
        taosMemoryFreeClear(pTableMeta);
        code = catalogRefreshTableMeta(info->pCatalog, &conn, &pName, -1);
        if (code != TSDB_CODE_SUCCESS) {
          goto end;
        }
        code = catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " catalogGetSTableMeta failed. super table name %s", info->id, pName.tname);
          goto end;
        }
      }

      needCheckMeta = true;
      taosHashCleanup(hashTmp);
      hashTmp = NULL;
    } else {
      uError("SML:0x%" PRIx64 " load table meta error: %s", info->id, tstrerror(code));
      goto end;
    }

    if (needCheckMeta) {
      code = smlCheckMeta(&(pTableMeta->schema[pTableMeta->tableInfo.numOfColumns]), pTableMeta->tableInfo.numOfTags,
                          sTableData->tags, true);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " check tag failed. super table name %s", info->id, pName.tname);
        goto end;
      }
      code = smlCheckMeta(&(pTableMeta->schema[0]), pTableMeta->tableInfo.numOfColumns, sTableData->cols, false);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " check cols failed. super table name %s", info->id, pName.tname);
        goto end;
      }
    }

    sTableData->tableMeta = pTableMeta;

    tableMetaSml = (SSmlSTableMeta **)taosHashIterate(info->superTables, tableMetaSml);
  }
  return 0;

end:
  taosHashCleanup(hashTmp);
  taosMemoryFreeClear(pTableMeta);
//  catalogRefreshTableMeta(info->pCatalog, &conn, &pName, 1);
  return code;
}

static bool smlParseNumber(SSmlKv *kvVal, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t     len = kvVal->length;
  char       *endptr = NULL;
  double      result = taosStr2Double(pVal, &endptr);
  if (pVal == endptr) {
    smlBuildInvalidDataMsg(msg, "invalid data", pVal);
    return false;
  }

  int32_t left = len - (endptr - pVal);
  if (left == 0 || (left == 3 && strncasecmp(endptr, "f64", left) == 0)) {
    kvVal->type = TSDB_DATA_TYPE_DOUBLE;
    kvVal->d = result;
  } else if ((left == 3 && strncasecmp(endptr, "f32", left) == 0)) {
    if (!IS_VALID_FLOAT(result)) {
      smlBuildInvalidDataMsg(msg, "float out of range[-3.402823466e+38,3.402823466e+38]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_FLOAT;
    kvVal->f = (float)result;
  } else if ((left == 1 && *endptr == 'i') || (left == 3 && strncasecmp(endptr, "i64", left) == 0)) {
    if (smlDoubleToInt64OverFlow(result)) {
      errno = 0;
      int64_t tmp = taosStr2Int64(pVal, &endptr, 10);
      if (errno == ERANGE) {
        smlBuildInvalidDataMsg(msg, "big int out of range[-9223372036854775808,9223372036854775807]", pVal);
        return false;
      }
      kvVal->type = TSDB_DATA_TYPE_BIGINT;
      kvVal->i = tmp;
      return true;
    }
    kvVal->type = TSDB_DATA_TYPE_BIGINT;
    kvVal->i = (int64_t)result;
  } else if ((left == 1 && *endptr == 'u') || (left == 3 && strncasecmp(endptr, "u64", left) == 0)) {
    if (result >= (double)UINT64_MAX || result < 0) {
      errno = 0;
      uint64_t tmp = taosStr2UInt64(pVal, &endptr, 10);
      if (errno == ERANGE || result < 0) {
        smlBuildInvalidDataMsg(msg, "unsigned big int out of range[0,18446744073709551615]", pVal);
        return false;
      }
      kvVal->type = TSDB_DATA_TYPE_UBIGINT;
      kvVal->u = tmp;
      return true;
    }
    kvVal->type = TSDB_DATA_TYPE_UBIGINT;
    kvVal->u = result;
  } else if (left == 3 && strncasecmp(endptr, "i32", left) == 0) {
    if (!IS_VALID_INT(result)) {
      smlBuildInvalidDataMsg(msg, "int out of range[-2147483648,2147483647]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_INT;
    kvVal->i = result;
  } else if (left == 3 && strncasecmp(endptr, "u32", left) == 0) {
    if (!IS_VALID_UINT(result)) {
      smlBuildInvalidDataMsg(msg, "unsigned int out of range[0,4294967295]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_UINT;
    kvVal->u = result;
  } else if (left == 3 && strncasecmp(endptr, "i16", left) == 0) {
    if (!IS_VALID_SMALLINT(result)) {
      smlBuildInvalidDataMsg(msg, "small int our of range[-32768,32767]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_SMALLINT;
    kvVal->i = result;
  } else if (left == 3 && strncasecmp(endptr, "u16", left) == 0) {
    if (!IS_VALID_USMALLINT(result)) {
      smlBuildInvalidDataMsg(msg, "unsigned small int out of rang[0,65535]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_USMALLINT;
    kvVal->u = result;
  } else if (left == 2 && strncasecmp(endptr, "i8", left) == 0) {
    if (!IS_VALID_TINYINT(result)) {
      smlBuildInvalidDataMsg(msg, "tiny int out of range[-128,127]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_TINYINT;
    kvVal->i = result;
  } else if (left == 2 && strncasecmp(endptr, "u8", left) == 0) {
    if (!IS_VALID_UTINYINT(result)) {
      smlBuildInvalidDataMsg(msg, "unsigned tiny int out of range[0,255]", pVal);
      return false;
    }
    kvVal->type = TSDB_DATA_TYPE_UTINYINT;
    kvVal->u = result;
  } else {
    smlBuildInvalidDataMsg(msg, "invalid data", pVal);
    return false;
  }
  return true;
}

static bool smlParseBool(SSmlKv *kvVal) {
  const char *pVal = kvVal->value;
  int32_t     len = kvVal->length;
  if ((len == 1) && (pVal[0] == 't' || pVal[0] == 'T')) {
    kvVal->i = true;
    return true;
  }

  if ((len == 1) && (pVal[0] == 'f' || pVal[0] == 'F')) {
    kvVal->i = false;
    return true;
  }

  if ((len == 4) && !strncasecmp(pVal, "true", len)) {
    kvVal->i = true;
    return true;
  }
  if ((len == 5) && !strncasecmp(pVal, "false", len)) {
    kvVal->i = false;
    return true;
  }
  return false;
}

static bool smlIsBinary(const char *pVal, uint16_t len) {
  // binary: "abc"
  if (len < 2) {
    return false;
  }
  if (pVal[0] == '"' && pVal[len - 1] == '"') {
    return true;
  }
  return false;
}

static bool smlIsNchar(const char *pVal, uint16_t len) {
  // nchar: L"abc"
  if (len < 3) {
    return false;
  }
  if ((pVal[0] == 'l' || pVal[0] == 'L') && pVal[1] == '"' && pVal[len - 1] == '"') {
    return true;
  }
  return false;
}

static int64_t smlGetTimeValue(const char *value, int32_t len, int8_t type) {
  char   *endPtr = NULL;
  int64_t tsInt64 = taosStr2Int64(value, &endPtr, 10);
  if (value + len != endPtr) {
    return -1;
  }
  double ts = tsInt64;
  switch (type) {
    case TSDB_TIME_PRECISION_HOURS:
      ts *= NANOSECOND_PER_HOUR;
      tsInt64 *= NANOSECOND_PER_HOUR;
      break;
    case TSDB_TIME_PRECISION_MINUTES:
      ts *= NANOSECOND_PER_MINUTE;
      tsInt64 *= NANOSECOND_PER_MINUTE;
      break;
    case TSDB_TIME_PRECISION_SECONDS:
      ts *= NANOSECOND_PER_SEC;
      tsInt64 *= NANOSECOND_PER_SEC;
      break;
    case TSDB_TIME_PRECISION_MILLI:
      ts *= NANOSECOND_PER_MSEC;
      tsInt64 *= NANOSECOND_PER_MSEC;
      break;
    case TSDB_TIME_PRECISION_MICRO:
      ts *= NANOSECOND_PER_USEC;
      tsInt64 *= NANOSECOND_PER_USEC;
      break;
    case TSDB_TIME_PRECISION_NANO:
      break;
    default:
      ASSERT(0);
  }
  if (ts >= (double)INT64_MAX || ts < 0) {
    return -1;
  }

  return tsInt64;
}

static int8_t smlGetTsTypeByLen(int32_t len) {
  if (len == TSDB_TIME_PRECISION_SEC_DIGITS) {
    return TSDB_TIME_PRECISION_SECONDS;
  } else if (len == TSDB_TIME_PRECISION_MILLI_DIGITS) {
    return TSDB_TIME_PRECISION_MILLI;
  } else {
    return -1;
  }
}

static int8_t smlGetTsTypeByPrecision(int8_t precision) {
  switch (precision) {
    case TSDB_SML_TIMESTAMP_HOURS:
      return TSDB_TIME_PRECISION_HOURS;
    case TSDB_SML_TIMESTAMP_MILLI_SECONDS:
      return TSDB_TIME_PRECISION_MILLI;
    case TSDB_SML_TIMESTAMP_NANO_SECONDS:
    case TSDB_SML_TIMESTAMP_NOT_CONFIGURED:
      return TSDB_TIME_PRECISION_NANO;
    case TSDB_SML_TIMESTAMP_MICRO_SECONDS:
      return TSDB_TIME_PRECISION_MICRO;
    case TSDB_SML_TIMESTAMP_SECONDS:
      return TSDB_TIME_PRECISION_SECONDS;
    case TSDB_SML_TIMESTAMP_MINUTES:
      return TSDB_TIME_PRECISION_MINUTES;
    default:
      return -1;
  }
}

static int64_t smlParseInfluxTime(SSmlHandle *info, const char *data, int32_t len) {
  if (len == 0 || (len == 1 && data[0] == '0')) {
    return taosGetTimestampNs();
  }

  int8_t tsType = smlGetTsTypeByPrecision(info->precision);
  if (tsType == -1) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp precision", NULL);
    return -1;
  }

  int64_t ts = smlGetTimeValue(data, len, tsType);
  if (ts == -1) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

static int64_t smlParseOpenTsdbTime(SSmlHandle *info, const char *data, int32_t len) {
  if (!data) {
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp can not be null", NULL);
    return -1;
  }
  if (len == 1 && data[0] == '0') {
    return taosGetTimestampNs();
  }
  int8_t tsType = smlGetTsTypeByLen(len);
  if (tsType == -1) {
    smlBuildInvalidDataMsg(&info->msgBuf,
                           "timestamp precision can only be seconds(10 digits) or milli seconds(13 digits)", data);
    return -1;
  }
  int64_t ts = smlGetTimeValue(data, len, tsType);
  if (ts == -1) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

static int32_t smlParseTS(SSmlHandle *info, const char *data, int32_t len, SArray *cols) {
  int64_t ts = 0;
  if (info->protocol == TSDB_SML_LINE_PROTOCOL) {
    //    uError("SML:data:%s,len:%d", data, len);
    ts = smlParseInfluxTime(info, data, len);
  } else if (info->protocol == TSDB_SML_TELNET_PROTOCOL) {
    ts = smlParseOpenTsdbTime(info, data, len);
  } else {
    ASSERT(0);
  }
  uDebug("SML:0x%" PRIx64 " smlParseTS:%" PRId64, info->id, ts);

  if (ts <= 0) {
    uError("SML:0x%" PRIx64 " smlParseTS error:%" PRId64, info->id, ts);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }

  // add ts to
  SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
  if (!kv) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  kv->key = TS;
  kv->keyLen = TS_LEN;
  kv->i = ts;
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
  kv->length = (int16_t)tDataTypes[kv->type].bytes;
  taosArrayPush(cols, &kv);

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseValue(SSmlKv *pVal, SSmlMsgBuf *msg) {
  // binary
  if (smlIsBinary(pVal->value, pVal->length)) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
    pVal->length -= BINARY_ADD_LEN;
    if (pVal->length > TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) {
      return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
    }
    pVal->value += (BINARY_ADD_LEN - 1);
    return TSDB_CODE_SUCCESS;
  }
  // nchar
  if (smlIsNchar(pVal->value, pVal->length)) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
    pVal->length -= NCHAR_ADD_LEN;
    if (pVal->length > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
      return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
    }
    pVal->value += (NCHAR_ADD_LEN - 1);
    return TSDB_CODE_SUCCESS;
  }

  // bool
  if (smlParseBool(pVal)) {
    pVal->type = TSDB_DATA_TYPE_BOOL;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return TSDB_CODE_SUCCESS;
  }
  // number
  if (smlParseNumber(pVal, msg)) {
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_TSC_INVALID_VALUE;
}

static int32_t smlParseInfluxString(const char *sql, const char *sqlEnd, SSmlLineInfo *elements, SSmlMsgBuf *msg) {
  if (!sql) return TSDB_CODE_SML_INVALID_DATA;
  JUMP_SPACE(sql, sqlEnd)
  if (*sql == COMMA) return TSDB_CODE_SML_INVALID_DATA;
  elements->measure = sql;

  // parse measure
  while (sql < sqlEnd) {
    if ((sql != elements->measure) && IS_SLASH_LETTER(sql)) {
      MOVE_FORWARD_ONE(sql, sqlEnd - sql);
      sqlEnd--;
      continue;
    }
    if (IS_COMMA(sql)) {
      break;
    }

    if (IS_SPACE(sql)) {
      break;
    }
    sql++;
  }
  elements->measureLen = sql - elements->measure;
  if (IS_INVALID_TABLE_LEN(elements->measureLen)) {
    smlBuildInvalidDataMsg(msg, "measure is empty or too large than 192", NULL);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  // parse tag
  if (*sql == SPACE) {
    elements->tagsLen = 0;
  } else {
    if (*sql == COMMA) sql++;
    elements->tags = sql;
    while (sql < sqlEnd) {
      if (IS_SPACE(sql)) {
        break;
      }
      sql++;
    }
    elements->tagsLen = sql - elements->tags;
  }
  elements->measureTagsLen = sql - elements->measure;

  // parse cols
  JUMP_SPACE(sql, sqlEnd)
  elements->cols = sql;
  bool isInQuote = false;
  while (sql < sqlEnd) {
    if (IS_QUOTE(sql)) {
      isInQuote = !isInQuote;
    }
    if (!isInQuote && IS_SPACE(sql)) {
      break;
    }
    sql++;
  }
  if (isInQuote) {
    smlBuildInvalidDataMsg(msg, "only one quote", elements->cols);
    return TSDB_CODE_SML_INVALID_DATA;
  }
  elements->colsLen = sql - elements->cols;
  if (elements->colsLen == 0) {
    smlBuildInvalidDataMsg(msg, "cols is empty", NULL);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  // parse timestamp
  JUMP_SPACE(sql, sqlEnd)
  elements->timestamp = sql;
  while (sql < sqlEnd) {
    if (isspace(*sql)) {
      break;
    }
    sql++;
  }
  elements->timestampLen = sql - elements->timestamp;

  return TSDB_CODE_SUCCESS;
}

static void smlParseTelnetElement(const char **sql, const char *sqlEnd, const char **data, int32_t *len) {
  while (*sql < sqlEnd) {
    if (**sql != SPACE && !(*data)) {
      *data = *sql;
    } else if (**sql == SPACE && *data) {
      *len = *sql - *data;
      break;
    }
    (*sql)++;
  }
}

static int32_t smlParseTelnetTags(const char *data, const char *sqlEnd, SArray *cols, char *childTableName,
                                  SHashObj *dumplicateKey, SSmlMsgBuf *msg) {
  if (!cols) return TSDB_CODE_OUT_OF_MEMORY;
  const char *sql = data;
  size_t      childTableNameLen = strlen(tsSmlChildTableName);
  while (sql < sqlEnd) {
    JUMP_SPACE(sql, sqlEnd)
    if (*sql == '\0') break;

    const char *key = sql;
    int32_t     keyLen = 0;

    // parse key
    while (sql < sqlEnd) {
      if (*sql == SPACE) {
        smlBuildInvalidDataMsg(msg, "invalid data", sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      if (*sql == EQUAL) {
        keyLen = sql - key;
        sql++;
        break;
      }
      sql++;
    }

    if (IS_INVALID_COL_LEN(keyLen)) {
      smlBuildInvalidDataMsg(msg, "invalid key or key is too long than 64", key);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }
    if (smlCheckDuplicateKey(key, keyLen, dumplicateKey)) {
      smlBuildInvalidDataMsg(msg, "dumplicate key", key);
      return TSDB_CODE_TSC_DUP_NAMES;
    }

    // parse value
    const char *value = sql;
    int32_t     valueLen = 0;
    while (sql < sqlEnd) {
      // parse value
      if (*sql == SPACE) {
        break;
      }
      if (*sql == EQUAL) {
        smlBuildInvalidDataMsg(msg, "invalid data", sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      sql++;
    }
    valueLen = sql - value;

    if (valueLen == 0) {
      smlBuildInvalidDataMsg(msg, "invalid value", value);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }

    // handle child table name
    if (childTableNameLen != 0 && strncmp(key, tsSmlChildTableName, keyLen) == 0) {
      memset(childTableName, 0, TSDB_TABLE_NAME_LEN);
      strncpy(childTableName, value, (valueLen < TSDB_TABLE_NAME_LEN ? valueLen : TSDB_TABLE_NAME_LEN));
      continue;
    }

    if (valueLen > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
      return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
    }

    // add kv to SSmlKv
    SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
    if (!kv) return TSDB_CODE_OUT_OF_MEMORY;
    kv->key = key;
    kv->keyLen = keyLen;
    kv->value = value;
    kv->length = valueLen;
    kv->type = TSDB_DATA_TYPE_NCHAR;

    taosArrayPush(cols, &kv);
  }

  return TSDB_CODE_SUCCESS;
}

// format: <metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
static int32_t smlParseTelnetString(SSmlHandle *info, const char *sql, const char *sqlEnd, SSmlTableInfo *tinfo,
                                    SArray *cols) {
  if (!sql) return TSDB_CODE_SML_INVALID_DATA;

  // parse metric
  smlParseTelnetElement(&sql, sqlEnd, &tinfo->sTableName, &tinfo->sTableNameLen);
  if (!(tinfo->sTableName) || IS_INVALID_TABLE_LEN(tinfo->sTableNameLen)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", sql);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  // parse timestamp
  const char *timestamp = NULL;
  int32_t     tLen = 0;
  smlParseTelnetElement(&sql, sqlEnd, &timestamp, &tLen);
  if (!timestamp || tLen == 0) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  int32_t ret = smlParseTS(info, timestamp, tLen, cols);
  if (ret != TSDB_CODE_SUCCESS) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", sql);
    return ret;
  }

  // parse value
  const char *value = NULL;
  int32_t     valueLen = 0;
  smlParseTelnetElement(&sql, sqlEnd, &value, &valueLen);
  if (!value || valueLen == 0) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", sql);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
  if (!kv) return TSDB_CODE_OUT_OF_MEMORY;
  taosArrayPush(cols, &kv);
  kv->key = VALUE;
  kv->keyLen = VALUE_LEN;
  kv->value = value;
  kv->length = valueLen;
  if ((ret = smlParseValue(kv, &info->msgBuf)) != TSDB_CODE_SUCCESS) {
    return ret;
  }

  // parse tags
  ret = smlParseTelnetTags(sql, sqlEnd, tinfo->tags, tinfo->childTableName, info->dumplicateKey, &info->msgBuf);
  if (ret != TSDB_CODE_SUCCESS) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", sql);
    return ret;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseCols(const char *data, int32_t len, SArray *cols, char *childTableName, bool isTag,
                            SHashObj *dumplicateKey, SSmlMsgBuf *msg) {
  if (len == 0) {
    return TSDB_CODE_SUCCESS;
  }

  size_t      childTableNameLen = strlen(tsSmlChildTableName);
  const char *sql = data;
  while (sql < data + len) {
    const char *key = sql;
    int32_t     keyLen = 0;

    while (sql < data + len) {
      // parse key
      if (IS_COMMA(sql)) {
        smlBuildInvalidDataMsg(msg, "invalid data", sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      if (IS_EQUAL(sql)) {
        keyLen = sql - key;
        sql++;
        break;
      }
      sql++;
    }

    if (IS_INVALID_COL_LEN(keyLen)) {
      smlBuildInvalidDataMsg(msg, "invalid key or key is too long than 64", key);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }
    if (smlCheckDuplicateKey(key, keyLen, dumplicateKey)) {
      smlBuildInvalidDataMsg(msg, "dumplicate key", key);
      return TSDB_CODE_TSC_DUP_NAMES;
    }

    // parse value
    const char *value = sql;
    int32_t     valueLen = 0;
    bool        isInQuote = false;
    while (sql < data + len) {
      // parse value
      if (!isTag && IS_QUOTE(sql)) {
        isInQuote = !isInQuote;
        sql++;
        continue;
      }
      if (!isInQuote && IS_COMMA(sql)) {
        break;
      }
      if (!isInQuote && IS_EQUAL(sql)) {
        smlBuildInvalidDataMsg(msg, "invalid data", sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      sql++;
    }
    valueLen = sql - value;
    sql++;

    if (isInQuote) {
      smlBuildInvalidDataMsg(msg, "only one quote", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    if (valueLen == 0) {
      smlBuildInvalidDataMsg(msg, "invalid value", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    PROCESS_SLASH(key, keyLen)
    PROCESS_SLASH(value, valueLen)

    // handle child table name
    if (childTableName && childTableNameLen != 0 && strncmp(key, tsSmlChildTableName, keyLen) == 0) {
      memset(childTableName, 0, TSDB_TABLE_NAME_LEN);
      strncpy(childTableName, value, (valueLen < TSDB_TABLE_NAME_LEN ? valueLen : TSDB_TABLE_NAME_LEN));
      continue;
    }

    // add kv to SSmlKv
    SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
    if (!kv) return TSDB_CODE_OUT_OF_MEMORY;
    if (cols) taosArrayPush(cols, &kv);

    kv->key = key;
    kv->keyLen = keyLen;
    kv->value = value;
    kv->length = valueLen;
    if (isTag) {
      if (valueLen > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
      }
      kv->type = TSDB_DATA_TYPE_NCHAR;
    } else {
      int32_t ret = smlParseValue(kv, msg);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlUpdateMeta(SHashObj *metaHash, SArray *metaArray, SArray *cols, SSmlMsgBuf *msg) {
  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, i);

    int16_t *index = (int16_t *)taosHashGet(metaHash, kv->key, kv->keyLen);
    if (index) {
      SSmlKv **value = (SSmlKv **)taosArrayGet(metaArray, *index);
      if (kv->type != (*value)->type) {
        smlBuildInvalidDataMsg(msg, "the type is not the same like before", kv->key);
        return TSDB_CODE_SML_NOT_SAME_TYPE;
      } else {
        if (IS_VAR_DATA_TYPE(kv->type)) {  // update string len, if bigger
          if (kv->length > (*value)->length) {
            *value = kv;
          }
        }
      }
    } else {
      size_t tmp = taosArrayGetSize(metaArray);
      ASSERT(tmp <= INT16_MAX);
      int16_t size = tmp;
      taosArrayPush(metaArray, &kv);
      taosHashPut(metaHash, kv->key, kv->keyLen, &size, SHORT_BYTES);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void smlInsertMeta(SHashObj *metaHash, SArray *metaArray, SArray *cols) {
  for (int16_t i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, i);
    taosArrayPush(metaArray, &kv);
    taosHashPut(metaHash, kv->key, kv->keyLen, &i, SHORT_BYTES);
  }
}

static SSmlTableInfo *smlBuildTableInfo() {
  SSmlTableInfo *tag = (SSmlTableInfo *)taosMemoryCalloc(sizeof(SSmlTableInfo), 1);
  if (!tag) {
    return NULL;
  }

  tag->cols = taosArrayInit(16, POINTER_BYTES);
  if (tag->cols == NULL) {
    uError("SML:smlBuildTableInfo failed to allocate memory");
    goto cleanup;
  }

  tag->tags = taosArrayInit(16, POINTER_BYTES);
  if (tag->tags == NULL) {
    uError("SML:smlBuildTableInfo failed to allocate memory");
    goto cleanup;
  }
  return tag;

cleanup:
  taosMemoryFree(tag);
  return NULL;
}

static void smlDestroyTableInfo(SSmlHandle *info, SSmlTableInfo *tag) {
  if (info->dataFormat) {
    for (size_t i = 0; i < taosArrayGetSize(tag->cols); i++) {
      SArray *kvArray = (SArray *)taosArrayGetP(tag->cols, i);
      for (int j = 0; j < taosArrayGetSize(kvArray); ++j) {
        SSmlKv *p = (SSmlKv *)taosArrayGetP(kvArray, j);
        taosMemoryFree(p);
      }
      taosArrayDestroy(kvArray);
    }
  } else {
    for (size_t i = 0; i < taosArrayGetSize(tag->cols); i++) {
      SHashObj *kvHash = (SHashObj *)taosArrayGetP(tag->cols, i);
      void    **p1 = (void **)taosHashIterate(kvHash, NULL);
      while (p1) {
        taosMemoryFree(*p1);
        p1 = (void **)taosHashIterate(kvHash, p1);
      }
      taosHashCleanup(kvHash);
    }
  }
  for (size_t i = 0; i < taosArrayGetSize(tag->tags); i++) {
    SSmlKv *p = (SSmlKv *)taosArrayGetP(tag->tags, i);
    taosMemoryFree(p);
  }
  taosArrayDestroy(tag->cols);
  taosArrayDestroy(tag->tags);
  taosMemoryFree(tag);
}

static int32_t smlKvTimeArrayCompare(const void *key1, const void *key2) {
  SArray *s1 = *(SArray **)key1;
  SArray *s2 = *(SArray **)key2;
  SSmlKv *kv1 = (SSmlKv *)taosArrayGetP(s1, 0);
  SSmlKv *kv2 = (SSmlKv *)taosArrayGetP(s2, 0);
  ASSERT(kv1->type == TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT(kv2->type == TSDB_DATA_TYPE_TIMESTAMP);
  if (kv1->i < kv2->i) {
    return -1;
  } else if (kv1->i > kv2->i) {
    return 1;
  } else {
    return 0;
  }
}

static int32_t smlKvTimeHashCompare(const void *key1, const void *key2) {
  SHashObj *s1 = *(SHashObj **)key1;
  SHashObj *s2 = *(SHashObj **)key2;
  SSmlKv  **kv1pp = (SSmlKv **)taosHashGet(s1, TS, TS_LEN);
  SSmlKv  **kv2pp = (SSmlKv **)taosHashGet(s2, TS, TS_LEN);
  if (!kv1pp || !kv2pp) {
    uError("smlKvTimeHashCompare kv is null");
    return -1;
  }
  SSmlKv *kv1 = *kv1pp;
  SSmlKv *kv2 = *kv2pp;
  if (!kv1 || kv1->type != TSDB_DATA_TYPE_TIMESTAMP) {
    uError("smlKvTimeHashCompare kv1");
    return -1;
  }
  if (!kv2 || kv2->type != TSDB_DATA_TYPE_TIMESTAMP) {
    uError("smlKvTimeHashCompare kv2");
    return -1;
  }
  if (kv1->i < kv2->i) {
    return -1;
  } else if (kv1->i > kv2->i) {
    return 1;
  } else {
    return 0;
  }
}

static int32_t smlDealCols(SSmlTableInfo *oneTable, bool dataFormat, SArray *cols) {
  if (dataFormat) {
    void *p = taosArraySearch(oneTable->cols, &cols, smlKvTimeArrayCompare, TD_GT);
    if (p == NULL) {
      taosArrayPush(oneTable->cols, &cols);
    } else {
      taosArrayInsert(oneTable->cols, TARRAY_ELEM_IDX(oneTable->cols, p), &cols);
    }
    return TSDB_CODE_SUCCESS;
  }

  SHashObj *kvHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (!kvHash) {
    uError("SML:smlDealCols failed to allocate memory");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
    SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, i);
    taosHashPut(kvHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
  }

  void *p = taosArraySearch(oneTable->cols, &kvHash, smlKvTimeHashCompare, TD_GT);
  if (p == NULL) {
    taosArrayPush(oneTable->cols, &kvHash);
  } else {
    taosArrayInsert(oneTable->cols, TARRAY_ELEM_IDX(oneTable->cols, p), &kvHash);
  }
  return TSDB_CODE_SUCCESS;
}

static SSmlSTableMeta *smlBuildSTableMeta() {
  SSmlSTableMeta *meta = (SSmlSTableMeta *)taosMemoryCalloc(sizeof(SSmlSTableMeta), 1);
  if (!meta) {
    return NULL;
  }
  meta->tagHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (meta->tagHash == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }

  meta->colHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (meta->colHash == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }

  meta->tags = taosArrayInit(32, POINTER_BYTES);
  if (meta->tags == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }

  meta->cols = taosArrayInit(32, POINTER_BYTES);
  if (meta->cols == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }
  return meta;

cleanup:
  taosMemoryFree(meta);
  return NULL;
}

static void smlDestroySTableMeta(SSmlSTableMeta *meta) {
  taosHashCleanup(meta->tagHash);
  taosHashCleanup(meta->colHash);
  taosArrayDestroy(meta->tags);
  taosArrayDestroy(meta->cols);
  taosMemoryFree(meta->tableMeta);
  taosMemoryFree(meta);
}

static void smlDestroyCols(SArray *cols) {
  if (!cols) return;
  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    void *kv = taosArrayGetP(cols, i);
    taosMemoryFree(kv);
  }
}

static void smlDestroyInfo(SSmlHandle *info) {
  if (!info) return;
  qDestroyQuery(info->pQuery);
  smlDestroyHandle(info->exec);

  // destroy info->childTables
  void **p1 = (void **)taosHashIterate(info->childTables, NULL);
  while (p1) {
    smlDestroyTableInfo(info, (SSmlTableInfo *)(*p1));
    p1 = (void **)taosHashIterate(info->childTables, p1);
  }
  taosHashCleanup(info->childTables);

  // destroy info->superTables
  p1 = (void **)taosHashIterate(info->superTables, NULL);
  while (p1) {
    smlDestroySTableMeta((SSmlSTableMeta *)(*p1));
    p1 = (void **)taosHashIterate(info->superTables, p1);
  }
  taosHashCleanup(info->superTables);

  // destroy info->pVgHash
  taosHashCleanup(info->pVgHash);
  taosHashCleanup(info->dumplicateKey);
  if (!info->dataFormat) {
    taosArrayDestroy(info->colsContainer);
  }

  cJSON_Delete(info->root);
  taosMemoryFreeClear(info);
}

static SSmlHandle *smlBuildSmlInfo(STscObj *pTscObj, SRequestObj *request, SMLProtocolType protocol, int8_t precision) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SSmlHandle *info = (SSmlHandle *)taosMemoryCalloc(1, sizeof(SSmlHandle));
  if (NULL == info) {
    return NULL;
  }
  info->id = smlGenId();

  info->pQuery = (SQuery *)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == info->pQuery) {
    uError("SML:0x%" PRIx64 " create info->pQuery error", info->id);
    goto cleanup;
  }
  info->pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  info->pQuery->haveResultSet = false;
  info->pQuery->msgType = TDMT_VND_SUBMIT;
  info->pQuery->pRoot = (SNode *)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if (NULL == info->pQuery->pRoot) {
    uError("SML:0x%" PRIx64 " create info->pQuery->pRoot error", info->id);
    goto cleanup;
  }

  if (pTscObj) {
    info->taos = pTscObj;
    code = catalogGetHandle(info->taos->pAppInfo->clusterId, &info->pCatalog);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " get catalog error %d", info->id, code);
      goto cleanup;
    }
  }

  info->precision = precision;
  info->protocol = protocol;
  if (protocol == TSDB_SML_LINE_PROTOCOL) {
    info->dataFormat = tsSmlDataFormat;
  } else {
    info->dataFormat = true;
  }

  if (request) {
    info->pRequest = request;
    info->msgBuf.buf = info->pRequest->msgBuf;
    info->msgBuf.len = ERROR_MSG_BUF_DEFAULT_SIZE;
    info->pRequest->stmtType = info->pQuery->pRoot->type;
  }

  info->exec = smlInitHandle(info->pQuery);
  info->childTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  info->superTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  info->pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  info->dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (!info->dataFormat) {
    info->colsContainer = taosArrayInit(32, POINTER_BYTES);
    if (NULL == info->colsContainer) {
      uError("SML:0x%" PRIx64 " create info failed", info->id);
      goto cleanup;
    }
  }
  if (NULL == info->exec || NULL == info->childTables || NULL == info->superTables || NULL == info->pVgHash ||
      NULL == info->dumplicateKey) {
    uError("SML:0x%" PRIx64 " create info failed", info->id);
    goto cleanup;
  }

  return info;
cleanup:
  smlDestroyInfo(info);
  return NULL;
}

/************* TSDB_SML_JSON_PROTOCOL function start **************/
static int32_t smlParseMetricFromJSON(SSmlHandle *info, cJSON *root, SSmlTableInfo *tinfo) {
  cJSON *metric = cJSON_GetObjectItem(root, "metric");
  if (!cJSON_IsString(metric)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  tinfo->sTableNameLen = strlen(metric->valuestring);
  if (IS_INVALID_TABLE_LEN(tinfo->sTableNameLen)) {
    uError("OTD:0x%" PRIx64 " Metric lenght is 0 or large than 192", info->id);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  tinfo->sTableName = metric->valuestring;
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseTSFromJSONObj(SSmlHandle *info, cJSON *root, int64_t *tsVal) {
  int32_t size = cJSON_GetArraySize(root);
  if (size != OTD_JSON_SUB_FIELDS_NUM) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *value = cJSON_GetObjectItem(root, "value");
  if (!cJSON_IsNumber(value)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *type = cJSON_GetObjectItem(root, "type");
  if (!cJSON_IsString(type)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  double timeDouble = value->valuedouble;
  if (smlDoubleToInt64OverFlow(timeDouble)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }

  if (timeDouble == 0) {
    *tsVal = taosGetTimestampNs();
    return TSDB_CODE_SUCCESS;
  }

  if (timeDouble < 0) {
    return TSDB_CODE_INVALID_TIMESTAMP;
  }

  *tsVal = timeDouble;
  size_t typeLen = strlen(type->valuestring);
  if (typeLen == 1 && (type->valuestring[0] == 's' || type->valuestring[0] == 'S')) {
    // seconds
    *tsVal = *tsVal * NANOSECOND_PER_SEC;
    timeDouble = timeDouble * NANOSECOND_PER_SEC;
    if (smlDoubleToInt64OverFlow(timeDouble)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
      return TSDB_CODE_INVALID_TIMESTAMP;
    }
  } else if (typeLen == 2 && (type->valuestring[1] == 's' || type->valuestring[1] == 'S')) {
    switch (type->valuestring[0]) {
      case 'm':
      case 'M':
        // milliseconds
        *tsVal = *tsVal * NANOSECOND_PER_MSEC;
        timeDouble = timeDouble * NANOSECOND_PER_MSEC;
        if (smlDoubleToInt64OverFlow(timeDouble)) {
          smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
          return TSDB_CODE_INVALID_TIMESTAMP;
        }
        break;
      case 'u':
      case 'U':
        // microseconds
        *tsVal = *tsVal * NANOSECOND_PER_USEC;
        timeDouble = timeDouble * NANOSECOND_PER_USEC;
        if (smlDoubleToInt64OverFlow(timeDouble)) {
          smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
          return TSDB_CODE_INVALID_TIMESTAMP;
        }
        break;
      case 'n':
      case 'N':
        break;
      default:
        return TSDB_CODE_TSC_INVALID_JSON;
    }
  } else {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  return TSDB_CODE_SUCCESS;
}

static uint8_t smlGetTimestampLen(int64_t num) {
  uint8_t len = 0;
  while ((num /= 10) != 0) {
    len++;
  }
  len++;
  return len;
}

static int32_t smlParseTSFromJSON(SSmlHandle *info, cJSON *root, SArray *cols) {
  // Timestamp must be the first KV to parse
  int64_t tsVal = 0;

  cJSON *timestamp = cJSON_GetObjectItem(root, "timestamp");
  if (cJSON_IsNumber(timestamp)) {
    // timestamp value 0 indicates current system time
    double timeDouble = timestamp->valuedouble;
    if (smlDoubleToInt64OverFlow(timeDouble)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
      return TSDB_CODE_INVALID_TIMESTAMP;
    }

    if (timeDouble < 0) {
      return TSDB_CODE_INVALID_TIMESTAMP;
    }

    uint8_t tsLen = smlGetTimestampLen((int64_t)timeDouble);
    tsVal = (int64_t)timeDouble;
    if (tsLen == TSDB_TIME_PRECISION_SEC_DIGITS) {
      tsVal = tsVal * NANOSECOND_PER_SEC;
      timeDouble = timeDouble * NANOSECOND_PER_SEC;
      if (smlDoubleToInt64OverFlow(timeDouble)) {
        smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
        return TSDB_CODE_INVALID_TIMESTAMP;
      }
    } else if (tsLen == TSDB_TIME_PRECISION_MILLI_DIGITS) {
      tsVal = tsVal * NANOSECOND_PER_MSEC;
      timeDouble = timeDouble * NANOSECOND_PER_MSEC;
      if (smlDoubleToInt64OverFlow(timeDouble)) {
        smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
        return TSDB_CODE_INVALID_TIMESTAMP;
      }
    } else if (timeDouble == 0) {
      tsVal = taosGetTimestampNs();
    } else {
      return TSDB_CODE_INVALID_TIMESTAMP;
    }
  } else if (cJSON_IsObject(timestamp)) {
    int32_t ret = smlParseTSFromJSONObj(info, timestamp, &tsVal);
    if (ret != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " Failed to parse timestamp from JSON Obj", info->id);
      return ret;
    }
  } else {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  // add ts to
  SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
  if (!kv) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  kv->key = TS;
  kv->keyLen = TS_LEN;
  kv->i = tsVal;
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
  kv->length = (int16_t)tDataTypes[kv->type].bytes;
  taosArrayPush(cols, &kv);
  return TSDB_CODE_SUCCESS;
}

static int32_t smlConvertJSONBool(SSmlKv *pVal, char *typeStr, cJSON *value) {
  if (strcasecmp(typeStr, "bool") != 0) {
    uError("OTD:invalid type(%s) for JSON Bool", typeStr);
    return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }
  pVal->type = TSDB_DATA_TYPE_BOOL;
  pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
  pVal->i = value->valueint;

  return TSDB_CODE_SUCCESS;
}

static int32_t smlConvertJSONNumber(SSmlKv *pVal, char *typeStr, cJSON *value) {
  // tinyint
  if (strcasecmp(typeStr, "i8") == 0 || strcasecmp(typeStr, "tinyint") == 0) {
    if (!IS_VALID_TINYINT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(tinyint)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_TINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  // smallint
  if (strcasecmp(typeStr, "i16") == 0 || strcasecmp(typeStr, "smallint") == 0) {
    if (!IS_VALID_SMALLINT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(smallint)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_SMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  // int
  if (strcasecmp(typeStr, "i32") == 0 || strcasecmp(typeStr, "int") == 0) {
    if (!IS_VALID_INT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(int)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_INT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->i = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  // bigint
  if (strcasecmp(typeStr, "i64") == 0 || strcasecmp(typeStr, "bigint") == 0) {
    pVal->type = TSDB_DATA_TYPE_BIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    if (value->valuedouble >= (double)INT64_MAX) {
      pVal->i = INT64_MAX;
    } else if (value->valuedouble <= (double)INT64_MIN) {
      pVal->i = INT64_MIN;
    } else {
      pVal->i = value->valuedouble;
    }
    return TSDB_CODE_SUCCESS;
  }
  // float
  if (strcasecmp(typeStr, "f32") == 0 || strcasecmp(typeStr, "float") == 0) {
    if (!IS_VALID_FLOAT(value->valuedouble)) {
      uError("OTD:JSON value(%f) cannot fit in type(float)", value->valuedouble);
      return TSDB_CODE_TSC_VALUE_OUT_OF_RANGE;
    }
    pVal->type = TSDB_DATA_TYPE_FLOAT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->f = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }
  // double
  if (strcasecmp(typeStr, "f64") == 0 || strcasecmp(typeStr, "double") == 0) {
    pVal->type = TSDB_DATA_TYPE_DOUBLE;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->d = value->valuedouble;
    return TSDB_CODE_SUCCESS;
  }

  // if reach here means type is unsupported
  uError("OTD:invalid type(%s) for JSON Number", typeStr);
  return TSDB_CODE_TSC_INVALID_JSON_TYPE;
}

static int32_t smlConvertJSONString(SSmlKv *pVal, char *typeStr, cJSON *value) {
  if (strcasecmp(typeStr, "binary") == 0) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
  } else if (strcasecmp(typeStr, "nchar") == 0) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
  } else {
    uError("OTD:invalid type(%s) for JSON String", typeStr);
    return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }
  pVal->length = (int16_t)strlen(value->valuestring);

  if (pVal->type == TSDB_DATA_TYPE_BINARY && pVal->length > TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) {
    return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
  }
  if (pVal->type == TSDB_DATA_TYPE_NCHAR &&
      pVal->length > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
    return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
  }

  pVal->value = value->valuestring;
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseValueFromJSONObj(cJSON *root, SSmlKv *kv) {
  int32_t ret = TSDB_CODE_SUCCESS;
  int32_t size = cJSON_GetArraySize(root);

  if (size != OTD_JSON_SUB_FIELDS_NUM) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *value = cJSON_GetObjectItem(root, "value");
  if (value == NULL) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  cJSON *type = cJSON_GetObjectItem(root, "type");
  if (!cJSON_IsString(type)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  switch (value->type) {
    case cJSON_True:
    case cJSON_False: {
      ret = smlConvertJSONBool(kv, type->valuestring, value);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    case cJSON_Number: {
      ret = smlConvertJSONNumber(kv, type->valuestring, value);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    case cJSON_String: {
      ret = smlConvertJSONString(kv, type->valuestring, value);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      break;
    }
    default:
      return TSDB_CODE_TSC_INVALID_JSON_TYPE;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseValueFromJSON(cJSON *root, SSmlKv *kv) {
  switch (root->type) {
    case cJSON_True:
    case cJSON_False: {
      kv->type = TSDB_DATA_TYPE_BOOL;
      kv->length = (int16_t)tDataTypes[kv->type].bytes;
      kv->i = root->valueint;
      break;
    }
    case cJSON_Number: {
      kv->type = TSDB_DATA_TYPE_DOUBLE;
      kv->length = (int16_t)tDataTypes[kv->type].bytes;
      kv->d = root->valuedouble;
      break;
    }
    case cJSON_String: {
      /* set default JSON type to binary/nchar according to
       * user configured parameter tsDefaultJSONStrType
       */

      char *tsDefaultJSONStrType = "nchar";  // todo
      smlConvertJSONString(kv, tsDefaultJSONStrType, root);
      break;
    }
    case cJSON_Object: {
      int32_t ret = smlParseValueFromJSONObj(root, kv);
      if (ret != TSDB_CODE_SUCCESS) {
        uError("OTD:Failed to parse value from JSON Obj");
        return ret;
      }
      break;
    }
    default:
      return TSDB_CODE_TSC_INVALID_JSON;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseColsFromJSON(cJSON *root, SArray *cols) {
  if (!cols) return TSDB_CODE_OUT_OF_MEMORY;
  cJSON *metricVal = cJSON_GetObjectItem(root, "value");
  if (metricVal == NULL) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
  if (!kv) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosArrayPush(cols, &kv);

  kv->key = VALUE;
  kv->keyLen = VALUE_LEN;
  int32_t ret = smlParseValueFromJSON(metricVal, kv);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseTagsFromJSON(cJSON *root, SArray *pKVs, char *childTableName, SHashObj *dumplicateKey,
                                    SSmlMsgBuf *msg) {
  int32_t ret = TSDB_CODE_SUCCESS;
  if (!pKVs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  cJSON *tags = cJSON_GetObjectItem(root, "tags");
  if (tags == NULL || tags->type != cJSON_Object) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  size_t  childTableNameLen = strlen(tsSmlChildTableName);
  int32_t tagNum = cJSON_GetArraySize(tags);
  for (int32_t i = 0; i < tagNum; ++i) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    if (tag == NULL) {
      return TSDB_CODE_TSC_INVALID_JSON;
    }
    size_t keyLen = strlen(tag->string);
    if (IS_INVALID_COL_LEN(keyLen)) {
      uError("OTD:Tag key length is 0 or too large than 64");
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }
    // check duplicate keys
    if (smlCheckDuplicateKey(tag->string, keyLen, dumplicateKey)) {
      return TSDB_CODE_TSC_DUP_NAMES;
    }

    // handle child table name
    if (childTableNameLen != 0 && strcmp(tag->string, tsSmlChildTableName) == 0) {
      if (!cJSON_IsString(tag)) {
        uError("OTD:ID must be JSON string");
        return TSDB_CODE_TSC_INVALID_JSON;
      }
      memset(childTableName, 0, TSDB_TABLE_NAME_LEN);
      tstrncpy(childTableName, tag->valuestring, TSDB_TABLE_NAME_LEN);
      continue;
    }

    // add kv to SSmlKv
    SSmlKv *kv = (SSmlKv *)taosMemoryCalloc(sizeof(SSmlKv), 1);
    if (!kv) return TSDB_CODE_OUT_OF_MEMORY;
    taosArrayPush(pKVs, &kv);

    // key
    kv->keyLen = keyLen;
    kv->key = tag->string;

    // value
    ret = smlParseValueFromJSON(tag, kv);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  return ret;
}

static int32_t smlParseJSONString(SSmlHandle *info, cJSON *root, SSmlTableInfo *tinfo, SArray *cols) {
  int32_t ret = TSDB_CODE_SUCCESS;

  if (!cJSON_IsObject(root)) {
    uError("OTD:0x%" PRIx64 " data point needs to be JSON object", info->id);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  int32_t size = cJSON_GetArraySize(root);
  // outmost json fields has to be exactly 4
  if (size != OTD_JSON_FIELDS_NUM) {
    uError("OTD:0x%" PRIx64 " Invalid number of JSON fields in data point %d", info->id, size);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  // Parse metric
  ret = smlParseMetricFromJSON(info, root, tinfo);
  if (ret != TSDB_CODE_SUCCESS) {
    uError("OTD:0x%" PRIx64 " Unable to parse metric from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%" PRIx64 " Parse metric from JSON payload finished", info->id);

  // Parse timestamp
  ret = smlParseTSFromJSON(info, root, cols);
  if (ret) {
    uError("OTD:0x%" PRIx64 " Unable to parse timestamp from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%" PRIx64 " Parse timestamp from JSON payload finished", info->id);

  // Parse metric value
  ret = smlParseColsFromJSON(root, cols);
  if (ret) {
    uError("OTD:0x%" PRIx64 " Unable to parse metric value from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%" PRIx64 " Parse metric value from JSON payload finished", info->id);

  // Parse tags
  ret = smlParseTagsFromJSON(root, tinfo->tags, tinfo->childTableName, info->dumplicateKey, &info->msgBuf);
  if (ret) {
    uError("OTD:0x%" PRIx64 " Unable to parse tags from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%" PRIx64 " Parse tags from JSON payload finished", info->id);

  return TSDB_CODE_SUCCESS;
}
/************* TSDB_SML_JSON_PROTOCOL function end **************/

static int32_t smlParseInfluxLine(SSmlHandle *info, const char *sql, const int len) {
  SSmlLineInfo elements = {0};
  uDebug("SML:0x%" PRIx64 " smlParseInfluxLine raw:%d, len:%d, sql:%s", info->id, info->isRawLine, len, (info->isRawLine ? "rawdata" : sql));

  int ret = smlParseInfluxString(sql, sql + len, &elements, &info->msgBuf);
  if (ret != TSDB_CODE_SUCCESS) {
    uError("SML:0x%" PRIx64 " smlParseInfluxLine failed", info->id);
    return ret;
  }

  SArray *cols = NULL;
  if (info->dataFormat) {  // if dataFormat, cols need new memory to save data
    cols = taosArrayInit(16, POINTER_BYTES);
    if (cols == NULL) {
      uError("SML:0x%" PRIx64 " smlParseInfluxLine failed to allocate memory", info->id);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {  // if dataFormat is false, cols do not need to save data, there is another new memory to save data
    cols = info->colsContainer;
  }

  ret = smlParseTS(info, elements.timestamp, elements.timestampLen, cols);
  if (ret != TSDB_CODE_SUCCESS) {
    uError("SML:0x%" PRIx64 " smlParseTS failed", info->id);
    if (info->dataFormat) taosArrayDestroy(cols);
    return ret;
  }
  ret = smlParseCols(elements.cols, elements.colsLen, cols, NULL, false, info->dumplicateKey, &info->msgBuf);
  if (ret != TSDB_CODE_SUCCESS) {
    uError("SML:0x%" PRIx64 " smlParseCols parse cloums fields failed", info->id);
    smlDestroyCols(cols);
    if (info->dataFormat) taosArrayDestroy(cols);
    return ret;
  }

  bool            hasTable = true;
  SSmlTableInfo  *tinfo = NULL;
  SSmlTableInfo **oneTable =
      (SSmlTableInfo **)taosHashGet(info->childTables, elements.measure, elements.measureTagsLen);
  if (!oneTable) {
    tinfo = smlBuildTableInfo();
    if (!tinfo) {
      smlDestroyCols(cols);
      if (info->dataFormat) taosArrayDestroy(cols);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosHashPut(info->childTables, elements.measure, elements.measureTagsLen, &tinfo, POINTER_BYTES);
    oneTable = &tinfo;
    hasTable = false;
  }

  ret = smlDealCols(*oneTable, info->dataFormat, cols);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  if (!hasTable) {
    ret = smlParseCols(elements.tags, elements.tagsLen, (*oneTable)->tags, (*oneTable)->childTableName, true,
                       info->dumplicateKey, &info->msgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlParseCols parse tag fields failed", info->id);
      return ret;
    }

    if (taosArrayGetSize((*oneTable)->tags) > TSDB_MAX_TAGS) {
      smlBuildInvalidDataMsg(&info->msgBuf, "too many tags than 128", NULL);
      return TSDB_CODE_PAR_INVALID_TAGS_NUM;
    }

    if (taosArrayGetSize(cols) + taosArrayGetSize((*oneTable)->tags) > TSDB_MAX_COLUMNS) {
      smlBuildInvalidDataMsg(&info->msgBuf, "too many columns than 4096", NULL);
      return TSDB_CODE_PAR_TOO_MANY_COLUMNS;
    }

    (*oneTable)->sTableName = elements.measure;
    (*oneTable)->sTableNameLen = elements.measureLen;
    if (strlen((*oneTable)->childTableName) == 0) {
      RandTableName rName = {(*oneTable)->tags, (*oneTable)->sTableName, (uint8_t)(*oneTable)->sTableNameLen,
                             (*oneTable)->childTableName};

      buildChildTableName(&rName);
    }
    (*oneTable)->uid = info->uid++;
  }

  SSmlSTableMeta **tableMeta = (SSmlSTableMeta **)taosHashGet(info->superTables, elements.measure, elements.measureLen);
  if (tableMeta) {  // update meta
    ret = smlUpdateMeta((*tableMeta)->colHash, (*tableMeta)->cols, cols, &info->msgBuf);
    if (!hasTable && ret == TSDB_CODE_SUCCESS) {
      ret = smlUpdateMeta((*tableMeta)->tagHash, (*tableMeta)->tags, (*oneTable)->tags, &info->msgBuf);
    }
    if (ret != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlUpdateMeta failed", info->id);
      return ret;
    }
  } else {
    SSmlSTableMeta *meta = smlBuildSTableMeta();
    smlInsertMeta(meta->tagHash, meta->tags, (*oneTable)->tags);
    smlInsertMeta(meta->colHash, meta->cols, cols);
    taosHashPut(info->superTables, elements.measure, elements.measureLen, &meta, POINTER_BYTES);
  }

  if (!info->dataFormat) {
    taosArrayClear(info->colsContainer);
  }
  taosHashClear(info->dumplicateKey);
  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseTelnetLine(SSmlHandle *info, void *data, const int len) {
  int            ret = TSDB_CODE_SUCCESS;
  SSmlTableInfo *tinfo = smlBuildTableInfo();
  if (!tinfo) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SArray *cols = taosArrayInit(16, POINTER_BYTES);
  if (cols == NULL) {
    uError("SML:0x%" PRIx64 " smlParseTelnetLine failed to allocate memory", info->id);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (info->protocol == TSDB_SML_TELNET_PROTOCOL) {
    ret = smlParseTelnetString(info, (const char *)data, (char *)data + len, tinfo, cols);
  } else if (info->protocol == TSDB_SML_JSON_PROTOCOL) {
    ret = smlParseJSONString(info, (cJSON *)data, tinfo, cols);
  } else {
    ASSERT(0);
  }
  if (ret != TSDB_CODE_SUCCESS) {
    uError("SML:0x%" PRIx64 " smlParseTelnetLine failed", info->id);
    smlDestroyTableInfo(info, tinfo);
    smlDestroyCols(cols);
    taosArrayDestroy(cols);
    return ret;
  }

  if (taosArrayGetSize(tinfo->tags) <= 0 || taosArrayGetSize(tinfo->tags) > TSDB_MAX_TAGS) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalidate tags length:[1,128]", NULL);
    smlDestroyTableInfo(info, tinfo);
    smlDestroyCols(cols);
    taosArrayDestroy(cols);
    return TSDB_CODE_PAR_INVALID_TAGS_NUM;
  }
  taosHashClear(info->dumplicateKey);

  if (strlen(tinfo->childTableName) == 0) {
    RandTableName rName = {tinfo->tags, tinfo->sTableName, (uint8_t)tinfo->sTableNameLen, tinfo->childTableName};
    buildChildTableName(&rName);
  }

  bool            hasTable = true;
  SSmlTableInfo **oneTable =
      (SSmlTableInfo **)taosHashGet(info->childTables, tinfo->childTableName, strlen(tinfo->childTableName));
  if (!oneTable) {
    taosHashPut(info->childTables, tinfo->childTableName, strlen(tinfo->childTableName), &tinfo, POINTER_BYTES);
    oneTable = &tinfo;
    tinfo->uid = info->uid++;
    hasTable = false;
  } else {
    smlDestroyTableInfo(info, tinfo);
  }

  taosArrayPush((*oneTable)->cols, &cols);
  SSmlSTableMeta **tableMeta =
      (SSmlSTableMeta **)taosHashGet(info->superTables, (*oneTable)->sTableName, (*oneTable)->sTableNameLen);
  if (tableMeta) {  // update meta
    ret = smlUpdateMeta((*tableMeta)->colHash, (*tableMeta)->cols, cols, &info->msgBuf);
    if (!hasTable && ret == TSDB_CODE_SUCCESS) {
      ret = smlUpdateMeta((*tableMeta)->tagHash, (*tableMeta)->tags, (*oneTable)->tags, &info->msgBuf);
    }
    if (ret != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlUpdateMeta failed", info->id);
      return ret;
    }
  } else {
    SSmlSTableMeta *meta = smlBuildSTableMeta();
    smlInsertMeta(meta->tagHash, meta->tags, (*oneTable)->tags);
    smlInsertMeta(meta->colHash, meta->cols, cols);
    taosHashPut(info->superTables, (*oneTable)->sTableName, (*oneTable)->sTableNameLen, &meta, POINTER_BYTES);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseJSON(SSmlHandle *info, char *payload) {
  int32_t payloadNum = 0;
  int32_t ret = TSDB_CODE_SUCCESS;

  if (payload == NULL) {
    uError("SML:0x%" PRIx64 " empty JSON Payload", info->id);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  info->root = cJSON_Parse(payload);
  if (info->root == NULL) {
    uError("SML:0x%" PRIx64 " parse json failed:%s", info->id, payload);
    return TSDB_CODE_TSC_INVALID_JSON;
  }
  // multiple data points must be sent in JSON array
  if (cJSON_IsObject(info->root)) {
    payloadNum = 1;
  } else if (cJSON_IsArray(info->root)) {
    payloadNum = cJSON_GetArraySize(info->root);
  } else {
    uError("SML:0x%" PRIx64 " Invalid JSON Payload", info->id);
    ret = TSDB_CODE_TSC_INVALID_JSON;
    goto end;
  }

  for (int32_t i = 0; i < payloadNum; ++i) {
    cJSON *dataPoint = (payloadNum == 1 && cJSON_IsObject(info->root)) ? info->root : cJSON_GetArrayItem(info->root, i);
    ret = smlParseTelnetLine(info, dataPoint, -1);
    if (ret != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " Invalid JSON Payload", info->id);
      goto end;
    }
  }

end:
  return ret;
}

static int32_t smlInsertData(SSmlHandle *info) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSmlTableInfo **oneTable = (SSmlTableInfo **)taosHashIterate(info->childTables, NULL);
  while (oneTable) {
    SSmlTableInfo *tableData = *oneTable;

    SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
    tstrncpy(pName.dbname, info->pRequest->pDb, sizeof(pName.dbname));
    memcpy(pName.tname, tableData->childTableName, strlen(tableData->childTableName));

    SRequestConnInfo conn = {0};
    conn.pTrans = info->taos->pAppInfo->pTransporter;
    conn.requestId = info->pRequest->requestId;
    conn.requestObjRefId = info->pRequest->self;
    conn.mgmtEps = getEpSet_s(&info->taos->pAppInfo->mgmtEp);

    SVgroupInfo vg;
    code = catalogGetTableHashVgroup(info->pCatalog, &conn, &pName, &vg);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " catalogGetTableHashVgroup failed. table name: %s", info->id, tableData->childTableName);
      return code;
    }
    taosHashPut(info->pVgHash, (const char *)&vg.vgId, sizeof(vg.vgId), (char *)&vg, sizeof(vg));

    SSmlSTableMeta **pMeta =
        (SSmlSTableMeta **)taosHashGet(info->superTables, tableData->sTableName, tableData->sTableNameLen);
    ASSERT(NULL != pMeta && NULL != *pMeta);

    // use tablemeta of stable to save vgid and uid of child table
    (*pMeta)->tableMeta->vgId = vg.vgId;
    (*pMeta)->tableMeta->uid = tableData->uid;  // one table merge data block together according uid

    code = smlBindData(info->exec, tableData->tags, (*pMeta)->cols, tableData->cols, info->dataFormat,
                       (*pMeta)->tableMeta, tableData->childTableName, tableData->sTableName, tableData->sTableNameLen,
                       info->ttl, info->msgBuf.buf, info->msgBuf.len);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlBindData failed", info->id);
      return code;
    }
    oneTable = (SSmlTableInfo **)taosHashIterate(info->childTables, oneTable);
  }

  code = smlBuildOutput(info->exec, info->pVgHash);
  if (code != TSDB_CODE_SUCCESS) {
    uError("SML:0x%" PRIx64 " smlBuildOutput failed", info->id);
    return code;
  }
  info->cost.insertRpcTime = taosGetTimestampUs();

  SAppClusterSummary *pActivity = &info->taos->pAppInfo->summary;
  atomic_add_fetch_64((int64_t *)&pActivity->numOfInsertsReq, 1);

  launchQueryImpl(info->pRequest, info->pQuery, true, NULL);
  return info->pRequest->code;
}

static void smlPrintStatisticInfo(SSmlHandle *info) {
  uError("SML:0x%" PRIx64
         " smlInsertLines result, code:%d,lineNum:%d,stable num:%d,ctable num:%d,create stable num:%d,alter stable tag num:%d,alter stable col num:%d \
        parse cost:%" PRId64 ",schema cost:%" PRId64 ",bind cost:%" PRId64 ",rpc cost:%" PRId64 ",total cost:%" PRId64
         "",
         info->id, info->cost.code, info->cost.lineNum, info->cost.numOfSTables, info->cost.numOfCTables,
         info->cost.numOfCreateSTables, info->cost.numOfAlterTagSTables, info->cost.numOfAlterColSTables,
         info->cost.schemaTime - info->cost.parseTime,
         info->cost.insertBindTime - info->cost.schemaTime, info->cost.insertRpcTime - info->cost.insertBindTime,
         info->cost.endTime - info->cost.insertRpcTime, info->cost.endTime - info->cost.parseTime);
}

static int32_t smlParseLine(SSmlHandle *info, char *lines[], char *rawLine, char *rawLineEnd, int numLines) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (info->protocol == TSDB_SML_JSON_PROTOCOL) {
    if (lines) {
      code = smlParseJSON(info, *lines);
    } else if (rawLine) {
      code = smlParseJSON(info, rawLine);
    }
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlParseJSON failed:%s", info->id, lines ? *lines : rawLine);
      return code;
    }
    return code;
  }

  for (int32_t i = 0; i < numLines; ++i) {
    char *tmp = NULL;
    int   len = 0;
    if (lines) {
      tmp = lines[i];
      len = strlen(tmp);
    } else if (rawLine) {
      tmp = rawLine;
      while (rawLine < rawLineEnd) {
        if (*(rawLine++) == '\n') {
          break;
        }
        len++;
      }
      if (info->protocol == TSDB_SML_LINE_PROTOCOL && tmp[0] == '#') {  // this line is comment
        continue;
      }
    }

    if (info->protocol == TSDB_SML_LINE_PROTOCOL) {
      code = smlParseInfluxLine(info, tmp, len);
    } else if (info->protocol == TSDB_SML_TELNET_PROTOCOL) {
      code = smlParseTelnetLine(info, tmp, len);
    } else {
      ASSERT(0);
    }
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlParseLine failed. line %d : %s", info->id, i, tmp);
      return code;
    }
  }
  return code;
}

static int smlProcess(SSmlHandle *info, char *lines[], char *rawLine, char *rawLineEnd, int numLines) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t retryNum = 0;

  info->cost.parseTime = taosGetTimestampUs();

  code = smlParseLine(info, lines, rawLine, rawLineEnd, numLines);
  if (code != 0) {
    uError("SML:0x%" PRIx64 " smlParseLine error : %s", info->id, tstrerror(code));
    return code;
  }

  info->cost.lineNum = numLines;
  info->cost.numOfSTables = taosHashGetSize(info->superTables);
  info->cost.numOfCTables = taosHashGetSize(info->childTables);

  info->cost.schemaTime = taosGetTimestampUs();

  do {
    code = smlModifyDBSchemas(info);
    if (code == 0) break;
  } while (retryNum++ < taosHashGetSize(info->superTables) * MAX_RETRY_TIMES);

  if (code != 0) {
    uError("SML:0x%" PRIx64 " smlModifyDBSchemas error : %s", info->id, tstrerror(code));
    return code;
  }

  info->cost.insertBindTime = taosGetTimestampUs();
  code = smlInsertData(info);
  if (code != 0) {
    uError("SML:0x%" PRIx64 " smlInsertData error : %s", info->id, tstrerror(code));
    return code;
  }

  return code;
}

static int32_t isSchemalessDb(STscObj *taos, SRequestObj *request) {
  //  SCatalog *catalog = NULL;
  //  int32_t   code = catalogGetHandle(((STscObj *)taos)->pAppInfo->clusterId, &catalog);
  //  if (code != TSDB_CODE_SUCCESS) {
  //    uError("SML get catalog error %d", code);
  //    return code;
  //  }
  //
  //  SName name;
  //  tNameSetDbName(&name, taos->acctId, taos->db, strlen(taos->db));
  //  char dbFname[TSDB_DB_FNAME_LEN] = {0};
  //  tNameGetFullDbName(&name, dbFname);
  //  SDbCfgInfo pInfo = {0};
  //
  //  SRequestConnInfo conn = {0};
  //  conn.pTrans = taos->pAppInfo->pTransporter;
  //  conn.requestId = request->requestId;
  //  conn.requestObjRefId = request->self;
  //  conn.mgmtEps = getEpSet_s(&taos->pAppInfo->mgmtEp);
  //
  //  code = catalogGetDBCfg(catalog, &conn, dbFname, &pInfo);
  //  if (code != TSDB_CODE_SUCCESS) {
  //    return code;
  //  }
  //  taosArrayDestroy(pInfo.pRetensions);
  //
  //  if (!pInfo.schemaless) {
  //    return TSDB_CODE_SML_INVALID_DB_CONF;
  //  }
  return TSDB_CODE_SUCCESS;
}

TAOS_RES *taos_schemaless_insert_inner(SRequestObj *request, char *lines[], char *rawLine, char *rawLineEnd,
                                       int numLines, int protocol, int precision, int32_t ttl) {
  STscObj *pTscObj = request->pTscObj;
  SSmlHandle *info = NULL;
  pTscObj->schemalessType = 1;
  SSmlMsgBuf msg = {ERROR_MSG_BUF_DEFAULT_SIZE, request->msgBuf};

  if (request->pDb == NULL) {
    request->code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    smlBuildInvalidDataMsg(&msg, "Database not specified", NULL);
    goto end;
  }

  if (isSchemalessDb(pTscObj, request) != TSDB_CODE_SUCCESS) {
    request->code = TSDB_CODE_SML_INVALID_DB_CONF;
    smlBuildInvalidDataMsg(&msg, "Cannot write data to a non schemaless database", NULL);
    goto end;
  }

  if (protocol < TSDB_SML_LINE_PROTOCOL || protocol > TSDB_SML_JSON_PROTOCOL) {
    request->code = TSDB_CODE_SML_INVALID_PROTOCOL_TYPE;
    smlBuildInvalidDataMsg(&msg, "protocol invalidate", NULL);
    goto end;
  }

  if (protocol == TSDB_SML_LINE_PROTOCOL &&
      (precision < TSDB_SML_TIMESTAMP_NOT_CONFIGURED || precision > TSDB_SML_TIMESTAMP_NANO_SECONDS)) {
    request->code = TSDB_CODE_SML_INVALID_PRECISION_TYPE;
    smlBuildInvalidDataMsg(&msg, "precision invalidate for line protocol", NULL);
    goto end;
  }

  if (protocol == TSDB_SML_JSON_PROTOCOL) {
    numLines = 1;
  } else if (numLines <= 0) {
    request->code = TSDB_CODE_SML_INVALID_DATA;
    smlBuildInvalidDataMsg(&msg, "line num is invalid", NULL);
    goto end;
  }

  info = smlBuildSmlInfo(pTscObj, request, (SMLProtocolType)protocol, precision);
  if (!info) {
    request->code = TSDB_CODE_OUT_OF_MEMORY;
    uError("SML:taos_schemaless_insert error SSmlHandle is null");
    goto end;
  }

  info->isRawLine = (rawLine == NULL);
  info->ttl       = ttl;
  request->code = smlProcess(info, lines, rawLine, rawLineEnd, numLines);

end:
  uDebug("SML:0x%" PRIx64 " insert finished, code: %d", info->id, request->code);
  info->cost.endTime = taosGetTimestampUs();
  info->cost.code = request->code;
  smlPrintStatisticInfo(info);
  smlDestroyInfo(info);

  return (TAOS_RES *)request;
}

/**
 * taos_schemaless_insert() parse and insert data points into database according to
 * different protocol.
 *
 * @param $lines input array may contain multiple lines, each line indicates a data point.
 *               If protocol=2 is used input array should contain single JSON
 *               string(e.g. char *lines[] = {"$JSON_string"}). If need to insert
 *               multiple data points in JSON format, should include them in $JSON_string
 *               as a JSON array.
 * @param $numLines indicates how many data points in $lines.
 *                  If protocol = 2 is used this param will be ignored as $lines should
 *                  contain single JSON string.
 * @param $protocol indicates which protocol to use for parsing:
 *                  0 - influxDB line protocol
 *                  1 - OpenTSDB telnet line protocol
 *                  2 - OpenTSDB JSON format protocol
 * @return TAOS_RES
 */

TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision,
                                                int32_t ttl, int64_t reqid) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  SRequestObj *request = (SRequestObj *)createRequest(*(int64_t *)taos, TSDB_SQL_INSERT, reqid);
  if (!request) {
    uError("SML:taos_schemaless_insert error request is null");
    return NULL;
  }

  if (!lines) {
    SSmlMsgBuf msg = {ERROR_MSG_BUF_DEFAULT_SIZE, request->msgBuf};
    request->code = TSDB_CODE_SML_INVALID_DATA;
    smlBuildInvalidDataMsg(&msg, "lines is null", NULL);
    return (TAOS_RES *)request;
  }

  return taos_schemaless_insert_inner(request, lines, NULL, NULL, numLines, protocol, precision, ttl);
}

TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision) {
  return taos_schemaless_insert_ttl_with_reqid(taos, lines, numLines, protocol, precision, TSDB_DEFAULT_TABLE_TTL, 0);
}

TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl) {
  return taos_schemaless_insert_ttl_with_reqid(taos, lines, numLines, protocol, precision, ttl, 0);
}

TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int64_t reqid) {
  return taos_schemaless_insert_ttl_with_reqid(taos, lines, numLines, protocol, precision, TSDB_DEFAULT_TABLE_TTL, reqid);
}

TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol,
                                                int precision, int32_t ttl, int64_t reqid) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  SRequestObj *request = (SRequestObj *)createRequest(*(int64_t *)taos, TSDB_SQL_INSERT, reqid);
  if (!request) {
    uError("SML:taos_schemaless_insert error request is null");
    return NULL;
  }

  if (!lines || len <= 0) {
    SSmlMsgBuf msg = {ERROR_MSG_BUF_DEFAULT_SIZE, request->msgBuf};
    request->code = TSDB_CODE_SML_INVALID_DATA;
    smlBuildInvalidDataMsg(&msg, "lines is null", NULL);
    return (TAOS_RES *)request;
  }

  int numLines = 0;
  *totalRows = 0;
  char *tmp = lines;
  for (int i = 0; i < len; i++) {
    if (lines[i] == '\n' || i == len - 1) {
      numLines++;
      if (tmp[0] != '#' || protocol != TSDB_SML_LINE_PROTOCOL) {  // ignore comment
        (*totalRows)++;
      }
      tmp = lines + i + 1;
    }
  }
  return taos_schemaless_insert_inner(request, NULL, lines, lines + len, numLines, protocol, precision, ttl);
}

TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int64_t reqid) {
  return taos_schemaless_insert_raw_ttl_with_reqid(taos, lines, len, totalRows, protocol, precision, TSDB_DEFAULT_TABLE_TTL, reqid);
}
TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl) {
  return taos_schemaless_insert_raw_ttl_with_reqid(taos, lines, len, totalRows, protocol, precision, ttl, 0);
}

TAOS_RES *taos_schemaless_insert_raw(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision) {
  return taos_schemaless_insert_raw_ttl_with_reqid(taos, lines, len, totalRows, protocol, precision, TSDB_DEFAULT_TABLE_TTL, 0);
}

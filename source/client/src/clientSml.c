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

#if (defined(__GNUC__) && (__GNUC__ >= 3)) || (defined(__INTEL_COMPILER) && (__INTEL_COMPILER >= 800)) || defined(__clang__)
#  define expect(expr,value)    (__builtin_expect ((expr),(value)) )
#else
#  define expect(expr,value)    (expr)
#endif

#ifndef likely
#define likely(expr)     expect((expr) != 0, 1)
#endif
#ifndef unlikely
#define unlikely(expr)   expect((expr) != 0, 0)
#endif

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
//#define IS_SLASH_COMMA(sql) (*(sql) == COMMA && *((sql)-1) == SLASH)
#define IS_COMMA(sql)       (*(sql) == COMMA && *((sql)-1) != SLASH)
// space
//#define IS_SLASH_SPACE(sql) (*(sql) == SPACE && *((sql)-1) == SLASH)
#define IS_SPACE(sql)       (*(sql) == SPACE && *((sql)-1) != SLASH)
// equal =
//#define IS_SLASH_EQUAL(sql) (*(sql) == EQUAL && *((sql)-1) == SLASH)
#define IS_EQUAL(sql)       (*(sql) == EQUAL && *((sql)-1) != SLASH)
// quote "
//#define IS_SLASH_QUOTE(sql) (*(sql) == QUOTE && *((sql)-1) == SLASH)
#define IS_QUOTE(sql)       (*(sql) == QUOTE && *((sql)-1) != SLASH)
// SLASH
//#define IS_SLASH_SLASH(sql) (*(sql) == SLASH && *((sql)-1) == SLASH)

#define IS_SLASH_LETTER(sql) \
  (*((sql)-1) == SLASH && (*(sql) == COMMA || *(sql) == SPACE || *(sql) == EQUAL || *(sql) == QUOTE || *(sql) == SLASH))                          \
//  (IS_SLASH_COMMA(sql) || IS_SLASH_SPACE(sql) || IS_SLASH_EQUAL(sql) || IS_SLASH_QUOTE(sql) || IS_SLASH_SLASH(sql))

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

#define JSON_METERS_NAME "__JM"

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

/*********************** list start *********************************/
typedef struct {
  const void  *key;
  int32_t      keyLen;
  void        *value;
  bool         used;
}Node;

typedef struct NodeList{
  Node             data;
  struct NodeList* next;
}NodeList;

typedef int32_t (*_equal_fn_sml)(const void *, const void *);

static void* nodeListGet(NodeList* list, const void *key, int32_t len, _equal_fn_sml fn){
  NodeList *tmp = list;
  while(tmp){
    if(fn == NULL){
      if(tmp->data.used && tmp->data.keyLen == len && memcmp(tmp->data.key, key, len) == 0) {
        return tmp->data.value;
      }
    }else{
      if(tmp->data.used && fn(tmp->data.key, key) == 0) {
        return tmp->data.value;
      }
    }

    tmp = tmp->next;
  }
  return NULL;
}

static int nodeListSet(NodeList** list, const void *key, int32_t len, void* value){
  NodeList *tmp = *list;
  while (tmp){
    if(!tmp->data.used) break;
    if(tmp->data.keyLen == len && memcmp(tmp->data.key, key, len) == 0) {
      return -1;
    }
    tmp = tmp->next;
  }
  if(tmp){
    tmp->data.key = key;
    tmp->data.keyLen = len;
    tmp->data.value = value;
    tmp->data.used = true;
  }else{
    NodeList *newNode = taosMemoryCalloc(1, sizeof(NodeList));
    if(newNode == NULL){
      return -1;
    }
    newNode->data.key = key;
    newNode->data.keyLen = len;
    newNode->data.value = value;
    newNode->data.used = true;
    newNode->next = *list;
    *list = newNode;
  }
  return 0;
}

static int nodeListSize(NodeList* list){
  int cnt = 0;
  while(list){
    if(list->data.used) cnt++;
    else break;
    list = list->next;
  }
  return cnt;
}
/*********************** list end *********************************/

typedef struct {
  char *measure;
  char *tags;
  char *cols;
  char *timestamp;

  int32_t measureLen;
  int32_t measureTagsLen;
  int32_t tagsLen;
  int32_t colsLen;
  int32_t timestampLen;

  SArray *colArray;
} SSmlLineInfo;

typedef struct {
  const char *sTableName;  // super table name
  int32_t     sTableNameLen;
  char        childTableName[TSDB_TABLE_NAME_LEN];
  uint64_t    uid;

  SArray *tags;

  // elements are SHashObj<cols key string, SSmlKv*> for find by key quickly
  SArray *cols;
  STableDataCxt *tableDataCtx;
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
  bool            reRun;
  bool            dataFormat;  // true means that the name and order of keys in each line are the same(only for influx protocol)
  bool            isRawLine;
  int32_t         ttl;

  NodeList *childTables;
  NodeList *superTables;
  SHashObj *pVgHash;

  STscObj     *taos;
  SCatalog    *pCatalog;
  SRequestObj *pRequest;
  SQuery      *pQuery;

  SSmlCostInfo cost;
  int32_t      lineNum;
  SSmlMsgBuf   msgBuf;

  cJSON       *root;  // for parse json
  SSmlLineInfo      *lines; // element is SSmlLineInfo

  //
  SArray      *preLineTagKV;
  SArray      *preLineColKV;

  SSmlLineInfo preLine;
  STableMeta  *currSTableMeta;
  STableDataCxt *currTableDataCtx;
  bool         needModifySchema;
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
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, j);
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
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
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
    SSmlKv       *kv = (SSmlKv *)taosArrayGet(cols, j);
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
  if(info->dataFormat && !info->needModifySchema){
    return TSDB_CODE_SUCCESS;
  }
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

  NodeList *tmp = info->superTables;
  while (tmp) {
    SSmlSTableMeta *sTableData = tmp->data.value;
    bool            needCheckMeta = false;  // for multi thread

    size_t superTableLen = (size_t)tmp->data.keyLen;
    const void  *superTable = tmp->data.key;
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

    tmp = tmp->next;
  }
  return 0;

  end:
  taosHashCleanup(hashTmp);
  taosMemoryFreeClear(pTableMeta);
//  catalogRefreshTableMeta(info->pCatalog, &conn, &pName, 1);
  return code;
}

/******************************* parse basic type function **********************/
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
    kvVal->i = TSDB_TRUE;
    return true;
  }

  if ((len == 1) && (pVal[0] == 'f' || pVal[0] == 'F')) {
    kvVal->i = TSDB_FALSE;
    return true;
  }

  if ((len == 4) && !strncasecmp(pVal, "true", len)) {
    kvVal->i = TSDB_TRUE;
    return true;
  }
  if ((len == 5) && !strncasecmp(pVal, "false", len)) {
    kvVal->i = TSDB_FALSE;
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
  if (pVal[1] == '"' && pVal[len - 1] == '"' && (pVal[0] == 'l' || pVal[0] == 'L')) {
    return true;
  }
  return false;
}
/******************************* parse basic type function end **********************/

/******************************* time function **********************/
static uint8_t smlPrecisionConvert[7] = {TSDB_TIME_PRECISION_NANO, TSDB_TIME_PRECISION_HOURS, TSDB_TIME_PRECISION_MINUTES,
                                     TSDB_TIME_PRECISION_SECONDS, TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO,
                                     TSDB_TIME_PRECISION_NANO};
static int64_t smlFactorNS[3] = {NANOSECOND_PER_MSEC, NANOSECOND_PER_USEC, 1};
static int64_t smlFactorS[3] = {1000LL, 1000000LL, 1000000000LL};
static int64_t smlToMilli[3] = {3600LL, 60LL, 1LL};

static int64_t smlGetTimeValue(const char *value, int32_t len, uint8_t fromPrecision, uint8_t toPrecision) {
  char   *endPtr = NULL;
  int64_t tsInt64 = taosStr2Int64(value, &endPtr, 10);
  if (unlikely(value + len != endPtr)) {
    return -1;
  }

  if(unlikely(fromPrecision >= TSDB_TIME_PRECISION_HOURS)){
    fromPrecision = TSDB_TIME_PRECISION_MILLI;
    int64_t unit = smlToMilli[fromPrecision - TSDB_TIME_PRECISION_HOURS];
    if(unit > INT64_MAX / tsInt64){
      return -1;
    }
    tsInt64 *= unit;
  }

  return convertTimePrecision(tsInt64, fromPrecision, toPrecision);
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

static int64_t smlParseInfluxTime(SSmlHandle *info, const char *data, int32_t len) {
  uint8_t toPrecision = info->currSTableMeta ? info->currSTableMeta->tableInfo.precision : TSDB_TIME_PRECISION_NANO;

  if(unlikely(len == 0 || (len == 1 && data[0] == '0'))){
    return taosGetTimestampNs()/smlFactorNS[toPrecision];
  }

  uint8_t fromPrecision = smlPrecisionConvert[info->precision];

  int64_t ts = smlGetTimeValue(data, len, fromPrecision, toPrecision);
  if (unlikely(ts == -1)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

static int64_t smlParseOpenTsdbTime(SSmlHandle *info, const char *data, int32_t len) {
  uint8_t toPrecision = info->currSTableMeta ? info->currSTableMeta->tableInfo.precision : TSDB_TIME_PRECISION_NANO;

  if (unlikely(!data)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp can not be null", NULL);
    return -1;
  }
  if (unlikely(len == 1 && data[0] == '0')) {
    return taosGetTimestampNs()/smlFactorNS[toPrecision];
  }
  uint8_t fromPrecision = smlGetTsTypeByLen(len);
  if (unlikely(fromPrecision == -1)) {
    smlBuildInvalidDataMsg(&info->msgBuf,
                           "timestamp precision can only be seconds(10 digits) or milli seconds(13 digits)", data);
    return -1;
  }
  int64_t ts = smlGetTimeValue(data, len, fromPrecision, toPrecision);
  if (unlikely(ts == -1)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

static int64_t smlParseTS(SSmlHandle *info, const char *data, int32_t len) {
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

  return ts;
}
/******************************* time function end **********************/

/******************************* Sml struct related function **********************/
static SSmlTableInfo *smlBuildTableInfo(int numRows, const char* measure, int32_t measureLen) {
  SSmlTableInfo *tag = (SSmlTableInfo *)taosMemoryCalloc(sizeof(SSmlTableInfo), 1);
  if (!tag) {
    return NULL;
  }

  tag->sTableName = measure;
  tag->sTableNameLen = measureLen;

  tag->cols = taosArrayInit(numRows, POINTER_BYTES);
  if (tag->cols == NULL) {
    uError("SML:smlBuildTableInfo failed to allocate memory");
    goto cleanup;
  }

  tag->tags = taosArrayInit(16, sizeof(SSmlKv));
  if (tag->tags == NULL) {
    uError("SML:smlBuildTableInfo failed to allocate memory");
    goto cleanup;
  }
  return tag;

  cleanup:
  taosMemoryFree(tag);
  return NULL;
}

static int32_t smlCheckDupUnit(SHashObj *dumplicateKey, SArray *tags, SSmlMsgBuf *msg){
  for(int i = 0; i < taosArrayGetSize(tags); i++) {
    SSmlKv *tag = taosArrayGet(tags, i);
    if (smlCheckDuplicateKey(tag->key, tag->keyLen, dumplicateKey)) {
      smlBuildInvalidDataMsg(msg, "dumplicate key", tag->key);
      return TSDB_CODE_TSC_DUP_NAMES;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t smlJudgeDupColName(SArray *cols, SArray *tags, SSmlMsgBuf *msg) {
  SHashObj *dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  int ret = smlCheckDupUnit(dumplicateKey, cols, msg);
  if(ret != TSDB_CODE_SUCCESS){
    goto end;
  }
  ret = smlCheckDupUnit(dumplicateKey, tags, msg);
  if(ret != TSDB_CODE_SUCCESS){
    goto end;
  }

  end:
  taosHashCleanup(dumplicateKey);
  return ret;
}

static int32_t smlParseTableName(SArray *tags, char *childTableName) {
  size_t      childTableNameLen = strlen(tsSmlChildTableName);
  if (childTableNameLen <= 0) return TSDB_CODE_SUCCESS;

  for(int i = 0; i < taosArrayGetSize(tags); i++){
    SSmlKv *tag = taosArrayGet(tags, i);
    // handle child table name
    if (childTableNameLen == tag->keyLen && strncmp(tag->key, tsSmlChildTableName, tag->keyLen) == 0) {
      memset(childTableName, 0, TSDB_TABLE_NAME_LEN);
      strncpy(childTableName, tag->value, (tag->length < TSDB_TABLE_NAME_LEN ? tag->length : TSDB_TABLE_NAME_LEN));
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlSetCTableName(SSmlTableInfo *oneTable){
  smlParseTableName(oneTable->tags, oneTable->childTableName);

  if (strlen(oneTable->childTableName) == 0) {
    SArray* dst = taosArrayDup(oneTable->tags, NULL);
    RandTableName rName = {dst, oneTable->sTableName, (uint8_t)oneTable->sTableNameLen,
                           oneTable->childTableName, 0};

    buildChildTableName(&rName);
    taosArrayDestroy(dst);
    oneTable->uid = rName.uid;
  } else {
    oneTable->uid = *(uint64_t *)(oneTable->childTableName);
  }
  return TSDB_CODE_SUCCESS;
}

static SSmlSTableMeta *smlBuildSTableMeta(bool isDataFormat) {
  SSmlSTableMeta *meta = (SSmlSTableMeta *)taosMemoryCalloc(sizeof(SSmlSTableMeta), 1);
  if (!meta) {
    return NULL;
  }

  if(unlikely(!isDataFormat)){
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
  }

  meta->tags = taosArrayInit(32, sizeof(SSmlKv));
  if (meta->tags == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }

  meta->cols = taosArrayInit(32, sizeof(SSmlKv));
  if (meta->cols == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }
  return meta;

  cleanup:
  taosMemoryFree(meta);
  return NULL;
}

static void smlInsertMeta(SHashObj *metaHash, SArray *metaArray, SArray *cols){
  for (int16_t i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    taosArrayPush(metaArray, kv);
    if(unlikely(metaHash != NULL)) {
      taosHashPut(metaHash, kv->key, kv->keyLen, &i, SHORT_BYTES);
    }
  }
}

static STableMeta* smlGetMeta(SSmlHandle *info, const void* measure, int32_t measureLen){
  STableMeta *pTableMeta = NULL;

  SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
  tstrncpy(pName.dbname, info->pRequest->pDb, sizeof(pName.dbname));

  SRequestConnInfo conn = {0};
  conn.pTrans = info->taos->pAppInfo->pTransporter;
  conn.requestId = info->pRequest->requestId;
  conn.requestObjRefId = info->pRequest->self;
  conn.mgmtEps = getEpSet_s(&info->taos->pAppInfo->mgmtEp);
  memset(pName.tname, 0, TSDB_TABLE_NAME_LEN);
  memcpy(pName.tname, measure, measureLen);

  catalogGetSTableMeta(info->pCatalog, &conn, &pName, &pTableMeta);
  return pTableMeta;
}

static void smlDestroySTableMeta(SSmlSTableMeta *meta) {
  taosHashCleanup(meta->tagHash);
  taosHashCleanup(meta->colHash);
  taosArrayDestroy(meta->tags);
  taosArrayDestroy(meta->cols);
  taosMemoryFree(meta->tableMeta);
  taosMemoryFree(meta);
}
/******************************* Sml struct related function end **********************/

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

int32_t is_same_child_table_json(const void *a, const void *b){
  return (cJSON_Compare((const cJSON *)a, (const cJSON *)b, true)) ? 0 : 1;
}

#define IS_SAME_CHILD_TABLE (elements->measureTagsLen == info->preLine.measureTagsLen \
&& memcmp(elements->measure, info->preLine.measure, elements->measureTagsLen) == 0)

#define IS_SAME_SUPER_TABLE (elements->measureLen == info->preLine.measureLen \
&& memcmp(elements->measure, info->preLine.measure, elements->measureLen) == 0)

#define IS_SAME_KEY (preKV->keyLen == kv.keyLen && memcmp(preKV->key, kv.key, kv.keyLen) == 0)

static int32_t smlParseTagKv(SSmlHandle *info, char **sql, char *sqlEnd,
                          SSmlLineInfo* currElement, bool isSameMeasure, bool isSameCTable){
  if(isSameCTable){
    return TSDB_CODE_SUCCESS;
  }

  int     cnt = 0;
  SArray *preLineKV = info->preLineTagKV;
  bool    isSuperKVInit = true;
  SArray *superKV = NULL;
  if(info->dataFormat){
    if(!isSameMeasure){
      SSmlSTableMeta *sMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, currElement->measure, currElement->measureLen, NULL);

      if(unlikely(sMeta == NULL)){
        sMeta = smlBuildSTableMeta(info->dataFormat);
        STableMeta * pTableMeta = smlGetMeta(info, currElement->measure, currElement->measureLen);
        sMeta->tableMeta = pTableMeta;
        if(pTableMeta == NULL){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        nodeListSet(&info->superTables, currElement->measure, currElement->measureLen, sMeta);
      }
      info->currSTableMeta = sMeta->tableMeta;
      superKV = sMeta->tags;

      if(unlikely(taosArrayGetSize(superKV) == 0)){
        isSuperKVInit = false;
      }
      taosArraySetSize(preLineKV, 0);
    }
  }else{
    taosArraySetSize(preLineKV, 0);
  }


  while (*sql < sqlEnd) {
    if (unlikely(IS_SPACE(*sql))) {
      break;
    }

    bool hasSlash = false;
    // parse key
    const char *key = *sql;
    int32_t     keyLen = 0;
    while (*sql < sqlEnd) {
      if (unlikely(IS_COMMA(*sql))) {
        smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", *sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      if (unlikely(IS_EQUAL(*sql))) {
        keyLen = *sql - key;
        (*sql)++;
        break;
      }
      if(!hasSlash){
        hasSlash = (*(*sql) == SLASH);
      }
      (*sql)++;
    }
    if(unlikely(hasSlash)) {
      PROCESS_SLASH(key, keyLen)
    }

    if (unlikely(IS_INVALID_COL_LEN(keyLen))) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid key or key is too long than 64", key);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }

    // parse value
    const char *value = *sql;
    int32_t     valueLen = 0;
    hasSlash = false;
    while (*sql < sqlEnd) {
      // parse value
      if (unlikely(IS_SPACE(*sql) || IS_COMMA(*sql))) {
        break;
      }else if (unlikely(IS_EQUAL(*sql))) {
        smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", *sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }

      if(!hasSlash){
        hasSlash = (*(*sql) == SLASH);
      }

      (*sql)++;
    }
    valueLen = *sql - value;

    if (unlikely(valueLen == 0)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    if(unlikely(hasSlash)) {
      PROCESS_SLASH(value, valueLen)
    }

    if (unlikely(valueLen > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE)) {
      return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
    }

    SSmlKv kv = {.key = key, .type = TSDB_DATA_TYPE_NCHAR, .keyLen = keyLen, .value = value, .length = valueLen};
    if(info->dataFormat){
      if(unlikely(cnt + 1 > info->currSTableMeta->tableInfo.numOfTags)){
        info->needModifySchema = true;
      }

      if(isSameMeasure){
        if(unlikely(cnt >= taosArrayGetSize(preLineKV))) {
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        SSmlKv *preKV = taosArrayGet(preLineKV, cnt);
        if(unlikely(kv.length > preKV->length)){
          preKV->length = kv.length;
          SSmlSTableMeta *tableMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, currElement->measure, currElement->measureLen, NULL);
          ASSERT(tableMeta != NULL);

          SSmlKv *oldKV = taosArrayGet(tableMeta->tags, cnt);
          oldKV->length = kv.length;
          info->needModifySchema = true;
        }
        if(unlikely(!IS_SAME_KEY)){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
      }else{
        if(isSuperKVInit){
          if(unlikely(cnt >= taosArrayGetSize(superKV))) {
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
          SSmlKv *preKV = taosArrayGet(superKV, cnt);
          if(unlikely(kv.length > preKV->length)) {
            preKV->length = kv.length;
          }else{
            kv.length = preKV->length;
          }
          info->needModifySchema = true;

          if(unlikely(!IS_SAME_KEY)){
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
        }else{
          taosArrayPush(superKV, &kv);
        }
        taosArrayPush(preLineKV, &kv);
      }
    }else{
      taosArrayPush(preLineKV, &kv);
    }

    cnt++;
    if(IS_SPACE(*sql)){
      break;
    }
    (*sql)++;
  }

  void* oneTable = nodeListGet(info->childTables, currElement->measure, currElement->measureTagsLen, NULL);
  if ((oneTable != NULL)) {
    return TSDB_CODE_SUCCESS;
  }

  SSmlTableInfo *tinfo = smlBuildTableInfo(1, currElement->measure, currElement->measureLen);
  if (!tinfo) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for(int i = 0; i < taosArrayGetSize(preLineKV); i++){
    taosArrayPush(tinfo->tags, taosArrayGet(preLineKV, i));
  }
  smlSetCTableName(tinfo);
  if(info->dataFormat) {
    info->currSTableMeta->uid = tinfo->uid;
    tinfo->tableDataCtx = smlInitTableDataCtx(info->pQuery, info->currSTableMeta);
    if(tinfo->tableDataCtx == NULL){
      smlBuildInvalidDataMsg(&info->msgBuf, "smlInitTableDataCtx error", NULL);
      return TSDB_CODE_SML_INVALID_DATA;
    }
  }

  nodeListSet(&info->childTables, currElement->measure, currElement->measureTagsLen, tinfo);

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseColKv(SSmlHandle *info, char **sql, char *sqlEnd,
                          SSmlLineInfo* currElement, bool isSameMeasure, bool isSameCTable){
  int     cnt = 0;
  SArray *preLineKV = info->preLineColKV;
  bool    isSuperKVInit = true;
  SArray *superKV = NULL;
  if(info->dataFormat){
    if(unlikely(!isSameCTable)){
      SSmlTableInfo *oneTable = (SSmlTableInfo *)nodeListGet(info->childTables, currElement->measure, currElement->measureTagsLen, NULL);
      if (unlikely(oneTable == NULL)) {
        smlBuildInvalidDataMsg(&info->msgBuf, "child table should inside", currElement->measure);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      info->currTableDataCtx = oneTable->tableDataCtx;
    }

    if(unlikely(!isSameMeasure)){
      SSmlSTableMeta *sMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, currElement->measure, currElement->measureLen, NULL);

      if(unlikely(sMeta == NULL)){
        sMeta = smlBuildSTableMeta(info->dataFormat);
        STableMeta * pTableMeta = smlGetMeta(info, currElement->measure, currElement->measureLen);
        sMeta->tableMeta = pTableMeta;
        if(pTableMeta == NULL){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        nodeListSet(&info->superTables, currElement->measure, currElement->measureLen, sMeta);
      }
      info->currSTableMeta = sMeta->tableMeta;
      superKV = sMeta->cols;
      if(unlikely(taosArrayGetSize(superKV) == 0)){
        isSuperKVInit = false;
      }
      taosArraySetSize(preLineKV, 0);
    }
  }

  while (*sql < sqlEnd) {
    if (unlikely(IS_SPACE(*sql))) {
      break;
    }

    bool hasSlash = false;
    // parse key
    const char *key = *sql;
    int32_t     keyLen = 0;
    while (*sql < sqlEnd) {
      if (unlikely(IS_COMMA(*sql))) {
        smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", *sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      if (unlikely(IS_EQUAL(*sql))) {
        keyLen = *sql - key;
        (*sql)++;
        break;
      }
      if(!hasSlash){
        hasSlash = (*(*sql) == SLASH);
      }
      (*sql)++;
    }
    if(unlikely(hasSlash)) {
      PROCESS_SLASH(key, keyLen)
    }

    if (unlikely(IS_INVALID_COL_LEN(keyLen))) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid key or key is too long than 64", key);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }

    // parse value
    const char *value = *sql;
    int32_t     valueLen = 0;
    hasSlash              = false;
    bool        isInQuote = false;
    while (*sql < sqlEnd) {
      // parse value
      if (IS_QUOTE(*sql)) {
        isInQuote = !isInQuote;
        (*sql)++;
        continue;
      }
      if (!isInQuote){
        if (unlikely(IS_SPACE(*sql) || IS_COMMA(*sql))) {
          break;
        } else if (unlikely(IS_EQUAL(*sql))) {
          smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", *sql);
          return TSDB_CODE_SML_INVALID_DATA;
        }
      }
      if(!hasSlash){
        hasSlash = (*(*sql) == SLASH);
      }

      (*sql)++;
    }
    valueLen = *sql - value;

    if (unlikely(isInQuote)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "only one quote", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    if (unlikely(valueLen == 0)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    if(unlikely(hasSlash)) {
      PROCESS_SLASH(value, valueLen)
    }

    SSmlKv kv = {.key = key, .keyLen = keyLen, .value = value, .length = valueLen};
    int32_t ret = smlParseValue(&kv, &info->msgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    if(info->dataFormat){
      //cnt begin 0, add ts so + 2
      if(unlikely(cnt + 2 > info->currSTableMeta->tableInfo.numOfColumns)){
        info->needModifySchema = true;
      }
      // bind data
      ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kv, cnt + 1);
      if (unlikely(ret != TSDB_CODE_SUCCESS)) {
        smlBuildInvalidDataMsg(&info->msgBuf, "smlBuildCol error", NULL);
        return ret;
      }

      if(isSameMeasure){
        if(cnt >= taosArrayGetSize(preLineKV)) {
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        SSmlKv *preKV = taosArrayGet(preLineKV, cnt);
        if(kv.type != preKV->type){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }

        if(unlikely(IS_VAR_DATA_TYPE(kv.type) && kv.length > preKV->length)){
          preKV->length = kv.length;
          SSmlSTableMeta *tableMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, currElement->measure, currElement->measureLen, NULL);
          ASSERT(tableMeta != NULL);

          SSmlKv *oldKV = taosArrayGet(tableMeta->cols, cnt);
          oldKV->length = kv.length;
          info->needModifySchema = true;
        }
        if(unlikely(!IS_SAME_KEY)){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
      }else{
        if(isSuperKVInit){
          if(unlikely(cnt >= taosArrayGetSize(superKV))) {
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
          SSmlKv *preKV = taosArrayGet(superKV, cnt);
          if(unlikely(kv.type != preKV->type)){
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }

          if(IS_VAR_DATA_TYPE(kv.type)){
            if(kv.length > preKV->length) {
              preKV->length = kv.length;
            }else{
              kv.length = preKV->length;
            }
            info->needModifySchema = true;
          }
          if(unlikely(!IS_SAME_KEY)){
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
        }else{
          taosArrayPush(superKV, &kv);
        }
        taosArrayPush(preLineKV, &kv);
      }
    }else{
      if(currElement->colArray == NULL){
        currElement->colArray = taosArrayInit(16, sizeof(SSmlKv));
        taosArraySetSize(currElement->colArray, 1);
      }
      taosArrayPush(currElement->colArray, &kv);   //reserve for timestamp
    }

    cnt++;
    if(IS_SPACE(*sql)){
      break;
    }
    (*sql)++;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseInfluxString(SSmlHandle *info, char *sql, char *sqlEnd, SSmlLineInfo *elements) {
  if (!sql) return TSDB_CODE_SML_INVALID_DATA;
  JUMP_SPACE(sql, sqlEnd)
  if (unlikely(*sql == COMMA)) return TSDB_CODE_SML_INVALID_DATA;
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
  if (unlikely(IS_INVALID_TABLE_LEN(elements->measureLen))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "measure is empty or too large than 192", NULL);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  // to get measureTagsLen before
  const char* tmp = sql;
  while (tmp < sqlEnd){
    if (IS_SPACE(tmp)) {
      break;
    }
    tmp++;
  }
  elements->measureTagsLen = tmp - elements->measure;

  bool isSameCTable = false;
  bool isSameMeasure = false;
  if(IS_SAME_CHILD_TABLE){
    isSameCTable = true;
    isSameMeasure = true;
  }else if(info->dataFormat) {
    isSameMeasure = IS_SAME_SUPER_TABLE;
  }
  // parse tag
  if (*sql == SPACE) {
    elements->tagsLen = 0;
  } else {
    if (*sql == COMMA) sql++;
    elements->tags = sql;

    // tinfo != NULL means child table has never occur before
    int ret = smlParseTagKv(info, &sql, sqlEnd, elements, isSameMeasure, isSameCTable);
    if(unlikely(ret != TSDB_CODE_SUCCESS)){
      return ret;
    }
    if(unlikely(info->reRun)){
      return TSDB_CODE_SUCCESS;
    }

    sql = elements->measure + elements->measureTagsLen;

    elements->tagsLen = sql - elements->tags;
  }

  // parse cols
  JUMP_SPACE(sql, sqlEnd)
  elements->cols = sql;

  int ret = smlParseColKv(info, &sql, sqlEnd, elements, isSameMeasure, isSameCTable);
  if(unlikely(ret != TSDB_CODE_SUCCESS)){
    return ret;
  }

  if(unlikely(info->reRun)){
    return TSDB_CODE_SUCCESS;
  }

  elements->colsLen = sql - elements->cols;
  if (unlikely(elements->colsLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "cols is empty", NULL);
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

  int64_t ts = smlParseTS(info, elements->timestamp, elements->timestampLen);
  if (ts <= 0) {
    uError("SML:0x%" PRIx64 " smlParseTS error:%" PRId64, info->id, ts);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }
  // add ts to
  SSmlKv kv = { .key = TS, .keyLen = TS_LEN, .i = ts, .type = TSDB_DATA_TYPE_TIMESTAMP, .length = (int16_t)tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes};
  if(info->dataFormat){
    smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kv, 0);
    smlBuildRow(info->currTableDataCtx);
  }else{
    taosArraySet(elements->colArray, 0, &kv);
  }
  info->preLine = *elements;

  return ret;
}

static void smlParseTelnetElement(char **sql, char *sqlEnd, char **data, int32_t *len) {
  while (*sql < sqlEnd) {
    if (unlikely((**sql != SPACE && !(*data)))) {
      *data = *sql;
    } else if (unlikely(**sql == SPACE && *data)) {
      *len = *sql - *data;
      break;
    }
    (*sql)++;
  }
}

static int32_t smlParseTelnetTags(SSmlHandle *info, char *data, char *sqlEnd, SSmlLineInfo *elements, SSmlMsgBuf *msg) {
  if(IS_SAME_CHILD_TABLE){
    return TSDB_CODE_SUCCESS;
  }

  bool isSameMeasure = IS_SAME_SUPER_TABLE;

  int     cnt = 0;
  SArray *preLineKV = info->preLineTagKV;
  bool    isSuperKVInit = true;
  SArray *superKV = NULL;
  if(info->dataFormat){
    if(!isSameMeasure){
      SSmlSTableMeta *sMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, elements->measure, elements->measureLen, NULL);

      if(unlikely(sMeta == NULL)){
        sMeta = smlBuildSTableMeta(info->dataFormat);
        STableMeta * pTableMeta = smlGetMeta(info, elements->measure, elements->measureLen);
        sMeta->tableMeta = pTableMeta;
        if(pTableMeta == NULL){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        nodeListSet(&info->superTables, elements->measure, elements->measureLen, sMeta);
      }
      info->currSTableMeta = sMeta->tableMeta;
      superKV = sMeta->tags;

      if(unlikely(taosArrayGetSize(superKV) == 0)){
        isSuperKVInit = false;
      }
      taosArraySetSize(preLineKV, 0);
    }
  }else{
    taosArraySetSize(preLineKV, 0);
  }

  const char *sql = data;
  size_t      childTableNameLen = strlen(tsSmlChildTableName);
  while (sql < sqlEnd) {
    JUMP_SPACE(sql, sqlEnd)
    if (unlikely(*sql == '\0')) break;

    const char *key = sql;
    int32_t     keyLen = 0;

    // parse key
    while (sql < sqlEnd) {
      if (unlikely(*sql == SPACE)) {
        smlBuildInvalidDataMsg(msg, "invalid data", sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      if (unlikely(*sql == EQUAL)) {
        keyLen = sql - key;
        sql++;
        break;
      }
      sql++;
    }

    if (unlikely(IS_INVALID_COL_LEN(keyLen))) {
      smlBuildInvalidDataMsg(msg, "invalid key or key is too long than 64", key);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }
//    if (smlCheckDuplicateKey(key, keyLen, dumplicateKey)) {
//      smlBuildInvalidDataMsg(msg, "dumplicate key", key);
//      return TSDB_CODE_TSC_DUP_NAMES;
//    }

    // parse value
    const char *value = sql;
    int32_t     valueLen = 0;
    while (sql < sqlEnd) {
      // parse value
      if (unlikely(*sql == SPACE)) {
        break;
      }
      if (unlikely(*sql == EQUAL)) {
        smlBuildInvalidDataMsg(msg, "invalid data", sql);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      sql++;
    }
    valueLen = sql - value;

    if (unlikely(valueLen == 0)) {
      smlBuildInvalidDataMsg(msg, "invalid value", value);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }

    if (unlikely(valueLen > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE)) {
      return TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN;
    }

    SSmlKv kv = {.key = key, .keyLen = keyLen, .value = value, .length = valueLen, .type = TSDB_DATA_TYPE_NCHAR};

    if(info->dataFormat){
      if(unlikely(cnt + 1 > info->currSTableMeta->tableInfo.numOfTags)){
        info->needModifySchema = true;
      }

      if(isSameMeasure){
        if(unlikely(cnt >= taosArrayGetSize(preLineKV))) {
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        SSmlKv *preKV = taosArrayGet(preLineKV, cnt);
        if(unlikely(kv.length > preKV->length)){
          preKV->length = kv.length;
          SSmlSTableMeta *tableMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, elements->measure, elements->measureLen, NULL);
          ASSERT(tableMeta != NULL);

          SSmlKv *oldKV = taosArrayGet(tableMeta->tags, cnt);
          oldKV->length = kv.length;
          info->needModifySchema = true;
        }
        if(unlikely(!IS_SAME_KEY)){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
      }else{
        if(isSuperKVInit){
          if(unlikely(cnt >= taosArrayGetSize(superKV))) {
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
          SSmlKv *preKV = taosArrayGet(superKV, cnt);
          if(unlikely(kv.length > preKV->length)) {
            preKV->length = kv.length;
          }else{
            kv.length = preKV->length;
          }
          info->needModifySchema = true;

          if(unlikely(!IS_SAME_KEY)){
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
        }else{
          taosArrayPush(superKV, &kv);
        }
        taosArrayPush(preLineKV, &kv);
      }
    }else{
      taosArrayPush(preLineKV, &kv);
    }
    cnt++;
  }
  SSmlTableInfo *tinfo = (SSmlTableInfo *)nodeListGet(info->childTables, elements->measure, elements->measureTagsLen, NULL);
  if (unlikely(tinfo == NULL)) {
    tinfo = smlBuildTableInfo(1, elements->measure, elements->measureLen);
    if (!tinfo) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    for(int i = 0; i < taosArrayGetSize(preLineKV); i++){
      taosArrayPush(tinfo->tags, taosArrayGet(preLineKV, i));
    }
    smlSetCTableName(tinfo);
    if (info->dataFormat) {
      info->currSTableMeta->uid = tinfo->uid;
      tinfo->tableDataCtx = smlInitTableDataCtx(info->pQuery, info->currSTableMeta);
      if (tinfo->tableDataCtx == NULL) {
        smlBuildInvalidDataMsg(&info->msgBuf, "smlInitTableDataCtx error", NULL);
        return TSDB_CODE_SML_INVALID_DATA;
      }
    }

    nodeListSet(&info->childTables, elements->measure, elements->measureTagsLen, tinfo);
  }
  return TSDB_CODE_SUCCESS;
}

// format: <metric> <timestamp> <value> <tagk_1>=<tagv_1>[ <tagk_n>=<tagv_n>]
static int32_t smlParseTelnetString(SSmlHandle *info, char *sql, char *sqlEnd, SSmlLineInfo *elements) {
  if (!sql) return TSDB_CODE_SML_INVALID_DATA;

  // parse metric
  smlParseTelnetElement(&sql, sqlEnd, &elements->measure, &elements->measureLen);
  if (unlikely((!(elements->measure) || IS_INVALID_TABLE_LEN(elements->measureLen)))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid data", sql);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  // parse timestamp
  smlParseTelnetElement(&sql, sqlEnd, &elements->timestamp, &elements->timestampLen);
  if (unlikely(!elements->timestamp || elements->timestampLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  bool needConverTime = false;  // get TS before parse tag(get meta), so need conver time
  if(info->dataFormat && info->currSTableMeta == NULL){
    needConverTime = true;
  }
  int64_t ts = smlParseTS(info, elements->timestamp, elements->timestampLen);
  if (unlikely(ts < 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", sql);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }
  SSmlKv kvTs = { .key = TS, .keyLen = TS_LEN, .i = ts, .type = TSDB_DATA_TYPE_TIMESTAMP, .length = (int16_t)tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes};

  // parse value
  smlParseTelnetElement(&sql, sqlEnd, &elements->cols, &elements->colsLen);
  if (unlikely(!elements->cols || elements->colsLen == 0)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid value", sql);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  SSmlKv kv = {.key = VALUE, .keyLen = VALUE_LEN, .value = elements->cols, .length = elements->colsLen};
  if (smlParseNumber(&kv, &info->msgBuf)) {
    kv.length = (int16_t)tDataTypes[kv.type].bytes;
    return TSDB_CODE_SUCCESS;
  }else{
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  // move measure before tags to combine keys to identify child table
  memcpy(sql - elements->measureLen, elements->measure, elements->measureLen);
  elements->measure = sql - elements->measureLen;
  elements->measureLen += sqlEnd - sql;


  int ret = smlParseTelnetTags(info, sql, sqlEnd, elements, &info->msgBuf);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    return ret;
  }

  if(unlikely(info->reRun)){
    return TSDB_CODE_SUCCESS;
  }

  if(info->dataFormat){
    if(needConverTime) {
      kvTs.i = convertTimePrecision(kvTs.i, TSDB_TIME_PRECISION_NANO, info->currSTableMeta->tableInfo.precision);
    }
    ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kvTs, 0);
    if(ret == TSDB_CODE_SUCCESS){
      ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kv, 1);
    }
    if(ret == TSDB_CODE_SUCCESS){
      ret = smlBuildRow(info->currTableDataCtx);
    }
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "smlBuildCol error", NULL);
      return ret;
    }
  }else{
    if(elements->colArray == NULL){
      elements->colArray = taosArrayInit(16, sizeof(SSmlKv));
    }
    taosArrayPush(elements->colArray, &kvTs);
    taosArrayPush(elements->colArray, &kv);
  }
  info->preLine = *elements;

  return TSDB_CODE_SUCCESS;
}

static int32_t smlUpdateMeta(SHashObj *metaHash, SArray *metaArray, SArray *cols, bool isTag, SSmlMsgBuf *msg) {
  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);

    int16_t *index = (int16_t *)taosHashGet(metaHash, kv->key, kv->keyLen);
    if (index) {
      SSmlKv *value = (SSmlKv *)taosArrayGet(metaArray, *index);
      if (isTag){
        if (kv->length > value->length) {
          value->length = kv->length;
        }
        continue;
      }
      if (kv->type != value->type) {
        smlBuildInvalidDataMsg(msg, "the type is not the same like before", kv->key);
        return TSDB_CODE_SML_NOT_SAME_TYPE;
      }

      if (IS_VAR_DATA_TYPE(kv->type) && (kv->length > value->length)) {  // update string len, if bigger
        value->length = kv->length;
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

static void smlDestroyTableInfo(SSmlTableInfo *tag) {
  for (size_t i = 0; i < taosArrayGetSize(tag->cols); i++) {
    SHashObj *kvHash = (SHashObj *)taosArrayGetP(tag->cols, i);
    taosHashCleanup(kvHash);
  }

  taosArrayDestroy(tag->cols);
  taosArrayDestroy(tag->tags);
  taosMemoryFree(tag);
}

static int32_t smlPushCols(SArray *colsArray, SArray *cols) {
  SHashObj *kvHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (!kvHash) {
    uError("SML:smlDealCols failed to allocate memory");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
    SSmlKv *kv = (SSmlKv *)taosArrayGet(cols, i);
    taosHashPut(kvHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
  }

  taosArrayPush(colsArray, &kvHash);
  return TSDB_CODE_SUCCESS;
}

void smlDestroyInfo(SSmlHandle *info) {
  if (!info) return;
  qDestroyQuery(info->pQuery);

  // destroy info->childTables
  NodeList* tmp = info->childTables;
  while (tmp) {
    if(tmp->data.used) {
      smlDestroyTableInfo(tmp->data.value);
    }
    NodeList* t = tmp->next;
    taosMemoryFree(tmp);
    tmp = t;
  }

  // destroy info->superTables
  tmp = info->superTables;
  while (tmp) {
    if(tmp->data.used) {
      smlDestroySTableMeta(tmp->data.value);
    }
    NodeList* t = tmp->next;
    taosMemoryFree(tmp);
    tmp = t;
  }

  // destroy info->pVgHash
  taosHashCleanup(info->pVgHash);

  taosArrayDestroy(info->preLineTagKV);
  taosArrayDestroy(info->preLineColKV);

  if(!info->dataFormat){
    for(int i = 0; i < info->lineNum; i++){
      taosArrayDestroy(info->lines[i].colArray);
    }
    taosMemoryFree(info->lines);
  }

  cJSON_Delete(info->root);
  taosMemoryFreeClear(info);
}

static SSmlHandle *smlBuildSmlInfo(TAOS *taos) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SSmlHandle *info = (SSmlHandle *)taosMemoryCalloc(1, sizeof(SSmlHandle));
  if (NULL == info) {
    return NULL;
  }
  info->taos = acquireTscObj(*(int64_t *)taos);
  code = catalogGetHandle(info->taos->pAppInfo->clusterId, &info->pCatalog);
  if (code != TSDB_CODE_SUCCESS) {
    uError("SML:0x%" PRIx64 " get catalog error %d", info->id, code);
    goto cleanup;
  }
  info->pVgHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  info->id = smlGenId();
  info->pQuery = smlInitHandle();
  info->dataFormat = true;

  info->preLineTagKV = taosArrayInit(8, sizeof(SSmlKv));
  info->preLineColKV = taosArrayInit(8, sizeof(SSmlKv));

  if (NULL == info->pVgHash) {
    uError("create SSmlHandle failed");
    goto cleanup;
  }

  return info;

cleanup:
  smlDestroyInfo(info);
  return NULL;
}

/************* TSDB_SML_JSON_PROTOCOL function start **************/
static int32_t smlParseMetricFromJSON(SSmlHandle *info, cJSON *root, SSmlLineInfo *elements) {
  cJSON *metric = cJSON_GetObjectItem(root, "metric");
  if (!cJSON_IsString(metric)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  elements->measureLen = strlen(metric->valuestring);
  if (IS_INVALID_TABLE_LEN(elements->measureLen)) {
    uError("OTD:0x%" PRIx64 " Metric lenght is 0 or large than 192", info->id);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  elements->measure = metric->valuestring;
  return TSDB_CODE_SUCCESS;
}

static int64_t smlParseTSFromJSONObj(SSmlHandle *info, cJSON *root, int32_t toPrecision) {
  int32_t size = cJSON_GetArraySize(root);
  if (unlikely(size != OTD_JSON_SUB_FIELDS_NUM)) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalidate json", NULL);
    return -1;
  }

  cJSON *value = cJSON_GetObjectItem(root, "value");
  if (unlikely(!cJSON_IsNumber(value))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalidate json", NULL);
    return -1;
  }

  cJSON *type = cJSON_GetObjectItem(root, "type");
  if (unlikely(!cJSON_IsString(type))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalidate json", NULL);
    return -1;
  }

  double timeDouble = value->valuedouble;
  if (unlikely(smlDoubleToInt64OverFlow(timeDouble))) {
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
    return -1;
  }

  if (timeDouble == 0) {
    return taosGetTimestampNs()/smlFactorNS[toPrecision];
  }

  if (timeDouble < 0) {
    return timeDouble;
  }

  int64_t tsInt64 = timeDouble;
  size_t typeLen = strlen(type->valuestring);
  if (typeLen == 1 && (type->valuestring[0] == 's' || type->valuestring[0] == 'S')) {
    // seconds
    int8_t fromPrecision = TSDB_TIME_PRECISION_SECONDS;
    if(smlFactorS[toPrecision] < INT64_MAX / tsInt64){
      return tsInt64 * smlFactorS[toPrecision];
    }
    return -1;
  } else if (typeLen == 2 && (type->valuestring[1] == 's' || type->valuestring[1] == 'S')) {
    switch (type->valuestring[0]) {
      case 'm':
      case 'M':
        // milliseconds
        return convertTimePrecision(tsInt64, TSDB_TIME_PRECISION_MILLI, toPrecision);
        break;
      case 'u':
      case 'U':
        // microseconds
        return convertTimePrecision(tsInt64, TSDB_TIME_PRECISION_MICRO, toPrecision);
        break;
      case 'n':
      case 'N':
        return convertTimePrecision(tsInt64, TSDB_TIME_PRECISION_NANO, toPrecision);
        break;
      default:
        return -1;
    }
  } else {
    return -1;
  }
}

static uint8_t smlGetTimestampLen(int64_t num) {
  uint8_t len = 0;
  while ((num /= 10) != 0) {
    len++;
  }
  len++;
  return len;
}

static int64_t smlParseTSFromJSON(SSmlHandle *info, cJSON *root) {
  // Timestamp must be the first KV to parse
  int32_t toPrecision = info->currSTableMeta ? info->currSTableMeta->tableInfo.precision : TSDB_TIME_PRECISION_NANO;
  cJSON *timestamp = cJSON_GetObjectItem(root, "timestamp");
  if (cJSON_IsNumber(timestamp)) {
    // timestamp value 0 indicates current system time
    double timeDouble = timestamp->valuedouble;
    if (unlikely(smlDoubleToInt64OverFlow(timeDouble))) {
      smlBuildInvalidDataMsg(&info->msgBuf, "timestamp is too large", NULL);
      return -1;
    }

    if (unlikely(timeDouble < 0)) {
      smlBuildInvalidDataMsg(&info->msgBuf,
                             "timestamp is negative", NULL);
      return timeDouble;
    }else if (unlikely(timeDouble == 0)) {
      return taosGetTimestampNs()/smlFactorNS[toPrecision];
    }

    uint8_t tsLen = smlGetTimestampLen((int64_t)timeDouble);
    int8_t fromPrecision = smlGetTsTypeByLen(tsLen);
    if (unlikely(fromPrecision == -1)) {
      smlBuildInvalidDataMsg(&info->msgBuf,
                             "timestamp precision can only be seconds(10 digits) or milli seconds(13 digits)", NULL);
      return -1;
    }

    return convertTimePrecision(timeDouble, fromPrecision, toPrecision);
  } else if (cJSON_IsObject(timestamp)) {
    return smlParseTSFromJSONObj(info, timestamp, toPrecision);
  } else {
    smlBuildInvalidDataMsg(&info->msgBuf,
                           "invalidate json", NULL);
    return -1;
  }
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

static int32_t smlParseColsFromJSON(cJSON *root, SSmlKv *kv) {
  cJSON *metricVal = cJSON_GetObjectItem(root, "value");
  if (metricVal == NULL) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  int32_t ret = smlParseValueFromJSON(metricVal, kv);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseTagsFromJSON(SSmlHandle *info, cJSON *root, SSmlLineInfo *elements) {
  int32_t ret = TSDB_CODE_SUCCESS;

  cJSON *tags = cJSON_GetObjectItem(root, "tags");
  if (unlikely(tags == NULL || tags->type != cJSON_Object)) {
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  // add measure to tags to identify one child table
  cJSON *cMeasure = cJSON_AddStringToObject(tags, JSON_METERS_NAME, elements->measure);
  if(unlikely(cMeasure == NULL)){
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  if(is_same_child_table_json(elements->tags, info->preLine.tags) == 0){
    return TSDB_CODE_SUCCESS;
  }

  bool isSameMeasure = IS_SAME_SUPER_TABLE;

  int     cnt = 0;
  SArray *preLineKV = info->preLineTagKV;
  bool    isSuperKVInit = true;
  SArray *superKV = NULL;
  if(info->dataFormat){
    if(unlikely(!isSameMeasure)){
      SSmlSTableMeta *sMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, elements->measure, elements->measureLen, NULL);

      if(unlikely(sMeta == NULL)){
        sMeta = smlBuildSTableMeta(info->dataFormat);
        STableMeta * pTableMeta = smlGetMeta(info, elements->measure, elements->measureLen);
        sMeta->tableMeta = pTableMeta;
        if(pTableMeta == NULL){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        nodeListSet(&info->superTables, elements->measure, elements->measureLen, sMeta);
      }
      info->currSTableMeta = sMeta->tableMeta;
      superKV = sMeta->tags;

      if(unlikely(taosArrayGetSize(superKV) == 0)){
        isSuperKVInit = false;
      }
      taosArraySetSize(preLineKV, 0);
    }
  }else{
    taosArraySetSize(preLineKV, 0);
  }

  int32_t tagNum = cJSON_GetArraySize(tags);
  for (int32_t i = 0; i < tagNum; ++i) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    if (unlikely(tag == NULL)) {
      return TSDB_CODE_TSC_INVALID_JSON;
    }
    if(unlikely(tag == cMeasure)) continue;
    size_t keyLen = strlen(tag->string);
    if (unlikely(IS_INVALID_COL_LEN(keyLen))) {
      uError("OTD:Tag key length is 0 or too large than 64");
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }

    // add kv to SSmlKv
    SSmlKv kv ={.key = tag->string, .keyLen = keyLen};
    // value
    ret = smlParseValueFromJSON(tag, &kv);
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      return ret;
    }

    if(info->dataFormat){
      if(unlikely(cnt + 1 > info->currSTableMeta->tableInfo.numOfTags)){
        info->needModifySchema = true;
      }

      if(isSameMeasure){
        if(unlikely(cnt >= taosArrayGetSize(preLineKV))) {
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
        SSmlKv *preKV = taosArrayGet(preLineKV, cnt);
        if(unlikely(kv.length > preKV->length)){
          preKV->length = kv.length;
          SSmlSTableMeta *tableMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, elements->measure, elements->measureLen, NULL);
          ASSERT(tableMeta != NULL);

          SSmlKv *oldKV = taosArrayGet(tableMeta->tags, cnt);
          oldKV->length = kv.length;
          info->needModifySchema = true;
        }
        if(unlikely(!IS_SAME_KEY)){
          info->dataFormat = false;
          info->reRun      = true;
          return TSDB_CODE_SUCCESS;
        }
      }else{
        if(isSuperKVInit){
          if(unlikely(cnt >= taosArrayGetSize(superKV))) {
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
          SSmlKv *preKV = taosArrayGet(superKV, cnt);
          if(unlikely(kv.length > preKV->length)) {
            preKV->length = kv.length;
          }else{
            kv.length = preKV->length;
          }
          info->needModifySchema = true;

          if(unlikely(!IS_SAME_KEY)){
            info->dataFormat = false;
            info->reRun      = true;
            return TSDB_CODE_SUCCESS;
          }
        }else{
          taosArrayPush(superKV, &kv);
        }
        taosArrayPush(preLineKV, &kv);
      }
    }else{
      taosArrayPush(preLineKV, &kv);
    }
    cnt++;
  }

  void* oneTable = nodeListGet(info->childTables, elements->tags, POINTER_BYTES, is_same_child_table_json);
  if ((oneTable != NULL)) {
    return TSDB_CODE_SUCCESS;
  }

  SSmlTableInfo *tinfo = smlBuildTableInfo(1, elements->measure, elements->measureLen);
  if (unlikely(!tinfo)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for(int i = 0; i < taosArrayGetSize(preLineKV); i++){
    taosArrayPush(tinfo->tags, taosArrayGet(preLineKV, i));
  }
  smlSetCTableName(tinfo);
  if(info->dataFormat) {
    info->currSTableMeta->uid = tinfo->uid;
    tinfo->tableDataCtx = smlInitTableDataCtx(info->pQuery, info->currSTableMeta);
    if(tinfo->tableDataCtx == NULL){
      smlBuildInvalidDataMsg(&info->msgBuf, "smlInitTableDataCtx error", NULL);
      return TSDB_CODE_SML_INVALID_DATA;
    }
  }

  nodeListSet(&info->childTables, tags, POINTER_BYTES, tinfo);

  return ret;
}

static int32_t smlParseJSONString(SSmlHandle *info, cJSON *root, SSmlLineInfo *elements) {
  int32_t ret = TSDB_CODE_SUCCESS;

  int32_t size = cJSON_GetArraySize(root);
  // outmost json fields has to be exactly 4
  if (unlikely(size != OTD_JSON_FIELDS_NUM)) {
    uError("OTD:0x%" PRIx64 " Invalid number of JSON fields in data point %d", info->id, size);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  // Parse metric
  ret = smlParseMetricFromJSON(info, root, elements);
  if (unlikely(ret != TSDB_CODE_SUCCESS)) {
    uError("OTD:0x%" PRIx64 " Unable to parse metric from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%" PRIx64 " Parse metric from JSON payload finished", info->id);

  // Parse metric value
  SSmlKv kv = {.key = VALUE, .keyLen = VALUE_LEN};
  ret = smlParseColsFromJSON(root, &kv);
  if (unlikely(ret)) {
    uError("OTD:0x%" PRIx64 " Unable to parse metric value from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%" PRIx64 " Parse metric value from JSON payload finished", info->id);

  // Parse tags
  ret = smlParseTagsFromJSON(info, root, elements);
  if (unlikely(ret)) {
    uError("OTD:0x%" PRIx64 " Unable to parse tags from JSON payload", info->id);
    return ret;
  }
  uDebug("OTD:0x%" PRIx64 " Parse tags from JSON payload finished", info->id);

  if(unlikely(info->reRun)){
    return TSDB_CODE_SUCCESS;
  }

  // Parse timestamp
  // notice!!! put ts back to tag to ensure get meta->precision
  int64_t ts = smlParseTSFromJSON(info, root);
  if (unlikely(ts < 0)) {
    uError("OTD:0x%" PRIx64 " Unable to parse timestamp from JSON payload", info->id);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }
  uDebug("OTD:0x%" PRIx64 " Parse timestamp from JSON payload finished", info->id);
  SSmlKv kvTs = { .key = TS, .keyLen = TS_LEN, .i = ts, .type = TSDB_DATA_TYPE_TIMESTAMP, .length = (int16_t)tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes};

  if(info->dataFormat){
    ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kvTs, 0);
    if(ret == TSDB_CODE_SUCCESS){
      ret = smlBuildCol(info->currTableDataCtx, info->currSTableMeta->schema, &kv, 1);
    }
    if(ret == TSDB_CODE_SUCCESS){
      ret = smlBuildRow(info->currTableDataCtx);
    }
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      smlBuildInvalidDataMsg(&info->msgBuf, "smlBuildCol error", NULL);
      return ret;
    }
  }else{
    if(elements->colArray == NULL){
      elements->colArray = taosArrayInit(16, sizeof(SSmlKv));
    }
    taosArrayPush(elements->colArray, &kvTs);
    taosArrayPush(elements->colArray, &kv);
  }
  info->preLine = *elements;

  return TSDB_CODE_SUCCESS;
}
/************* TSDB_SML_JSON_PROTOCOL function end **************/
static int32_t smlParseLineBottom(SSmlHandle *info) {
  if(info->dataFormat) return TSDB_CODE_SUCCESS;

  for(int32_t i = 0; i < info->lineNum; i ++){
    SSmlLineInfo* elements = info->lines + i;
    SSmlTableInfo *tinfo = NULL;
    if(info->protocol != TSDB_SML_JSON_PROTOCOL){
      tinfo = (SSmlTableInfo *)nodeListGet(info->childTables, elements->measure, elements->measureTagsLen, NULL);
    }else{
      tinfo = (SSmlTableInfo *)nodeListGet(info->childTables, elements->tags, POINTER_BYTES, is_same_child_table_json);
    }

    if(tinfo == NULL){
      uError("SML:0x%" PRIx64 "get oneTable failed, line num:%d", info->id, i);
      smlBuildInvalidDataMsg(&info->msgBuf, "get oneTable failed", elements->measure);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    if (taosArrayGetSize(tinfo->tags) > TSDB_MAX_TAGS) {
      smlBuildInvalidDataMsg(&info->msgBuf, "too many tags than 128", NULL);
      return TSDB_CODE_PAR_INVALID_TAGS_NUM;
    }

    if (taosArrayGetSize(elements->colArray) + taosArrayGetSize(tinfo->tags) > TSDB_MAX_COLUMNS) {
      smlBuildInvalidDataMsg(&info->msgBuf, "too many columns than 4096", NULL);
      return TSDB_CODE_PAR_TOO_MANY_COLUMNS;
    }

    int ret = smlPushCols(tinfo->cols, elements->colArray);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    SSmlSTableMeta *tableMeta = (SSmlSTableMeta *)nodeListGet(info->superTables, elements->measure, elements->measureLen, NULL);
    if (tableMeta) {  // update meta
      ret = smlUpdateMeta(tableMeta->colHash, tableMeta->cols, elements->colArray, false, &info->msgBuf);
      if (ret == TSDB_CODE_SUCCESS) {
        ret = smlUpdateMeta(tableMeta->tagHash, tableMeta->tags, tinfo->tags, true, &info->msgBuf);
      }
      if (ret != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " smlUpdateMeta failed", info->id);
        return ret;
      }
    } else {
      ret = smlJudgeDupColName(elements->colArray, tinfo->tags, &info->msgBuf);
      if (ret != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " smlUpdateMeta failed", info->id);
        return ret;
      }

      SSmlSTableMeta *meta = smlBuildSTableMeta(info->dataFormat);
      smlInsertMeta(meta->tagHash, meta->tags, tinfo->tags);
      smlInsertMeta(meta->colHash, meta->cols, elements->colArray);
      nodeListSet(&info->superTables, elements->measure, elements->measureLen, meta);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseJSON(SSmlHandle *info, char *payload) {
  int32_t payloadNum = 0;
  int32_t ret = TSDB_CODE_SUCCESS;

  if (unlikely(payload == NULL)) {
    uError("SML:0x%" PRIx64 " empty JSON Payload", info->id);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  info->root = cJSON_Parse(payload);
  if (unlikely(info->root == NULL)) {
    uError("SML:0x%" PRIx64 " parse json failed:%s", info->id, payload);
    return TSDB_CODE_TSC_INVALID_JSON;
  }
  // multiple data points must be sent in JSON array
  if (cJSON_IsArray(info->root)) {
    payloadNum = cJSON_GetArraySize(info->root);
  } else if (cJSON_IsObject(info->root)) {
    payloadNum = 1;
  } else {
    uError("SML:0x%" PRIx64 " Invalid JSON Payload", info->id);
    return TSDB_CODE_TSC_INVALID_JSON;
  }

  int32_t i = 0;
  while (i < payloadNum) {
    cJSON *dataPoint = (payloadNum == 1 && cJSON_IsObject(info->root)) ? info->root : cJSON_GetArrayItem(info->root, i);
    if(info->dataFormat) {
      SSmlLineInfo element = {0};
      ret = smlParseJSONString(info, dataPoint, &element);
    }else{
      ret = smlParseJSONString(info, dataPoint, info->lines + i);
    }
    if (unlikely(ret != TSDB_CODE_SUCCESS)) {
      uError("SML:0x%" PRIx64 " Invalid JSON Payload", info->id);
      return ret;
    }

    if(unlikely(info->reRun)){
      i = 0;
      info->reRun = false;
      // clear info->childTables
      NodeList* pList = info->childTables;
      while (pList) {
        if(pList->data.used) {
          smlDestroyTableInfo(pList->data.value);
          pList->data.used = false;
        }
        pList = pList->next;
      }

      // clear info->superTables
      pList = info->superTables;
      while (pList) {
        if(pList->data.used) {
          smlDestroySTableMeta(pList->data.value);
          pList->data.used = false;
        }
        pList = pList->next;
      }

      if(unlikely(info->lines != NULL)){
        uError("SML:0x%" PRIx64 " info->lines != NULL", info->id);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      info->lineNum = payloadNum;
      info->lines = taosMemoryCalloc(info->lineNum, sizeof(SSmlLineInfo));

      memset(&info->preLine, 0, sizeof(SSmlLineInfo));
      SVnodeModifOpStmt* stmt= (SVnodeModifOpStmt*)(info->pQuery->pRoot);
      stmt->freeHashFunc(stmt->pTableBlockHashObj);
      stmt->pTableBlockHashObj = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
      continue;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlInsertData(SSmlHandle *info) {
  int32_t code = TSDB_CODE_SUCCESS;

  NodeList* tmp = info->childTables;
  while (tmp) {
    SSmlTableInfo *tableData = (SSmlTableInfo *)tmp->data.value;

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

    SSmlSTableMeta *pMeta =
        (SSmlSTableMeta *)nodeListGet(info->superTables, tableData->sTableName, tableData->sTableNameLen, NULL);
    ASSERT(NULL != pMeta);

    // use tablemeta of stable to save vgid and uid of child table
    pMeta->tableMeta->vgId = vg.vgId;
    pMeta->tableMeta->uid = tableData->uid;  // one table merge data block together according uid

    code = smlBindData(info->pQuery, info->dataFormat, tableData->tags, pMeta->cols, tableData->cols,
                       pMeta->tableMeta, tableData->childTableName, tableData->sTableName, tableData->sTableNameLen,
                       info->ttl, info->msgBuf.buf, info->msgBuf.len);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlBindData failed", info->id);
      return code;
    }
    tmp = tmp->next;
  }

  code = smlBuildOutput(info->pQuery, info->pVgHash);
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

  int32_t i = 0;
  while (i < numLines) {
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
        i++;
        continue;
      }
    }

    uDebug("SML:0x%" PRIx64 " smlParseLine israw:%d, len:%d, sql:%s", info->id, info->isRawLine, len, (info->isRawLine ? "rawdata" : tmp));

    if (info->protocol == TSDB_SML_LINE_PROTOCOL) {
      if(info->dataFormat){
        SSmlLineInfo element = {0};
        code = smlParseInfluxString(info, tmp, tmp + len, &element);
      }else{
        code = smlParseInfluxString(info, tmp, tmp + len, info->lines + i);
      }
    } else if (info->protocol == TSDB_SML_TELNET_PROTOCOL) {
      if(info->dataFormat) {
        SSmlLineInfo element = {0};
        code = smlParseTelnetString(info, (char *)tmp, (char *)tmp + len, &element);
      }else{
        code = smlParseTelnetString(info, (char *)tmp, (char *)tmp + len, info->lines + i);
      }

    } else {
      ASSERT(0);
    }
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%" PRIx64 " smlParseLine failed. line %d : %s", info->id, i, tmp);
      return code;
    }
    if(info->reRun){
      i = 0;
      info->reRun = false;
      // clear info->childTables
      NodeList* pList = info->childTables;
      while (pList) {
        if(pList->data.used) {
          smlDestroyTableInfo(pList->data.value);
          pList->data.used = false;
        }
        pList = pList->next;
      }

      // clear info->superTables
      pList = info->superTables;
      while (pList) {
        if(pList->data.used) {
          smlDestroySTableMeta(pList->data.value);
          pList->data.used = false;
        }
        pList = pList->next;
      }

      if(info->lines != NULL){
        uError("SML:0x%" PRIx64 " info->lines != NULL", info->id);
        return TSDB_CODE_SML_INVALID_DATA;
      }
      info->lines = taosMemoryCalloc(info->lineNum, sizeof(SSmlLineInfo));

      memset(&info->preLine, 0, sizeof(SSmlLineInfo));
      SVnodeModifOpStmt* stmt= (SVnodeModifOpStmt*)(info->pQuery->pRoot);
      stmt->freeHashFunc(stmt->pTableBlockHashObj);
      stmt->pTableBlockHashObj = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
      continue;
    }
    i++;
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

  code = smlParseLineBottom(info);
  if (code != 0) {
    uError("SML:0x%" PRIx64 " smlParseLineBottom error : %s", info->id, tstrerror(code));
    return code;
  }

  info->cost.lineNum = numLines;
  info->cost.numOfSTables = nodeListSize(info->superTables);
  info->cost.numOfCTables = nodeListSize(info->childTables);

  info->cost.schemaTime = taosGetTimestampUs();

  do {
    code = smlModifyDBSchemas(info);
    if (code == 0) break;
  } while (retryNum++ < nodeListSize(info->superTables) * MAX_RETRY_TIMES);

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

TAOS_RES *taos_schemaless_insert_inner(TAOS *taos, char *lines[], char *rawLine, char *rawLineEnd,
                                       int numLines, int protocol, int precision, int32_t ttl, int64_t reqid) {
  if (NULL == taos) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return NULL;
  }

  SRequestObj *request = (SRequestObj *)createRequest(*(int64_t *)taos, TSDB_SQL_INSERT, reqid);
  if (request == NULL) {
    uError("SML:taos_schemaless_insert error request is null");
    return NULL;
  }

  SSmlHandle *info = smlBuildSmlInfo(taos);
  if (info == NULL) {
    request->code = TSDB_CODE_OUT_OF_MEMORY;
    uError("SML:taos_schemaless_insert error SSmlHandle is null");
    goto end;
  }
  info->pRequest = request;
  info->isRawLine = rawLine != NULL;
  info->ttl       = ttl;
  info->precision = precision;
  info->protocol = protocol;
  info->msgBuf.buf = info->pRequest->msgBuf;
  info->msgBuf.len = ERROR_MSG_BUF_DEFAULT_SIZE;
  info->lineNum = numLines;

  SSmlMsgBuf msg = {ERROR_MSG_BUF_DEFAULT_SIZE, request->msgBuf};
  if (request->pDb == NULL) {
    request->code = TSDB_CODE_PAR_DB_NOT_SPECIFIED;
    smlBuildInvalidDataMsg(&msg, "Database not specified", NULL);
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

  int32_t code = smlProcess(info, lines, rawLine, rawLineEnd, numLines);
  request->code = code;
  info->cost.endTime = taosGetTimestampUs();
  info->cost.code = code;
  smlPrintStatisticInfo(info);

end:
  uDebug("resultend:%s", request->msgBuf);
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
  return taos_schemaless_insert_inner(taos, lines, NULL, NULL, numLines, protocol, precision, ttl, reqid);
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
  return taos_schemaless_insert_inner(taos, NULL, lines, lines + len, *totalRows, protocol, precision, ttl, reqid);
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

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "query.h"
#include "taos.h"
#include "taoserror.h"
#include "tdef.h"
#include "tlog.h"
#include "tmsg.h"
#include "tstrbuild.h"
#include "ttime.h"
#include "ttypes.h"
#include "tcommon.h"
#include "catalog.h"
#include "clientInt.h"
//=================================================================================================

#define SPACE ' '
#define COMMA ','
#define EQUAL '='
#define QUOTE '"'
#define SLASH '\\'
#define tsMaxSQLStringLen (1024*1024)

//=================================================================================================
typedef TSDB_SML_PROTOCOL_TYPE SMLProtocolType;

typedef enum {
  SCHEMA_ACTION_CREATE_STABLE,
  SCHEMA_ACTION_ADD_COLUMN,
  SCHEMA_ACTION_ADD_TAG,
  SCHEMA_ACTION_CHANGE_COLUMN_SIZE,
  SCHEMA_ACTION_CHANGE_TAG_SIZE,
} ESchemaAction;

typedef struct {
  char sTableName[TSDB_TABLE_NAME_LEN];
  SArray *tags;
  SArray *fields;
} SCreateSTableActionInfo;

typedef struct {
  char sTableName[TSDB_TABLE_NAME_LEN];
  SSmlKv * field;
} SAlterSTableActionInfo;

typedef struct {
  ESchemaAction action;
  union {
    SCreateSTableActionInfo createSTable;
    SAlterSTableActionInfo alterSTable;
  };
} SSchemaAction;

typedef struct {
  const char* measure;
  const char* tags;
  const char* cols;
  const char* timestamp;

  int32_t measureLen;
  int32_t measureTagsLen;
  int32_t tagsLen;
  int32_t colsLen;
  int32_t timestampLen;
} SSmlLineInfo;

typedef struct {
  const char     *sTableName;   // super table name
  uint8_t        sTableNameLen;
  char           childTableName[TSDB_TABLE_NAME_LEN];
  uint64_t       uid;

  SArray         *tags;

  // colsFormat store cols formated, for quick parse, if info->formatData is true
  SArray         *colsFormat;  // elements are SArray<SSmlKv*>

  // cols store cols un formated
  SArray         *cols;        // elements are SHashObj<cols key string, SSmlKv*> for find by key quickly
} SSmlTableInfo;

typedef struct {
  SArray     *tags;       // save the origin order to create table
  SHashObj   *tagHash;    // elements are <key, index in tags>

  SArray     *cols;
  SHashObj   *fieldHash;

  STableMeta *tableMeta;
} SSmlSTableMeta;

typedef struct {
  int32_t   len;
  char      *buf;
} SSmlMsgBuf;

typedef struct {
  int32_t code;
  int32_t lineNum;

  int32_t numOfSTables;
  int32_t numOfCTables;
  int32_t numOfCreateSTables;

  int64_t parseTime;
  int64_t schemaTime;
  int64_t insertBindTime;
  int64_t insertRpcTime;
  int64_t endTime;
} SSmlCostInfo;

typedef struct {
  uint64_t          id;

  SMLProtocolType   protocol;
  int8_t            precision;
  bool              dataFormat;     // true means that the name, number and order of keys in each line are the same

  SHashObj          *childTables;
  SHashObj          *superTables;
  SHashObj          *pVgHash;
  void              *exec;

  STscObj           *taos;
  SCatalog          *pCatalog;
  SRequestObj       *pRequest;
  SQuery            *pQuery;

  SSmlCostInfo      cost;
  int32_t           affectedRows;
  SSmlMsgBuf        msgBuf;
  SHashObj          *dumplicateKey;  // for dumplicate key
  SArray            *colsContainer;  // for cols parse, if is dataFormat == false
} SSmlHandle;
//=================================================================================================

static uint64_t linesSmlHandleId = 0;
static const char* TS = "_ts";
static const char* TAG = "_tagNone";

//=================================================================================================

static uint64_t smlGenId() {
  uint64_t id;

  do {
    id = atomic_add_fetch_64(&linesSmlHandleId, 1);
  } while (id == 0);

  return id;
}

static int32_t smlBuildInvalidDataMsg(SSmlMsgBuf* pBuf, const char *msg1, const char *msg2) {
  if(msg1) strncat(pBuf->buf, msg1, pBuf->len);
  int32_t left = pBuf->len - strlen(pBuf->buf);
  if(left > 2 && msg2) {
    strncat(pBuf->buf, ":", left - 1);
    strncat(pBuf->buf, msg2, left - 2);
  }
  return TSDB_CODE_SML_INVALID_DATA;
}

static int smlCompareKv(const void* p1, const void* p2) {
  SSmlKv* kv1 = *(SSmlKv**)p1;
  SSmlKv* kv2 = *(SSmlKv**)p2;
  int32_t kvLen1 = kv1->keyLen;
  int32_t kvLen2 = kv2->keyLen;
  int32_t res = strncasecmp(kv1->key, kv2->key, TMIN(kvLen1, kvLen2));
  if (res != 0) {
    return res;
  } else {
    return kvLen1-kvLen2;
  }
}

static void smlBuildChildTableName(SSmlTableInfo *tags) {
  int32_t size = taosArrayGetSize(tags->tags);
  ASSERT(size > 0);
  taosArraySort(tags->tags, smlCompareKv);

  SStringBuilder sb = {0};
  taosStringBuilderAppendStringLen(&sb, tags->sTableName, tags->sTableNameLen);
  for (int j = 0; j < size; ++j) {
    SSmlKv *tagKv = taosArrayGetP(tags->tags, j);
    taosStringBuilderAppendStringLen(&sb, tagKv->key, tagKv->keyLen);
    taosStringBuilderAppendStringLen(&sb, tagKv->value, tagKv->valueLen);
  }
  size_t len = 0;
  char* keyJoined = taosStringBuilderGetResult(&sb, &len);
  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, (uint8_t *)keyJoined, (uint32_t)len);
  tMD5Final(&context);
  uint64_t digest1 = *(uint64_t*)(context.digest);
  //uint64_t digest2 = *(uint64_t*)(context.digest + 8);
  //snprintf(tags->childTableName, TSDB_TABLE_NAME_LEN, "t_%016"PRIx64"%016"PRIx64, digest1, digest2);
  snprintf(tags->childTableName, TSDB_TABLE_NAME_LEN, "t_%016"PRIx64, digest1);
  taosStringBuilderDestroy(&sb);
  tags->uid = digest1;
}

static int32_t smlGenerateSchemaAction(SSchema* pointColField, SHashObj* dbAttrHash, SArray* dbAttrArray, bool isTag, char sTableName[],
                                       SSchemaAction* action, bool* actionNeeded, SSmlHandle* info) {
//  char fieldName[TSDB_COL_NAME_LEN] = {0};
//  strcpy(fieldName, pointColField->name);
//
//  size_t* pDbIndex = taosHashGet(dbAttrHash, fieldName, strlen(fieldName));
//  if (pDbIndex) {
//    SSchema* dbAttr = taosArrayGet(dbAttrArray, *pDbIndex);
//    assert(strcasecmp(dbAttr->name, pointColField->name) == 0);
//    if (pointColField->type != dbAttr->type) {
//      uError("SML:0x%"PRIx64" point type and db type mismatch. key: %s. point type: %d, db type: %d", info->id, pointColField->name,
//               pointColField->type, dbAttr->type);
//      return TSDB_CODE_TSC_INVALID_VALUE;
//    }
//
//    if (IS_VAR_DATA_TYPE(pointColField->type) && (pointColField->bytes > dbAttr->bytes)) {
//      if (isTag) {
//        action->action = SCHEMA_ACTION_CHANGE_TAG_SIZE;
//      } else {
//        action->action = SCHEMA_ACTION_CHANGE_COLUMN_SIZE;
//      }
//      memset(&action->alterSTable, 0,  sizeof(SAlterSTableActionInfo));
//      memcpy(action->alterSTable.sTableName, sTableName, TSDB_TABLE_NAME_LEN);
//      action->alterSTable.field = pointColField;
//      *actionNeeded = true;
//    }
//  } else {
//    if (isTag) {
//      action->action = SCHEMA_ACTION_ADD_TAG;
//    } else {
//      action->action = SCHEMA_ACTION_ADD_COLUMN;
//    }
//    memset(&action->alterSTable, 0, sizeof(SAlterSTableActionInfo));
//    memcpy(action->alterSTable.sTableName, sTableName, TSDB_TABLE_NAME_LEN);
//    action->alterSTable.field = pointColField;
//    *actionNeeded = true;
//  }
//  if (*actionNeeded) {
//    uDebug("SML:0x%" PRIx64 " generate schema action. column name: %s, action: %d", info->id, fieldName,
//             action->action);
//  }
  return 0;
}

static int32_t smlBuildColumnDescription(SSmlKv* field, char* buf, int32_t bufSize, int32_t* outBytes) {
  uint8_t type = field->type;
  char    tname[TSDB_TABLE_NAME_LEN] = {0};
  memcpy(tname, field->key, field->keyLen);
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    int32_t bytes = field->valueLen;   // todo
    int out = snprintf(buf, bufSize,"%s %s(%d)",
                       tname,tDataTypes[field->type].name, bytes);
    *outBytes = out;
  } else {
    int out = snprintf(buf, bufSize, "%s %s", tname, tDataTypes[type].name);
    *outBytes = out;
  }

  return 0;
}

static int32_t smlApplySchemaAction(SSmlHandle* info, SSchemaAction* action) {
  int32_t code = 0;
  int32_t outBytes = 0;
  char *result = (char *)taosMemoryCalloc(1, tsMaxSQLStringLen+1);
  int32_t capacity = tsMaxSQLStringLen +  1;

  uDebug("SML:0x%"PRIx64" apply schema action. action: %d", info->id, action->action);
  switch (action->action) {
    case SCHEMA_ACTION_ADD_COLUMN: {
      int n = sprintf(result, "alter stable %s add column ", action->alterSTable.sTableName);
      smlBuildColumnDescription(action->alterSTable.field, result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(info->taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      const char* errStr = taos_errstr(res);
      char* begin = strstr(errStr, "duplicated column names");
      bool tscDupColNames = (begin != NULL);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error: %s", info->id, errStr);
      }
      taos_free_result(res);

//      if (code == TSDB_CODE_MND_FIELD_ALREADY_EXIST || code == TSDB_CODE_MND_TAG_ALREADY_EXIST || tscDupColNames) {
      if (code == TSDB_CODE_MND_TAG_ALREADY_EXIST || tscDupColNames) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }
    case SCHEMA_ACTION_ADD_TAG: {
      int n = sprintf(result, "alter stable %s add tag ", action->alterSTable.sTableName);
      smlBuildColumnDescription(action->alterSTable.field,
                             result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(info->taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      const char* errStr = taos_errstr(res);
      char* begin = strstr(errStr, "duplicated column names");
      bool tscDupColNames = (begin != NULL);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

//      if (code ==TSDB_CODE_MND_TAG_ALREADY_EXIST || code == TSDB_CODE_MND_FIELD_ALREAY_EXIST || tscDupColNames) {
      if (code ==TSDB_CODE_MND_TAG_ALREADY_EXIST || tscDupColNames) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }
    case SCHEMA_ACTION_CHANGE_COLUMN_SIZE: {
      int n = sprintf(result, "alter stable %s modify column ", action->alterSTable.sTableName);
      smlBuildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(info->taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

//      if (code == TSDB_CODE_MND_INVALID_COLUMN_LENGTH || code == TSDB_CODE_TSC_INVALID_COLUMN_LENGTH) {
      if (code == TSDB_CODE_TSC_INVALID_COLUMN_LENGTH) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }
    case SCHEMA_ACTION_CHANGE_TAG_SIZE: {
      int n = sprintf(result, "alter stable %s modify tag ", action->alterSTable.sTableName);
      smlBuildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(info->taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

//      if (code == TSDB_CODE_MND_INVALID_TAG_LENGTH || code == TSDB_CODE_TSC_INVALID_TAG_LENGTH) {
      if (code == TSDB_CODE_TSC_INVALID_TAG_LENGTH) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }
    case SCHEMA_ACTION_CREATE_STABLE: {
      int n = sprintf(result, "create stable %s (", action->createSTable.sTableName);
      char* pos = result + n; int freeBytes = capacity - n;

      SArray *cols = action->createSTable.fields;

      for(int i = 0; i < taosArrayGetSize(cols); i++){
        SSmlKv *kv = taosArrayGetP(cols, i);
        smlBuildColumnDescription(kv, pos, freeBytes, &outBytes);
        pos += outBytes; freeBytes -= outBytes;
        *pos = ','; ++pos; --freeBytes;
      }

      --pos; ++freeBytes;

      outBytes = snprintf(pos, freeBytes, ") tags (");
      pos += outBytes; freeBytes -= outBytes;

      cols = action->createSTable.tags;
      for(int i = 0; i < taosArrayGetSize(cols); i++){
        SSmlKv *kv = taosArrayGetP(cols, i);
        smlBuildColumnDescription(kv, pos, freeBytes, &outBytes);
        pos += outBytes; freeBytes -= outBytes;
        *pos = ','; ++pos; --freeBytes;
      }
      pos--; ++freeBytes;
      outBytes = snprintf(pos, freeBytes, ")");
      TAOS_RES* res = taos_query(info->taos, result);
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_STB_ALREADY_EXIST) {
        TAOS_RES* res2 = taos_query(info->taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          uError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }

    default:
      break;
  }

  taosMemoryFreeClear(result);
  if (code != 0) {
    uError("SML:0x%"PRIx64 " apply schema action failure. %s", info->id, tstrerror(code));
  }
  return code;
}

static int32_t smlModifyDBSchemas(SSmlHandle* info) {
  int32_t code = 0;

  SSmlSTableMeta** tableMetaSml = taosHashIterate(info->superTables, NULL);
  while (tableMetaSml) {
    SSmlSTableMeta* sTableData = *tableMetaSml;

    STableMeta *pTableMeta = NULL;
    SEpSet ep = getEpSet_s(&info->taos->pAppInfo->mgmtEp);

    size_t superTableLen = 0;
    void *superTable = taosHashGetKey(tableMetaSml, &superTableLen);    // todo escape
    SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
    strcpy(pName.dbname, info->pRequest->pDb);
    memcpy(pName.tname, superTable, superTableLen);

    code = catalogGetSTableMeta(info->pCatalog, info->taos->pAppInfo->pTransporter, &ep, &pName, &pTableMeta);

    if (code == TSDB_CODE_TDB_INVALID_TABLE_ID || code == TSDB_CODE_MND_INVALID_STB) {
      SSchemaAction schemaAction = {0};
      schemaAction.action = SCHEMA_ACTION_CREATE_STABLE;
      memcpy(schemaAction.createSTable.sTableName, superTable, superTableLen);
      schemaAction.createSTable.tags = sTableData->tags;
      schemaAction.createSTable.fields = sTableData->cols;
      code = smlApplySchemaAction(info, &schemaAction);
      if (code != 0) {
        uError("SML:0x%"PRIx64" smlApplySchemaAction failed. can not create %s", info->id, schemaAction.createSTable.sTableName);
        return code;
      }

      code = catalogGetSTableMeta(info->pCatalog, info->taos->pAppInfo->pTransporter, &ep, &pName, &pTableMeta);
      if (code != 0) {
        uError("SML:0x%"PRIx64" catalogGetSTableMeta failed. super table name %s", info->id, schemaAction.createSTable.sTableName);
        return code;
      }
      info->cost.numOfCreateSTables++;
    }else if (code == TSDB_CODE_SUCCESS) {
    } else {
      uError("SML:0x%"PRIx64" load table meta error: %s", info->id, tstrerror(code));
      return code;
    }
    sTableData->tableMeta = pTableMeta;

    tableMetaSml = taosHashIterate(info->superTables, tableMetaSml);
  }
  return 0;
}

//=========================================================================

/*        Field                          Escape charaters
    1: measurement                        Comma,Space
    2: tag_key, tag_value, field_key  Comma,Equal Sign,Space
    3: field_value                    Double quote,Backslash
*/
//static void escapeSpecialCharacter(uint8_t field, const char **pos) {
//  const char *cur = *pos;
//  if (*cur != '\\') {
//    return;
//  }
//  switch (field) {
//    case 1:
//      switch (*(cur + 1)) {
//        case ',':
//        case ' ':
//          cur++;
//          break;
//        default:
//          break;
//      }
//      break;
//    case 2:
//      switch (*(cur + 1)) {
//        case ',':
//        case ' ':
//        case '=':
//          cur++;
//          break;
//        default:
//          break;
//      }
//      break;
//    case 3:
//      switch (*(cur + 1)) {
//        case '"':
//        case '\\':
//          cur++;
//          break;
//        default:
//          break;
//      }
//      break;
//    default:
//      break;
//  }
//  *pos = cur;
//}

static bool smlParseTinyInt(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 2) {
    return false;
  }
  const char *signalPos = pVal + len - 2;
  if (!strncasecmp(signalPos, "i8", 2)) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid tiny int", endptr);
    }else if(!IS_VALID_TINYINT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "tiny int out of range[-128,127]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseTinyUint(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 2) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  const char *signalPos = pVal + len - 2;
  if (!strncasecmp(signalPos, "u8", 2)) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid unsigned tiny int", endptr);
    }else if(!IS_VALID_UTINYINT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "unsigned tiny int out of range[0,255]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseSmallInt(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (!strncasecmp(signalPos, "i16", 3)) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid small int", endptr);
    }else if(!IS_VALID_SMALLINT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "small int our of range[-32768,32767]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseSmallUint(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (strncasecmp(signalPos, "u16", 3) == 0) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid unsigned small int", endptr);
    }else if(!IS_VALID_USMALLINT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "unsigned small int out of rang[0,65535]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseInt(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (strncasecmp(signalPos, "i32", 3) == 0) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid int", endptr);
    }else if(!IS_VALID_INT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "int out of range[-2147483648,2147483647]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseUint(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (strncasecmp(signalPos, "u32", 3) == 0) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid unsigned int", endptr);
    }else if(!IS_VALID_UINT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "unsigned int out of range[0,4294967295]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseBigInt(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len > 3 && strncasecmp(pVal + len - 3, "i64", 3) == 0) {
    char *endptr = NULL;
    errno = 0;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != pVal + len - 3){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid big int", endptr);
    }else if(errno == ERANGE || !IS_VALID_BIGINT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "big int out of range[-9223372036854775808,9223372036854775807]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }else if (len > 1 && pVal[len - 1] == 'i') {
    char *endptr = NULL;
    errno = 0;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != pVal + len - 1){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid big int", endptr);
    }else if(errno == ERANGE || !IS_VALID_BIGINT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "big int out of range[-9223372036854775808,9223372036854775807]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseBigUint(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (strncasecmp(signalPos, "u64", 3) == 0) {
    char *endptr = NULL;
    errno = 0;
    uint64_t result = strtoull(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid unsigned big int", endptr);
    }else if(errno == ERANGE || !IS_VALID_UBIGINT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "unsigned big int out of range[0,18446744073709551615]", endptr);
    }else{
      kvVal->u = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseFloat(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  char *endptr = NULL;
  errno = 0;
  float result = strtof(pVal, &endptr);
  if(endptr == pVal + len && errno != ERANGE && IS_VALID_FLOAT(result)){       // 78
    kvVal->f = result;
    *isValid = true;
    return true;
  }

  if (len > 3 && strncasecmp(pVal + len - 3, "f32", 3) == 0) {
    if(endptr != pVal + len - 3){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid float", endptr);
    }else if(errno == ERANGE || !IS_VALID_FLOAT(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "float out of range[-3.402823466e+38,3.402823466e+38]", endptr);
    }else{
      kvVal->f = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseDouble(SSmlKv *kvVal, bool *isValid, SSmlMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (strncasecmp(signalPos, "f64", 3) == 0) {
    char *endptr = NULL;
    errno = 0;
    double result = strtod(pVal, &endptr);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "invalid double", endptr);
    }else if(errno == ERANGE || !IS_VALID_DOUBLE(result)){
      *isValid = false;
      smlBuildInvalidDataMsg(msg, "double out of range[-1.7976931348623158e+308,1.7976931348623158e+308]", endptr);
    }else{
      kvVal->d = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool smlParseBool(SSmlKv *kvVal) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if ((len == 1) && pVal[len - 1] == 't') {
    kvVal->i = true;
    return true;
  }

  if ((len == 1) && pVal[len - 1] == 'f') {
    kvVal->i = false;
    return true;
  }

  if((len == 4) && !strncasecmp(pVal, "true", len)) {
    kvVal->i = true;
    return true;
  }
  if((len == 5) && !strncasecmp(pVal, "false", len)) {
    kvVal->i = false;
    return true;
  }
  return false;
}

static bool smlIsBinary(const char *pVal, uint16_t len) {
  //binary: "abc"
  if (len < 2) {
    return false;
  }
  if (pVal[0] == '"' && pVal[len - 1] == '"') {
    return true;
  }
  return false;
}

static bool smlIsNchar(const char *pVal, uint16_t len) {
  //nchar: L"abc"
  if (len < 3) {
    return false;
  }
  if ((pVal[0] == 'l' || pVal[0] == 'L')&& pVal[1] == '"' && pVal[len - 1] == '"') {
    return true;
  }
  return false;
}

static bool smlParseValue(SSmlKv *pVal, SSmlMsgBuf *msg) {
  // put high probability matching type first
  bool isValid = false;

  //binary
  if (smlIsBinary(pVal->value, pVal->valueLen)) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
    pVal->valueLen -= 2;
    pVal->length = pVal->valueLen;
    pVal->value++;
    return true;
  }
  //nchar
  if (smlIsNchar(pVal->value, pVal->valueLen)) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
    pVal->valueLen -= 3;
    pVal->length = pVal->valueLen;
    pVal->value += 2;
    return true;
  }
  //float
  if (smlParseFloat(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_FLOAT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  //double
  if (smlParseDouble(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_DOUBLE;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;

    return true;
  }
  //bool
  if (smlParseBool(pVal)) {
    pVal->type = TSDB_DATA_TYPE_BOOL;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }

  if (smlParseTinyInt(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_TINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (smlParseTinyUint(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_UTINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (smlParseSmallInt(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_SMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (smlParseSmallUint(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_USMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (smlParseInt(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_INT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (smlParseUint(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_UINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (smlParseBigInt(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_BIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (smlParseBigUint(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_UBIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }

  smlBuildInvalidDataMsg(msg, "invalid data", pVal->value);
  return false;
}

static bool checkDuplicateKey(char *key, SHashObj *pHash, SSmlHandle* info) {
  char *val = NULL;
  val = taosHashGet(pHash, key, strlen(key));
  if (val) {
    uError("SML:0x%"PRIx64" Duplicate key detected:%s", info->id, key);
    return true;
  }

  uint8_t dummy_val = 0;
  taosHashPut(pHash, key, strlen(key), &dummy_val, sizeof(uint8_t));

  return false;
}

static int32_t smlParseString(const char* sql, SSmlLineInfo *elements, SSmlMsgBuf *msg){
  if(!sql) return TSDB_CODE_SML_INVALID_DATA;
  while (*sql != '\0') {           // jump the space at the begining
    if(*sql != SPACE) {
      elements->measure = sql;
      break;
    }
    sql++;
  }
  if (!elements->measure || *sql == COMMA) {
    smlBuildInvalidDataMsg(msg, "invalid data", sql);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  // parse measure and tag
  while (*sql != '\0') {
    if (elements->measureLen == 0 && *sql == COMMA && *(sql - 1) != SLASH) {  // find the first comma
      elements->measureLen = sql - elements->measure;
      sql++;
      elements->tags = sql;
      continue;
    }

    if (*sql == SPACE && *(sql - 1) != SLASH) {   // find the first space
      if (elements->measureLen == 0) {
        elements->measureLen = sql - elements->measure;
        elements->tags = sql;
      }
      elements->tagsLen = sql - elements->tags;
      elements->measureTagsLen = sql - elements->measure;
      break;
    }

    sql++;
  }
  if(elements->tagsLen == 0){     // measure, cols1=a         measure cols1=a
    elements->measureTagsLen = elements->measureLen;
  }
  if(elements->measureLen == 0) {
    smlBuildInvalidDataMsg(msg, "invalid measure", elements->measure);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  // parse cols
  while (*sql != '\0') {
    if(*sql != SPACE) {
      elements->cols = sql;
      break;
    }
    sql++;
  }
  if(!elements->cols) {
    smlBuildInvalidDataMsg(msg, "invalid columns", elements->cols);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  bool isInQuote = false;
  while (*sql != '\0') {
    if(*sql == QUOTE && *(sql - 1) != SLASH){
      isInQuote = !isInQuote;
    }
    if(!isInQuote && *sql == SPACE && *(sql - 1) != SLASH) {
      break;
    }
    sql++;
  }
  if(isInQuote){
    smlBuildInvalidDataMsg(msg, "only one quote", elements->cols);
    return TSDB_CODE_SML_INVALID_DATA;
  }
  elements->colsLen = sql - elements->cols;

  // parse ts,ts can be empty
  while (*sql != '\0') {
    if(*sql != SPACE && elements->timestamp == NULL) {
      elements->timestamp = sql;
    }
    if(*sql == SPACE && elements->timestamp != NULL){
      break;
    }
    sql++;
  }
  if(elements->timestamp){
    elements->timestampLen = sql - elements->timestamp;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlParseCols(const char* data, int32_t len, SArray *cols, bool isTag, SHashObj *dumplicateKey, SSmlMsgBuf *msg){
  if(isTag && len == 0){
    SSmlKv *kv = taosMemoryCalloc(sizeof(SSmlKv), 1);
    kv->key = TAG;
    kv->keyLen = strlen(TAG);
    kv->value = TAG;
    kv->valueLen = strlen(TAG);
    kv->type = TSDB_DATA_TYPE_NCHAR;
    if(cols) taosArrayPush(cols, &kv);
    return TSDB_CODE_SUCCESS;
  }

  for(int i = 0; i < len; i++){
    // parse key
    const char *key = data + i;
    int32_t keyLen = 0;
    while(i < len){
      if(data[i] == EQUAL && i > 0 && data[i-1] != SLASH){
        keyLen = data + i - key;
        break;
      }
      i++;
    }
    if(keyLen == 0 || keyLen >= TSDB_COL_NAME_LEN){
      smlBuildInvalidDataMsg(msg, "invalid key or key is too long than 64", key);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    if(taosHashGet(dumplicateKey, key, keyLen)){
      smlBuildInvalidDataMsg(msg, "dumplicate key", key);
      return TSDB_CODE_SML_INVALID_DATA;
    }else{
      taosHashPut(dumplicateKey, key, keyLen, key, CHAR_BYTES);
    }

    // parse value
    i++;
    const char *value = data + i;
    bool isInQuote = false;
    while(i < len){
      if(data[i] == QUOTE && data[i-1] != SLASH){
        isInQuote = !isInQuote;
      }
      if(!isInQuote && data[i] == COMMA && i > 0 && data[i-1] != SLASH){
        break;
      }
      i++;
    }
    if(isInQuote){
      smlBuildInvalidDataMsg(msg, "only one quote", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    int32_t valueLen = data + i - value;
    if(valueLen == 0){
      smlBuildInvalidDataMsg(msg, "invalid value", value);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    // add kv to SSmlKv
    SSmlKv *kv = taosMemoryCalloc(sizeof(SSmlKv), 1);
    kv->key = key;
    kv->keyLen = keyLen;
    kv->value = value;
    kv->valueLen = valueLen;
    if(isTag){
      kv->type = TSDB_DATA_TYPE_NCHAR;
    }else{
      if(!smlParseValue(kv, msg)){
        return TSDB_CODE_SML_INVALID_DATA;
      }
    }

    if(cols) taosArrayPush(cols, &kv);
  }

  return TSDB_CODE_SUCCESS;
}

static int64_t smlGetTimeValue(const char *value, int32_t len, int8_t type) {
  char *endPtr = NULL;
  double ts = (double)strtoll(value, &endPtr, 10);
  if(value + len != endPtr){
    return -1;
  }
  switch (type) {
    case TSDB_TIME_PRECISION_HOURS:
      ts *= (3600 * 1e9);
      break;
    case TSDB_TIME_PRECISION_MINUTES:
      ts *= (60 * 1e9);
      break;
    case TSDB_TIME_PRECISION_SECONDS:
      ts *= (1e9);
      break;
    case TSDB_TIME_PRECISION_MILLI:
      ts *= (1e6);
      break;
    case TSDB_TIME_PRECISION_MICRO:
      ts *= (1e3);
      break;
    case TSDB_TIME_PRECISION_NANO:
      break;
    default:
      ASSERT(0);
  }
  if(ts > (double)INT64_MAX || ts < 0){
    return -1;
  }

  return (int64_t)ts;
}

static int64_t smlGetTimeNow(int8_t precision) {
  switch (precision) {
    case TSDB_TIME_PRECISION_HOURS:
      return taosGetTimestampMs()/1000/3600;
    case TSDB_TIME_PRECISION_MINUTES:
      return taosGetTimestampMs()/1000/60;
    case TSDB_TIME_PRECISION_SECONDS:
      return taosGetTimestampMs()/1000;
    case TSDB_TIME_PRECISION_MILLI:
    case TSDB_TIME_PRECISION_MICRO:
    case TSDB_TIME_PRECISION_NANO:
      return taosGetTimestamp(precision);
    default:
      ASSERT(0);
  }
}

static int8_t smlGetTsTypeByLen(int32_t len) {
  if (len == TSDB_TIME_PRECISION_SEC_DIGITS) {
    return TSDB_TIME_PRECISION_SECONDS;
  } else if (len == TSDB_TIME_PRECISION_MILLI_DIGITS) {
    return TSDB_TIME_PRECISION_MILLI_DIGITS;
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

static int64_t smlParseInfluxTime(SSmlHandle* info, const char* data, int32_t len){
  int8_t tsType = smlGetTsTypeByPrecision(info->precision);
  if (tsType == -1) {
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp precision", NULL);
    return -1;
  }
  if(!data){
    return smlGetTimeNow(tsType);
  }

  int64_t ts = smlGetTimeValue(data, len, tsType);
  if(ts == -1){
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

static int64_t smlParseOpenTsdbTime(SSmlHandle* info, const char* data, int32_t len){
  if(!data){
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp can not be null", NULL);
    return -1;
  }
  int8_t tsType = smlGetTsTypeByLen(len);
  if (tsType == -1) {
    smlBuildInvalidDataMsg(&info->msgBuf, "timestamp precision can only be seconds(10 digits) or milli seconds(13 digits)", data);
    return -1;
  }
  int64_t ts = smlGetTimeValue(data, len, tsType);
  if(ts == -1){
    smlBuildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
    return -1;
  }
  return ts;
}

static int32_t smlParseTS(SSmlHandle* info, const char* data, int32_t len, SArray *cols){
  int64_t ts = 0;
  if(info->protocol == TSDB_SML_LINE_PROTOCOL){
    ts = smlParseInfluxTime(info, data, len);
  }else{
    ts = smlParseOpenTsdbTime(info, data, len);
  }
  if(ts == -1)  return TSDB_CODE_TSC_INVALID_TIME_STAMP;

  // add ts to
  SSmlKv *kv = taosMemoryCalloc(sizeof(SSmlKv), 1);
  if(!kv){
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  kv->key = TS;
  kv->keyLen = strlen(kv->key);
  kv->i = ts;
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
  kv->length = (int16_t)tDataTypes[kv->type].bytes;
  if(cols) taosArrayPush(cols, &kv);
  return TSDB_CODE_SUCCESS;
}

//static int32_t parseSmlCols(const char* data, SArray *cols){
//  while(*data != '\0'){
//    if(*data == EQUAL) return TSDB_CODE_SML_INVALID_DATA;
//    const char *key = data;
//    int32_t keyLen = 0;
//    while(*data != '\0'){
//      if(*data == EQUAL && *(data-1) != SLASH){
//        keyLen = data - key;
//        data ++;
//        break;
//      }
//      data++;
//    }
//    if(keyLen == 0){
//      return TSDB_CODE_SML_INVALID_DATA;
//    }
//
//    if(*data == COMMA) return TSDB_CODE_SML_INVALID_DATA;
//    const char *value = data;
//    int32_t valueLen = 0;
//    while(*data != '\0'){
//      if(*data == COMMA && *(data-1) != SLASH){
//        valueLen = data - value;
//        data ++;
//        break;
//      }
//      data++;
//    }
//    if(valueLen == 0){
//      return TSDB_CODE_SML_INVALID_DATA;
//    }
//
//    TAOS_SML_KV *kv = taosMemoryCalloc(sizeof(TAOS_SML_KV), 1);
//    kv->key = key;
//    kv->keyLen = keyLen;
//    kv->value = value;
//    kv->valueLen = valueLen;
//    kv->type = TSDB_DATA_TYPE_NCHAR;
//    if(cols) taosArrayPush(cols, &kv);
//  }
//  return TSDB_CODE_SUCCESS;
//}

static bool smlUpdateMeta(SSmlSTableMeta* tableMeta, SArray *tags, SArray *cols, SSmlMsgBuf *msg){
  if(tags){
    for (int i = 0; i < taosArrayGetSize(tags); ++i) {
      SSmlKv *kv = taosArrayGetP(tags, i);
      ASSERT(kv->type == TSDB_DATA_TYPE_NCHAR);

      uint8_t *index = taosHashGet(tableMeta->tagHash, kv->key, kv->keyLen);
      if(index){
        SSmlKv **value = taosArrayGet(tableMeta->tags, *index);
        ASSERT((*value)->type == TSDB_DATA_TYPE_NCHAR);
        if(kv->valueLen > (*value)->valueLen){    // tags type is nchar
          *value = kv;
        }
      }else{
        size_t tmp = taosArrayGetSize(tableMeta->tags);
        ASSERT(tmp <= UINT8_MAX);
        uint8_t size = tmp;
        taosArrayPush(tableMeta->tags, &kv);
        taosHashPut(tableMeta->tagHash, kv->key, kv->keyLen, &size, CHAR_BYTES);
      }
    }
  }

  if(cols){
    for (int i = 1; i < taosArrayGetSize(cols); ++i) {  //jump timestamp
      SSmlKv *kv = taosArrayGetP(cols, i);

      int16_t *index = taosHashGet(tableMeta->fieldHash, kv->key, kv->keyLen);
      if(index){
        SSmlKv **value = taosArrayGet(tableMeta->cols, *index);
        if(kv->type != (*value)->type){
          smlBuildInvalidDataMsg(msg, "the type is not the same like before", kv->key);
          return false;
        }else{
          if(IS_VAR_DATA_TYPE(kv->type)){     // update string len, if bigger
            if(kv->valueLen > (*value)->valueLen){
              *value = kv;
            }
          }
        }
      }else{
        size_t tmp = taosArrayGetSize(tableMeta->cols);
        ASSERT(tmp <= INT16_MAX);
        int16_t size = tmp;
        taosArrayPush(tableMeta->cols, &kv);
        taosHashPut(tableMeta->fieldHash, kv->key, kv->keyLen, &size, SHORT_BYTES);
      }
    }
  }
  return true;
}

static void smlInsertMeta(SSmlSTableMeta* tableMeta, SArray *tags, SArray *cols){
  if(tags){
    for (uint8_t i = 0; i < taosArrayGetSize(tags); ++i) {
      SSmlKv *kv = taosArrayGetP(tags, i);
      taosArrayPush(tableMeta->tags, &kv);
      taosHashPut(tableMeta->tagHash, kv->key, kv->keyLen, &i, CHAR_BYTES);
    }
  }

  if(cols){
    for (int16_t i = 0; i < taosArrayGetSize(cols); ++i) {
      SSmlKv *kv = taosArrayGetP(cols, i);
      taosArrayPush(tableMeta->cols, &kv);
      taosHashPut(tableMeta->fieldHash, kv->key, kv->keyLen, &i, SHORT_BYTES);
    }
  }
}

static SSmlTableInfo* smlBuildTableInfo(bool format){
  SSmlTableInfo *tag = taosMemoryCalloc(sizeof(SSmlTableInfo), 1);
  if(!tag){
    return NULL;
  }

  if(format){
    tag->colsFormat = taosArrayInit(16, POINTER_BYTES);
    if (tag->colsFormat == NULL) {
      uError("SML:smlParseLine failed to allocate memory");
      goto cleanup;
    }
  }else{
    tag->cols = taosArrayInit(16, POINTER_BYTES);
    if (tag->cols == NULL) {
      uError("SML:smlParseLine failed to allocate memory");
      goto cleanup;
    }
  }

  tag->tags = taosArrayInit(16, POINTER_BYTES);
  if (tag->tags == NULL) {
    uError("SML:smlParseLine failed to allocate memory");
    goto cleanup;
  }
  return tag;

cleanup:
  taosMemoryFreeClear(tag);
  return NULL;
}

static void smlDestroyBuildTableInfo(SSmlTableInfo *tag, bool format){
  if(format){
    taosArrayDestroy(tag->colsFormat);
  }else{
    tag->cols = taosArrayInit(16, POINTER_BYTES);
    for(size_t i = 0; i < taosArrayGetSize(tag->cols); i++){
      SHashObj *kvHash = taosArrayGetP(tag->cols, i);
      void** p1 = taosHashIterate(kvHash, NULL);
      while (p1) {
        SSmlKv* kv = *p1;
        taosMemoryFreeClear(kv);
        p1 = taosHashIterate(kvHash, p1);
      }
      taosHashCleanup(kvHash);
    }
  }
  taosArrayDestroy(tag->tags);
  taosMemoryFreeClear(tag);
}

static int32_t smlDealCols(SSmlTableInfo* oneTable, bool dataFormat, SArray *cols){
  if(dataFormat){
    taosArrayPush(oneTable->colsFormat, &cols);
    return TSDB_CODE_SUCCESS;
  }

  SHashObj *kvHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if(!kvHash){
    uError("SML:smlDealCols failed to allocate memory");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  for(size_t i = 0; i < taosArrayGetSize(cols); i++){
    SSmlKv *kv = taosArrayGetP(cols, i);
    taosHashPut(kvHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);   // todo key need escape, like \=, because find by schema name later
  }
  taosArrayPush(oneTable->cols, &kvHash);

  return TSDB_CODE_SUCCESS;
}

static SSmlSTableMeta* smlBuildSTableMeta(){
  SSmlSTableMeta* meta = taosMemoryCalloc(sizeof(SSmlSTableMeta), 1);
  if(!meta){
    return NULL;
  }
  meta->tagHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (meta->tagHash == NULL) {
    uError("SML:smlBuildSTableMeta failed to allocate memory");
    goto cleanup;
  }

  meta->fieldHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (meta->fieldHash == NULL) {
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
  taosMemoryFreeClear(meta);
  return NULL;
}

static void smlDestroySTableMeta(SSmlSTableMeta *meta){
  taosHashCleanup(meta->tagHash);
  taosHashCleanup(meta->fieldHash);
  taosArrayDestroy(meta->tags);
  taosArrayDestroy(meta->cols);
  taosMemoryFree(meta->tableMeta);
}

static int32_t smlParseLine(SSmlHandle* info, const char* sql) {
  SSmlLineInfo elements = {0};
  int ret = smlParseString(sql, &elements, &info->msgBuf);
  if(ret != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" smlParseString failed", info->id);
    return ret;
  }

  SArray *cols = NULL;
  if(info->dataFormat){   // if dataFormat, cols need new memory to save data
    cols = taosArrayInit(16, POINTER_BYTES);
    if (cols == NULL) {
      uError("SML:0x%"PRIx64" smlParseLine failed to allocate memory", info->id);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }else{      // if dataFormat is false, cols do not need to save data, there is another new memory to save data
    cols = info->colsContainer;
  }

  ret = smlParseTS(info, elements.timestamp, elements.timestampLen, cols);
  if(ret != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" smlParseTS failed", info->id);
    return ret;
  }
  ret = smlParseCols(elements.cols, elements.colsLen, cols, false, info->dumplicateKey, &info->msgBuf);
  if(ret != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" smlParseCols parse cloums fields failed", info->id);
    return ret;
  }
  if(taosArrayGetSize(cols) > TSDB_MAX_COLUMNS){
    smlBuildInvalidDataMsg(&info->msgBuf, "too many columns than 4096", NULL);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  SSmlTableInfo **oneTable = taosHashGet(info->childTables, elements.measure, elements.measureTagsLen);
  if(oneTable){
    SSmlSTableMeta** tableMeta = taosHashGet(info->superTables, elements.measure, elements.measureLen);
    ASSERT(tableMeta);
    ret = smlUpdateMeta(*tableMeta, NULL, cols, &info->msgBuf);    // update meta cols
    if(!ret){
      uError("SML:0x%"PRIx64" smlUpdateMeta cols failed", info->id);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    ret = smlDealCols(*oneTable, info->dataFormat, cols);
    if(ret != TSDB_CODE_SUCCESS){
      return ret;
    }
  }else{
    SSmlTableInfo *tinfo = smlBuildTableInfo(info->dataFormat);
    if(!tinfo){
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    ret = smlDealCols(tinfo, info->dataFormat, cols);
    if(ret != TSDB_CODE_SUCCESS){
      return ret;
    }

    ret = smlParseCols(elements.tags, elements.tagsLen, tinfo->tags, true, info->dumplicateKey, &info->msgBuf);
    if(ret != TSDB_CODE_SUCCESS){
      uError("SML:0x%"PRIx64" smlParseCols parse tag fields failed", info->id);
      return ret;
    }

    if(taosArrayGetSize(tinfo->tags) > TSDB_MAX_TAGS){
      smlBuildInvalidDataMsg(&info->msgBuf, "too many tags than 128", NULL);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    tinfo->sTableName = elements.measure;
    tinfo->sTableNameLen = elements.measureLen;
    smlBuildChildTableName(tinfo);
    //uDebug("SML:0x%"PRIx64" child table name: %s", info->id, tinfo->childTableName);

    SSmlSTableMeta** tableMeta = taosHashGet(info->superTables, elements.measure, elements.measureLen);
    if(tableMeta){  // update meta
      ret = smlUpdateMeta(*tableMeta, tinfo->tags, cols, &info->msgBuf);
      if(!ret){
        uError("SML:0x%"PRIx64" smlUpdateMeta failed", info->id);
        return TSDB_CODE_SML_INVALID_DATA;
      }
    }else{
      SSmlSTableMeta *meta = smlBuildSTableMeta();
      smlInsertMeta(meta, tinfo->tags, cols);
      taosHashPut(info->superTables, elements.measure, elements.measureLen, &meta, POINTER_BYTES);
    }

    taosHashPut(info->childTables, elements.measure, elements.measureTagsLen, &tinfo, POINTER_BYTES);
  }

  if(!info->dataFormat){
    taosArrayClear(info->colsContainer);
  }
  taosHashClear(info->dumplicateKey);
  return TSDB_CODE_SUCCESS;
}

static void smlDestroyInfo(SSmlHandle* info){
  if(!info) return;
  qDestroyQuery(info->pQuery);
  smlDestroyHandle(info->exec);

  // destroy info->childTables
  void** p1 = taosHashIterate(info->childTables, NULL);
  while (p1) {
    SSmlTableInfo* oneTable = *p1;
    smlDestroyBuildTableInfo(oneTable, info->dataFormat);
    p1 = taosHashIterate(info->childTables, p1);
  }
  taosHashCleanup(info->childTables);

  // destroy info->superTables
  p1 = taosHashIterate(info->superTables, NULL);
  while (p1) {
    SSmlSTableMeta* oneTable = *p1;
    smlDestroySTableMeta(oneTable);
    p1 = taosHashIterate(info->superTables, p1);
  }
  taosHashCleanup(info->superTables);

  // destroy info->pVgHash
  taosHashCleanup(info->pVgHash);
  taosHashCleanup(info->dumplicateKey);

  taosMemoryFreeClear(info);
}

static SSmlHandle* smlBuildSmlInfo(TAOS* taos, SRequestObj* request, SMLProtocolType protocol, int8_t precision, bool dataFormat){
  int32_t code = TSDB_CODE_SUCCESS;
  SSmlHandle* info = taosMemoryMalloc(sizeof(SSmlHandle));
  if (NULL == info) {
    return NULL;
  }
  info->id          = smlGenId();

  info->pQuery      = taosMemoryCalloc(1, sizeof(SQuery));
  if (NULL == info->pQuery) {
    uError("SML:0x%"PRIx64" create info->pQuery error", info->id);
    goto cleanup;
  }
  info->pQuery->execMode      = QUERY_EXEC_MODE_SCHEDULE;
  info->pQuery->haveResultSet = false;
  info->pQuery->msgType       = TDMT_VND_SUBMIT;
  info->pQuery->pRoot         = (SNode*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if(NULL == info->pQuery->pRoot){
    uError("SML:0x%"PRIx64" create info->pQuery->pRoot error", info->id);
    goto cleanup;
  }
  ((SVnodeModifOpStmt*)(info->pQuery->pRoot))->payloadType = PAYLOAD_TYPE_KV;

  info->taos        = taos;
  code = catalogGetHandle(info->taos->pAppInfo->clusterId, &info->pCatalog);
  if(code != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" get catalog error %d", info->id, code);
    goto cleanup;
  }

  info->precision   = precision;
  info->protocol    = protocol;
  info->dataFormat  = dataFormat;
  info->pRequest    = request;
  info->msgBuf.buf  = info->pRequest->msgBuf;
  info->msgBuf.len  = ERROR_MSG_BUF_DEFAULT_SIZE;

  info->exec        = smlInitHandle(info->pQuery);
  info->childTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  info->superTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  info->pVgHash     = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  info->dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if(!dataFormat){
    info->colsContainer = taosArrayInit(32, POINTER_BYTES);
    if(NULL == info->colsContainer){
      uError("SML:0x%"PRIx64" create info failed", info->id);
      goto cleanup;
    }
  }
  if(NULL == info->exec || NULL == info->childTables
      || NULL == info->superTables || NULL == info->pVgHash
      || NULL == info->dumplicateKey){
    uError("SML:0x%"PRIx64" create info failed", info->id);
    goto cleanup;
  }

  return info;
cleanup:
  smlDestroyInfo(info);
  return NULL;
}
static int32_t smlInsertData(SSmlHandle* info) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSmlTableInfo** oneTable = taosHashIterate(info->childTables, NULL);
  while (oneTable) {
    SSmlTableInfo* tableData = *oneTable;

    SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
    strcpy(pName.dbname, info->pRequest->pDb);
    memcpy(pName.tname, tableData->childTableName, strlen(tableData->childTableName));
    SEpSet ep = getEpSet_s(&info->taos->pAppInfo->mgmtEp);
    SVgroupInfo vg;
    code = catalogGetTableHashVgroup(info->pCatalog, info->taos->pAppInfo->pTransporter, &ep, &pName, &vg);
    if (code != 0) {
      uError("SML:0x%"PRIx64" catalogGetTableHashVgroup failed. table name: %s", info->id, tableData->childTableName);
      return code;
    }
    taosHashPut(info->pVgHash, (const char*)&vg.vgId, sizeof(vg.vgId), (char*)&vg, sizeof(vg));

    SSmlSTableMeta** pMeta = taosHashGet(info->superTables, tableData->sTableName, tableData->sTableNameLen);
    ASSERT (NULL != pMeta && NULL != *pMeta);

    // use tablemeta of stable to save vgid and uid of child table
    (*pMeta)->tableMeta->vgId = vg.vgId;
    (*pMeta)->tableMeta->uid = tableData->uid; // one table merge data block together according uid

    code = smlBindData(info->exec, tableData->tags, tableData->colsFormat, (*pMeta)->cols,
                       tableData->cols, info->dataFormat, (*pMeta)->tableMeta, tableData->childTableName, info->msgBuf.buf, info->msgBuf.len);
    if(code != TSDB_CODE_SUCCESS){
      return code;
    }
    oneTable = taosHashIterate(info->childTables, oneTable);
  }

  smlBuildOutput(info->exec, info->pVgHash);
  info->cost.insertRpcTime = taosGetTimestampUs();

  launchQueryImpl(info->pRequest, info->pQuery, TSDB_CODE_SUCCESS, true);

  info->affectedRows = taos_affected_rows(info->pRequest);
  return info->pRequest->code;
}

int32_t numOfSTables;
int32_t numOfCTables;
int32_t numOfCreateSTables;

int64_t parseTime;
int64_t schemaTime;
int64_t insertBindTime;
int64_t insertRpcTime;
int64_t endTime;

static void printStatisticInfo(SSmlHandle *info){
  uError("SML:0x%"PRIx64" smlInsertLines result, code:%d,lineNum:%d,stable num:%d,ctable num:%d,create stable num:%d \
        parse cost:%lld,schema cost:%lld,bind cost:%lld,rpc cost:%lld,total cost:%lld", info->id, info->cost.code,
         info->cost.lineNum, info->cost.numOfSTables, info->cost.numOfCTables, info->cost.numOfCreateSTables,
         info->cost.schemaTime-info->cost.parseTime, info->cost.insertBindTime-info->cost.schemaTime,
         info->cost.insertRpcTime-info->cost.insertBindTime, info->cost.endTime-info->cost.insertRpcTime,
         info->cost.endTime-info->cost.parseTime);
}

static int smlInsertLines(SSmlHandle *info, char* lines[], int numLines) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (numLines <= 0 || numLines > 65536) {
    uError("SML:0x%"PRIx64" smlInsertLines numLines should be between 1 and 65536. numLines: %d", info->id, numLines);
    code = TSDB_CODE_TSC_APP_ERROR;
    goto cleanup;
  }

  info->cost.parseTime = taosGetTimestampUs();
  for (int32_t i = 0; i < numLines; ++i) {
    code = smlParseLine(info, lines[i]);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%"PRIx64" smlParseLine failed. line %d : %s", info->id, i, lines[i]);
      goto cleanup;
    }
  }

  info->cost.lineNum = numLines;
  info->cost.numOfSTables = taosHashGetSize(info->superTables);
  info->cost.numOfCTables = taosHashGetSize(info->childTables);

  info->cost.schemaTime = taosGetTimestampUs();
  code = smlModifyDBSchemas(info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" smlModifyDBSchemas error : %s", info->id, tstrerror(code));
    goto cleanup;
  }

  info->cost.insertBindTime = taosGetTimestampUs();
  code = smlInsertData(info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" smlInsertData error : %s", info->id, tstrerror(code));
    goto cleanup;
  }
  info->cost.endTime = taosGetTimestampUs();

cleanup:
  info->cost.code = code;
  printStatisticInfo(info);
  return code;
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
 * @return return zero for successful insertion. Otherwise return none-zero error code of
 *         failure reason.
 *
 */

TAOS_RES* taos_schemaless_insert(TAOS* taos, char* lines[], int numLines, int protocol, int precision) {
  SRequestObj* request = createRequest(taos, NULL, NULL, TSDB_SQL_INSERT);
  if(!request){
    return NULL;
  }

  SSmlHandle* info = smlBuildSmlInfo(taos, request, protocol, precision, true);
  if(!info){
    return (TAOS_RES*)request;
  }

  switch (protocol) {
    case TSDB_SML_LINE_PROTOCOL:{
      smlInsertLines(info, lines, numLines);
      break;
    }
    case TSDB_SML_TELNET_PROTOCOL:
      //code = taos_insert_telnet_lines(taos, lines, numLines, protocol, tsType, &affected_rows);
      break;
    case TSDB_SML_JSON_PROTOCOL:
      //code = taos_insert_json_payload(taos, *lines, protocol, tsType, &affected_rows);
      break;
    default:
      break;
  }
  smlDestroyInfo(info);

end:
  return (TAOS_RES*)request;
}


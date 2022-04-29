#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "clientSml.h"

#include "tdef.h"
#include "ttypes.h"
#include "tmsg.h"
#include "tlog.h"
#include "query.h"
#include "taoserror.h"
#include "taos.h"
#include "ttime.h"
#include "tstrbuild.h"


typedef struct  {
  char sTableName[TSDB_TABLE_NAME_LEN];
  SHashObj* tagHash;
  SHashObj* fieldHash;
  SArray* tags; //SArray<SSchema>
  SArray* fields; //SArray<SSchema>
  uint8_t precision;
} SSmlSTableSchema;

#define SPACE ' '
#define COMMA ','
#define EQUAL '='
#define QUOTE '"'
#define SLASH '\\'
#define tsMaxSQLStringLen (1024*1024)

#define TSNAMELEN 2
#define TAGNAMELEN 3
//=================================================================================================

static uint64_t linesSmlHandleId = 0;
static const char* TS = "ts";
static const char* TAG = "tag";


uint64_t genLinesSmlId() {
  uint64_t id;

  do {
    id = atomic_add_fetch_64(&linesSmlHandleId, 1);
  } while (id == 0);

  return id;
}

static int32_t buildInvalidDataMsg(SMsgBuf* pBuf, const char *msg1, const char *msg2) {
  if(msg1) snprintf(pBuf->buf, pBuf->len, "%s:", msg1);
  if(msg2) strncpy(pBuf->buf, msg2, pBuf->len);
  return TSDB_CODE_SML_INVALID_DATA;
}

int compareSmlColKv(const void* p1, const void* p2) {
  SSmlKv* kv1 = (SSmlKv *)p1;
  SSmlKv* kv2 = (SSmlKv*)p2;
  int kvLen1 = (int)strlen(kv1->key);
  int kvLen2 = (int)strlen(kv2->key);
  int res = strncasecmp(kv1->key, kv2->key, MIN(kvLen1, kvLen2));
  if (res != 0) {
    return res;
  } else {
    return kvLen1-kvLen2;
  }
}

typedef enum {
  SCHEMA_ACTION_CREATE_STABLE,
  SCHEMA_ACTION_ADD_COLUMN,
  SCHEMA_ACTION_ADD_TAG,
  SCHEMA_ACTION_CHANGE_COLUMN_SIZE,
  SCHEMA_ACTION_CHANGE_TAG_SIZE,
} ESchemaAction;

typedef struct {
  char sTableName[TSDB_TABLE_NAME_LEN];
  SHashObj *tags;
  SHashObj *fields;
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

static int32_t buildSmlChildTableName(TAOS_SML_DATA_POINT_TAGS *tags) {
  int32_t size = taosArrayGetSize(tags->tags);
  ASSERT(size > 0);
  qsort(tags->tags, size, POINTER_BYTES, compareSmlColKv);

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
  uint64_t digest2 = *(uint64_t*)(context.digest + 8);
  snprintf(tags->childTableName, TSDB_TABLE_NAME_LEN, "t_%016"PRIx64"%016"PRIx64, digest1, digest2);
  taosStringBuilderDestroy(&sb);
  tags->uid = digest1;
  uDebug("SML: child table name: %s", tags->childTableName);
  return 0;
}

static int32_t generateSchemaAction(SSchema* pointColField, SHashObj* dbAttrHash, SArray* dbAttrArray, bool isTag, char sTableName[],
                                       SSchemaAction* action, bool* actionNeeded, SSmlLinesInfo* info) {
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

static int32_t buildColumnDescription(SSmlKv* field, char* buf, int32_t bufSize, int32_t* outBytes) {
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

static int32_t applySchemaAction(TAOS* taos, SSchemaAction* action, SSmlLinesInfo* info) {
  int32_t code = 0;
  int32_t outBytes = 0;
  char *result = (char *)taosMemoryCalloc(1, tsMaxSQLStringLen+1);
  int32_t capacity = tsMaxSQLStringLen +  1;

  uDebug("SML:0x%"PRIx64" apply schema action. action: %d", info->id, action->action);
  switch (action->action) {
    case SCHEMA_ACTION_ADD_COLUMN: {
      int n = sprintf(result, "alter stable %s add column ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field, result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
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
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
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
      buildColumnDescription(action->alterSTable.field,
                             result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
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
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
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
      buildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

//      if (code == TSDB_CODE_MND_INVALID_COLUMN_LENGTH || code == TSDB_CODE_TSC_INVALID_COLUMN_LENGTH) {
      if (code == TSDB_CODE_TSC_INVALID_COLUMN_LENGTH) {
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
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
      buildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

//      if (code == TSDB_CODE_MND_INVALID_TAG_LENGTH || code == TSDB_CODE_TSC_INVALID_TAG_LENGTH) {
      if (code == TSDB_CODE_TSC_INVALID_TAG_LENGTH) {
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
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

      SSmlKv **kv = taosHashIterate(action->createSTable.fields, NULL);
      while(kv){
        buildColumnDescription(*kv, pos, freeBytes, &outBytes);
        pos += outBytes; freeBytes -= outBytes;
        *pos = ','; ++pos; --freeBytes;
        kv = taosHashIterate(action->createSTable.fields, kv);
      }
      --pos; ++freeBytes;

      outBytes = snprintf(pos, freeBytes, ") tags (");
      pos += outBytes; freeBytes -= outBytes;

      kv = taosHashIterate(action->createSTable.tags, NULL);
      while(kv){
        buildColumnDescription(*kv, pos, freeBytes, &outBytes);
        pos += outBytes; freeBytes -= outBytes;
        *pos = ','; ++pos; --freeBytes;
        kv = taosHashIterate(action->createSTable.tags, kv);
      }
      pos--; ++freeBytes;
      outBytes = snprintf(pos, freeBytes, ")");
      TAOS_RES* res = taos_query(taos, result);
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_STB_ALREADY_EXIST) {
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
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

  taosMemoryFree(result);
  if (code != 0) {
    uError("SML:0x%"PRIx64 " apply schema action failure. %s", info->id, tstrerror(code));
  }
  return code;
}

static int32_t modifyDBSchemas(TAOS* taos, SSmlLinesInfo* info) {
  int32_t code = 0;

  SSmlSTableMeta** tableMetaSml = taosHashIterate(info->superTables, NULL);
  while (tableMetaSml) {
    SSmlSTableMeta* cTablePoints = *tableMetaSml;

    STableMeta *pTableMeta = NULL;
    SEpSet ep = getEpSet_s(&info->taos->pAppInfo->mgmtEp);

    size_t superTableLen = 0;
    void *superTable = taosHashGetKey(tableMetaSml, &superTableLen);
    SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
    strcpy(pName.dbname, info->pRequest->pDb);
    memcpy(pName.tname, superTable, superTableLen);

    code = catalogGetSTableMeta(info->pCatalog, info->taos->pAppInfo->pTransporter, &ep, &pName, &pTableMeta);

    if (code == TSDB_CODE_TDB_INVALID_TABLE_ID) {
      SSchemaAction schemaAction = {0};
      schemaAction.action = SCHEMA_ACTION_CREATE_STABLE;
      memcpy(schemaAction.createSTable.sTableName, superTable, superTableLen);
      schemaAction.createSTable.tags = cTablePoints->tagHash;
      schemaAction.createSTable.fields = cTablePoints->fieldHash;
      applySchemaAction(taos, &schemaAction, info);
      code = catalogGetSTableMeta(info->pCatalog, info->taos->pAppInfo->pTransporter, &ep, &pName, &pTableMeta);
      if (code != 0) {
        uError("SML:0x%"PRIx64" reconcile point schema failed. can not create %s", info->id, schemaAction.createSTable.sTableName);
        return code;
      }
    }else if (code == TSDB_CODE_SUCCESS) {
    } else {
      uError("SML:0x%"PRIx64" load table meta error: %s", info->id, tstrerror(code));
      return code;
    }
    taosHashPut(info->metaHashObj, superTable, superTableLen, &pTableMeta, POINTER_BYTES);

    tableMetaSml = taosHashIterate(info->superTables, tableMetaSml);
  }
  return 0;
}

static int32_t applyDataPoints(SSmlLinesInfo* info) {
  int32_t code = TSDB_CODE_SUCCESS;

  TAOS_SML_DATA_POINT_TAGS** oneTable = taosHashIterate(info->childTables, NULL);
  while (oneTable) {
    TAOS_SML_DATA_POINT_TAGS* tableData = *oneTable;

    SName pName = {TSDB_TABLE_NAME_T, info->taos->acctId, {0}, {0}};
    strcpy(pName.dbname, info->pRequest->pDb);
    memcpy(pName.tname, tableData->childTableName, strlen(tableData->childTableName));
    SEpSet ep = getEpSet_s(&info->taos->pAppInfo->mgmtEp);
    SVgroupInfo vg;
    catalogGetTableHashVgroup(info->pCatalog, info->taos->pAppInfo->pTransporter, &ep, &pName, &vg);
    taosHashPut(info->pVgHash, (const char*)&vg.vgId, sizeof(vg.vgId), (char*)&vg, sizeof(vg));

    STableMeta** pMeta = taosHashGet(info->metaHashObj, tableData->sTableName, tableData->sTableNameLen);
    ASSERT (NULL != pMeta && NULL != *pMeta);
    (*pMeta)->vgId = vg.vgId;
    (*pMeta)->uid = tableData->uid; // one table merge data block together according uid

    code = smlBind(info->exec, tableData->tags, tableData->cols, *pMeta, info->msgBuf.buf, info->msgBuf.len);
    if(code != TSDB_CODE_SUCCESS){
      return code;
    }
    oneTable = taosHashIterate(info->childTables, oneTable);
  }

  smlBuildOutput(info->exec, info->pVgHash);
  launchQueryImpl(info->pRequest, info->pQuery, TSDB_CODE_SUCCESS, true);

  info->affectedRows = taos_affected_rows(info->pRequest);
  return info->pRequest->code;
}

int smlInsert(TAOS* taos, SSmlLinesInfo* info) {
  uDebug("SML:0x%"PRIx64" taos_sml_insert. number of super tables: %d", info->id, taosHashGetSize(info->superTables));

  uDebug("SML:0x%"PRIx64" modify db schemas", info->id);
  int32_t code = modifyDBSchemas(taos, info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" error change db schema : %s", info->id, tstrerror(code));
    return code;
  }

  uDebug("SML:0x%"PRIx64" apply data points", info->id);
  code = applyDataPoints(info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" error apply data points : %s", info->id, tstrerror(code));
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

//=========================================================================

/*        Field                          Escape charaters
    1: measurement                        Comma,Space
    2: tag_key, tag_value, field_key  Comma,Equal Sign,Space
    3: field_value                    Double quote,Backslash
*/
static void escapeSpecialCharacter(uint8_t field, const char **pos) {
  const char *cur = *pos;
  if (*cur != '\\') {
    return;
  }
  switch (field) {
    case 1:
      switch (*(cur + 1)) {
        case ',':
        case ' ':
          cur++;
          break;
        default:
          break;
      }
      break;
    case 2:
      switch (*(cur + 1)) {
        case ',':
        case ' ':
        case '=':
          cur++;
          break;
        default:
          break;
      }
      break;
    case 3:
      switch (*(cur + 1)) {
        case '"':
        case '\\':
          cur++;
          break;
        default:
          break;
      }
      break;
    default:
      break;
  }
  *pos = cur;
}

static bool parseTinyInt(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 2) {
    return false;
  }
  const char *signalPos = pVal + len - 2;
  if (!strcasecmp(signalPos, "i8")) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid tiny int", endptr);
    }else if(!IS_VALID_TINYINT(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "tiny int out of range[-128,127]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseTinyUint(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 2) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  const char *signalPos = pVal + len - 2;
  if (!strcasecmp(signalPos, "u8")) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid unsigned tiny int", endptr);
    }else if(!IS_VALID_UTINYINT(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "unsigned tiny int out of range[0,255]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseSmallInt(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (!strcasecmp(signalPos, "i16")) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid small int", endptr);
    }else if(!IS_VALID_SMALLINT(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "small int our of range[-32768,32767]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseSmallUint(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (strcasecmp(signalPos, "u16") == 0) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid unsigned small int", endptr);
    }else if(!IS_VALID_USMALLINT(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "unsigned small int out of rang[0,65535]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseInt(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (strcasecmp(signalPos, "i32") == 0) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid int", endptr);
    }else if(!IS_VALID_INT(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "int out of range[-2147483648,2147483647]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseUint(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (strcasecmp(signalPos, "u32") == 0) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid unsigned int", endptr);
    }else if(!IS_VALID_UINT(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "unsigned int out of range[0,4294967295]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseBigInt(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len > 3 && strcasecmp(pVal + len - 3, "i64") == 0) {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != pVal + len - 3){       // 78ri8
      *isValid = false;
    }else if(!IS_VALID_BIGINT(result)){
      *isValid = false;
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }else if (len > 1 && pVal[len - 1] == 'i') {
    char *endptr = NULL;
    int64_t result = strtoll(pVal, &endptr, 10);
    if(endptr != pVal + len - 1){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid big int", endptr);
    }else if(!IS_VALID_BIGINT(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "big int out of range[-9223372036854775808,9223372036854775807]", endptr);
    }else{
      kvVal->i = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseBigUint(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (strcasecmp(signalPos, "u64") == 0) {
    char *endptr = NULL;
    uint64_t result = strtoull(pVal, &endptr, 10);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid unsigned big int", endptr);
    }else if(!IS_VALID_UBIGINT(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "unsigned big int out of range[0,18446744073709551615]", endptr);
    }else{
      kvVal->u = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseFloat(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  char *endptr = NULL;
  float result = strtof(pVal, &endptr);
  if(endptr == pVal + len && IS_VALID_FLOAT(result)){       // 78
    kvVal->f = result;
    *isValid = true;
    return true;
  }

  if (len > 3 && len <strcasecmp(pVal + len - 3, "f32") == 0) {
    if(endptr != pVal + len - 3){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid float", endptr);
    }else if(!IS_VALID_FLOAT(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "float out of range[-3.402823466e+38,3.402823466e+38]", endptr);
    }else{
      kvVal->f = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseDouble(SSmlKv *kvVal, bool *isValid, SMsgBuf *msg) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if (len <= 3) {
    return false;
  }
  const char *signalPos = pVal + len - 3;
  if (len <strcasecmp(signalPos, "f64") == 0) {
    char *endptr = NULL;
    double result = strtod(pVal, &endptr);
    if(endptr != signalPos){       // 78ri8
      *isValid = false;
      buildInvalidDataMsg(msg, "invalid double", endptr);
    }else if(!IS_VALID_DOUBLE(result)){
      *isValid = false;
      buildInvalidDataMsg(msg, "double out of range[-1.7976931348623158e+308,1.7976931348623158e+308]", endptr);
    }else{
      kvVal->d = result;
      *isValid = true;
    }
    return true;
  }
  return false;
}

static bool parseBool(SSmlKv *kvVal) {
  const char *pVal = kvVal->value;
  int32_t len = kvVal->valueLen;
  if ((len == 1) && pVal[len - 1] == 't') {
    //printf("Type is bool(%c)\n", pVal[len - 1]);
    kvVal->i = true;
    return true;
  }

  if ((len == 1) && pVal[len - 1] == 'f') {
    //printf("Type is bool(%c)\n", pVal[len - 1]);
    kvVal->i = false;
    return true;
  }

  if((len == 4) && !strcasecmp(pVal, "true")) {
    //printf("Type is bool(%s)\n", &pVal[len - 4]);
    kvVal->i = true;
    return true;
  }
  if((len == 5) && !strcasecmp(pVal, "false")) {
    //printf("Type is bool(%s)\n", &pVal[len - 5]);
    kvVal->i = false;
    return true;
  }
  return false;
}

static bool isBinary(const char *pVal, uint16_t len) {
  //binary: "abc"
  if (len < 2) {
    return false;
  }
  //binary
  if (pVal[0] == '"' && pVal[len - 1] == '"') {
    //printf("Type is binary(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isNchar(const char *pVal, uint16_t len) {
  //nchar: L"abc"
  if (len < 3) {
    return false;
  }
  if ((pVal[0] == 'l' || pVal[0] == 'L')&& pVal[1] == '"' && pVal[len - 1] == '"') {
    //printf("Type is nchar(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool convertSmlValue(SSmlKv *pVal, SMsgBuf *msg) {
  // put high probability matching type first
  bool isValid = false;
  if (parseFloat(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_FLOAT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  //binary
  if (isBinary(pVal->value, pVal->valueLen)) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
    pVal->length = pVal->valueLen - 2;
    pVal->valueLen -= 2;
    pVal->value = pVal->value++;
    return true;
  }
  //nchar
  if (isNchar(pVal->value, pVal->valueLen)) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
    pVal->length = pVal->valueLen - 3;
    pVal->value = pVal->value+2;
    return true;
  }
  if (parseDouble(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_DOUBLE;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;

    return true;
  }
  //bool
  if (parseBool(pVal)) {
    pVal->type = TSDB_DATA_TYPE_BOOL;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }

  if (parseTinyInt(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_TINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (parseTinyUint(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_UTINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (parseSmallInt(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_SMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (parseSmallUint(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_USMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (parseInt(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_INT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (parseUint(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_UINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (parseBigInt(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_BIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }
  if (parseBigUint(pVal, &isValid, msg)) {
    if(!isValid) return false;
    pVal->type = TSDB_DATA_TYPE_UBIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    return true;
  }

  buildInvalidDataMsg(msg, "invalid data", pVal->value);
  return false;
}

bool checkDuplicateKey(char *key, SHashObj *pHash, SSmlLinesInfo* info) {
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

int32_t parseSml(const char* sql, TAOS_PARSE_ELEMENTS *elements){
  if(!sql) return TSDB_CODE_SML_INVALID_DATA;
  while (*sql != '\0') {           // jump the space at the begining
    if(*sql != SPACE) {
      elements->measure = sql;
      break;
    }
    sql++;
  }
  if (!elements->measure || *sql == COMMA) return TSDB_CODE_SML_INVALID_DATA;

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
  if(elements->measureLen == 0) return TSDB_CODE_SML_INVALID_DATA;

  // parse cols
  while (*sql != '\0') {
    if(*sql != SPACE) {
      elements->cols = sql;
      break;
    }
    sql++;
  }
  if(!elements->cols) return TSDB_CODE_SML_INVALID_DATA;

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
  elements->colsLen = sql - elements->cols;

  // parse ts,ts can be empty
  while (*sql != '\0') {
    if(*sql != SPACE) {
      elements->timestamp = sql;
      break;
    }
    sql++;
  }
  if(elements->timestamp){
    elements->timestampLen = sql - elements->timestamp;
  }

  return TSDB_CODE_SUCCESS;
}

bool parseSmlCols(const char* data, int32_t len, SArray *cols, bool isTag, SMsgBuf *msg){
  if(isTag && len == 0){
    SSmlKv *kv = taosMemoryCalloc(sizeof(SSmlKv), 1);
    kv->key = TAG;
    kv->keyLen = TAGNAMELEN;
    kv->value = TAG;
    kv->valueLen = TAGNAMELEN;
    kv->type = TSDB_DATA_TYPE_NCHAR;
    if(cols) taosArrayPush(cols, &kv);
    return true;
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
      buildInvalidDataMsg(msg, "invalid key or key is too long than 64", key);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    // parse value
    i++;
    const char *value = data + i;
    while(i < len){
      if(data[i] == COMMA && i > 0 && data[i-1] != SLASH){
        break;
      }
      i++;
    }
    int32_t valueLen = data + i - value;
    if(valueLen == 0){
      buildInvalidDataMsg(msg, "invalid value", value);
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
      if(!convertSmlValue(kv, msg)){
        return TSDB_CODE_SML_INVALID_DATA;
      }
    }

    if(cols) taosArrayPush(cols, &kv);
  }

  return TSDB_CODE_SUCCESS;
}

static int64_t getTimeStampValue(const char *value, int32_t type) {
  double ts = (double)strtoll(value, NULL, 10);
  switch (type) {
    case TSDB_TIME_PRECISION_HOURS:
      ts *= (3600 * 1e9);
    case TSDB_TIME_PRECISION_MINUTES:
      ts *= (60 * 1e9);
    case TSDB_TIME_PRECISION_SECONDS:
      ts *= (1e9);
    case TSDB_TIME_PRECISION_MICRO:
      ts *= (1e6);
    case TSDB_TIME_PRECISION_MILLI:
      ts *= (1e3);
    default:
      break;
  }
  if(ts > (double)INT64_MAX || ts < 0){
    return -1;
  }else{
    return (int64_t)ts;
  }
}

static int64_t getTimeStampNow(int32_t precision) {
  switch (precision) {
    case TSDB_TIME_PRECISION_HOURS:
      return taosGetTimestampMs()/1000/3600;
    case TSDB_TIME_PRECISION_MINUTES:
      return taosGetTimestampMs()/1000/60;

    case TSDB_TIME_PRECISION_SECONDS:
      return taosGetTimestampMs()/1000;
    default:
      return taosGetTimestamp(precision);
  }
}

static bool isValidateTimeStamp(const char *pVal, int32_t len) {
  for (int i = 0; i < len; ++i) {
    if (!isdigit(pVal[i])) {
      return false;
    }
  }
  return true;
}

static int32_t getTsType(int32_t len) {
  if (len == TSDB_TIME_PRECISION_SEC_DIGITS) {
    return TSDB_TIME_PRECISION_SECONDS;
  } else if (len == TSDB_TIME_PRECISION_MILLI_DIGITS) {
    return TSDB_TIME_PRECISION_MILLI_DIGITS;
  } else {
    return -1;
  }
}

static int32_t parseSmlTS(const char* data, int32_t len, SArray *tags, SSmlLinesInfo* info){
  int64_t ts = 0;
  if(data == NULL){
    if(info->protocol != TSDB_SML_LINE_PROTOCOL){
      buildInvalidDataMsg(&info->msgBuf, "timestamp can not be null", NULL);
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
    ts = getTimeStampNow(info->tsType);
  }else{
    int ret = isValidateTimeStamp(data, len);
    if(!ret){
      buildInvalidDataMsg(&info->msgBuf, "timestamp must be digit", data);
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
    int32_t tsType = -1;
    if(info->protocol != TSDB_SML_LINE_PROTOCOL){
      tsType = getTsType(len);
      if (tsType == -1) {
        buildInvalidDataMsg(&info->msgBuf, "timestamp precision can only be seconds(10 digits) or milli seconds(13 digits)", data);
        return TSDB_CODE_TSC_INVALID_TIME_STAMP;
      }
    }else{
      tsType = info->tsType;
    }
    ts = getTimeStampValue(data, tsType);
    if(ts == -1){
      buildInvalidDataMsg(&info->msgBuf, "invalid timestamp", data);
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
  }

  SSmlKv *kv = taosMemoryCalloc(sizeof(SSmlKv), 1);
  if(!kv){
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  kv->key = TS;
  kv->keyLen = TSNAMELEN;
  kv->i = ts;
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
  kv->length = (int16_t)tDataTypes[kv->type].bytes;
  if(tags) taosArrayPush(tags, &kv);
  return TSDB_CODE_SUCCESS;
}

//int32_t parseSmlCols(const char* data, SArray *cols){
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

bool updateMeta(SSmlSTableMeta* tableMeta, SArray *tags, SArray *cols, SMsgBuf *msg){
  if(tags){
    for (int i = 0; i < taosArrayGetSize(tags); ++i) {
      SSmlKv *kv = taosArrayGetP(tags, i);
      ASSERT(kv->type == TSDB_DATA_TYPE_NCHAR);

      SSmlKv **value = taosHashGet(tableMeta->tagHash, kv->key, kv->keyLen);
      if(value){
        ASSERT((*value)->type == TSDB_DATA_TYPE_NCHAR);
        if(kv->valueLen > (*value)->valueLen){    // tags type is nchar
          *value = kv;
        }
      }else{
        taosHashPut(tableMeta->tagHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
      }
    }
  }

  if(cols){
    for (int i = 1; i < taosArrayGetSize(cols); ++i) {  //jump timestamp
      SSmlKv *kv = taosArrayGetP(cols, i);
      SSmlKv **value = taosHashGet(tableMeta->fieldHash, kv->key, kv->keyLen);
      if(value){
        if(kv->type != (*value)->type){
          buildInvalidDataMsg(msg, "the type is not the same like before", kv->key);
          return false;
        }else{
          if(IS_VAR_DATA_TYPE(kv->type)){     // update string len, if bigger
            if(kv->valueLen > (*value)->valueLen){
              *value = kv;
            }
          }
        }
      }else{
        taosHashPut(tableMeta->fieldHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
      }
    }
  }
}

void insertMeta(SSmlSTableMeta* tableMeta, SArray *tags, SArray *cols){
  if(tags){
    for (int i = 0; i < taosArrayGetSize(tags); ++i) {
      SSmlKv *kv = taosArrayGetP(tags, i);
      taosHashPut(tableMeta->tagHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
    }
  }

  if(cols){
    for (int i = 0; i < taosArrayGetSize(cols); ++i) {
      SSmlKv *kv = taosArrayGetP(cols, i);
      taosHashPut(tableMeta->fieldHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
    }
  }
}

static int32_t smlParseLine(const char* sql, SSmlLinesInfo* info) {
  TAOS_PARSE_ELEMENTS elements = {0};
  int ret = parseSml(sql, &elements);
  if(ret != TSDB_CODE_SUCCESS){
    return ret;
  }

  SArray *cols = taosArrayInit(16, POINTER_BYTES);
  if (cols == NULL) {
    uError("SML:0x%"PRIx64" taos_insert_lines failed to allocate memory", info->id);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  ret = parseSmlTS(elements.timestamp, elements.timestampLen, cols, info);
  if(ret != TSDB_CODE_SUCCESS){
    return ret;
  }
  ret = parseSmlCols(elements.cols, elements.colsLen, cols, false, &info->msgBuf);
  if(ret != TSDB_CODE_SUCCESS){
    return ret;
  }
  if(taosArrayGetSize(cols) > TSDB_MAX_COLUMNS){
    buildInvalidDataMsg(&info->msgBuf, "too many columns than 4096", NULL);
    return TSDB_CODE_SML_INVALID_DATA;
  }

  TAOS_SML_DATA_POINT_TAGS** oneTable = taosHashGet(info->childTables, elements.measure, elements.measureTagsLen);
  if(oneTable){
    SSmlSTableMeta** tableMeta = taosHashGet(info->superTables, elements.measure, elements.measureLen);
    ASSERT(tableMeta);
    ret = updateMeta(*tableMeta, NULL, cols, &info->msgBuf);    // update meta
    if(!ret){
      return TSDB_CODE_SML_INVALID_DATA;
    }
    taosArrayPush((*oneTable)->cols, &cols);
  }else{
    TAOS_SML_DATA_POINT_TAGS *tag = taosMemoryCalloc(sizeof(TAOS_SML_DATA_POINT_TAGS), 1);
    if(!tag){
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    tag->cols = taosArrayInit(16, POINTER_BYTES);
    if (tag->cols == NULL) {
      uError("SML:0x%"PRIx64" taos_insert_lines failed to allocate memory", info->id);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    taosArrayPush(tag->cols, &cols);

    tag->colsColumn = taosArrayInit(16, POINTER_BYTES);
    if (tag->cols == NULL) {
      uError("SML:0x%"PRIx64" taos_insert_lines failed to allocate memory", info->id);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    tag->tags = taosArrayInit(16, POINTER_BYTES);
    if (tag->tags == NULL) {
      uError("SML:0x%"PRIx64" taos_insert_lines failed to allocate memory", info->id);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    ret = parseSmlCols(elements.tags, elements.tagsLen, tag->tags, true, &info->msgBuf);
    if(ret != TSDB_CODE_SUCCESS){
      return ret;
    }

    if(taosArrayGetSize(tag->tags) > TSDB_MAX_TAGS){
      buildInvalidDataMsg(&info->msgBuf, "too many tags than 128", NULL);
      return TSDB_CODE_SML_INVALID_DATA;
    }

    tag->sTableName = elements.measure;
    tag->sTableNameLen = elements.measureLen;
    buildSmlChildTableName(tag);

    SSmlSTableMeta** tableMeta = taosHashGet(info->superTables, elements.measure, elements.measureLen);
    if(tableMeta){  // update meta
      ret = updateMeta(*tableMeta, tag->tags, cols, &info->msgBuf);
      if(!ret){
        return TSDB_CODE_SML_INVALID_DATA;
      }
    }else{
      SSmlSTableMeta* meta = taosMemoryCalloc(sizeof(SSmlSTableMeta), 1);
      insertMeta(meta, tag->tags, cols);
      taosHashPut(info->superTables, elements.measure, elements.measureLen, &meta, POINTER_BYTES);
    }

    taosHashPut(info->childTables, elements.measure, elements.measureTagsLen, &tag, POINTER_BYTES);
  }
  return TSDB_CODE_SUCCESS;
}

static void smlDestroyInfo(SSmlLinesInfo* info){
  if(!info) return;
  qDestroyQuery(info->pQuery);
  tscSmlDestroyHandle(info->exec);
  taosHashCleanup(info->childTables);
  taosHashCleanup(info->superTables);
  taosHashCleanup(info->metaHashObj);
  taosHashCleanup(info->pVgHash);
  taosMemoryFree(info);
}
static SSmlLinesInfo* smlBuildInfo(TAOS* taos, SRequestObj* request, SMLProtocolType protocol, int32_t tsType){
  SSmlLinesInfo* info = taosMemoryMalloc(sizeof(SSmlLinesInfo));
  if (NULL == info) {
    return NULL;
  }
  info->id = genLinesSmlId();
  info->tsType = tsType;
  info->taos = taos;
  info->protocol = protocol;

  info->pQuery = taosMemoryCalloc(1, sizeof(SQuery));
  if (NULL == info->pQuery) {
    goto cleanup;
  }
  info->pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  info->pQuery->haveResultSet = false;
  info->pQuery->msgType = TDMT_VND_SUBMIT;
  info->pQuery->pRoot = (SNode*)nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  ((SVnodeModifOpStmt*)(info->pQuery->pRoot))->payloadType = PAYLOAD_TYPE_KV;

  info->exec = tscSmlInitHandle(info->pQuery);

  int32_t code = catalogGetHandle(info->taos->pAppInfo->clusterId, &info->pCatalog);
  if(code != TSDB_CODE_SUCCESS){
    uError("SML:0x%"PRIx64" get catalog error %d", info->id, code);
    goto cleanup;
  }
  info->pRequest = request;
  info->msgBuf.buf = info->pRequest->msgBuf;
  info->msgBuf.len = ERROR_MSG_BUF_DEFAULT_SIZE;

  info->childTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);
  info->superTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);
  info->metaHashObj = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, false);
  info->pVgHash     = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false);

  return info;

cleanup:
  smlDestroyInfo(info);
  return NULL;
}

int sml_insert_lines(TAOS* taos, SRequestObj* request, char* lines[], int numLines, SMLProtocolType protocol, int32_t tsType) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSmlLinesInfo* info = smlBuildInfo(taos, request, protocol, tsType);
  if(!info){
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto cleanup;
  }
  if (numLines <= 0 || numLines > 65536) {
    uError("SML:0x%"PRIx64" taos_insert_lines numLines should be between 1 and 65536. numLines: %d", info->id, numLines);
    code = TSDB_CODE_TSC_APP_ERROR;
    goto cleanup;
  }
  for (int32_t i = 0; i < numLines; ++i) {
    code = smlParseLine(lines[i], info);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%"PRIx64" data point line parse failed. line %d : %s", info->id, i, lines[i]);
      goto cleanup;
    }
  }
  uDebug("SML:0x%"PRIx64" data point line parse success. tables %d", info->id, taosHashGetSize(info->childTables));

  code = smlInsert(taos, info);
  if (code != TSDB_CODE_SUCCESS) {
    uError("SML:0x%"PRIx64" taos_sml_insert error: %s", info->id, tstrerror((code)));
    goto cleanup;
  }

  uDebug("SML:0x%"PRIx64" taos_insert_lines finish inserting %d lines. code: %d", info->id, numLines, code);

cleanup:
  smlDestroyInfo(info);
  return code;
}

static int32_t convertPrecisionType(int precision) {
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
  int code = TSDB_CODE_SUCCESS;

  SRequestObj* request = createRequest(taos, NULL, NULL, TSDB_SQL_INSERT);
  switch (protocol) {
    case TSDB_SML_LINE_PROTOCOL:{
      int32_t tsType = convertPrecisionType(precision);
      if(tsType == -1){
        request->code = TSDB_CODE_SML_INVALID_PRECISION_TYPE;
        goto end;
      }

      code = sml_insert_lines(taos, request, lines, numLines, protocol, tsType);
      break;
    }
    case TSDB_SML_TELNET_PROTOCOL:
      //code = taos_insert_telnet_lines(taos, lines, numLines, protocol, tsType, &affected_rows);
      break;
    case TSDB_SML_JSON_PROTOCOL:
      //code = taos_insert_json_payload(taos, *lines, protocol, tsType, &affected_rows);
      break;
    default:
      code = TSDB_CODE_SML_INVALID_PROTOCOL_TYPE;
      break;
  }

end:
  return (TAOS_RES*)request;
}

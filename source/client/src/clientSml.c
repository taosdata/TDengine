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

//=================================================================================================

static uint64_t linesSmlHandleId = 0;

static int32_t insertChildTablePointsBatch(void* pVoid, char* name, char* name1, SArray* pArray, SArray* pArray1,
                                           SArray* pArray2, SArray* pArray3, size_t size, SSmlLinesInfo* info);
static int32_t doInsertChildTablePoints(void* pVoid, char* sql, char* name, SArray* pArray, SArray* pArray1,
                                        SSmlLinesInfo* info);
uint64_t genLinesSmlId() {
  uint64_t id;

  do {
    id = atomic_add_fetch_64(&linesSmlHandleId, 1);
  } while (id == 0);

  return id;
}

int compareSmlColKv(const void* p1, const void* p2) {
  TAOS_SML_KV* kv1 = (TAOS_SML_KV*)p1;
  TAOS_SML_KV* kv2 = (TAOS_SML_KV*)p2;
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
  SSchema* field;
} SAlterSTableActionInfo;

typedef struct {
  ESchemaAction action;
  union {
    SCreateSTableActionInfo createSTable;
    SAlterSTableActionInfo alterSTable;
  };
} SSchemaAction;

static int32_t getFieldBytesFromSmlKv(TAOS_SML_KV* kv, int32_t* bytes, uint64_t id) {
  if (!IS_VAR_DATA_TYPE(kv->type)) {
    *bytes = tDataTypes[kv->type].bytes;
  } else {
    if (kv->type == TSDB_DATA_TYPE_NCHAR) {
      TdUcs4 *ucs = taosMemoryMalloc(kv->length * TSDB_NCHAR_SIZE + 1);
      int32_t bytesNeeded = 0;
      bool succ = taosMbsToUcs4(kv->value, kv->length, ucs, kv->length * TSDB_NCHAR_SIZE, &bytesNeeded);
      if (!succ) {
        taosMemoryFree(ucs);
        uError("SML:0x%"PRIx64" convert nchar string to UCS4_LE failed:%s", id, kv->value);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      taosMemoryFree(ucs);
      *bytes =  bytesNeeded + VARSTR_HEADER_SIZE;
    } else if (kv->type == TSDB_DATA_TYPE_BINARY) {
      *bytes = kv->length + VARSTR_HEADER_SIZE;
    }
  }
  return 0;
}

static int32_t buildSmlKvSchema(TAOS_SML_KV* smlKv, SHashObj* hash, SArray* array, SSmlLinesInfo* info) {
  SSchema* pField = NULL;
  size_t* pFieldIdx = taosHashGet(hash, smlKv->key, strlen(smlKv->key));
  size_t fieldIdx = -1;
  int32_t code = 0;
  if (pFieldIdx) {
    fieldIdx = *pFieldIdx;
    pField = taosArrayGet(array, fieldIdx);

    if (pField->type != smlKv->type) {
      uError("SML:0x%"PRIx64" type mismatch. key %s, type %d. type before %d", info->id, smlKv->key, smlKv->type, pField->type);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }

    int32_t bytes = 0;
    code = getFieldBytesFromSmlKv(smlKv, &bytes, info->id);
    if (code != 0) {
      return code;
    }
    pField->bytes = MAX(pField->bytes, bytes);

  } else {
    SSchema field = {0};
    size_t tagKeyLen = strlen(smlKv->key);
    strncpy(field.name, smlKv->key, tagKeyLen);
    field.name[tagKeyLen] = '\0';
    field.type = smlKv->type;

    int32_t bytes = 0;
    code = getFieldBytesFromSmlKv(smlKv, &bytes, info->id);
    if (code != 0) {
      return code;
    }
    field.bytes = bytes;

    pField = taosArrayPush(array, &field);
    fieldIdx = taosArrayGetSize(array) - 1;
    taosHashPut(hash, field.name, tagKeyLen, &fieldIdx, sizeof(fieldIdx));
  }

  smlKv->fieldSchemaIdx = (uint32_t)fieldIdx;

  return 0;
}

static int32_t getSmlMd5ChildTableName(TAOS_SML_DATA_POINT* point, char* tableName, int* tableNameLen,
                                       SSmlLinesInfo* info) {
  uDebug("SML:0x%"PRIx64" taos_sml_insert get child table name through md5", info->id);
  if (point->tagNum) {
    qsort(point->tags, point->tagNum, sizeof(TAOS_SML_KV), compareSmlColKv);
  }

  SStringBuilder sb; memset(&sb, 0, sizeof(sb));
  char sTableName[TSDB_TABLE_NAME_LEN] = {0};
  strncpy(sTableName, point->stableName, strlen(point->stableName));
  //strtolower(sTableName, point->stableName);
  taosStringBuilderAppendString(&sb, sTableName);
  for (int j = 0; j < point->tagNum; ++j) {
    taosStringBuilderAppendChar(&sb, ',');
    TAOS_SML_KV* tagKv = point->tags + j;
    char tagName[TSDB_COL_NAME_LEN] = {0};
    strncpy(tagName, tagKv->key, strlen(tagKv->key));
    //strtolower(tagName, tagKv->key);
    taosStringBuilderAppendString(&sb, tagName);
    taosStringBuilderAppendChar(&sb, '=');
    taosStringBuilderAppend(&sb, tagKv->value, tagKv->length);
  }
  size_t len = 0;
  char* keyJoined = taosStringBuilderGetResult(&sb, &len);
  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, (uint8_t *)keyJoined, (uint32_t)len);
  tMD5Final(&context);
  uint64_t digest1 = *(uint64_t*)(context.digest);
  uint64_t digest2 = *(uint64_t*)(context.digest + 8);
  *tableNameLen = snprintf(tableName, *tableNameLen,
                           "t_%016"PRIx64"%016"PRIx64, digest1, digest2);
  taosStringBuilderDestroy(&sb);
  uDebug("SML:0x%"PRIx64" child table name: %s", info->id, tableName);
  return 0;
}

static int32_t buildSmlChildTableName(TAOS_SML_DATA_POINT* point, SSmlLinesInfo* info) {
  uDebug("SML:0x%"PRIx64" taos_sml_insert build child table name", info->id);
  char childTableName[TSDB_TABLE_NAME_LEN];
  int32_t tableNameLen = TSDB_TABLE_NAME_LEN;
  getSmlMd5ChildTableName(point, childTableName, &tableNameLen, info);
  point->childTableName = calloc(1, tableNameLen+1);
  strncpy(point->childTableName, childTableName, tableNameLen);
  point->childTableName[tableNameLen] = '\0';
  return 0;
}

static int32_t buildDataPointSchemas(TAOS_SML_DATA_POINT* points, int numPoint, SArray* stableSchemas, SSmlLinesInfo* info) {
  int32_t code = 0;
  SHashObj* sname2shema = taosHashInit(32,
                                       taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);

  for (int i = 0; i < numPoint; ++i) {
    TAOS_SML_DATA_POINT* point = &points[i];
    size_t stableNameLen = strlen(point->stableName);
    size_t* pStableIdx = taosHashGet(sname2shema, point->stableName, stableNameLen);
    SSmlSTableSchema* pStableSchema = NULL;
    size_t stableIdx = -1;
    if (pStableIdx) {
      pStableSchema= taosArrayGet(stableSchemas, *pStableIdx);
      stableIdx = *pStableIdx;
    } else {
      SSmlSTableSchema schema;
      strncpy(schema.sTableName, point->stableName, stableNameLen);
      schema.sTableName[stableNameLen] = '\0';
      schema.fields = taosArrayInit(64, sizeof(SSchema));
      schema.tags = taosArrayInit(8, sizeof(SSchema));
      schema.tagHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
      schema.fieldHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);

      pStableSchema = taosArrayPush(stableSchemas, &schema);
      stableIdx = taosArrayGetSize(stableSchemas) - 1;
      taosHashPut(sname2shema, schema.sTableName, stableNameLen, &stableIdx, sizeof(size_t));
    }

    for (int j = 0; j < point->tagNum; ++j) {
      TAOS_SML_KV* tagKv = point->tags + j;
      if (!point->childTableName) {
        buildSmlChildTableName(point, info);
      }

      code = buildSmlKvSchema(tagKv, pStableSchema->tagHash, pStableSchema->tags, info);
      if (code != 0) {
        uError("SML:0x%"PRIx64" build data point schema failed. point no.: %d, tag key: %s", info->id, i, tagKv->key);
        return code;
      }
    }

    //for Line Protocol tags may be omitted, add a tag with NULL value
    if (point->tagNum == 0) {
      if (!point->childTableName) {
        buildSmlChildTableName(point, info);
      }
      char tagNullName[TSDB_COL_NAME_LEN] = {0};
      size_t nameLen = strlen(tsSmlTagNullName);
      strncpy(tagNullName, tsSmlTagNullName, nameLen);
      addEscapeCharToString(tagNullName, (int32_t)nameLen);
      size_t* pTagNullIdx = taosHashGet(pStableSchema->tagHash, tagNullName, nameLen);
      if (!pTagNullIdx) {
        SSchema tagNull = {0};
        tagNull.type  = TSDB_DATA_TYPE_NCHAR;
        tagNull.bytes = TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
        strncpy(tagNull.name, tagNullName, nameLen);
        taosArrayPush(pStableSchema->tags, &tagNull);
        size_t tagNullIdx = taosArrayGetSize(pStableSchema->tags) - 1;
        taosHashPut(pStableSchema->tagHash, tagNull.name, nameLen, &tagNullIdx, sizeof(tagNullIdx));
      }
    }

    for (int j = 0; j < point->fieldNum; ++j) {
      TAOS_SML_KV* fieldKv = point->fields + j;
      code = buildSmlKvSchema(fieldKv, pStableSchema->fieldHash, pStableSchema->fields, info);
      if (code != 0) {
        uError("SML:0x%"PRIx64" build data point schema failed. point no.: %d, tag key: %s", info->id, i, fieldKv->key);
        return code;
      }
    }

    point->schemaIdx = (uint32_t)stableIdx;
  }

  size_t numStables = taosArrayGetSize(stableSchemas);
  for (int32_t i = 0; i < numStables; ++i) {
    SSmlSTableSchema* schema = taosArrayGet(stableSchemas, i);
    taosHashCleanup(schema->tagHash);
    taosHashCleanup(schema->fieldHash);
  }
  taosHashCleanup(sname2shema);

  uDebug("SML:0x%"PRIx64" build point schema succeed. num of super table: %zu", info->id, numStables);
  for (int32_t i = 0; i < numStables; ++i) {
    SSmlSTableSchema* schema = taosArrayGet(stableSchemas, i);
    uDebug("\ttable name: %s, tags number: %zu, fields number: %zu", schema->sTableName,
             taosArrayGetSize(schema->tags), taosArrayGetSize(schema->fields));
  }

  return 0;
}

static int32_t generateSchemaAction(SSchema* pointColField, SHashObj* dbAttrHash, SArray* dbAttrArray, bool isTag, char sTableName[],
                                       SSchemaAction* action, bool* actionNeeded, SSmlLinesInfo* info) {
  char fieldName[TSDB_COL_NAME_LEN] = {0};
  strcpy(fieldName, pointColField->name);

  size_t* pDbIndex = taosHashGet(dbAttrHash, fieldName, strlen(fieldName));
  if (pDbIndex) {
    SSchema* dbAttr = taosArrayGet(dbAttrArray, *pDbIndex);
    assert(strcasecmp(dbAttr->name, pointColField->name) == 0);
    if (pointColField->type != dbAttr->type) {
      uError("SML:0x%"PRIx64" point type and db type mismatch. key: %s. point type: %d, db type: %d", info->id, pointColField->name,
               pointColField->type, dbAttr->type);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }

    if (IS_VAR_DATA_TYPE(pointColField->type) && (pointColField->bytes > dbAttr->bytes)) {
      if (isTag) {
        action->action = SCHEMA_ACTION_CHANGE_TAG_SIZE;
      } else {
        action->action = SCHEMA_ACTION_CHANGE_COLUMN_SIZE;
      }
      memset(&action->alterSTable, 0,  sizeof(SAlterSTableActionInfo));
      memcpy(action->alterSTable.sTableName, sTableName, TSDB_TABLE_NAME_LEN);
      action->alterSTable.field = pointColField;
      *actionNeeded = true;
    }
  } else {
    if (isTag) {
      action->action = SCHEMA_ACTION_ADD_TAG;
    } else {
      action->action = SCHEMA_ACTION_ADD_COLUMN;
    }
    memset(&action->alterSTable, 0, sizeof(SAlterSTableActionInfo));
    memcpy(action->alterSTable.sTableName, sTableName, TSDB_TABLE_NAME_LEN);
    action->alterSTable.field = pointColField;
    *actionNeeded = true;
  }
  if (*actionNeeded) {
    uDebug("SML:0x%" PRIx64 " generate schema action. column name: %s, action: %d", info->id, fieldName,
             action->action);
  }
  return 0;
}

static int32_t buildColumnDescription(TAOS_SML_KV* field,
                               char* buf, int32_t bufSize, int32_t* outBytes) {
  uint8_t type = field->type;
  char    tname[TSDB_TABLE_NAME_LEN] = {0};
  memcpy(tname, field->key, field->keyLen);
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    int32_t bytes = field->length - VARSTR_HEADER_SIZE;
    if (type == TSDB_DATA_TYPE_NCHAR) {
      bytes =  bytes/TSDB_NCHAR_SIZE;
    }
    int out = snprintf(buf, bufSize,"%s %s(%d)",
                       tname,tDataTypes[field->type].name, bytes);
    *outBytes = out;
  } else {
    int out = snprintf(buf, bufSize, "%s %s",
                       tname, tDataTypes[type].name);
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

      TAOS_SML_KV **kv = taosHashIterate(action->createSTable.fields, NULL);
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

static int32_t destroySmlSTableSchema(SSmlSTableSchema* schema) {
  taosHashCleanup(schema->tagHash);
  taosHashCleanup(schema->fieldHash);
  taosArrayDestroy(&schema->tags);
  taosArrayDestroy(&schema->fields);
  return 0;
}

static int32_t fillDbSchema(STableMeta* tableMeta, char* tableName, SSmlSTableSchema* schema, SSmlLinesInfo* info) {
  schema->tags = taosArrayInit(8, sizeof(SSchema));
  schema->fields = taosArrayInit(64, sizeof(SSchema));
  schema->tagHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
  schema->fieldHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);

  tstrncpy(schema->sTableName, tableName, strlen(tableName)+1);
  schema->precision = tableMeta->tableInfo.precision;
  for (int i=0; i<tableMeta->tableInfo.numOfColumns; ++i) {
    SSchema field;
    tstrncpy(field.name, tableMeta->schema[i].name, strlen(tableMeta->schema[i].name)+1);
    addEscapeCharToString(field.name, (int16_t)strlen(field.name));
    field.type = tableMeta->schema[i].type;
    field.bytes = tableMeta->schema[i].bytes;
    taosArrayPush(schema->fields, &field);
    size_t fieldIndex = taosArrayGetSize(schema->fields) - 1;
    taosHashPut(schema->fieldHash, field.name, strlen(field.name), &fieldIndex, sizeof(fieldIndex));
  }

  for (int i=0; i<tableMeta->tableInfo.numOfTags; ++i) {
    int j = i + tableMeta->tableInfo.numOfColumns;
    SSchema field;
    tstrncpy(field.name, tableMeta->schema[j].name, strlen(tableMeta->schema[j].name)+1);
    addEscapeCharToString(field.name, (int16_t)strlen(field.name));
    field.type = tableMeta->schema[j].type;
    field.bytes = tableMeta->schema[j].bytes;
    taosArrayPush(schema->tags, &field);
    size_t tagIndex = taosArrayGetSize(schema->tags) - 1;
    taosHashPut(schema->tagHash, field.name, strlen(field.name), &tagIndex, sizeof(tagIndex));
  }
  uDebug("SML:0x%"PRIx64 " load table schema succeed. table name: %s, columns number: %d, tag number: %d, precision: %d",
           info->id, tableName, tableMeta->tableInfo.numOfColumns, tableMeta->tableInfo.numOfTags, schema->precision);
  return TSDB_CODE_SUCCESS;
}

static int32_t getSuperTableMetaFromLocalCache(TAOS* taos, char* tableName, STableMeta** outTableMeta, SSmlLinesInfo* info) {
  int32_t     code = 0;
  STableMeta* tableMeta = NULL;

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
  if (pSql == NULL) {
    uError("SML:0x%" PRIx64 " failed to allocate memory, reason:%s", info->id, strerror(errno));
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return code;
  }
  pSql->pTscObj = taos;
  pSql->signature = pSql;
  pSql->fp = NULL;

  registerSqlObj(pSql);
  char tableNameBuf[TSDB_TABLE_NAME_LEN + TS_BACKQUOTE_CHAR_SIZE] = {0};
  memcpy(tableNameBuf, tableName, strlen(tableName));
  SStrToken tableToken = {.z = tableNameBuf, .n = (uint32_t)strlen(tableName), .type = TK_ID};
  tGetToken(tableNameBuf, &tableToken.type);
  bool dbIncluded = false;
  // Check if the table name available or not
  if (tscValidateName(&tableToken, true, &dbIncluded) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
    sprintf(pSql->cmd.payload, "table name is invalid");
    taosReleaseRef(tscObjRef, pSql->self);
    return code;
  }

  SName sname = {0};
  if ((code = tscSetTableFullName(&sname, &tableToken, pSql, dbIncluded)) != TSDB_CODE_SUCCESS) {
    taosReleaseRef(tscObjRef, pSql->self);
    return code;
  }

  char fullTableName[TSDB_TABLE_FNAME_LEN] = {0};
  memset(fullTableName, 0, tListLen(fullTableName));
  tNameExtractFullName(&sname, fullTableName);

  size_t size = 0;
  taosHashGetCloneExt(UTIL_GET_TABLEMETA(pSql), fullTableName, strlen(fullTableName), NULL, (void**)&tableMeta, &size);

  STableMeta* stableMeta = tableMeta;
  if (tableMeta != NULL && tableMeta->tableType == TSDB_CHILD_TABLE) {
      taosHashGetCloneExt(UTIL_GET_TABLEMETA(pSql), tableMeta->sTableName, strlen(tableMeta->sTableName), NULL,
                          (void**)stableMeta, &size);
  }
  taosReleaseRef(tscObjRef, pSql->self);

  if (stableMeta != tableMeta) {
    taosMemoryFree(tableMeta);
  }

  if (stableMeta != NULL) {
    if (outTableMeta != NULL) {
      *outTableMeta = stableMeta;
    } else {
      taosMemoryFree(stableMeta);
    }
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_TSC_NO_META_CACHED;
  }
}

static int32_t retrieveTableMeta(TAOS* taos, char* tableName, STableMeta** pTableMeta, SSmlLinesInfo* info) {
  int32_t code = 0;
  int32_t retries = 0;
  STableMeta* tableMeta = NULL;
  while (retries++ <= TSDB_MAX_REPLICA && tableMeta == NULL) {
    STscObj* pObj = (STscObj*)taos;
    if (pObj == NULL || pObj->signature != pObj) {
      terrno = TSDB_CODE_TSC_DISCONNECTED;
      return TSDB_CODE_TSC_DISCONNECTED;
    }

    uDebug("SML:0x%" PRIx64 " retrieve table meta. super table name: %s", info->id, tableName);
    code = getSuperTableMetaFromLocalCache(taos, tableName, &tableMeta, info);
    if (code == TSDB_CODE_SUCCESS) {
      uDebug("SML:0x%" PRIx64 " successfully retrieved table meta. super table name: %s", info->id, tableName);
      break;
    } else if (code == TSDB_CODE_TSC_NO_META_CACHED) {
      char sql[256];
      snprintf(sql, 256, "describe %s", tableName);
      TAOS_RES* res = taos_query(taos, sql);
      code = taos_errno(res);
      if (code != 0) {
        uError("SML:0x%" PRIx64 " describe table failure. %s", info->id, taos_errstr(res));
        taos_free_result(res);
        return code;
      }
      taos_free_result(res);
    } else {
      return code;
    }
  }

  if (tableMeta != NULL) {
    *pTableMeta = tableMeta;
    return TSDB_CODE_SUCCESS;
  } else {
    uError("SML:0x%" PRIx64 " failed to retrieve table meta. super table name: %s", info->id, tableName);
    return TSDB_CODE_TSC_NO_META_CACHED;
  }
}

static int32_t loadTableSchemaFromDB(TAOS* taos, char* tableName, SSmlSTableSchema* schema, SSmlLinesInfo* info) {
  int32_t code = 0;
  STableMeta* tableMeta = NULL;
  code = retrieveTableMeta(taos, tableName, &tableMeta, info);
  if (code == TSDB_CODE_SUCCESS) {
    assert(tableMeta != NULL);
    fillDbSchema(tableMeta, tableName, schema, info);
    taosMemoryFree(tableMeta);
    tableMeta = NULL;
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
      memset(&schemaAction.createSTable, 0, sizeof(SCreateSTableActionInfo));
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

static int32_t applyDataPoints(TAOS* taos, SSmlLinesInfo* info) {
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
    (*pMeta)->uid = tableData->uid;

    smlBind(info->exec, tableData->tags, tableData->cols, *pMeta, info->msgBuf, info->msgLen);

    oneTable = taosHashIterate(info->childTables, oneTable);
  }

  smlBuildOutput(info->exec, info->pVgHash);
  launchQueryImpl(info->pRequest, info->pQuery, TSDB_CODE_SUCCESS, true);
  if(info->pRequest->code != TSDB_CODE_SUCCESS){

  }

  info->affectedRows = taos_affected_rows(info->pRequest);
  return code;
}

int tscSmlInsert(TAOS* taos, SSmlLinesInfo* info) {
  uDebug("SML:0x%"PRIx64" taos_sml_insert. number of super tables: %d", info->id, taosHashGetSize(info->superTables));
  int32_t code = TSDB_CODE_SUCCESS;
  info->affectedRows = 0;

  uDebug("SML:0x%"PRIx64" modify db schemas", info->id);
  code = modifyDBSchemas(taos, info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" error change db schema : %s", info->id, tstrerror(code));
    goto clean_up;
  }

  uDebug("SML:0x%"PRIx64" apply data points", info->id);
  code = applyDataPoints(taos, info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" error apply data points : %s", info->id, tstrerror(code));
  }

clean_up:
  return code;
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

bool isValidInteger(char *str) {
  char *c = str;
  if (*c != '+' && *c != '-' && !isdigit(*c)) {
    return false;
  }
  c++;
  while (*c != '\0') {
    if (!isdigit(*c)) {
      return false;
    }
    c++;
  }
  return true;
}

bool isValidFloat(char *str) {
  char *c = str;
  uint8_t has_dot, has_exp, has_sign;
  has_dot = 0;
  has_exp = 0;
  has_sign = 0;

  if (*c != '+' && *c != '-' && *c != '.' && !isdigit(*c)) {
    return false;
  }
  if (*c == '.' && isdigit(*(c + 1))) {
    has_dot = 1;
  }
  c++;
  while (*c != '\0') {
    if (!isdigit(*c)) {
      switch (*c) {
        case '.': {
          if (!has_dot && !has_exp && isdigit(*(c + 1))) {
            has_dot = 1;
          } else {
            return false;
          }
          break;
        }
        case 'e':
        case 'E': {
          if (!has_exp && isdigit(*(c - 1)) &&
              (isdigit(*(c + 1)) ||
               *(c + 1) == '+' ||
               *(c + 1) == '-')) {
            has_exp = 1;
          } else {
            return false;
          }
          break;
        }
        case '+':
        case '-': {
          if (!has_sign && has_exp && isdigit(*(c + 1))) {
            has_sign = 1;
          } else {
            return false;
          }
          break;
        }
        default: {
          return false;
        }
      }
    }
    c++;
  } //while
  return true;
}

static bool isInteger(char *pVal, uint16_t len, bool *has_sign) {
  if (len <= 1) {
    return false;
  }
  if (pVal[len - 1] == 'i') {
    *has_sign = true;
    return true;
  }
  if (pVal[len - 1] == 'u') {
    *has_sign = false;
    return true;
  }

  return false;
}

static bool isTinyInt(char *pVal, uint16_t len) {
  if (len <= 2) {
    return false;
  }
  if (!strcasecmp(&pVal[len - 2], "i8")) {
    //printf("Type is int8(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isTinyUint(char *pVal, uint16_t len) {
  if (len <= 2) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  if (!strcasecmp(&pVal[len - 2], "u8")) {
    //printf("Type is uint8(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isSmallInt(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (!strcasecmp(&pVal[len - 3], "i16")) {
    //printf("Type is int16(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isSmallUint(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  if (strcasecmp(&pVal[len - 3], "u16") == 0) {
    //printf("Type is uint16(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isInt(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcasecmp(&pVal[len - 3], "i32") == 0) {
    //printf("Type is int32(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isUint(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  if (strcasecmp(&pVal[len - 3], "u32") == 0) {
    //printf("Type is uint32(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isBigInt(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcasecmp(&pVal[len - 3], "i64") == 0) {
    //printf("Type is int64(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isBigUint(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  if (strcasecmp(&pVal[len - 3], "u64") == 0) {
    //printf("Type is uint64(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isFloat(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcasecmp(&pVal[len - 3], "f32") == 0) {
    //printf("Type is float(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isDouble(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcasecmp(&pVal[len - 3], "f64") == 0) {
    //printf("Type is double(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isBool(char *pVal, uint16_t len, bool *bVal) {
  if ((len == 1) && !strcasecmp(&pVal[len - 1], "t")) {
    //printf("Type is bool(%c)\n", pVal[len - 1]);
    *bVal = true;
    return true;
  }

  if ((len == 1) && !strcasecmp(&pVal[len - 1], "f")) {
    //printf("Type is bool(%c)\n", pVal[len - 1]);
    *bVal = false;
    return true;
  }

  if((len == 4) && !strcasecmp(&pVal[len - 4], "true")) {
    //printf("Type is bool(%s)\n", &pVal[len - 4]);
    *bVal = true;
    return true;
  }
  if((len == 5) && !strcasecmp(&pVal[len - 5], "false")) {
    //printf("Type is bool(%s)\n", &pVal[len - 5]);
    *bVal = false;
    return true;
  }
  return false;
}

static bool isBinary(char *pVal, uint16_t len) {
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

static bool isNchar(char *pVal, uint16_t len) {
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

static bool convertStrToNumber(TAOS_SML_KV *pVal, char *str, SSmlLinesInfo* info) {
  errno = 0;
  uint8_t type = pVal->type;
  int16_t length = pVal->length;
  int64_t val_s = 0;
  uint64_t val_u = 0;
  double val_d = 0.0;

  strntolower_s(str, str, (int32_t)strlen(str));
  if (IS_FLOAT_TYPE(type)) {
    val_d = strtod(str, NULL);
  } else {
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      val_s = strtoll(str, NULL, 10);
    } else {
      val_u = strtoull(str, NULL, 10);
    }
  }

  if (errno == ERANGE) {
    uError("SML:0x%"PRIx64" Convert number(%s) out of range", info->id, str);
    return false;
  }

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
      if (!IS_VALID_TINYINT(val_s)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(int8_t *)(pVal->value) = (int8_t)val_s;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      if (!IS_VALID_UTINYINT(val_u)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(uint8_t *)(pVal->value) = (uint8_t)val_u;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (!IS_VALID_SMALLINT(val_s)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(int16_t *)(pVal->value) = (int16_t)val_s;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      if (!IS_VALID_USMALLINT(val_u)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(uint16_t *)(pVal->value) = (uint16_t)val_u;
      break;
    case TSDB_DATA_TYPE_INT:
      if (!IS_VALID_INT(val_s)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(int32_t *)(pVal->value) = (int32_t)val_s;
      break;
    case TSDB_DATA_TYPE_UINT:
      if (!IS_VALID_UINT(val_u)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(uint32_t *)(pVal->value) = (uint32_t)val_u;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      if (!IS_VALID_BIGINT(val_s)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(int64_t *)(pVal->value) = (int64_t)val_s;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      if (!IS_VALID_UBIGINT(val_u)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(uint64_t *)(pVal->value) = (uint64_t)val_u;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      if (!IS_VALID_FLOAT(val_d)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(float *)(pVal->value) = (float)val_d;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (!IS_VALID_DOUBLE(val_d)) {
        return false;
      }
      pVal->value = calloc(length, 1);
      *(double *)(pVal->value) = (double)val_d;
      break;
    default:
      return false;
  }
  return true;
}
//len does not include '\0' from value.
bool convertSmlValueType(TAOS_SML_KV *pVal, char *value,
                         uint16_t len, SSmlLinesInfo* info, bool isTag) {
  if (len <= 0) {
    return false;
  }

  //convert tags value to Nchar
  if (isTag) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
    pVal->length = len;
    pVal->value = calloc(pVal->length, 1);
    memcpy(pVal->value, value, pVal->length);
    return true;
  }

  //integer number
  bool has_sign;
  if (isInteger(value, len, &has_sign)) {
    pVal->type = has_sign ? TSDB_DATA_TYPE_BIGINT : TSDB_DATA_TYPE_UBIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 1] = '\0';
    if (!isValidInteger(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  if (isTinyInt(value, len)) {
    pVal->type = TSDB_DATA_TYPE_TINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 2] = '\0';
    if (!isValidInteger(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  if (isTinyUint(value, len)) {
    pVal->type = TSDB_DATA_TYPE_UTINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 2] = '\0';
    if (!isValidInteger(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  if (isSmallInt(value, len)) {
    pVal->type = TSDB_DATA_TYPE_SMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!isValidInteger(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  if (isSmallUint(value, len)) {
    pVal->type = TSDB_DATA_TYPE_USMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!isValidInteger(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  if (isInt(value, len)) {
    pVal->type = TSDB_DATA_TYPE_INT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!isValidInteger(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  if (isUint(value, len)) {
    pVal->type = TSDB_DATA_TYPE_UINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!isValidInteger(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  if (isBigInt(value, len)) {
    pVal->type = TSDB_DATA_TYPE_BIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!isValidInteger(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  if (isBigUint(value, len)) {
    pVal->type = TSDB_DATA_TYPE_UBIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!isValidInteger(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  //floating number
  if (isFloat(value, len)) {
    pVal->type = TSDB_DATA_TYPE_FLOAT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!isValidFloat(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  if (isDouble(value, len)) {
    pVal->type = TSDB_DATA_TYPE_DOUBLE;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!isValidFloat(value) || !convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  //binary
  if (isBinary(value, len)) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
    pVal->length = len - 2;
    pVal->value = calloc(pVal->length, 1);
    //copy after "
    memcpy(pVal->value, value + 1, pVal->length);
    return true;
  }
  //nchar
  if (isNchar(value, len)) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
    pVal->length = len - 3;
    pVal->value = calloc(pVal->length, 1);
    //copy after L"
    memcpy(pVal->value, value + 2, pVal->length);
    return true;
  }
  //bool
  bool bVal;
  if (isBool(value, len, &bVal)) {
    pVal->type = TSDB_DATA_TYPE_BOOL;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->value = calloc(pVal->length, 1);
    memcpy(pVal->value, &bVal, pVal->length);
    return true;
  }

  //Handle default(no appendix) type as DOUBLE
  if (isValidInteger(value) || isValidFloat(value)) {
    pVal->type = TSDB_DATA_TYPE_DOUBLE;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    if (!convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
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

//Table name can only contain digits(0-9),alphebet(a-z),underscore(_)
int32_t isValidChildTableName(const char *pTbName, int16_t len, SSmlLinesInfo* info) {
  if (len > TSDB_TABLE_NAME_LEN - 1) {
    uError("SML:0x%"PRIx64" child table name cannot exceeds %d characters", info->id, TSDB_TABLE_NAME_LEN - 1);
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }
  const char *cur = pTbName;
  for (int i = 0; i < len; ++i) {
    if(!isdigit(cur[i]) && !isalpha(cur[i]) && (cur[i] != '_')) {
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    }
  }
  return TSDB_CODE_SUCCESS;
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

  while (*sql != '\0') {
    if(*sql == SPACE && *(sql - 1) != SLASH) {
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

  return TSDB_CODE_SUCCESS;
}

int32_t parseSmlKV(const char* data, int32_t len, SArray *cols, bool isTag){
  for(int i = 0; i < len; i++){
    const char *key = data + i;
    int32_t keyLen = 0;
    while(i < len){
      if(data[i] == EQUAL && i > 0 && data[i-1] != SLASH){
        keyLen = data + i - key;
        break;
      }
      i++;
    }
    if(keyLen == 0){
      return TSDB_CODE_SML_INVALID_DATA;
    }

    i++;
    const char *value = data + i;
    int32_t valueLen = 0;
    while(i < len){
      if(data[i] == COMMA && i > 0 && data[i-1] != SLASH){
        valueLen = data + i - value;
        break;
      }
      i++;
    }
    if(valueLen == 0){
      return TSDB_CODE_SML_INVALID_DATA;
    }
    SSmlKv *kv = taosMemoryCalloc(sizeof(SSmlKv), 1);
    kv->key = key;
    kv->keyLen = keyLen;
    kv->value = value;
    kv->valueLen = valueLen;
    if(isTag){
      kv->type = TSDB_DATA_TYPE_NCHAR;
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

static int32_t isValidateTimeStamp(const char *pVal, int32_t len) {
  for (int i = 0; i < len; ++i) {
    if (!isdigit(pVal[i])) {
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getTsType(int32_t len) {
  if (len == TSDB_TIME_PRECISION_SEC_DIGITS) {
    return TSDB_TIME_PRECISION_SECONDS;
  } else if (len == TSDB_TIME_PRECISION_MILLI_DIGITS) {
    return TSDB_TIME_PRECISION_MILLI_DIGITS;
  } else {
    return TSDB_CODE_TSC_INVALID_TIME_STAMP;
  }
}

static int32_t parseSmlTS(const char* data, SArray *tags, int8_t tsType, SMLProtocolType protocolType){
  int64_t *ts = taosMemoryCalloc(1, sizeof(int64_t));
  if(data == NULL){
    if(protocolType == TSDB_SML_LINE_PROTOCOL){
      *ts = getTimeStampNow(tsType);
    }else{
      goto cleanup;
    }
  }else{
    int32_t len = strlen(data);
    int ret = isValidateTimeStamp(data, len);
    if(!ret){
      goto cleanup;
    }
    if(protocolType != TSDB_SML_LINE_PROTOCOL){
      tsType = getTsType(len);
      if (tsType == TSDB_CODE_TSC_INVALID_TIME_STAMP) {
        goto cleanup;
      }
    }
    *ts = getTimeStampValue(data, tsType);
    if(*ts == -1){
      goto cleanup;
    }
  }

  SSmlKv *kv = taosMemoryCalloc(sizeof(SSmlKv), 1);
  kv->value = (const char*)ts;
  kv->valueLen = sizeof(int64_t);
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
  kv->length = (int16_t)tDataTypes[kv->type].bytes;
  if(tags) taosArrayPush(tags, &kv);
  return TSDB_CODE_SUCCESS;

cleanup:
  taosMemoryFree(ts);
  return TSDB_CODE_TSC_INVALID_TIME_STAMP;
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

void updateMeta(SSmlSTableMeta* tableMeta, SArray *tags, SArray *cols){
  if(tags){
    for (int i = 0; i < taosArrayGetSize(tags); ++i) {
      SSmlKv *kv = taosArrayGetP(tags, i);
      SSmlKv **value = taosHashGet(tableMeta->tagHash, kv->key, kv->keyLen);
      if(value){
        if(kv->type != (*value)->type){
          // todo
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
          // todo
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

  parseSmlTS(elements.timestamp, cols, info->tsType);
  ret = parseSmlCols(elements.cols, elements.colsLen, cols, false);
  if(ret != TSDB_CODE_SUCCESS){
    return ret;
  }

  TAOS_SML_DATA_POINT_TAGS** oneTable = taosHashGet(info->childTables, elements.measure, elements.measureTagsLen);
  if(oneTable){
    SSmlSTableMeta** tableMeta = taosHashGet(info->superTables, elements.measure, elements.measureLen);
    ASSERT(tableMeta);
    updateMeta(*tableMeta, NULL, cols);    // update meta

    taosArrayPush((*oneTable)->cols, &cols);
  }else{
    TAOS_SML_DATA_POINT_TAGS *tag = taosMemoryCalloc(sizeof(TAOS_SML_DATA_POINT_TAGS), 1);
    tag->cols = taosArrayInit(16, POINTER_BYTES);
    if (tag->cols == NULL) {
      uError("SML:0x%"PRIx64" taos_insert_lines failed to allocate memory", info->id);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    taosArrayPush(tag->cols, &cols);

    tag->tags = taosArrayInit(16, POINTER_BYTES);
    if (tag->tags == NULL) {
      uError("SML:0x%"PRIx64" taos_insert_lines failed to allocate memory", info->id);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    ret = parseSmlTags(elements.tags, elements.tagsLen, tag->tags);
    if(ret != TSDB_CODE_SUCCESS){
      return ret;
    }

    SSmlSTableMeta** tableMeta = taosHashGet(info->superTables, elements.measure, elements.measureLen);
    if(tableMeta){  // update meta
      updateMeta(*tableMeta, tag->tags, cols);
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
  info->msgBuf = info->pRequest->msgBuf;
  info->msgLen = ERROR_MSG_BUF_DEFAULT_SIZE;


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
    case TSDB_SML_LINE_PROTOCOL:
      int32_t tsType = convertPrecisionType(precision);
      if(tsType == -1){
        request->code = TSDB_CODE_SML_INVALID_PRECISION_TYPE;
        goto end;
      }

      code = sml_insert_lines(taos, request, lines, numLines, protocol, tsType);
      break;
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

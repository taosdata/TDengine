#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tscParseLine.h"

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
  SArray* tags; //SArray<SSchema>
  SArray* fields; //SArray<SSchema>
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

static int32_t buildColumnDescription(SSchema* field,
                               char* buf, int32_t bufSize, int32_t* outBytes) {
  uint8_t type = field->type;

  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    int32_t bytes = field->bytes - VARSTR_HEADER_SIZE;
    if (type == TSDB_DATA_TYPE_NCHAR) {
      bytes =  bytes/TSDB_NCHAR_SIZE;
    }
    int out = snprintf(buf, bufSize,"%s %s(%d)",
                       field->name,tDataTypes[field->type].name, bytes);
    *outBytes = out;
  } else {
    int out = snprintf(buf, bufSize, "%s %s",
                       field->name, tDataTypes[type].name);
    *outBytes = out;
  }

  return 0;
}


static int32_t applySchemaAction(TAOS* taos, SSchemaAction* action, SSmlLinesInfo* info) {
  int32_t code = 0;
  int32_t outBytes = 0;
  char *result = (char *)calloc(1, tsMaxSQLStringLen+1);
  int32_t capacity = tsMaxSQLStringLen +  1;

  uDebug("SML:0x%"PRIx64" apply schema action. action: %d", info->id, action->action);
  switch (action->action) {
    case SCHEMA_ACTION_ADD_COLUMN: {
      int n = sprintf(result, "alter stable %s add column ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field, result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      char* errStr = taos_errstr(res);
      char* begin = strstr(errStr, "duplicated column names");
      bool tscDupColNames = (begin != NULL);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error: %s", info->id, errStr);
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_FIELD_ALREAY_EXIST || code == TSDB_CODE_MND_TAG_ALREAY_EXIST || tscDupColNames) {
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
      char* errStr = taos_errstr(res);
      char* begin = strstr(errStr, "duplicated column names");
      bool tscDupColNames = (begin != NULL);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_TAG_ALREAY_EXIST || code == TSDB_CODE_MND_FIELD_ALREAY_EXIST || tscDupColNames) {
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

      if (code == TSDB_CODE_MND_INVALID_COLUMN_LENGTH || code == TSDB_CODE_TSC_INVALID_COLUMN_LENGTH) {
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

      if (code == TSDB_CODE_MND_INVALID_TAG_LENGTH || code == TSDB_CODE_TSC_INVALID_TAG_LENGTH) {
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
      size_t numCols = taosArrayGetSize(action->createSTable.fields);
      for (int32_t i = 0; i < numCols; ++i) {
        SSchema* field = taosArrayGet(action->createSTable.fields, i);
        buildColumnDescription(field, pos, freeBytes, &outBytes);
        pos += outBytes; freeBytes -= outBytes;
        *pos = ','; ++pos; --freeBytes;
      }
      --pos; ++freeBytes;

      outBytes = snprintf(pos, freeBytes, ") tags (");
      pos += outBytes; freeBytes -= outBytes;

      size_t numTags = taosArrayGetSize(action->createSTable.tags);
      for (int32_t i = 0; i < numTags; ++i) {
        SSchema* field = taosArrayGet(action->createSTable.tags, i);
        buildColumnDescription(field, pos, freeBytes, &outBytes);
        pos += outBytes; freeBytes -= outBytes;
        *pos = ','; ++pos; --freeBytes;
      }
      pos--; ++freeBytes;
      outBytes = snprintf(pos, freeBytes, ")");
      TAOS_RES* res = taos_query(taos, result);
      code = taos_errno(res);
      if (code != TSDB_CODE_SUCCESS) {
        uError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_TABLE_ALREADY_EXIST) {
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
  size_t numStable = taosHashGetSize(info->superTables);

  SSmlSTableMeta** tableMetaSml = taosHashIterate(info->superTables, NULL);
  while (tableMetaSml) {
    SSmlSTableMeta* cTablePoints = *tableMetaSml;

    if (NULL == pStmt->pCatalog) {
      STMT_ERR_RET(catalogGetHandle(pStmt->taos->pAppInfo->clusterId, &pStmt->pCatalog));
    }

    STableMeta *pTableMeta = NULL;
    SEpSet ep = getEpSet_s(&pStmt->taos->pAppInfo->mgmtEp);
    STMT_ERR_RET(catalogGetTableMeta(pStmt->pCatalog, pStmt->taos->pAppInfo->pTransporter, &ep, &pStmt->bInfo.sname, &pTableMeta));

    if (pTableMeta->uid == pStmt->bInfo.tbUid) {
      pStmt->bInfo.needParse = false;

      return TSDB_CODE_SUCCESS;
    }

//  for (int i = 0; i < numStable; ++i) {
    SSmlSTableSchema* pointSchema = taosArrayGet(stableSchemas, i);
    SSmlSTableSchema  dbSchema;
    memset(&dbSchema, 0, sizeof(SSmlSTableSchema));

    code = loadTableSchemaFromDB(taos, pointSchema->sTableName, &dbSchema, info);
    if (code == TSDB_CODE_MND_INVALID_TABLE_NAME) {
      SSchemaAction schemaAction = {0};
      schemaAction.action = SCHEMA_ACTION_CREATE_STABLE;
      memset(&schemaAction.createSTable, 0, sizeof(SCreateSTableActionInfo));
      memcpy(schemaAction.createSTable.sTableName, pointSchema->sTableName, TSDB_TABLE_NAME_LEN);
      schemaAction.createSTable.tags = pointSchema->tags;
      schemaAction.createSTable.fields = pointSchema->fields;
      applySchemaAction(taos, &schemaAction, info);
      code = loadTableSchemaFromDB(taos, pointSchema->sTableName, &dbSchema, info);
      if (code != 0) {
        uError("SML:0x%"PRIx64" reconcile point schema failed. can not create %s", info->id, pointSchema->sTableName);
        return code;
      }
    }

    if (code == TSDB_CODE_SUCCESS) {
      pointSchema->precision = dbSchema.precision;

      size_t pointTagSize = taosArrayGetSize(pointSchema->tags);
      size_t pointFieldSize = taosArrayGetSize(pointSchema->fields);

      SHashObj* dbTagHash = dbSchema.tagHash;
      SHashObj* dbFieldHash = dbSchema.fieldHash;

      for (int j = 0; j < pointTagSize; ++j) {
        SSchema* pointTag = taosArrayGet(pointSchema->tags, j);
        SSchemaAction schemaAction = {0};
        bool actionNeeded = false;
        generateSchemaAction(pointTag, dbTagHash, dbSchema.tags, true, pointSchema->sTableName,
                             &schemaAction, &actionNeeded, info);
        if (actionNeeded) {
          code = applySchemaAction(taos, &schemaAction, info);
          if (code != 0) {
            destroySmlSTableSchema(&dbSchema);
            return code;
          }
        }
      }

      SSchema* pointColTs = taosArrayGet(pointSchema->fields, 0);
      SSchema* dbColTs = taosArrayGet(dbSchema.fields, 0);
      memcpy(pointColTs->name, dbColTs->name, TSDB_COL_NAME_LEN);

      for (int j = 1; j < pointFieldSize; ++j) {
        SSchema* pointCol = taosArrayGet(pointSchema->fields, j);
        SSchemaAction schemaAction = {0};
        bool actionNeeded = false;
        generateSchemaAction(pointCol, dbFieldHash, dbSchema.fields,false, pointSchema->sTableName,
                             &schemaAction, &actionNeeded, info);
        if (actionNeeded) {
          code = applySchemaAction(taos, &schemaAction, info);
          if (code != 0) {
            destroySmlSTableSchema(&dbSchema);
            return code;
          }
        }
      }

      pointSchema->precision = dbSchema.precision;

      destroySmlSTableSchema(&dbSchema);
    } else {
      uError("SML:0x%"PRIx64" load table meta error: %s", info->id, tstrerror(code));
      return code;
    }
    tableMetaSml = taosHashIterate(info->superTables, tableMetaSml);
  }
//  }
  return 0;
}

static int32_t arrangePointsByChildTableName(TAOS_SML_DATA_POINT* points, int numPoints,
                                             SHashObj* cname2points, SArray* stableSchemas, SSmlLinesInfo* info) {
  for (int32_t i = 0; i < numPoints; ++i) {
    TAOS_SML_DATA_POINT * point = points + i;
    SSmlSTableSchema* stableSchema = taosArrayGet(stableSchemas, point->schemaIdx);

    for (int j = 0; j < point->tagNum; ++j) {
      TAOS_SML_KV* kv =  point->tags + j;
      if (kv->type == TSDB_DATA_TYPE_TIMESTAMP) {
        int64_t ts = *(int64_t*)(kv->value);
        ts = convertTimePrecision(ts, TSDB_TIME_PRECISION_NANO, stableSchema->precision);
        *(int64_t*)(kv->value) = ts;
      }
    }

    for (int j = 0; j < point->fieldNum; ++j) {
      TAOS_SML_KV* kv =  point->fields + j;
      if (kv->type == TSDB_DATA_TYPE_TIMESTAMP) {
        int64_t ts = *(int64_t*)(kv->value);
        ts = convertTimePrecision(ts, TSDB_TIME_PRECISION_NANO, stableSchema->precision);
        *(int64_t*)(kv->value) = ts;
      }
    }

    SArray* cTablePoints = NULL;
    SArray** pCTablePoints = taosHashGet(cname2points, point->childTableName, strlen(point->childTableName));
    if (pCTablePoints) {
      cTablePoints = *pCTablePoints;
    } else {
      cTablePoints = taosArrayInit(64, sizeof(point));
      taosHashPut(cname2points, point->childTableName, strlen(point->childTableName), &cTablePoints, POINTER_BYTES);
    }
    taosArrayPush(cTablePoints, &point);
  }

  return 0;
}

static int32_t applyChildTableDataPointsWithInsertSQL(TAOS* taos, char* cTableName, char* sTableName, SSmlSTableSchema* sTableSchema,
                                                  SArray* cTablePoints, size_t rowSize, SSmlLinesInfo* info) {
  int32_t code = TSDB_CODE_SUCCESS;
  size_t  numTags = taosArrayGetSize(sTableSchema->tags);
  size_t  numCols = taosArrayGetSize(sTableSchema->fields);
  size_t  rows = taosArrayGetSize(cTablePoints);
  SArray* tagsSchema = sTableSchema->tags;
  SArray* colsSchema = sTableSchema->fields;

  TAOS_SML_KV* tagKVs[TSDB_MAX_TAGS] = {0};
  for (int i = 0; i < rows; ++i) {
    TAOS_SML_DATA_POINT* pDataPoint = taosArrayGetP(cTablePoints, i);
    for (int j = 0; j < pDataPoint->tagNum; ++j) {
      TAOS_SML_KV* kv = pDataPoint->tags + j;
      tagKVs[kv->fieldSchemaIdx] = kv;
    }
  }

  char* sql = taosMemoryMalloc(tsMaxSQLStringLen + 1);
  if (sql == NULL) {
    uError("taosMemoryMalloc sql memory error");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int32_t freeBytes = tsMaxSQLStringLen + 1;
  int32_t totalLen = 0;
  totalLen += sprintf(sql, "insert into %s using %s (", cTableName, sTableName);
  for (int i = 0; i < numTags; ++i) {
    SSchema* tagSchema = taosArrayGet(tagsSchema, i);
    totalLen += snprintf(sql + totalLen, freeBytes - totalLen, "%s,", tagSchema->name);
  }
  --totalLen;
  totalLen += snprintf(sql + totalLen, freeBytes - totalLen, ")");

  totalLen += snprintf(sql + totalLen, freeBytes - totalLen, " tags (");

  //  for (int i = 0; i < numTags; ++i) {
  //    snprintf(sql+strlen(sql), freeBytes-strlen(sql), "?,");
  //  }
  for (int i = 0; i < numTags; ++i) {
    if (tagKVs[i] == NULL) {
      totalLen += snprintf(sql + totalLen, freeBytes - totalLen, "NULL,");
    } else {
      TAOS_SML_KV* kv = tagKVs[i];
      size_t       beforeLen = totalLen;
      int32_t      len = 0;
      converToStr(sql + beforeLen, kv->type, kv->value, kv->length, &len);
      totalLen += len;
      totalLen += snprintf(sql + totalLen, freeBytes - totalLen, ",");
    }
  }
  --totalLen;
  totalLen += snprintf(sql + totalLen, freeBytes - totalLen, ") (");

  for (int i = 0; i < numCols; ++i) {
    SSchema* colSchema = taosArrayGet(colsSchema, i);
    totalLen += snprintf(sql + totalLen, freeBytes - totalLen, "%s,", colSchema->name);
  }
  --totalLen;
  totalLen += snprintf(sql + totalLen, freeBytes - totalLen, ") values ");

  TAOS_SML_KV** colKVs = taosMemoryMalloc(numCols * sizeof(TAOS_SML_KV*));
  for (int r = 0; r < rows; ++r) {
    totalLen += snprintf(sql + totalLen, freeBytes - totalLen, "(");

    memset(colKVs, 0, numCols * sizeof(TAOS_SML_KV*));

    TAOS_SML_DATA_POINT* point = taosArrayGetP(cTablePoints, r);
    for (int i = 0; i < point->fieldNum; ++i) {
      TAOS_SML_KV* kv = point->fields + i;
      colKVs[kv->fieldSchemaIdx] = kv;
    }

    for (int i = 0; i < numCols; ++i) {
      if (colKVs[i] == NULL) {
        totalLen += snprintf(sql + totalLen, freeBytes - totalLen, "NULL,");
      } else {
        TAOS_SML_KV* kv = colKVs[i];
        size_t       beforeLen = totalLen;
        int32_t      len = 0;
        converToStr(sql + beforeLen, kv->type, kv->value, kv->length, &len);
        totalLen += len;
        totalLen += snprintf(sql + totalLen, freeBytes - totalLen, ",");
      }
    }
    --totalLen;
    totalLen += snprintf(sql + totalLen, freeBytes - totalLen, ")");
  }
  taosMemoryFree(colKVs);
  sql[totalLen] = '\0';

  uDebug("SML:0x%" PRIx64 " insert child table table %s of super table %s sql: %s", info->id, cTableName, sTableName,
           sql);

  bool tryAgain = false;
  int32_t try = 0;
  do {
    TAOS_RES* res = taos_query(taos, sql);
    code = taos_errno(res);
    if (code != 0) {
      uError("SML:0x%"PRIx64 " taos_query return %d:%s", info->id, code, taos_errstr(res));
    }

    uDebug("SML:0x%"PRIx64 " taos_query inserted %d rows", info->id, taos_affected_rows(res));
    info->affectedRows += taos_affected_rows(res);
    taos_free_result(res);

    tryAgain = false;
    if ((code == TSDB_CODE_TDB_INVALID_TABLE_ID
         || code == TSDB_CODE_VND_INVALID_VGROUP_ID
         || code == TSDB_CODE_TDB_TABLE_RECONFIGURE
         || code == TSDB_CODE_APP_NOT_READY
         || code == TSDB_CODE_RPC_NETWORK_UNAVAIL) && try++ < TSDB_MAX_REPLICA) {
      tryAgain = true;
    }

    if (code == TSDB_CODE_TDB_INVALID_TABLE_ID || code == TSDB_CODE_VND_INVALID_VGROUP_ID) {
      TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
      int32_t   code2 = taos_errno(res2);
      if (code2 != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " insert child table by sql. reset query cache. error: %s", info->id, taos_errstr(res2));
      }
      taos_free_result(res2);
      if (tryAgain) {
        taosMsleep(100 * (2 << try));
      }
    }

    if (code == TSDB_CODE_APP_NOT_READY || code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
      if (tryAgain) {
        taosMsleep( 100 * (2 << try));
      }
    }
  } while (tryAgain);

  taosMemoryFree(sql);

  return code;
}

static int32_t applyChildTableDataPointsWithStmt(TAOS* taos, char* cTableName, char* sTableName, SSmlSTableSchema* sTableSchema,
                                         SArray* cTablePoints, size_t rowSize, SSmlLinesInfo* info) {
  size_t numTags = taosArrayGetSize(sTableSchema->tags);
  size_t numCols = taosArrayGetSize(sTableSchema->fields);
  size_t rows = taosArrayGetSize(cTablePoints);

  TAOS_SML_KV* tagKVs[TSDB_MAX_TAGS] = {0};
  for (int i= 0; i < rows; ++i) {
    TAOS_SML_DATA_POINT * pDataPoint = taosArrayGetP(cTablePoints, i);
    for (int j = 0; j < pDataPoint->tagNum; ++j) {
      TAOS_SML_KV* kv = pDataPoint->tags + j;
      tagKVs[kv->fieldSchemaIdx] = kv;
    }
  }

  //tag bind
  SArray* tagBinds = taosArrayInit(numTags, sizeof(TAOS_BIND));
  taosArraySetSize(tagBinds, numTags);
  int isNullColBind = TSDB_TRUE;
  for (int j = 0; j < numTags; ++j) {
    TAOS_BIND* bind = taosArrayGet(tagBinds, j);
    bind->is_null = &isNullColBind;
  }
  for (int j = 0; j < numTags; ++j) {
    if (tagKVs[j] == NULL) continue;
    TAOS_SML_KV* kv =  tagKVs[j];
    TAOS_BIND* bind = taosArrayGet(tagBinds, kv->fieldSchemaIdx);
    bind->buffer_type = kv->type;
    bind->length = taosMemoryMalloc(sizeof(uintptr_t*));
    *bind->length = kv->length;
    bind->buffer = kv->value;
    bind->is_null = NULL;
  }

  //rows bind
  SArray* rowsBind = taosArrayInit(rows, POINTER_BYTES);
  for (int i = 0; i < rows; ++i) {
    TAOS_SML_DATA_POINT* point = taosArrayGetP(cTablePoints, i);

    TAOS_BIND* colBinds = calloc(numCols, sizeof(TAOS_BIND));
    if (colBinds == NULL) {
      uError("SML:0x%"PRIx64" taos_sml_insert insert points, failed to allocated memory for TAOS_BIND, "
               "num of rows: %zu, num of cols: %zu", info->id, rows, numCols);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    for (int j = 0; j < numCols; ++j) {
      TAOS_BIND* bind = colBinds + j;
      bind->is_null = &isNullColBind;
    }
    for (int j = 0; j < point->fieldNum; ++j) {
      TAOS_SML_KV* kv = point->fields + j;
      TAOS_BIND* bind = colBinds + kv->fieldSchemaIdx;
      bind->buffer_type = kv->type;
      bind->length = taosMemoryMalloc(sizeof(uintptr_t*));
      *bind->length = kv->length;
      bind->buffer = kv->value;
      bind->is_null = NULL;
    }
    taosArrayPush(rowsBind, &colBinds);
  }

  int32_t code = 0;
  code = insertChildTablePointsBatch(taos, cTableName, sTableName, sTableSchema->tags, tagBinds, sTableSchema->fields, rowsBind, rowSize, info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" insert into child table %s failed. error %s", info->id, cTableName, tstrerror(code));
  }

  //taosMemoryFree rows bind
  for (int i = 0; i < rows; ++i) {
    TAOS_BIND* colBinds = taosArrayGetP(rowsBind, i);
    for (int j = 0; j < numCols; ++j) {
      TAOS_BIND* bind = colBinds + j;
      taosMemoryFree(bind->length);
    }
    taosMemoryFree(colBinds);
  }
  taosArrayDestroy(&rowsBind);
  //taosMemoryFree tag bind
  for (int i = 0; i < taosArrayGetSize(tagBinds); ++i) {
    TAOS_BIND* bind = taosArrayGet(tagBinds, i);
    taosMemoryFree(bind->length);
  }
  taosArrayDestroy(&tagBinds);
  return code;
}

static int32_t insertChildTablePointsBatch(TAOS* taos, char* cTableName, char* sTableName,
                                           SArray* tagsSchema, SArray* tagsBind,
                                           SArray* colsSchema, SArray* rowsBind,
                                           size_t rowSize, SSmlLinesInfo* info) {
  size_t numTags = taosArrayGetSize(tagsSchema);
  size_t numCols = taosArrayGetSize(colsSchema);
  char* sql = taosMemoryMalloc(tsMaxSQLStringLen+1);
  if (sql == NULL) {
    uError("taosMemoryMalloc sql memory error");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int32_t freeBytes = tsMaxSQLStringLen + 1 ;
  sprintf(sql, "insert into ? using %s (", sTableName);
  for (int i = 0; i < numTags; ++i) {
    SSchema* tagSchema = taosArrayGet(tagsSchema, i);
    snprintf(sql+strlen(sql), freeBytes-strlen(sql), "%s,", tagSchema->name);
  }
  snprintf(sql + strlen(sql) - 1, freeBytes-strlen(sql)+1, ")");

  snprintf(sql + strlen(sql), freeBytes-strlen(sql), " tags (");

  for (int i = 0; i < numTags; ++i) {
    snprintf(sql+strlen(sql), freeBytes-strlen(sql), "?,");
  }
  snprintf(sql + strlen(sql) - 1, freeBytes-strlen(sql)+1, ") (");

  for (int i = 0; i < numCols; ++i) {
    SSchema* colSchema = taosArrayGet(colsSchema, i);
    snprintf(sql+strlen(sql), freeBytes-strlen(sql), "%s,", colSchema->name);
  }
  snprintf(sql + strlen(sql)-1, freeBytes-strlen(sql)+1, ") values (");

  for (int i = 0; i < numCols; ++i) {
    snprintf(sql+strlen(sql), freeBytes-strlen(sql), "?,");
  }
  snprintf(sql + strlen(sql)-1, freeBytes-strlen(sql)+1, ")");
  sql[strlen(sql)] = '\0';

  uDebug("SML:0x%"PRIx64" insert child table table %s of super table %s : %s", info->id, cTableName, sTableName, sql);

  size_t maxBatchSize = TSDB_MAX_WAL_SIZE/rowSize * 2 / 3;
  size_t rows = taosArrayGetSize(rowsBind);
  size_t batchSize = MIN(maxBatchSize, rows);
  uDebug("SML:0x%"PRIx64" insert rows into child table %s. num of rows: %zu, batch size: %zu",
           info->id, cTableName, rows, batchSize);
  SArray* batchBind = taosArrayInit(batchSize, POINTER_BYTES);
  int32_t code = TSDB_CODE_SUCCESS;
  for (int i = 0; i < rows;) {
    int j = i;
    for (; j < i + batchSize && j<rows; ++j) {
      taosArrayPush(batchBind, taosArrayGet(rowsBind, j));
    }
    if (j > i) {
      uDebug("SML:0x%"PRIx64" insert child table batch from line %d to line %d.", info->id, i, j - 1);
      code = doInsertChildTablePoints(taos, sql, cTableName, tagsBind, batchBind, info);
      if (code != 0) {
        taosArrayDestroy(&batchBind);
        tfree(sql);
        return code;
      }
      taosArrayClear(batchBind);
    }
    i = j;
  }
  taosArrayDestroy(&batchBind);
  tfree(sql);
  return code;

}
static int32_t doInsertChildTablePoints(TAOS* taos, char* sql, char* cTableName, SArray* tagsBind, SArray* batchBind,
                                        SSmlLinesInfo* info) {
  int32_t code = 0;

  TAOS_STMT* stmt = taos_stmt_init(taos);
  if (stmt == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  code = taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));

  if (code != 0) {
    uError("SML:0x%"PRIx64" taos_stmt_prepare return %d:%s", info->id, code, taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return code;
  }

  bool tryAgain = false;
  int32_t try = 0;
  do {
    code = taos_stmt_set_tbname_tags(stmt, cTableName, TARRAY_GET_START(tagsBind));
    if (code != 0) {
      uError("SML:0x%"PRIx64" taos_stmt_set_tbname return %d:%s", info->id, code, taos_stmt_errstr(stmt));

      int affectedRows = taos_stmt_affected_rows(stmt);
      info->affectedRows += affectedRows;

      taos_stmt_close(stmt);
      return code;
    }

    size_t rows = taosArrayGetSize(batchBind);
    for (int32_t i = 0; i < rows; ++i) {
      TAOS_BIND* colsBinds = taosArrayGetP(batchBind, i);
      code = taos_stmt_bind_param(stmt, colsBinds);
      if (code != 0) {
        uError("SML:0x%"PRIx64" taos_stmt_bind_param return %d:%s", info->id, code, taos_stmt_errstr(stmt));

        int affectedRows = taos_stmt_affected_rows(stmt);
        info->affectedRows += affectedRows;

        taos_stmt_close(stmt);
        return code;
      }
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        uError("SML:0x%"PRIx64" taos_stmt_add_batch return %d:%s", info->id, code, taos_stmt_errstr(stmt));

        int affectedRows = taos_stmt_affected_rows(stmt);
        info->affectedRows += affectedRows;

        taos_stmt_close(stmt);
        return code;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      uError("SML:0x%"PRIx64" taos_stmt_execute return %d:%s, try:%d", info->id, code, taos_stmt_errstr(stmt), try);
    }
    uDebug("SML:0x%"PRIx64" taos_stmt_execute inserted %d rows", info->id, taos_stmt_affected_rows(stmt));

    tryAgain = false;
    if ((code == TSDB_CODE_TDB_INVALID_TABLE_ID
         || code == TSDB_CODE_VND_INVALID_VGROUP_ID
         || code == TSDB_CODE_TDB_TABLE_RECONFIGURE
         || code == TSDB_CODE_APP_NOT_READY
         || code == TSDB_CODE_RPC_NETWORK_UNAVAIL) && try++ < TSDB_MAX_REPLICA) {
      tryAgain = true;
    }

    if (code == TSDB_CODE_TDB_INVALID_TABLE_ID || code == TSDB_CODE_VND_INVALID_VGROUP_ID) {
      TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
      int32_t   code2 = taos_errno(res2);
      if (code2 != TSDB_CODE_SUCCESS) {
        uError("SML:0x%" PRIx64 " insert child table. reset query cache. error: %s", info->id, taos_errstr(res2));
      }
      taos_free_result(res2);
      if (tryAgain) {
        taosMsleep(100 * (2 << try));
      }
    }
    if (code == TSDB_CODE_APP_NOT_READY || code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
      if (tryAgain) {
        taosMsleep( 100 * (2 << try));
      }
    }
  } while (tryAgain);

  int affectedRows = taos_stmt_affected_rows(stmt);
  info->affectedRows += affectedRows;

  taos_stmt_close(stmt);
  return code;

  return 0;
}

static int32_t applyChildTableDataPoints(TAOS* taos, char* cTableName, char* sTableName, SSmlSTableSchema* sTableSchema,
                                                 SArray* cTablePoints, size_t rowSize, SSmlLinesInfo* info) {
  int32_t code = TSDB_CODE_SUCCESS;
  size_t childTableDataPoints = taosArrayGetSize(cTablePoints);
  if (childTableDataPoints < 10) {
    code = applyChildTableDataPointsWithInsertSQL(taos, cTableName, sTableName, sTableSchema, cTablePoints, rowSize, info);
  } else {
    code = applyChildTableDataPointsWithStmt(taos, cTableName, sTableName, sTableSchema, cTablePoints, rowSize, info);
  }
  return code;
}

static int32_t applyDataPoints(TAOS* taos, TAOS_SML_DATA_POINT* points, int32_t numPoints, SArray* stableSchemas, SSmlLinesInfo* info) {
  int32_t code = TSDB_CODE_SUCCESS;

  SHashObj* cname2points = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
  arrangePointsByChildTableName(points, numPoints, cname2points, stableSchemas, info);

  SArray** pCTablePoints = taosHashIterate(cname2points, NULL);
  while (pCTablePoints) {
    SArray* cTablePoints = *pCTablePoints;

    TAOS_SML_DATA_POINT* point = taosArrayGetP(cTablePoints, 0);
    SSmlSTableSchema*    sTableSchema = taosArrayGet(stableSchemas, point->schemaIdx);

    size_t rowSize = 0;
    size_t numCols = taosArrayGetSize(sTableSchema->fields);
    for (int i = 0; i < numCols; ++i) {
      SSchema* colSchema = taosArrayGet(sTableSchema->fields, i);
      rowSize += colSchema->bytes;
    }

    uDebug("SML:0x%"PRIx64" apply child table points. child table: %s of super table %s, row size: %zu",
             info->id, point->childTableName, point->stableName, rowSize);
    code = applyChildTableDataPoints(taos, point->childTableName, point->stableName, sTableSchema, cTablePoints, rowSize, info);
    if (code != 0) {
      uError("SML:0x%"PRIx64" Apply child table points failed. child table %s, error %s", info->id, point->childTableName, tstrerror(code));
      goto cleanup;
    }

    uDebug("SML:0x%"PRIx64" successfully applied data points of child table %s", info->id, point->childTableName);

    pCTablePoints = taosHashIterate(cname2points, pCTablePoints);
  }

cleanup:
  pCTablePoints = taosHashIterate(cname2points, NULL);
  while (pCTablePoints) {
    SArray* pPoints = *pCTablePoints;
    taosArrayDestroy(&pPoints);
    pCTablePoints = taosHashIterate(cname2points, pCTablePoints);
  }
  taosHashCleanup(cname2points);
  return code;
}

static int doSmlInsertOneDataPoint(TAOS* taos, TAOS_SML_DATA_POINT* point, SSmlLinesInfo* info) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (!point->childTableName) {
    int tableNameLen = TSDB_TABLE_NAME_LEN;
    point->childTableName = calloc(1, tableNameLen + 1);
    getSmlMd5ChildTableName(point, point->childTableName, &tableNameLen, info);
    point->childTableName[tableNameLen] = '\0';
  }

  STableMeta* tableMeta = NULL;
  int32_t ret = getSuperTableMetaFromLocalCache(taos, point->stableName, &tableMeta, info);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }
  uint8_t precision = tableMeta->tableInfo.precision;
  taosMemoryFree(tableMeta);

  char* sql = taosMemoryMalloc(TSDB_MAX_SQL_LEN + 1);
  int   freeBytes = TSDB_MAX_SQL_LEN;
  int   sqlLen = 0;
  sqlLen += snprintf(sql + sqlLen, freeBytes - sqlLen, "insert into %s(", point->childTableName);
  for (int col = 0; col < point->fieldNum; ++col) {
    TAOS_SML_KV* kv = point->fields + col;
    sqlLen += snprintf(sql + sqlLen, freeBytes - sqlLen, "%s,", kv->key);
  }
  --sqlLen;
  sqlLen += snprintf(sql + sqlLen, freeBytes - sqlLen, ") values (");
  TAOS_SML_KV* tsField = point->fields + 0;
  int64_t      ts = *(int64_t*)(tsField->value);
  ts = convertTimePrecision(ts, TSDB_TIME_PRECISION_NANO, precision);
  sqlLen += snprintf(sql + sqlLen, freeBytes - sqlLen, "%" PRId64 ",", ts);
  for (int col = 1; col < point->fieldNum; ++col) {
    TAOS_SML_KV* kv = point->fields + col;
    int32_t      len = 0;
    converToStr(sql + sqlLen, kv->type, kv->value, kv->length, &len);
    sqlLen += len;
    sqlLen += snprintf(sql + sqlLen, freeBytes - sqlLen, ",");
  }
  --sqlLen;
  sqlLen += snprintf(sql + sqlLen, freeBytes - sqlLen, ")");
  sql[sqlLen] = 0;

  uDebug("SML:0x%" PRIx64 " insert child table table %s of super table %s sql: %s", info->id,
           point->childTableName, point->stableName, sql);
  TAOS_RES* res = taos_query(taos, sql);
  taosMemoryFree(sql);
  code = taos_errno(res);
  info->affectedRows = taos_affected_rows(res);
  taos_free_result(res);

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
  code = applyDataPoints(taos, points, numPoint, stableSchemas, info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" error apply data points : %s", info->id, tstrerror(code));
  }

clean_up:
  for (int i = 0; i < taosArrayGetSize(stableSchemas); ++i) {
    SSmlSTableSchema* schema = taosArrayGet(stableSchemas, i);
    taosArrayDestroy(&schema->fields);
    taosArrayDestroy(&schema->tags);
  }
  taosArrayDestroy(&stableSchemas);
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

char* addEscapeCharToString(char *str, int32_t len) {
  if (str == NULL) {
    return NULL;
  }
  memmove(str + 1, str, len);
  str[0] = str[len + 1] = TS_BACKQUOTE_CHAR;
  str[len + 2] = '\0';
  return str;
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

static int32_t isTimeStamp(char *pVal, uint16_t len, SMLTimeStampType *tsType, SSmlLinesInfo* info) {
  if (len == 0) {
    return TSDB_CODE_SUCCESS;
  }
  if ((len == 1) && pVal[0] == '0') {
    *tsType = SML_TIME_STAMP_NOW;
    return TSDB_CODE_SUCCESS;
  }

  for (int i = 0; i < len; ++i) {
    if(!isdigit(pVal[i])) {
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
  }

  /* For InfluxDB line protocol use user passed timestamp precision
   * For OpenTSDB protocols only 10 digit(seconds) or 13 digits(milliseconds)
   *     precision allowed
   */
  if (info->protocol == TSDB_SML_LINE_PROTOCOL) {
    if (info->tsType != SML_TIME_STAMP_NOT_CONFIGURED) {
      *tsType = info->tsType;
    } else {
      *tsType = SML_TIME_STAMP_NANO_SECONDS;
    }
  } else if (info->protocol == TSDB_SML_TELNET_PROTOCOL) {
    if (len == SML_TIMESTAMP_SECOND_DIGITS) {
      *tsType = SML_TIME_STAMP_SECONDS;
    } else if (len == SML_TIMESTAMP_MILLI_SECOND_DIGITS) {
      *tsType = SML_TIME_STAMP_MILLI_SECONDS;
    } else {
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
  }
  return TSDB_CODE_SUCCESS;

  //if (pVal[len - 1] == 's') {
  //  switch (pVal[len - 2]) {
  //    case 'm':
  //      *tsType = SML_TIME_STAMP_MILLI_SECONDS;
  //      break;
  //    case 'u':
  //      *tsType = SML_TIME_STAMP_MICRO_SECONDS;
  //      break;
  //    case 'n':
  //      *tsType = SML_TIME_STAMP_NANO_SECONDS;
  //      break;
  //    default:
  //      if (isdigit(pVal[len - 2])) {
  //        *tsType = SML_TIME_STAMP_SECONDS;
  //        break;
  //      } else {
  //        return false;
  //      }
  //  }
  //  //printf("Type is timestamp(%s)\n", pVal);
  //  return true;
  //}
  //return false;
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

static int32_t getTimeStampValue(char *value, uint16_t len,
                                 SMLTimeStampType type, int64_t *ts, SSmlLinesInfo* info) {

  //No appendix or no timestamp given (len = 0)
  if (len != 0 && type != SML_TIME_STAMP_NOW) {
    *ts = (int64_t)strtoll(value, NULL, 10);
  } else {
    type = SML_TIME_STAMP_NOW;
  }
  switch (type) {
    case SML_TIME_STAMP_NOW: {
      *ts = taosGetTimestampNs();
      break;
    }
    case SML_TIME_STAMP_HOURS: {
      *ts = (int64_t)(*ts * 3600 * 1e9);
      break;
    }
    case SML_TIME_STAMP_MINUTES: {
      *ts = (int64_t)(*ts * 60 * 1e9);
      break;
    }
    case SML_TIME_STAMP_SECONDS: {
      *ts = (int64_t)(*ts * 1e9);
      break;
    }
    case SML_TIME_STAMP_MILLI_SECONDS: {
      *ts = convertTimePrecision(*ts, TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_NANO);
      break;
    }
    case SML_TIME_STAMP_MICRO_SECONDS: {
      *ts = convertTimePrecision(*ts, TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_NANO);
      break;
    }
    case SML_TIME_STAMP_NANO_SECONDS: {
      *ts = *ts * 1;
      break;
    }
    default: {
      return TSDB_CODE_TSC_INVALID_TIME_STAMP;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t convertSmlTimeStamp(TAOS_SML_KV *pVal, char *value,
                            uint16_t len, SSmlLinesInfo* info) {
  int32_t ret;
  SMLTimeStampType type = SML_TIME_STAMP_NOW;
  int64_t tsVal;

  ret = isTimeStamp(value, len, &type, info);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  ret = getTimeStampValue(value, len, type, &tsVal, info);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }
  uDebug("SML:0x%"PRIx64"Timestamp after conversion:%"PRId64, info->id, tsVal);

  pVal->type = TSDB_DATA_TYPE_TIMESTAMP;
  pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
  pVal->value = calloc(pVal->length, 1);
  memcpy(pVal->value, &tsVal, pVal->length);
  return TSDB_CODE_SUCCESS;
}

static int32_t parseSmlTimeStamp(TAOS_SML_KV **pTS, const char **index, SSmlLinesInfo* info) {
  const char *start, *cur;
  int32_t ret = TSDB_CODE_SUCCESS;
  int len = 0;
  char key[] = "ts";
  char *value = NULL;

  start = cur = *index;
  *pTS = calloc(1, sizeof(TAOS_SML_KV));

  while(*cur != '\0') {
    cur++;
    len++;
  }

  if (len > 0) {
    value = calloc(len + 1, 1);
    memcpy(value, start, len);
  }

  ret = convertSmlTimeStamp(*pTS, value, len, info);
  if (ret) {
    taosMemoryFree(value);
    taosMemoryFree(*pTS);
    return ret;
  }
  taosMemoryFree(value);

  (*pTS)->key = calloc(sizeof(key), 1);
  memcpy((*pTS)->key, key, sizeof(key));
  return ret;
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

static int32_t parseSmlKey(TAOS_SML_KV *pKV, const char **index, SHashObj *pHash, SSmlLinesInfo* info) {
  const char *cur = *index;
  char key[TSDB_COL_NAME_LEN + 1];  // +1 to avoid key[len] over write
  int16_t len = 0;

  while (*cur != '\0') {
    if (len > TSDB_COL_NAME_LEN - 1) {
      uError("SML:0x%"PRIx64" Key field cannot exceeds %d characters", info->id, TSDB_COL_NAME_LEN - 1);
      return TSDB_CODE_TSC_INVALID_COLUMN_LENGTH;
    }
    //unescaped '=' identifies a tag key
    if (*cur == '=' && *(cur - 1) != '\\') {
      break;
    }
    //Escape special character
    if (*cur == '\\') {
      escapeSpecialCharacter(2, &cur);
    }
    key[len] = *cur;
    cur++;
    len++;
  }
  if (len == 0) {
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }
  key[len] = '\0';

  if (checkDuplicateKey(key, pHash, info)) {
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  pKV->key = calloc(len + TS_BACKQUOTE_CHAR_SIZE + 1, 1);
  memcpy(pKV->key, key, len + 1);
  addEscapeCharToString(pKV->key, len);
  uDebug("SML:0x%"PRIx64" Key:%s|len:%d", info->id, pKV->key, len);
  *index = cur + 1;
  return TSDB_CODE_SUCCESS;
}


static int32_t parseSmlValue(TAOS_SML_KV *pKV, const char **index,
                          bool *is_last_kv, SSmlLinesInfo* info, bool isTag) {
  const char *start, *cur;
  int32_t     ret = TSDB_CODE_SUCCESS;
  char       *value = NULL;
  int16_t     len = 0;

  bool   kv_done = false;
  bool   back_slash = false;
  bool   double_quote = false;
  size_t line_len = 0;

  enum {
    tag_common,
    tag_lqoute,
    tag_rqoute
  } tag_state;

  enum {
    val_common,
    val_lqoute,
    val_rqoute
  } val_state;

  start = cur = *index;
  tag_state = tag_common;
  val_state = val_common;

  while (1) {
    if (isTag) {
      /* ',', '=' and spaces MUST be escaped */
      switch (tag_state) {
      case tag_common:
        if (back_slash == true) {
          if (*cur != ',' && *cur != '=' && *cur != ' ') {
            uError("SML:0x%"PRIx64" tag value: state(%d), incorrect character(%c) escaped", info->id, tag_state, *cur);
            ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
            goto error;
          }

          back_slash = false;
          cur++;
          len++;
	  break;
	}

        if (*cur == '"') {
          if (cur == *index) {
            tag_state = tag_lqoute;
          }
          cur += 1;
          len += 1;
          break;
        } else if (*cur == 'L') {
          line_len = strlen(*index);

          /* common character at the end */
          if (cur + 1 >= *index + line_len) {
            *is_last_kv = true;
            kv_done = true;
            break;
          }

          if (*(cur + 1) == '"') {
            /* string starts here */
            if (cur + 1 == *index + 1) {
              tag_state = tag_lqoute;
            }
            cur += 2;
            len += 2;
            break;
          }
        }

        switch (*cur) {
        case '\\':
          back_slash = true;
          cur++;
          len++;
          break;
        case ',':
          kv_done = true;
          break;

        case ' ':
          /* fall through */
        case '\0':
          *is_last_kv = true;
          kv_done = true;
          break;

        default:
          cur++;
          len++;
        }

        break;
      case tag_lqoute:
        if (back_slash == true) {
          if (*cur != ',' && *cur != '=' && *cur != ' ') {
            uError("SML:0x%"PRIx64" tag value: state(%d), incorrect character(%c) escaped", info->id, tag_state, *cur);
            ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
            goto error;
          }

          back_slash = false;
          cur++;
          len++;
          break;
        } else if (double_quote == true) {
          if (*cur != ' ' && *cur != ',' && *cur != '\0') {
            uError("SML:0x%"PRIx64" tag value: state(%d), incorrect character(%c) behind closing \"", info->id, tag_state, *cur);
            ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
            goto error;
          }

          if (*cur == ' ' || *cur == '\0') {
            *is_last_kv = true;
          }

          double_quote = false;
          tag_state = tag_rqoute;
          break;
        }

        switch (*cur) {
        case '\\':
          back_slash = true;
          cur++;
          len++;
          break;

        case '"':
          double_quote = true;
          cur++;
          len++;
          break;

        case ',':
          /* fall through */
        case '=':
          /* fall through */
        case ' ':
          if (*(cur - 1) != '\\') {
            uError("SML:0x%"PRIx64" tag value: state(%d), character(%c) not escaped", info->id, tag_state, *cur);
            ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
            kv_done = true;
	  }
	  break;

        case '\0':
          uError("SML:0x%"PRIx64" tag value: state(%d), closing \" not found", info->id, tag_state);
          ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
          kv_done = true;
          break;

        default:
          cur++;
          len++;
        }

        break;

      default:
        kv_done = true;
      }
    } else {
      switch (val_state) {
      case val_common:
        if (back_slash == true) {
          if (*cur != '\\' && *cur != '"') {
            uError("SML:0x%"PRIx64" field value: state(%d), incorrect character(%c) escaped", info->id, val_state, *cur);
            ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
            goto error;
          }

	  back_slash = false;
	  cur++;
	  len++;
          break;
        }

        if (*cur == '"') {
          if (cur == *index) {
            val_state = val_lqoute;
          } else {
            if (*(cur - 1) != '\\') {
              uError("SML:0x%"PRIx64" field value: state(%d), \" not escaped", info->id, val_state);
              ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
              goto error;
            }
          }

          cur += 1;
          len += 1;
          break;
        } else if (*cur == 'L') {
          line_len = strlen(*index);

          /* common character at the end */
          if (cur + 1 >= *index + line_len) {
            *is_last_kv = true;
            kv_done = true;
            break;
          }

          if (*(cur + 1) == '"') {
            /* string starts here */
            if (cur + 1 == *index + 1) {
              val_state = val_lqoute;
              cur += 2;
              len += 2;
            } else {
              /* MUST at the end of string */
              if (cur + 2 >= *index + line_len) {
                cur += 2;
                len += 2;
                *is_last_kv = true;
                kv_done = true;
              } else {
                if (*(cur + 2) != ' ' && *(cur + 2) != ',') {
                  uError("SML:0x%"PRIx64" field value: state(%d), not closing character(L\")", info->id, val_state);
                  ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
                  goto error;
                } else {
                  if (*(cur + 2) == ' ') {
                    *is_last_kv = true;
                  }

                  cur += 2;
                  len += 2;
                  kv_done = true;
                }
              }
            }
            break;
          }
        }

        switch (*cur) {
        case '\\':
          back_slash = true;
          cur++;
          len++;
          break;

        case ',':
          kv_done = true;
          break;

        case ' ':
          /* fall through */
        case '\0':
          *is_last_kv = true;
          kv_done = true;
          break;

        default:
          cur++;
          len++;
        }

        break;
      case val_lqoute:
        if (back_slash == true) {
          if (*cur != '\\' && *cur != '"') {
            uError("SML:0x%"PRIx64" field value: state(%d), incorrect character(%c) escaped", info->id, val_state, *cur);
            ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
            goto error;
          }

          back_slash = false;
          cur++;
          len++;
          break;
        } else if (double_quote == true) {
          if (*cur != ' ' && *cur != ',' && *cur != '\0') {
            uError("SML:0x%"PRIx64" field value: state(%d), incorrect character(%c) behind closing \"", info->id, val_state, *cur);
            ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
            goto error;
          }

          if (*cur == ' ' || *cur == '\0') {
            *is_last_kv = true;
          }

          double_quote = false;
          val_state = val_rqoute;
          break;
        }

        switch (*cur) {
        case '\\':
          back_slash = true;
          cur++;
          len++;
          break;

        case '"':
          double_quote = true;
          cur++;
          len++;
          break;

        case '\0':
          uError("SML:0x%"PRIx64" field value: state(%d), closing \" not found", info->id, val_state);
          ret = TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
          kv_done = true;
          break;

        default:
          cur++;
          len++;
        }

        break;
      default:
        kv_done = true;
      }
    }

    if (kv_done == true) {
      break;
    }
  }

  if (len == 0 || ret != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pKV->key);
    pKV->key = NULL;
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  value = calloc(len + 1, 1);
  memcpy(value, start, len);
  value[len] = '\0';
  if (!convertSmlValueType(pKV, value, len, info, isTag)) {
    uError("SML:0x%"PRIx64" Failed to convert sml value string(%s) to any type",
            info->id, value);
    taosMemoryFree(value);
    ret = TSDB_CODE_TSC_INVALID_VALUE;
    goto error;
  }
  taosMemoryFree(value);

  *index = (*cur == '\0') ? cur : cur + 1;
  return ret;

error:
  //taosMemoryFree previous alocated key field
  taosMemoryFree(pKV->key);
  pKV->key = NULL;
  return ret;
}

/*        Field                          Escape charaters
    1: measurement                        Comma,Space
    2: tag_key, tag_value, field_key  Comma,Equal Sign,Space
    3: field_value                    Double quote,Backslash
*/

static int32_t parseSmlMeasurement(TAOS_SML_DATA_POINT *pSml, const char **index,
                                   uint8_t *has_tags, SSmlLinesInfo* info) {
  const char *cur = *index;
  int16_t len = 0;

  pSml->stableName = calloc(TSDB_TABLE_NAME_LEN + TS_BACKQUOTE_CHAR_SIZE, 1);
  if (pSml->stableName == NULL){
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  while (*cur != '\0') {
    if (len > TSDB_TABLE_NAME_LEN - 1) {
      uError("SML:0x%"PRIx64" Measurement field cannot exceeds %d characters", info->id, TSDB_TABLE_NAME_LEN - 1);
      taosMemoryFree(pSml->stableName);
      pSml->stableName = NULL;
      return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
    }
    //first unescaped comma or space identifies measurement
    //if space detected first, meaning no tag in the input
    if (*cur == ',' && *(cur - 1) != '\\') {
      *has_tags = 1;
      break;
    }
    if (*cur == ' ' && *(cur - 1) != '\\') {
      if (*(cur + 1) != ' ') {
        break;
      }
      else {
        cur++;
        continue;
      }
    }
    //Comma, Space, Backslash needs to be escaped if any
    if (*cur == '\\') {
      escapeSpecialCharacter(1, &cur);
    }
    pSml->stableName[len] = *cur;
    cur++;
    len++;
  }
  if (len == 0) {
    taosMemoryFree(pSml->stableName);
    pSml->stableName = NULL;
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }
  addEscapeCharToString(pSml->stableName, len);
  *index = cur + 1;
  uDebug("SML:0x%"PRIx64" Stable name in measurement:%s|len:%d", info->id, pSml->stableName, len);

  return TSDB_CODE_SUCCESS;
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


static int32_t parseSmlKvPairs(TAOS_SML_KV **pKVs, int *num_kvs,
                               const char **index, bool isField,
                               TAOS_SML_DATA_POINT* smlData, SHashObj *pHash,
                               SSmlLinesInfo* info) {
  const char *cur = *index;
  int32_t ret = TSDB_CODE_SUCCESS;
  TAOS_SML_KV *pkv;
  bool is_last_kv = false;

  int32_t capacity = 0;
  if (isField) {
    capacity = 64;
    *pKVs = calloc(capacity, sizeof(TAOS_SML_KV));
    // leave space for timestamp;
    pkv = *pKVs;
    pkv++;
  } else {
    capacity = 8;
    *pKVs = calloc(capacity, sizeof(TAOS_SML_KV));
    pkv = *pKVs;
  }

  size_t childTableNameLen = strlen(tsSmlChildTableName);
  char childTableName[TSDB_TABLE_NAME_LEN + TS_BACKQUOTE_CHAR_SIZE] = {0};
  if (childTableNameLen != 0) {
    memcpy(childTableName, tsSmlChildTableName, childTableNameLen);
    addEscapeCharToString(childTableName, (int32_t)(childTableNameLen));
  }

  while (*cur != '\0') {
    ret = parseSmlKey(pkv, &cur, pHash, info);
    if (ret) {
      uError("SML:0x%"PRIx64" Unable to parse key", info->id);
      goto error;
    }
    ret = parseSmlValue(pkv, &cur, &is_last_kv, info, !isField);
    if (ret) {
      uError("SML:0x%"PRIx64" Unable to parse value", info->id);
      goto error;
    }

    if (!isField && childTableNameLen != 0 && strcasecmp(pkv->key, childTableName) == 0)  {
      smlData->childTableName = taosMemoryMalloc(pkv->length + TS_BACKQUOTE_CHAR_SIZE + 1);
      memcpy(smlData->childTableName, pkv->value, pkv->length);
      addEscapeCharToString(smlData->childTableName, (int32_t)pkv->length);
      taosMemoryFree(pkv->key);
      taosMemoryFree(pkv->value);
    } else {
      *num_kvs += 1;
    }
    if (is_last_kv) {
      goto done;
    }

    //reallocate addtional memory for more kvs
    TAOS_SML_KV *more_kvs = NULL;

    if (isField) {
      if ((*num_kvs + 2) > capacity) {
        capacity *= 3; capacity /= 2;
        more_kvs = realloc(*pKVs, capacity * sizeof(TAOS_SML_KV));
      } else {
        more_kvs = *pKVs;
      }
    } else {
      if ((*num_kvs + 1) > capacity) {
        capacity *= 3; capacity /= 2;
        more_kvs = realloc(*pKVs, capacity * sizeof(TAOS_SML_KV));
      } else {
        more_kvs = *pKVs;
      }
    }

    if (!more_kvs) {
      goto error;
    }
    *pKVs = more_kvs;
    //move pKV points to next TAOS_SML_KV block
    if (isField) {
      pkv = *pKVs + *num_kvs + 1;
    } else {
      pkv = *pKVs + *num_kvs;
    }
  }
  goto done;

error:
  return ret;
done:
  *index = cur;
  return ret;
}

static void moveTimeStampToFirstKv(TAOS_SML_DATA_POINT** smlData, TAOS_SML_KV *ts) {
  TAOS_SML_KV* tsField = (*smlData)->fields;
  tsField->length = ts->length;
  tsField->type = ts->type;
  tsField->value = taosMemoryMalloc(ts->length);
  tsField->key = taosMemoryMalloc(strlen(ts->key) + 1);
  memcpy(tsField->key, ts->key, strlen(ts->key) + 1);
  memcpy(tsField->value, ts->value, ts->length);
  (*smlData)->fieldNum = (*smlData)->fieldNum + 1;

  taosMemoryFree(ts->key);
  taosMemoryFree(ts->value);
  taosMemoryFree(ts);
}

/*        Field                          Escape charaters
    1: measurement                        Comma,Space
    2: tag_key, tag_value, field_key  Comma,Equal Sign,Space
    3: field_value                    Double quote,Backslash
*/

//void findSpace(const char** sql, const char **tags, int32_t *tagLen){
//  const char *cur = *sql;
//  *tagLen = 0;
//  *tags = NULL;
//  if(!cur) return;
//  while (*cur != '\0') {           // jump the space at the begining
//    if(*cur != SPACE) {
//      *tags = cur;
//      break;
//    }
//    cur++;
//  }
//
//  while (*cur != '\0') {         // find the first space
//    if (*cur == SPACE && *(cur - 1) != SLASH) {
//      *tagLen = cur - *tags;
//      break;
//    }
//
//    cur++;
//  }
//  *sql = cur;
//  return;
//}


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
      elements->colsLen = sql - elements->cols;
      break;
    }
    sql++;
  }
  if(elements->colsLen == 0) return TSDB_CODE_SML_INVALID_DATA;

  // parse ts
  while (*sql != '\0') {
    if(*sql != SPACE) {
      elements->timestamp = sql;
      break;
    }
    sql++;
  }
  if(!elements->timestamp) return TSDB_CODE_SML_INVALID_DATA;

  return TSDB_CODE_SUCCESS;
}

int32_t parseSmlKV(const char* data, int32_t len, SArray *tags){
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
    TAOS_SML_KV *kv = taosMemoryCalloc(sizeof(TAOS_SML_KV), 1);
    kv->key = key;
    kv->keyLen = keyLen;
    kv->value = value;
    kv->valueLen = valueLen;
    kv->type = TSDB_DATA_TYPE_NCHAR;
    if(tags) taosArrayPush(tags, &kv);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t parseSmlTS(const char* data, int32_t len, SArray *tags){
  TAOS_SML_KV *kv = taosMemoryCalloc(sizeof(TAOS_SML_KV), 1);
  kv->value = data;
  kv->valueLen = len;
  kv->type = TSDB_DATA_TYPE_TIMESTAMP;
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

void updateMeta(SSmlSTableMeta* tableMeta, SArray *tags, SArray *cols){
  if(tags){
    for (int i = 0; i < taosArrayGetSize(tags); ++i) {
      TAOS_SML_KV *kv = taosArrayGetP(tags, i);
      TAOS_SML_KV **value = taosHashGet(tableMeta->tagHash, kv->key, kv->keyLen);
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
      TAOS_SML_KV *kv = taosArrayGetP(cols, i);
      TAOS_SML_KV **value = taosHashGet(tableMeta->fieldHash, kv->key, kv->keyLen);
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
      TAOS_SML_KV *kv = taosArrayGetP(tags, i);
      taosHashPut(tableMeta->tagHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
    }
  }

  if(cols){
    for (int i = 0; i < taosArrayGetSize(cols); ++i) {
      TAOS_SML_KV *kv = taosArrayGetP(cols, i);
      taosHashPut(tableMeta->fieldHash, kv->key, kv->keyLen, &kv, POINTER_BYTES);
    }
  }
}

int32_t tscParseLine(const char* sql, SSmlLinesInfo* info) {
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
  parseSmlTS(elements.timestamp, strlen(elements.timestamp), cols);
  ret = parseSmlKV(elements.cols, elements.colsLen, cols);
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
    ret = parseSmlKV(elements.tags, elements.tagsLen, tag->tags);
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


int32_t tscParseLines(char* lines[], int numLines, SSmlLinesInfo* info) {
  for (int32_t i = 0; i < numLines; ++i) {
    int32_t code = tscParseLine(lines[i], info);
    if (code != TSDB_CODE_SUCCESS) {
      uError("SML:0x%"PRIx64" data point line parse failed. line %d : %s", info->id, i, lines[i]);
      return code;
    }
  }
  uDebug("SML:0x%"PRIx64" data point line parse success. tables %d", info->id, taosHashGetSize(info->childTables));

  return TSDB_CODE_SUCCESS;
}

int taos_insert_lines(TAOS* taos, char* lines[], int numLines, SMLProtocolType protocol, SMLTimeStampType tsType, int *affectedRows) {
  int32_t code = 0;

  SSmlLinesInfo* info = taosMemoryMalloc(sizeof(SSmlLinesInfo));
  info->id = genLinesSmlId();
  info->tsType = tsType;
  info->taos = (STscObj*)taos;
  info->protocol = protocol;

  if (numLines <= 0 || numLines > 65536) {
    uError("SML:0x%"PRIx64" taos_insert_lines numLines should be between 1 and 65536. numLines: %d", info->id, numLines);
    code = TSDB_CODE_TSC_APP_ERROR;
    goto cleanup;
  }

  info->childTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);
  info->superTables = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);

  uDebug("SML:0x%"PRIx64" taos_insert_lines begin inserting %d lines, first line: %s", info->id, numLines, lines[0]);
  code = tscParseLines(lines, numLines, info);

  if (code != 0) {
    goto cleanup;
  }

  code = tscSmlInsert(taos, info);
  if (code != 0) {
    uError("SML:0x%"PRIx64" taos_sml_insert error: %s", info->id, tstrerror((code)));
    goto cleanup;
  }
  if (affectedRows != NULL) {
    *affectedRows = info->affectedRows;
  }

  uDebug("SML:0x%"PRIx64" taos_insert_lines finish inserting %d lines. code: %d", info->id, numLines, code);

cleanup:
  taosMemoryFree(info);
  return code;
}

static int32_t convertPrecisionType(int precision, SMLTimeStampType *tsType) {
  switch (precision) {
    case TSDB_SML_TIMESTAMP_NOT_CONFIGURED:
      *tsType = SML_TIME_STAMP_NOT_CONFIGURED;
      break;
    case TSDB_SML_TIMESTAMP_HOURS:
      *tsType = SML_TIME_STAMP_HOURS;
      break;
    case TSDB_SML_TIMESTAMP_MILLI_SECONDS:
      *tsType = SML_TIME_STAMP_MILLI_SECONDS;
      break;
    case TSDB_SML_TIMESTAMP_NANO_SECONDS:
      *tsType = SML_TIME_STAMP_NANO_SECONDS;
      break;
    case TSDB_SML_TIMESTAMP_MICRO_SECONDS:
      *tsType = SML_TIME_STAMP_MICRO_SECONDS;
      break;
    case TSDB_SML_TIMESTAMP_SECONDS:
      *tsType = SML_TIME_STAMP_SECONDS;
      break;
    case TSDB_SML_TIMESTAMP_MINUTES:
      *tsType = SML_TIME_STAMP_MINUTES;
      break;
    default:
      return TSDB_CODE_SML_INVALID_PRECISION_TYPE;
  }

  return TSDB_CODE_SUCCESS;
}

//make a dummy SSqlObj
static SSqlObj* createSmlQueryObj(TAOS* taos, int32_t affected_rows, int32_t code) {
  SSqlObj *pNew = (SSqlObj*)calloc(1, sizeof(SSqlObj));
  if (pNew == NULL) {
    return NULL;
  }
  pNew->signature = pNew;
  pNew->pTscObj = taos;
  pNew->fp = NULL;

  tsem_init(&pNew->rspSem, 0, 0);
  registerSqlObj(pNew);

  pNew->res.numOfRows = affected_rows;
  pNew->res.code = code;


  return pNew;
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
  int affected_rows = 0;
  SMLTimeStampType tsType = SML_TIME_STAMP_NOW;

  if (protocol == TSDB_SML_LINE_PROTOCOL) {
    code = convertPrecisionType(precision, &tsType);
    if (code != TSDB_CODE_SUCCESS) {
      return NULL;
    }
  }

  switch (protocol) {
    case TSDB_SML_LINE_PROTOCOL:
      code = taos_insert_lines(taos, lines, numLines, protocol, tsType, &affected_rows);
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


  SSqlObj *pSql = createSmlQueryObj(taos, affected_rows, code);

  return (TAOS_RES*)pSql;
}

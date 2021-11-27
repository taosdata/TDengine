#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "os.h"
#include "osString.h"
#include "ttype.h"
#include "tmd5.h"
#include "tstrbuild.h"
#include "tname.h"
#include "hash.h"
#include "tskiplist.h"

#include "tscUtil.h"
#include "tsclient.h"
#include "tscLog.h"

#include "taos.h"

typedef struct  {
  char sTableName[TSDB_TABLE_NAME_LEN];
  SHashObj* tagHash;
  SHashObj* fieldHash;
  SArray* tags; //SArray<SSchema>
  SArray* fields; //SArray<SSchema>
  uint8_t precision;
} SSmlSTableSchema;

typedef struct {
  char* key;
  uint8_t type;
  int16_t length;
  char* value;

  uint32_t fieldSchemaIdx;
} TAOS_SML_KV;

typedef struct {
  char* stableName;

  char* childTableName;
  TAOS_SML_KV* tags;
  int32_t tagNum;

  // first kv must be timestamp
  TAOS_SML_KV* fields;
  int32_t fieldNum;

  uint32_t schemaIdx;
} TAOS_SML_DATA_POINT;

typedef enum {
  SML_TIME_STAMP_NOW,
  SML_TIME_STAMP_SECONDS,
  SML_TIME_STAMP_MILLI_SECONDS,
  SML_TIME_STAMP_MICRO_SECONDS,
  SML_TIME_STAMP_NANO_SECONDS
} SMLTimeStampType;

typedef struct {
  uint64_t id;
} SSmlLinesInfo;

//=================================================================================================

static uint64_t linesSmlHandleId = 0;

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
      char* ucs = malloc(kv->length * TSDB_NCHAR_SIZE + 1);
      int32_t bytesNeeded = 0;
      bool succ = taosMbsToUcs4(kv->value, kv->length, ucs, kv->length * TSDB_NCHAR_SIZE, &bytesNeeded);
      if (!succ) {
        free(ucs);
        tscError("SML:0x%"PRIx64" convert nchar string to UCS4_LE failed:%s", id, kv->value);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      free(ucs);
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
      tscError("SML:0x%"PRIx64" type mismatch. key %s, type %d. type before %d", info->id, smlKv->key, smlKv->type, pField->type);
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
  tscDebug("SML:0x%"PRIx64" taos_sml_insert get child table name through md5", info->id);
  qsort(point->tags, point->tagNum, sizeof(TAOS_SML_KV), compareSmlColKv);

  SStringBuilder sb; memset(&sb, 0, sizeof(sb));
  char sTableName[TSDB_TABLE_NAME_LEN] = {0};
  strtolower(sTableName, point->stableName);
  taosStringBuilderAppendString(&sb, sTableName);
  for (int j = 0; j < point->tagNum; ++j) {
    taosStringBuilderAppendChar(&sb, ',');
    TAOS_SML_KV* tagKv = point->tags + j;
    char tagName[TSDB_COL_NAME_LEN] = {0};
    strtolower(tagName, tagKv->key);
    taosStringBuilderAppendString(&sb, tagName);
    taosStringBuilderAppendChar(&sb, '=');
    taosStringBuilderAppend(&sb, tagKv->value, tagKv->length);
  }
  size_t len = 0;
  char* keyJoined = taosStringBuilderGetResult(&sb, &len);
  MD5_CTX context;
  MD5Init(&context);
  MD5Update(&context, (uint8_t *)keyJoined, (uint32_t)len);
  MD5Final(&context);
  *tableNameLen = snprintf(tableName, *tableNameLen,
                           "t_%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", context.digest[0],
                           context.digest[1], context.digest[2], context.digest[3], context.digest[4], context.digest[5], context.digest[6],
                           context.digest[7], context.digest[8], context.digest[9], context.digest[10], context.digest[11],
                           context.digest[12], context.digest[13], context.digest[14], context.digest[15]);
  taosStringBuilderDestroy(&sb);
  tscDebug("SML:0x%"PRIx64" child table name: %s", info->id, tableName);
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
      schema.tagHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
      schema.fieldHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);

      pStableSchema = taosArrayPush(stableSchemas, &schema);
      stableIdx = taosArrayGetSize(stableSchemas) - 1;
      taosHashPut(sname2shema, schema.sTableName, stableNameLen, &stableIdx, sizeof(size_t));
    }

    for (int j = 0; j < point->tagNum; ++j) {
      TAOS_SML_KV* tagKv = point->tags + j;
      if (!point->childTableName) {
        char childTableName[TSDB_TABLE_NAME_LEN];
        int32_t tableNameLen = TSDB_TABLE_NAME_LEN;
        getSmlMd5ChildTableName(point, childTableName, &tableNameLen, info);
        point->childTableName = calloc(1, tableNameLen+1);
        strncpy(point->childTableName, childTableName, tableNameLen);
        point->childTableName[tableNameLen] = '\0';
      }

      code = buildSmlKvSchema(tagKv, pStableSchema->tagHash, pStableSchema->tags, info);
      if (code != 0) {
        tscError("SML:0x%"PRIx64" build data point schema failed. point no.: %d, tag key: %s", info->id, i, tagKv->key);
        return code;
      }
    }

    for (int j = 0; j < point->fieldNum; ++j) {
      TAOS_SML_KV* fieldKv = point->fields + j;
      code = buildSmlKvSchema(fieldKv, pStableSchema->fieldHash, pStableSchema->fields, info);
      if (code != 0) {
        tscError("SML:0x%"PRIx64" build data point schema failed. point no.: %d, tag key: %s", info->id, i, fieldKv->key);
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

  tscDebug("SML:0x%"PRIx64" build point schema succeed. num of super table: %zu", info->id, numStables);
  for (int32_t i = 0; i < numStables; ++i) {
    SSmlSTableSchema* schema = taosArrayGet(stableSchemas, i);
    tscDebug("\ttable name: %s, tags number: %zu, fields number: %zu", schema->sTableName,
             taosArrayGetSize(schema->tags), taosArrayGetSize(schema->fields));
  }

  return 0;
}

static int32_t generateSchemaAction(SSchema* pointColField, SHashObj* dbAttrHash, SArray* dbAttrArray, bool isTag, char sTableName[],
                                       SSchemaAction* action, bool* actionNeeded, SSmlLinesInfo* info) {
  char fieldNameLowerCase[TSDB_COL_NAME_LEN] = {0};
  strtolower(fieldNameLowerCase, pointColField->name);

  size_t* pDbIndex = taosHashGet(dbAttrHash, fieldNameLowerCase, strlen(fieldNameLowerCase));
  if (pDbIndex) {
    SSchema* dbAttr = taosArrayGet(dbAttrArray, *pDbIndex);
    assert(strcasecmp(dbAttr->name, pointColField->name) == 0);
    if (pointColField->type != dbAttr->type) {
      tscError("SML:0x%"PRIx64" point type and db type mismatch. key: %s. point type: %d, db type: %d", info->id, pointColField->name,
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
    tscDebug("SML:0x%" PRIx64 " generate schema action. column name: %s, action: %d", info->id, fieldNameLowerCase,
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

  tscDebug("SML:0x%"PRIx64" apply schema action. action: %d", info->id, action->action);
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
        tscError("SML:0x%"PRIx64" apply schema action. error: %s", info->id, errStr);
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_FIELD_ALREAY_EXIST || code == TSDB_CODE_MND_TAG_ALREAY_EXIST || tscDupColNames) {
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          tscError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
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
        tscError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_TAG_ALREAY_EXIST || code == TSDB_CODE_MND_FIELD_ALREAY_EXIST || tscDupColNames) {
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          tscError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
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
        tscError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_INVALID_COLUMN_LENGTH || code == TSDB_CODE_TSC_INVALID_COLUMN_LENGTH) {
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          tscError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
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
        tscError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_INVALID_TAG_LENGTH || code == TSDB_CODE_TSC_INVALID_TAG_LENGTH) {
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          tscError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
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
        tscError("SML:0x%"PRIx64" apply schema action. error : %s", info->id, taos_errstr(res));
      }
      taos_free_result(res);

      if (code == TSDB_CODE_MND_TABLE_ALREADY_EXIST) {
        TAOS_RES* res2 = taos_query(taos, "RESET QUERY CACHE");
        code = taos_errno(res2);
        if (code != TSDB_CODE_SUCCESS) {
          tscError("SML:0x%" PRIx64 " apply schema action. reset query cache. error: %s", info->id, taos_errstr(res2));
        }
        taos_free_result(res2);
        taosMsleep(500);
      }
      break;
    }

    default:
      break;
  }

  free(result);
  if (code != 0) {
    tscError("SML:0x%"PRIx64 " apply schema action failure. %s", info->id, tstrerror(code));
  }
  return code;
}

static int32_t destroySmlSTableSchema(SSmlSTableSchema* schema) {
  taosHashCleanup(schema->tagHash);
  taosHashCleanup(schema->fieldHash);
  taosArrayDestroy(schema->tags);
  taosArrayDestroy(schema->fields);
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
    field.type = tableMeta->schema[j].type;
    field.bytes = tableMeta->schema[j].bytes;
    taosArrayPush(schema->tags, &field);
    size_t tagIndex = taosArrayGetSize(schema->tags) - 1;
    taosHashPut(schema->tagHash, field.name, strlen(field.name), &tagIndex, sizeof(tagIndex));
  }
  tscDebug("SML:0x%"PRIx64 " load table schema succeed. table name: %s, columns number: %d, tag number: %d, precision: %d",
           info->id, tableName, tableMeta->tableInfo.numOfColumns, tableMeta->tableInfo.numOfTags, schema->precision);
  return TSDB_CODE_SUCCESS;
}

static int32_t retrieveTableMeta(TAOS* taos, char* tableName, STableMeta** pTableMeta, SSmlLinesInfo* info) {
  int32_t code = 0;
  int32_t retries = 0;
  STableMeta* tableMeta = NULL;
  while (retries++ < TSDB_MAX_REPLICA && tableMeta == NULL) {
    STscObj* pObj = (STscObj*)taos;
    if (pObj == NULL || pObj->signature != pObj) {
      terrno = TSDB_CODE_TSC_DISCONNECTED;
      return TSDB_CODE_TSC_DISCONNECTED;
    }

    tscDebug("SML:0x%" PRIx64 " retrieve table meta. super table name: %s", info->id, tableName);

    char tableNameLowerCase[TSDB_TABLE_NAME_LEN];
    strtolower(tableNameLowerCase, tableName);

    char sql[256];
    snprintf(sql, 256, "describe %s", tableNameLowerCase);
    TAOS_RES* res = taos_query(taos, sql);
    code = taos_errno(res);
    if (code != 0) {
      tscError("SML:0x%" PRIx64 " describe table failure. %s", info->id, taos_errstr(res));
      taos_free_result(res);
      return code;
    }
    taos_free_result(res);

    SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
    if (pSql == NULL) {
      tscError("SML:0x%" PRIx64 " failed to allocate memory, reason:%s", info->id, strerror(errno));
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      return code;
    }
    pSql->pTscObj = taos;
    pSql->signature = pSql;
    pSql->fp = NULL;

    registerSqlObj(pSql);
    SStrToken tableToken = {.z = tableNameLowerCase, .n = (uint32_t)strlen(tableNameLowerCase), .type = TK_ID};
    tGetToken(tableNameLowerCase, &tableToken.type);
    // Check if the table name available or not
    if (tscValidateName(&tableToken) != TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
      sprintf(pSql->cmd.payload, "table name is invalid");
      taosReleaseRef(tscObjRef, pSql->self);
      return code;
    }

    SName sname = {0};
    if ((code = tscSetTableFullName(&sname, &tableToken, pSql)) != TSDB_CODE_SUCCESS) {
      taosReleaseRef(tscObjRef, pSql->self);
      return code;
    }
    char fullTableName[TSDB_TABLE_FNAME_LEN] = {0};
    memset(fullTableName, 0, tListLen(fullTableName));
    tNameExtractFullName(&sname, fullTableName);
    taosReleaseRef(tscObjRef, pSql->self);

    size_t size = 0;
    taosHashGetCloneExt(tscTableMetaMap, fullTableName, strlen(fullTableName), NULL, (void**)&tableMeta, &size);
  }

  if (tableMeta != NULL) {
    *pTableMeta = tableMeta;
    return TSDB_CODE_SUCCESS;
  } else {
    tscError("SML:0x%" PRIx64 " failed to retrieve table meta. super table name: %s", info->id, tableName);
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
    free(tableMeta);
    tableMeta = NULL;
  }

  return code;
}

static int32_t modifyDBSchemas(TAOS* taos, SArray* stableSchemas, SSmlLinesInfo* info) {
  int32_t code = 0;
  size_t numStable = taosArrayGetSize(stableSchemas);
  for (int i = 0; i < numStable; ++i) {
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
        tscError("SML:0x%"PRIx64" reconcile point schema failed. can not create %s", info->id, pointSchema->sTableName);
        return code;
      } else {
        pointSchema->precision = dbSchema.precision;
        destroySmlSTableSchema(&dbSchema);
      }
    } else if (code == TSDB_CODE_SUCCESS) {
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
      tscError("SML:0x%"PRIx64" load table meta error: %s", info->id, tstrerror(code));
      return code;
    }
  }
  return 0;
}

static int32_t creatChildTableIfNotExists(TAOS* taos, const char* cTableName, const char* sTableName,
                                          SArray* tagsSchema, SArray* tagsBind, SSmlLinesInfo* info) {
  size_t numTags = taosArrayGetSize(tagsSchema);
  char* sql = malloc(tsMaxSQLStringLen+1);
  if (sql == NULL) {
    tscError("malloc sql memory error");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  int freeBytes = tsMaxSQLStringLen + 1;
  sprintf(sql, "create table if not exists %s using %s", cTableName, sTableName);

  snprintf(sql+strlen(sql), freeBytes-strlen(sql), "(");
  for (int i = 0; i < numTags; ++i) {
    SSchema* tagSchema = taosArrayGet(tagsSchema, i);
    snprintf(sql+strlen(sql), freeBytes-strlen(sql), "%s,", tagSchema->name);
  }
  snprintf(sql + strlen(sql) - 1, freeBytes-strlen(sql)+1, ")");

  snprintf(sql + strlen(sql), freeBytes-strlen(sql), " tags (");

  for (int i = 0; i < numTags; ++i) {
    snprintf(sql+strlen(sql), freeBytes-strlen(sql), "?,");
  }
  snprintf(sql + strlen(sql) - 1, freeBytes-strlen(sql)+1, ")");
  sql[strlen(sql)] = '\0';

  tscDebug("SML:0x%"PRIx64" create table : %s", info->id, sql);

  TAOS_STMT* stmt = taos_stmt_init(taos);
  if (stmt == NULL) {
    free(sql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  int32_t code;
  code = taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  free(sql);

  if (code != 0) {
    tscError("SML:0x%"PRIx64" taos_stmt_prepare returns %d:%s", info->id, code, tstrerror(code));
    taos_stmt_close(stmt);
    return code;
  }

  code = taos_stmt_bind_param(stmt, TARRAY_GET_START(tagsBind));
  if (code != 0) {
    tscError("SML:0x%"PRIx64" taos_stmt_bind_param returns %d:%s", info->id, code, tstrerror(code));
    taos_stmt_close(stmt);
    return code;
  }

  code = taos_stmt_execute(stmt);
  if (code != 0) {
    tscError("SML:0x%"PRIx64" taos_stmt_execute returns %d:%s", info->id, code, tstrerror(code));
    taos_stmt_close(stmt);
    return code;
  }

  code = taos_stmt_close(stmt);
  if (code != 0) {
    tscError("SML:0x%"PRIx64" taos_stmt_close return %d:%s", info->id, code, tstrerror(code));
    return code;
  }
  return code;
}

static int32_t doInsertChildTableWithStmt(TAOS* taos, char* sql, char* cTableName, SArray* batchBind, SSmlLinesInfo* info) {
  int32_t code = 0;

  TAOS_STMT* stmt = taos_stmt_init(taos);
  if (stmt == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  code = taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));

  if (code != 0) {
    tscError("SML:0x%"PRIx64" taos_stmt_prepare return %d:%s", info->id, code, tstrerror(code));
    taos_stmt_close(stmt);
    return code;
  }

  bool tryAgain = false;
  int32_t try = 0;
  do {
    code = taos_stmt_set_tbname(stmt, cTableName);
    if (code != 0) {
      tscError("SML:0x%"PRIx64" taos_stmt_set_tbname return %d:%s", info->id, code, tstrerror(code));
      taos_stmt_close(stmt);
      return code;
    }

    size_t rows = taosArrayGetSize(batchBind);
    for (int32_t i = 0; i < rows; ++i) {
      TAOS_BIND* colsBinds = taosArrayGetP(batchBind, i);
      code = taos_stmt_bind_param(stmt, colsBinds);
      if (code != 0) {
        tscError("SML:0x%"PRIx64" taos_stmt_bind_param return %d:%s", info->id, code, tstrerror(code));
        taos_stmt_close(stmt);
        return code;
      }
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        tscError("SML:0x%"PRIx64" taos_stmt_add_batch return %d:%s", info->id, code, tstrerror(code));
        taos_stmt_close(stmt);
        return code;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      tscError("SML:0x%"PRIx64" taos_stmt_execute return %d:%s, try:%d", info->id, code, tstrerror(code), try);
    }

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
        tscError("SML:0x%" PRIx64 " insert child table. reset query cache. error: %s", info->id, taos_errstr(res2));
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


  taos_stmt_close(stmt);
  return code;
}

static int32_t insertChildTableBatch(TAOS* taos,  char* cTableName, SArray* colsSchema, SArray* rowsBind, size_t rowSize, SSmlLinesInfo* info) {
  size_t numCols = taosArrayGetSize(colsSchema);
  char* sql = malloc(tsMaxSQLStringLen+1);
  if (sql == NULL) {
    tscError("malloc sql memory error");
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int32_t freeBytes = tsMaxSQLStringLen + 1 ;
  sprintf(sql, "insert into ? (");

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

  size_t rows = taosArrayGetSize(rowsBind);
  size_t maxBatchSize = TSDB_MAX_WAL_SIZE/rowSize * 4 / 5;
  size_t batchSize = MIN(maxBatchSize, rows);
  tscDebug("SML:0x%"PRIx64" insert rows into child table %s. num of rows: %zu, batch size: %zu",
           info->id, cTableName, rows, batchSize);
  SArray* batchBind = taosArrayInit(batchSize, POINTER_BYTES);
  int32_t code = TSDB_CODE_SUCCESS;
  for (int i = 0; i < rows;) {
    int j = i;
    for (; j < i + batchSize && j<rows; ++j) {
      taosArrayPush(batchBind, taosArrayGet(rowsBind, j));
    }
    if (j > i) {
      tscDebug("SML:0x%"PRIx64" insert child table batch from line %d to line %d.", info->id, i, j - 1);
      code = doInsertChildTableWithStmt(taos, sql, cTableName, batchBind, info);
      if (code != 0) {
        taosArrayDestroy(batchBind);
        tfree(sql);
        return code;
      }
      taosArrayClear(batchBind);
    }
    i = j;
  }
  taosArrayDestroy(batchBind);
  tfree(sql);
  return code;
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

static int32_t applyChildTableTags(TAOS* taos, char* cTableName, char* sTableName,
                                   SSmlSTableSchema* sTableSchema, SArray* cTablePoints, SSmlLinesInfo* info) {
  size_t numTags = taosArrayGetSize(sTableSchema->tags);
  size_t rows = taosArrayGetSize(cTablePoints);

  TAOS_SML_KV* tagKVs[TSDB_MAX_TAGS] = {0};
  for (int i= 0; i < rows; ++i) {
    TAOS_SML_DATA_POINT * pDataPoint = taosArrayGetP(cTablePoints, i);
    for (int j = 0; j < pDataPoint->tagNum; ++j) {
      TAOS_SML_KV* kv = pDataPoint->tags + j;
      tagKVs[kv->fieldSchemaIdx] = kv;
    }
  }
  
  SArray* tagBinds = taosArrayInit(numTags, sizeof(TAOS_BIND));
  taosArraySetSize(tagBinds, numTags);
  int isNullColBind = TSDB_TRUE;
  for (int j = 0; j < numTags; ++j) {
    TAOS_BIND* tsc_bind = taosArrayGet(tagBinds, j);
    tsc_bind->is_null = &isNullColBind;
  }
  for (int j = 0; j < numTags; ++j) {
    if (tagKVs[j] == NULL) continue;
    TAOS_SML_KV* kv =  tagKVs[j];
    TAOS_BIND* tsc_bind = taosArrayGet(tagBinds, kv->fieldSchemaIdx);
    tsc_bind->buffer_type = kv->type;
    tsc_bind->length = malloc(sizeof(uintptr_t*));
    *tsc_bind->length = kv->length;
    tsc_bind->buffer = kv->value;
    tsc_bind->is_null = NULL;
  }

  int32_t code = creatChildTableIfNotExists(taos, cTableName, sTableName, sTableSchema->tags, tagBinds, info);

  for (int i = 0; i < taosArrayGetSize(tagBinds); ++i) {
    TAOS_BIND* tsc_bind = taosArrayGet(tagBinds, i);
    free(tsc_bind->length);
  }
  taosArrayDestroy(tagBinds);
  return code;
}

static int32_t applyChildTableFields(TAOS* taos, SSmlSTableSchema* sTableSchema, char* cTableName,
                                     SArray* cTablePoints, size_t rowSize, SSmlLinesInfo* info) {
  int32_t code = TSDB_CODE_SUCCESS;

  size_t numCols = taosArrayGetSize(sTableSchema->fields);
  size_t rows = taosArrayGetSize(cTablePoints);
  SArray* rowsBind = taosArrayInit(rows, POINTER_BYTES);

  int isNullColBind = TSDB_TRUE;
  for (int i = 0; i < rows; ++i) {
    TAOS_SML_DATA_POINT* point = taosArrayGetP(cTablePoints, i);

    TAOS_BIND* colBinds = calloc(numCols, sizeof(TAOS_BIND));
    if (colBinds == NULL) {
      tscError("SML:0x%"PRIx64" taos_sml_insert insert points, failed to allocated memory for TAOS_BIND, "
               "num of rows: %zu, num of cols: %zu", info->id, rows, numCols);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }

    for (int j = 0; j < numCols; ++j) {
      TAOS_BIND* tsc_bind = colBinds + j;
      tsc_bind->is_null = &isNullColBind;
    }
    for (int j = 0; j < point->fieldNum; ++j) {
      TAOS_SML_KV* kv = point->fields + j;
      TAOS_BIND* tsc_bind = colBinds + kv->fieldSchemaIdx;
      tsc_bind->buffer_type = kv->type;
      tsc_bind->length = malloc(sizeof(uintptr_t*));
      *tsc_bind->length = kv->length;
      tsc_bind->buffer = kv->value;
      tsc_bind->is_null = NULL;
    }
    taosArrayPush(rowsBind, &colBinds);
  }

  code = insertChildTableBatch(taos, cTableName, sTableSchema->fields, rowsBind, rowSize, info);
  if (code != 0) {
    tscError("SML:0x%"PRIx64" insert into child table %s failed. error %s", info->id, cTableName, tstrerror(code));
  }

  for (int i = 0; i < rows; ++i) {
    TAOS_BIND* colBinds = taosArrayGetP(rowsBind, i);
    for (int j = 0; j < numCols; ++j) {
      TAOS_BIND* tsc_bind = colBinds + j;
      free(tsc_bind->length);
    }
    free(colBinds);
  }
  taosArrayDestroy(rowsBind);
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

    tscDebug("SML:0x%"PRIx64" apply child table tags. child table: %s", info->id, point->childTableName);
    code = applyChildTableTags(taos, point->childTableName, point->stableName, sTableSchema, cTablePoints, info);
    if (code != 0) {
      tscError("apply child table tags failed. child table %s, error %s", point->childTableName, tstrerror(code));
      goto cleanup;
    }

    size_t rowSize = 0;
    size_t numCols = taosArrayGetSize(sTableSchema->fields);
    for (int i = 0; i < numCols; ++i) {
      SSchema* colSchema = taosArrayGet(sTableSchema->fields, i);
      rowSize += colSchema->bytes;
    }

    tscDebug("SML:0x%"PRIx64" apply child table points. child table: %s, row size: %zu", info->id, point->childTableName, rowSize);
    code = applyChildTableFields(taos, sTableSchema, point->childTableName, cTablePoints, rowSize, info);
    if (code != 0) {
      tscError("SML:0x%"PRIx64" Apply child table fields failed. child table %s, error %s", info->id, point->childTableName, tstrerror(code));
      goto cleanup;
    }

    tscDebug("SML:0x%"PRIx64" successfully applied data points of child table %s", info->id, point->childTableName);

    pCTablePoints = taosHashIterate(cname2points, pCTablePoints);
  }

cleanup:
  pCTablePoints = taosHashIterate(cname2points, NULL);
  while (pCTablePoints) {
    SArray* pPoints = *pCTablePoints;
    taosArrayDestroy(pPoints);
    pCTablePoints = taosHashIterate(cname2points, pCTablePoints);
  }
  taosHashCleanup(cname2points);
  return code;
}

int tscSmlInsert(TAOS* taos, TAOS_SML_DATA_POINT* points, int numPoint, SSmlLinesInfo* info) {
  tscDebug("SML:0x%"PRIx64" taos_sml_insert. number of points: %d", info->id, numPoint);

  int32_t code = TSDB_CODE_SUCCESS;

  tscDebug("SML:0x%"PRIx64" build data point schemas", info->id);
  SArray* stableSchemas = taosArrayInit(32, sizeof(SSmlSTableSchema)); // SArray<STableColumnsSchema>
  code = buildDataPointSchemas(points, numPoint, stableSchemas, info);
  if (code != 0) {
    tscError("SML:0x%"PRIx64" error building data point schemas : %s", info->id, tstrerror(code));
    goto clean_up;
  }

  tscDebug("SML:0x%"PRIx64" modify db schemas", info->id);
  code = modifyDBSchemas(taos, stableSchemas, info);
  if (code != 0) {
    tscError("SML:0x%"PRIx64" error change db schema : %s", info->id, tstrerror(code));
    goto clean_up;
  }

  tscDebug("SML:0x%"PRIx64" apply data points", info->id);
  code = applyDataPoints(taos, points, numPoint, stableSchemas, info);
  if (code != 0) {
    tscError("SML:0x%"PRIx64" error apply data points : %s", info->id, tstrerror(code));
  }

clean_up:
  for (int i = 0; i < taosArrayGetSize(stableSchemas); ++i) {
    SSmlSTableSchema* schema = taosArrayGet(stableSchemas, i);
    taosArrayDestroy(schema->fields);
    taosArrayDestroy(schema->tags);
  }
  taosArrayDestroy(stableSchemas);
  return code;
}

int tsc_sml_insert(TAOS* taos, TAOS_SML_DATA_POINT* points, int numPoint) {
  SSmlLinesInfo* info = calloc(1, sizeof(SSmlLinesInfo));
  info->id = genLinesSmlId();
  int code = tscSmlInsert(taos, points, numPoint, info);
  free(info);
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

static bool isValidInteger(char *str) {
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

static bool isValidFloat(char *str) {
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

static bool isTinyInt(char *pVal, uint16_t len) {
  if (len <= 2) {
    return false;
  }
  if (!strcmp(&pVal[len - 2], "i8")) {
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
  if (!strcmp(&pVal[len - 2], "u8")) {
    //printf("Type is uint8(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isSmallInt(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (!strcmp(&pVal[len - 3], "i16")) {
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
  if (strcmp(&pVal[len - 3], "u16") == 0) {
    //printf("Type is uint16(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isInt(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcmp(&pVal[len - 3], "i32") == 0) {
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
  if (strcmp(&pVal[len - 3], "u32") == 0) {
    //printf("Type is uint32(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isBigInt(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcmp(&pVal[len - 3], "i64") == 0) {
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
  if (strcmp(&pVal[len - 3], "u64") == 0) {
    //printf("Type is uint64(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isFloat(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcmp(&pVal[len - 3], "f32") == 0) {
    //printf("Type is float(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isDouble(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcmp(&pVal[len - 3], "f64") == 0) {
    //printf("Type is double(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isBool(char *pVal, uint16_t len, bool *bVal) {
  if ((len == 1) &&
      (pVal[len - 1] == 't' ||
       pVal[len - 1] == 'T')) {
    //printf("Type is bool(%c)\n", pVal[len - 1]);
    *bVal = true;
    return true;
  }

  if ((len == 1) &&
      (pVal[len - 1] == 'f' ||
       pVal[len - 1] == 'F')) {
    //printf("Type is bool(%c)\n", pVal[len - 1]);
    *bVal = false;
    return true;
  }

  if((len == 4) &&
     (!strcmp(&pVal[len - 4], "true") ||
      !strcmp(&pVal[len - 4], "True") ||
      !strcmp(&pVal[len - 4], "TRUE"))) {
    //printf("Type is bool(%s)\n", &pVal[len - 4]);
    *bVal = true;
    return true;
  }
  if((len == 5) &&
     (!strcmp(&pVal[len - 5], "false") ||
      !strcmp(&pVal[len - 5], "False") ||
      !strcmp(&pVal[len - 5], "FALSE"))) {
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
  if (pVal[0] == 'L' && pVal[1] == '"' && pVal[len - 1] == '"') {
    //printf("Type is nchar(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool isTimeStamp(char *pVal, uint16_t len, SMLTimeStampType *tsType) {
  if (len == 0) {
    return true;
  }
  if ((len == 1) && pVal[0] == '0') {
    *tsType = SML_TIME_STAMP_NOW;
    //printf("Type is timestamp(%s)\n", pVal);
    return true;
  }
  if (len < 2) {
    return false;
  }
  //No appendix use usec as default
  if (isdigit(pVal[len - 1]) && isdigit(pVal[len - 2])) {
    *tsType = SML_TIME_STAMP_MICRO_SECONDS;
    //printf("Type is timestamp(%s)\n", pVal);
    return true;
  }
  if (pVal[len - 1] == 's') {
    switch (pVal[len - 2]) {
      case 'm':
        *tsType = SML_TIME_STAMP_MILLI_SECONDS;
        break;
      case 'u':
        *tsType = SML_TIME_STAMP_MICRO_SECONDS;
        break;
      case 'n':
        *tsType = SML_TIME_STAMP_NANO_SECONDS;
        break;
      default:
        if (isdigit(pVal[len - 2])) {
          *tsType = SML_TIME_STAMP_SECONDS;
          break;
        } else {
          return false;
        }
    }
    //printf("Type is timestamp(%s)\n", pVal);
    return true;
  }
  return false;
}

static bool convertStrToNumber(TAOS_SML_KV *pVal, char*str, SSmlLinesInfo* info) {
  errno = 0;
  uint8_t type = pVal->type;
  int16_t length = pVal->length;
  int64_t val_s;
  uint64_t val_u;
  double val_d;

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
    tscError("SML:0x%"PRIx64" Convert number(%s) out of range", info->id, str);
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
static bool convertSmlValueType(TAOS_SML_KV *pVal, char *value,
                                uint16_t len, SSmlLinesInfo* info) {
  if (len <= 0) {
    return false;
  }

  //integer number
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
  //Handle default(no appendix) as float
  if (isValidInteger(value) || isValidFloat(value)) {
    pVal->type = TSDB_DATA_TYPE_FLOAT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    if (!convertStrToNumber(pVal, value, info)) {
      return false;
    }
    return true;
  }
  return false;
}

static int32_t getTimeStampValue(char *value, uint16_t len,
                                 SMLTimeStampType type, int64_t *ts) {

  if (len >= 2) {
    for (int i = 0; i < len - 2; ++i) {
      if(!isdigit(value[i])) {
        return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
      }
    }
  }
  //No appendix or no timestamp given (len = 0)
  if (len >= 1 && isdigit(value[len - 1]) && type != SML_TIME_STAMP_NOW) {
    type = SML_TIME_STAMP_MICRO_SECONDS;
  }
  if (len != 0) {
    *ts = (int64_t)strtoll(value, NULL, 10);
  } else {
    type = SML_TIME_STAMP_NOW;
  }
  switch (type) {
    case SML_TIME_STAMP_NOW: {
      *ts = taosGetTimestampNs();
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
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t convertSmlTimeStamp(TAOS_SML_KV *pVal, char *value,
                                   uint16_t len, SSmlLinesInfo* info) {
  int32_t ret;
  SMLTimeStampType type;
  int64_t tsVal;

  if (!isTimeStamp(value, len, &type)) {
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  ret = getTimeStampValue(value, len, type, &tsVal);
  if (ret) {
    return ret;
  }
  tscDebug("SML:0x%"PRIx64"Timestamp after conversion:%"PRId64, info->id, tsVal);

  pVal->type = TSDB_DATA_TYPE_TIMESTAMP;
  pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
  pVal->value = calloc(pVal->length, 1);
  memcpy(pVal->value, &tsVal, pVal->length);
  return TSDB_CODE_SUCCESS;
}

static int32_t parseSmlTimeStamp(TAOS_SML_KV **pTS, const char **tsc_index, SSmlLinesInfo* info) {
  const char *start, *cur;
  int32_t ret = TSDB_CODE_SUCCESS;
  int len = 0;
  char key[] = "_ts";
  char *value = NULL;

  start = cur = *tsc_index;
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
    free(value);
    free(*pTS);
    return ret;
  }
  free(value);

  (*pTS)->key = calloc(sizeof(key), 1);
  memcpy((*pTS)->key, key, sizeof(key));
  return ret;
}

static bool checkDuplicateKey(char *key, SHashObj *pHash, SSmlLinesInfo* info) {
  char *val = NULL;
  char *cur = key;
  char keyLower[TSDB_COL_NAME_LEN];
  size_t keyLen = 0;
  while(*cur != '\0') {
    keyLower[keyLen] = tolower(*cur);
    keyLen++;
    cur++;
  }
  keyLower[keyLen] = '\0';

  val = taosHashGet(pHash, keyLower, keyLen);
  if (val) {
    tscError("SML:0x%"PRIx64" Duplicate key detected:%s", info->id, keyLower);
    return true;
  }

  uint8_t dummy_val = 0;
  taosHashPut(pHash, keyLower, strlen(key), &dummy_val, sizeof(uint8_t));

  return false;
}

static int32_t parseSmlKey(TAOS_SML_KV *pKV, const char **tsc_index, SHashObj *pHash, SSmlLinesInfo* info) {
  const char *cur = *tsc_index;
  char key[TSDB_COL_NAME_LEN + 1];  // +1 to avoid key[len] over write
  uint16_t len = 0;

  //key field cannot start with digit
  if (isdigit(*cur)) {
    tscError("SML:0x%"PRIx64" Tag key cannnot start with digit", info->id);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }
  while (*cur != '\0') {
    if (len > TSDB_COL_NAME_LEN) {
      tscError("SML:0x%"PRIx64" Key field cannot exceeds 65 characters", info->id);
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
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
  key[len] = '\0';

  if (checkDuplicateKey(key, pHash, info)) {
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  pKV->key = calloc(len + 1, 1);
  memcpy(pKV->key, key, len + 1);
  //tscDebug("SML:0x%"PRIx64" Key:%s|len:%d", info->id, pKV->key, len);
  *tsc_index = cur + 1;
  return TSDB_CODE_SUCCESS;
}


static bool parseSmlValue(TAOS_SML_KV *pKV, const char **tsc_index,
                          bool *is_last_kv, SSmlLinesInfo* info) {
  const char *start, *cur;
  char *value = NULL;
  uint16_t len = 0;
  start = cur = *tsc_index;

  while (1) {
    // unescaped ',' or ' ' or '\0' identifies a value
    if ((*cur == ',' || *cur == ' ' || *cur == '\0') && *(cur - 1) != '\\') {
      //unescaped ' ' or '\0' indicates end of value
      *is_last_kv = (*cur == ' ' || *cur == '\0') ? true : false;
      break;
    }
    //Escape special character
    if (*cur == '\\') {
      escapeSpecialCharacter(2, &cur);
    }
    cur++;
    len++;
  }

  value = calloc(len + 1, 1);
  memcpy(value, start, len);
  value[len] = '\0';
  if (!convertSmlValueType(pKV, value, len, info)) {
    tscError("SML:0x%"PRIx64" Failed to convert sml value string(%s) to any type",
            info->id, value);
    //free previous alocated key field
    free(pKV->key);
    pKV->key = NULL;
    free(value);
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }
  free(value);

  *tsc_index = (*cur == '\0') ? cur : cur + 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t parseSmlMeasurement(TAOS_SML_DATA_POINT *pSml, const char **tsc_index,
                                   uint8_t *has_tags, SSmlLinesInfo* info) {
  const char *cur = *tsc_index;
  uint16_t len = 0;

  pSml->stableName = calloc(TSDB_TABLE_NAME_LEN + 1, 1);    // +1 to avoid 1772 line over write
  if (pSml->stableName == NULL){
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  if (isdigit(*cur)) {
    tscError("SML:0x%"PRIx64" Measurement field cannnot start with digit", info->id);
    free(pSml->stableName);
    pSml->stableName = NULL;
    return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
  }

  while (*cur != '\0') {
    if (len > TSDB_TABLE_NAME_LEN) {
      tscError("SML:0x%"PRIx64" Measurement field cannot exceeds 193 characters", info->id);
      free(pSml->stableName);
      pSml->stableName = NULL;
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    }
    //first unescaped comma or space identifies measurement
    //if space detected first, meaning no tag in the input
    if (*cur == ',' && *(cur - 1) != '\\') {
      *has_tags = 1;
      break;
    }
    if (*cur == ' ' && *(cur - 1) != '\\') {
      break;
    }
    //Comma, Space, Backslash needs to be escaped if any
    if (*cur == '\\') {
      escapeSpecialCharacter(1, &cur);
    }
    pSml->stableName[len] = *cur;
    cur++;
    len++;
  }
  pSml->stableName[len] = '\0';
  *tsc_index = cur + 1;
  tscDebug("SML:0x%"PRIx64" Stable name in measurement:%s|len:%d", info->id, pSml->stableName, len);

  return TSDB_CODE_SUCCESS;
}

//Table name can only contain digits(0-9),alphebet(a-z),underscore(_)
static int32_t isValidChildTableName(const char *pTbName, int16_t len) {
  const char *cur = pTbName;
  for (int i = 0; i < len; ++i) {
    if(!isdigit(cur[i]) && !isalpha(cur[i]) && (cur[i] != '_')) {
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    }
  }
  return TSDB_CODE_SUCCESS;
}


static int32_t parseSmlKvPairs(TAOS_SML_KV **pKVs, int *num_kvs,
                               const char **tsc_index, bool isField,
                               TAOS_SML_DATA_POINT* smlData, SHashObj *pHash,
                               SSmlLinesInfo* info) {
  const char *cur = *tsc_index;
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

  while (*cur != '\0') {
    ret = parseSmlKey(pkv, &cur, pHash, info);
    if (ret) {
      tscError("SML:0x%"PRIx64" Unable to parse key", info->id);
      goto error;
    }
    ret = parseSmlValue(pkv, &cur, &is_last_kv, info);
    if (ret) {
      tscError("SML:0x%"PRIx64" Unable to parse value", info->id);
      goto error;
    }
    if (!isField &&
        (strcasecmp(pkv->key, "ID") == 0) && pkv->type == TSDB_DATA_TYPE_BINARY) {
      ret = isValidChildTableName(pkv->value, pkv->length);
      if (ret) {
        goto error;
      }
      smlData->childTableName = malloc( pkv->length + 1);
      memcpy(smlData->childTableName, pkv->value, pkv->length);
      smlData->childTableName[pkv->length] = '\0';
      free(pkv->key);
      free(pkv->value);
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
  *tsc_index = cur;
  return ret;
}

static void moveTimeStampToFirstKv(TAOS_SML_DATA_POINT** smlData, TAOS_SML_KV *ts) {
  TAOS_SML_KV* tsField = (*smlData)->fields;
  tsField->length = ts->length;
  tsField->type = ts->type;
  tsField->value = malloc(ts->length);
  tsField->key = malloc(strlen(ts->key) + 1);
  memcpy(tsField->key, ts->key, strlen(ts->key) + 1);
  memcpy(tsField->value, ts->value, ts->length);
  (*smlData)->fieldNum = (*smlData)->fieldNum + 1;

  free(ts->key);
  free(ts->value);
  free(ts);
}

int32_t tscParseLine(const char* sql, TAOS_SML_DATA_POINT* smlData, SSmlLinesInfo* info) {
  const char* tsc_index = sql;
  int32_t ret = TSDB_CODE_SUCCESS;
  uint8_t has_tags = 0;
  TAOS_SML_KV *timestamp = NULL;
  SHashObj *keyHashTable = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);

  ret = parseSmlMeasurement(smlData, &tsc_index, &has_tags, info);
  if (ret) {
    tscError("SML:0x%"PRIx64" Unable to parse measurement", info->id);
    taosHashCleanup(keyHashTable);
    return ret;
  }
  tscDebug("SML:0x%"PRIx64" Parse measurement finished, has_tags:%d", info->id, has_tags);

  //Parse Tags
  if (has_tags) {
    ret = parseSmlKvPairs(&smlData->tags, &smlData->tagNum, &tsc_index, false, smlData, keyHashTable, info);
    if (ret) {
      tscError("SML:0x%"PRIx64" Unable to parse tag", info->id);
      taosHashCleanup(keyHashTable);
      return ret;
    }
  }
  tscDebug("SML:0x%"PRIx64" Parse tags finished, num of tags:%d", info->id, smlData->tagNum);

  //Parse fields
  ret = parseSmlKvPairs(&smlData->fields, &smlData->fieldNum, &tsc_index, true, smlData, keyHashTable, info);
  if (ret) {
    tscError("SML:0x%"PRIx64" Unable to parse field", info->id);
    taosHashCleanup(keyHashTable);
    return ret;
  }
  tscDebug("SML:0x%"PRIx64" Parse fields finished, num of fields:%d", info->id, smlData->fieldNum);
  taosHashCleanup(keyHashTable);

  //Parse timestamp
  ret = parseSmlTimeStamp(&timestamp, &tsc_index, info);
  if (ret) {
    tscError("SML:0x%"PRIx64" Unable to parse timestamp", info->id);
    return ret;
  }
  moveTimeStampToFirstKv(&smlData, timestamp);
  tscDebug("SML:0x%"PRIx64" Parse timestamp finished", info->id);

  return TSDB_CODE_SUCCESS;
}

//=========================================================================

void destroySmlDataPoint(TAOS_SML_DATA_POINT* point) {
  for (int i=0; i<point->tagNum; ++i) {
    free((point->tags+i)->key);
    free((point->tags+i)->value);
  }
  free(point->tags);
  for (int i=0; i<point->fieldNum; ++i) {
    free((point->fields+i)->key);
    free((point->fields+i)->value);
  }
  free(point->fields);
  free(point->stableName);
  free(point->childTableName);
}

int32_t tscParseLines(char* lines[], int numLines, SArray* points, SArray* failedLines, SSmlLinesInfo* info) {
  for (int32_t i = 0; i < numLines; ++i) {
    TAOS_SML_DATA_POINT point = {0};
    int32_t code = tscParseLine(lines[i], &point, info);
    if (code != TSDB_CODE_SUCCESS) {
      tscError("SML:0x%"PRIx64" data point line parse failed. line %d : %s", info->id, i, lines[i]);
      destroySmlDataPoint(&point);
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    } else {
      tscDebug("SML:0x%"PRIx64" data point line parse success. line %d", info->id, i);
    }

    taosArrayPush(points, &point);
  }
  return 0;
}

int taos_insert_lines(TAOS* taos, char* lines[], int numLines) {
  int32_t code = 0;

  SSmlLinesInfo* info = calloc(1, sizeof(SSmlLinesInfo));
  info->id = genLinesSmlId();

  if (numLines <= 0 || numLines > 65536) {
    tscError("SML:0x%"PRIx64" taos_insert_lines numLines should be between 1 and 65536. numLines: %d", info->id, numLines);
    code = TSDB_CODE_TSC_APP_ERROR;
    return code;
  }

  for (int i = 0; i < numLines; ++i) {
    if (lines[i] == NULL) {
      tscError("SML:0x%"PRIx64" taos_insert_lines line %d is NULL", info->id, i);
      free(info);
      code = TSDB_CODE_TSC_APP_ERROR;
      return code;
    }
  }

  SArray* lpPoints = taosArrayInit(numLines, sizeof(TAOS_SML_DATA_POINT));
  if (lpPoints == NULL) {
    tscError("SML:0x%"PRIx64" taos_insert_lines failed to allocate memory", info->id);
    free(info);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  tscDebug("SML:0x%"PRIx64" taos_insert_lines begin inserting %d lines, first line: %s", info->id, numLines, lines[0]);
  code = tscParseLines(lines, numLines, lpPoints, NULL, info);
  size_t numPoints = taosArrayGetSize(lpPoints);

  if (code != 0) {
    goto cleanup;
  }

  TAOS_SML_DATA_POINT* points = TARRAY_GET_START(lpPoints);
  code = tscSmlInsert(taos, points, (int)numPoints, info);
  if (code != 0) {
    tscError("SML:0x%"PRIx64" taos_sml_insert error: %s", info->id, tstrerror((code)));
  }

cleanup:
  tscDebug("SML:0x%"PRIx64" taos_insert_lines finish inserting %d lines. code: %d", info->id, numLines, code);
  points = TARRAY_GET_START(lpPoints);
  numPoints = taosArrayGetSize(lpPoints);
  for (int i=0; i<numPoints; ++i) {
    destroySmlDataPoint(points+i);
  }

  taosArrayDestroy(lpPoints);

  free(info);
  return code;
}


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
#include "taos.h"
#include "tsclient.h"
#include "tscLog.h"
#include "hash.h"
#include "tskiplist.h"
#include "tscUtil.h"

typedef struct  {
  char sTableName[TSDB_TABLE_NAME_LEN];
  SHashObj* tagHash;
  SHashObj* fieldHash;
  SArray* tags; //SArray<SSchema>
  SArray* fields; //SArray<SSchema>
} SSmlSTableSchema;

typedef struct {
  char* key;
  uint8_t type;
  int16_t length;
  char* value;

  //===================================
  SSchema* schema;
} TAOS_SML_KV;

typedef struct {
  char* stableName;

  char* childTableName;
  TAOS_SML_KV* tags;
  int tagNum;

  // first kv must be timestamp
  TAOS_SML_KV* fields;
  int fieldNum;

  //================================
  SSmlSTableSchema* schema;
} TAOS_SML_DATA_POINT;

//=================================================================================================

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

int32_t loadTableMeta(TAOS* taos, char* tableName, SSmlSTableSchema* schema) {
  int32_t code = 0;

  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  char sql[256];
  snprintf(sql, 256, "describe %s", tableName);
  TAOS_RES* res = taos_query(taos, sql);
  code = taos_errno(res);
  if (code != 0) {
    taos_free_result(res);
    return code;
  }
  taos_free_result(res);

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
  pSql->pTscObj = taos;
  pSql->signature = pSql;
  pSql->fp = NULL;

  SStrToken tableToken = {.z=tableName, .n=strlen(tableName), .type=TK_ID};
  tGetToken(tableName, &tableToken.type);
  // Check if the table name available or not
  if (tscValidateName(&tableToken) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
    sprintf(pSql->cmd.payload, "table name is invalid");
    return code;
  }

  SName sname = {0};
  if ((code = tscSetTableFullName(&sname, &tableToken, pSql)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  char  fullTableName[TSDB_TABLE_FNAME_LEN] = {0};
  memset(fullTableName, 0, tListLen(fullTableName));
  tNameExtractFullName(&sname, fullTableName);

  if (code != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pSql);
    return code;
  }

  tscFreeSqlObj(pSql);

  uint32_t size = tscGetTableMetaMaxSize();
  STableMeta* tableMeta = calloc(1, size);
  taosHashGetClone(tscTableMetaInfo, fullTableName, strlen(fullTableName), NULL, tableMeta, -1);

  tstrncpy(schema->sTableName, tableName, strlen(tableName)+1);
  for (int i=0; i<tableMeta->tableInfo.numOfColumns; ++i) {
    SSchema field;
    tstrncpy(field.name, tableMeta->schema[i].name, strlen(tableMeta->schema[i].name)+1);
    field.type = tableMeta->schema[i].type;
    field.bytes = tableMeta->schema[i].bytes;
    SSchema* pField = taosArrayPush(schema->fields, &field);
    taosHashPut(schema->fieldHash, field.name, strlen(field.name), &pField, POINTER_BYTES);
  }

  for (int i=0; i<tableMeta->tableInfo.numOfTags; ++i) {
    int j = i + tableMeta->tableInfo.numOfColumns;
    SSchema field;
    tstrncpy(field.name, tableMeta->schema[j].name, strlen(tableMeta->schema[j].name)+1);
    field.type = tableMeta->schema[j].type;
    field.bytes = tableMeta->schema[j].bytes;
    SSchema* pField = taosArrayPush(schema->tags, &field);
    taosHashPut(schema->tagHash, field.name, strlen(field.name), &pField, POINTER_BYTES);
  }
  free(tableMeta); tableMeta = NULL;
  return code;
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

int32_t getFieldBytesFromSmlKv(TAOS_SML_KV* kv, int32_t* bytes) {
  if (!IS_VAR_DATA_TYPE(kv->type)) {
    *bytes = tDataTypes[kv->type].bytes;
  } else {
    if (kv->type == TSDB_DATA_TYPE_NCHAR) {
      char* ucs = malloc(kv->length * TSDB_NCHAR_SIZE + 1);
      int32_t bytesNeeded = 0;
      //todo check conversion succeed
      taosMbsToUcs4(kv->value, kv->length, ucs, kv->length * TSDB_NCHAR_SIZE, &bytesNeeded);
      free(ucs);
      *bytes =  bytesNeeded + VARSTR_HEADER_SIZE;
    } else if (kv->type == TSDB_DATA_TYPE_BINARY) {
      *bytes = kv->length + VARSTR_HEADER_SIZE;
    }
  }
  return 0;
}

int32_t addTaosFieldToHashAndArray(TAOS_SML_KV* smlKv, SHashObj* hash, SArray* array) {
  SSchema* pField = NULL;
  SSchema** ppField = taosHashGet(hash, smlKv->key, strlen(smlKv->key));
  if (ppField) {
    pField = *ppField;

    if (pField->type != smlKv->type) {
      //TODO:
      tscError("type mismatch");
      return -1;
    }

    int32_t bytes = 0;
    getFieldBytesFromSmlKv(smlKv, &bytes);
    pField->bytes = MAX(pField->bytes, bytes);

  } else {
    SSchema field;
    size_t tagKeyLen = strlen(smlKv->key);
    strncpy(field.name, smlKv->key, tagKeyLen);
    field.name[tagKeyLen] = '\0';
    field.type = smlKv->type;

    int32_t bytes = 0;
    getFieldBytesFromSmlKv(smlKv, &bytes);
    field.bytes = bytes;

    pField = taosArrayPush(array, &field);
    taosHashPut(hash, field.name, tagKeyLen, &pField, POINTER_BYTES);
  }

  smlKv->schema = pField;

  return 0;
}

int32_t generateSchemaAction(SSchema* pointColField, SHashObj* dbAttrHash, bool isTag, char sTableName[],
                                       SSchemaAction* action, bool* actionNeeded) {
  SSchema** ppDbAttr = taosHashGet(dbAttrHash, pointColField->name, strlen(pointColField->name));
  if (*ppDbAttr) {
    SSchema* dbAttr = *ppDbAttr;
    if (pointColField->type != dbAttr->type) {
      //todo error
      return -5;
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
  return 0;
}

int32_t buildColumnDescription(SSchema* field,
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

int32_t applySchemaAction(TAOS* taos, SSchemaAction* action) {
  int32_t code = 0;
  int32_t capacity = TSDB_MAX_BINARY_LEN;
  int32_t outBytes = 0;
  char *result = (char *)calloc(1, capacity);

  switch (action->action) {
    case SCHEMA_ACTION_ADD_COLUMN: {
      int n = sprintf(result, "alter stable %s add column ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field, result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      break;
    }
    case SCHEMA_ACTION_ADD_TAG: {
      int n = sprintf(result, "alter stable %s add tag ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field,
                             result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      break;
    }
    case SCHEMA_ACTION_CHANGE_COLUMN_SIZE: {
      int n = sprintf(result, "alter stable %s modify column ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      break;
    }
    case SCHEMA_ACTION_CHANGE_TAG_SIZE: {
      int n = sprintf(result, "alter stable %s modify tag ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      break;
    }
    case SCHEMA_ACTION_CREATE_STABLE: {
      int n = sprintf(result, "create stable %s (", action->createSTable.sTableName);
      char* pos = result + n; int freeBytes = capacity - n;
      int numCols = taosArrayGetSize(action->createSTable.fields);
      for (int32_t i = 0; i < numCols; ++i) {
        SSchema* field = taosArrayGet(action->createSTable.fields, i);
        buildColumnDescription(field, pos, freeBytes, &outBytes);
        pos += outBytes; freeBytes -= outBytes;
        *pos = ','; ++pos; --freeBytes;
      }
      --pos; ++freeBytes;

      outBytes = snprintf(pos, freeBytes, ") tags (");
      pos += outBytes; freeBytes -= outBytes;

      int numTags = taosArrayGetSize(action->createSTable.tags);
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
      break;
    }

    default:
      break;
  }
  free(result);
  return code;
}

int32_t getChildTableName(TAOS_SML_DATA_POINT* point, char* tableName, int* tableNameLen) {
  qsort(point->tags, point->tagNum, sizeof(TAOS_SML_KV), compareSmlColKv);

  SStringBuilder sb; memset(&sb, 0, sizeof(sb));
  taosStringBuilderAppendString(&sb, point->stableName);
  for (int j = 0; j < point->tagNum; ++j) {
    taosStringBuilderAppendChar(&sb, ',');
    TAOS_SML_KV* tagKv = point->tags + j;
    taosStringBuilderAppendString(&sb, tagKv->key);
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
                           "tbl%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", context.digest[0],
                           context.digest[1], context.digest[2], context.digest[3], context.digest[4], context.digest[5], context.digest[6],
                           context.digest[7], context.digest[8], context.digest[9], context.digest[10], context.digest[11],
                           context.digest[12], context.digest[13], context.digest[14], context.digest[15]);
  taosStringBuilderDestroy(&sb);
  return 0;
}

int32_t getPreparedSQL(const char* sTableName, SArray* tagsSchema, SArray* colsSchema, char* result, int16_t freeBytes) {
  size_t numTags = taosArrayGetSize(tagsSchema);
  size_t numCols = taosArrayGetSize(colsSchema);
  sprintf(result, "insert into ? using %s", sTableName);

//  snprintf(result+strlen(result), freeBytes-strlen(result), "(");
//  for (int i = 0; i < numTags; ++i) {
//    SSchema* tagSchema = taosArrayGet(tagsSchema, i);
//    snprintf(result+strlen(result), freeBytes-strlen(result), "%s,", tagSchema->name);
//  }
//  snprintf(result + strlen(result)-1, freeBytes-strlen(result)+1, ")");

  snprintf(result + strlen(result), freeBytes-strlen(result), " tags (");

  for (int i = 0; i < numTags; ++i) {
    snprintf(result+strlen(result), freeBytes-strlen(result), "?,");
  }
  snprintf(result + strlen(result)-1, freeBytes-strlen(result)+1, ") (");

  for (int i = 0; i < numCols; ++i) {
    SSchema* colSchema = taosArrayGet(colsSchema, i);
    snprintf(result+strlen(result), freeBytes-strlen(result), "%s,", colSchema->name);
  }
  snprintf(result + strlen(result)-1, freeBytes-strlen(result)+1, ") values (");

  for (int i = 0; i < numCols; ++i) {
    snprintf(result+strlen(result), freeBytes-strlen(result), "?,");
  }
  snprintf(result + strlen(result)-1, freeBytes-strlen(result)+1, ")");
  return 0;
}

int32_t insertBatch(TAOS* taos, char* sql, char* cTableName, SArray* tagsBind, SArray* rowsBind) {
  TAOS_STMT* stmt = taos_stmt_init(taos);
  int32_t code;
  code = taos_stmt_prepare(stmt, sql, strlen(sql));
  if (code != 0) {
    printf("%s", taos_stmt_errstr(stmt));
    return code;
  }

  code = taos_stmt_set_tbname_tags(stmt, cTableName, TARRAY_GET_START(tagsBind));
  if (code != 0) {
    printf("%s", taos_stmt_errstr(stmt));
    return code;
  }
  size_t rows = taosArrayGetSize(rowsBind);
  for (int32_t i = 0; i < rows; ++i) {
    SArray* colBind = taosArrayGetP(rowsBind, i);
    code = taos_stmt_bind_param(stmt, TARRAY_GET_START(colBind));
    if (code != 0) {
      printf("%s", taos_stmt_errstr(stmt));
      return code;
    }
    code = taos_stmt_add_batch(stmt);
    if (code != 0) {
      printf("%s", taos_stmt_errstr(stmt));
      return code;
    }
  }

  code = taos_stmt_execute(stmt);
  if (code != 0) {
    printf("%s", taos_stmt_errstr(stmt));
    return code;
  }
  TAOS_RES* res = taos_stmt_use_result(stmt);
  return taos_errno(res);
}

int32_t insertPoints(TAOS* taos, TAOS_SML_DATA_POINT* points, int32_t numPoints) {
  SHashObj* cname2points = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
  for (int32_t i = 0; i < numPoints; ++i) {
    TAOS_SML_DATA_POINT * point = points + i;
    if (!point->childTableName) {
      char childTableName[TSDB_TABLE_NAME_LEN];
      int32_t tableNameLen = TSDB_TABLE_NAME_LEN;
      getChildTableName(point, childTableName, &tableNameLen);
      point->childTableName = calloc(1, tableNameLen+1);
      strncpy(point->childTableName, childTableName, tableNameLen);
      point->childTableName[tableNameLen] = '\0';
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

  int isNullColBind = TSDB_TRUE;
  SArray** pCTablePoints = taosHashIterate(cname2points, NULL);
  while (pCTablePoints) {
    SArray* cTablePoints = *pCTablePoints;

    TAOS_SML_DATA_POINT * point = taosArrayGetP(cTablePoints, 0);
    int32_t numTags = taosArrayGetSize(point->schema->tags);
    int32_t numCols = taosArrayGetSize(point->schema->fields);
    char* stableName = point->stableName;
    char* ctableName = point->childTableName;

    SArray* tagBinds = taosArrayInit(numTags, sizeof(TAOS_BIND));
    taosArraySetSize(tagBinds, numTags);
    for (int j = 0; j < numTags; ++j) {
      TAOS_BIND* bind = taosArrayGet(tagBinds, j);
      bind->is_null = &isNullColBind;
    }
    for (int j = 0; j < point->tagNum; ++j) {
      TAOS_SML_KV* kv =  point->tags + j;
      int32_t idx = TARRAY_ELEM_IDX(point->schema->tags, kv->schema);
      TAOS_BIND* bind = taosArrayGet(tagBinds, idx);
      bind->buffer_type = kv->type;
      bind->length = malloc(sizeof(uintptr_t*));
      *bind->length = kv->length;
      bind->buffer = kv->value;
      bind->is_null = NULL;
    }

    size_t rows = taosArrayGetSize(cTablePoints);
    SArray* rowsBind = taosArrayInit(rows, POINTER_BYTES);

    for (int i = 0; i < rows; ++i) {
      point = taosArrayGetP(cTablePoints, i);

      SArray* colBinds = taosArrayInit(numCols, sizeof(TAOS_BIND));
      taosArraySetSize(colBinds, numCols);
      for (int j = 0; j < numCols; ++j) {
        TAOS_BIND* bind = taosArrayGet(colBinds, j);
        bind->is_null = &isNullColBind;
      }
      for (int j = 0; j < point->fieldNum; ++j) {
        TAOS_SML_KV* kv = point->fields + j;
        int32_t idx = TARRAY_ELEM_IDX(point->schema->fields, kv->schema);
        TAOS_BIND* bind = taosArrayGet(colBinds, idx);
        bind->buffer_type = kv->type;
        bind->length = malloc(sizeof(uintptr_t*));
        *bind->length = kv->length;
        bind->buffer = kv->value;
        bind->is_null = NULL;
      }
      taosArrayPush(rowsBind, &colBinds);
    }

    char sql[TSDB_MAX_BINARY_LEN];
    getPreparedSQL(stableName, point->schema->tags, point->schema->fields, sql, TSDB_MAX_BINARY_LEN);
    insertBatch(taos, sql, ctableName, tagBinds, rowsBind);

    pCTablePoints = taosHashIterate(cname2points, pCTablePoints);
  }
  return 0;
}

int taos_sml_insert(TAOS* taos, TAOS_SML_DATA_POINT* points, int numPoint) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* stableArray = taosArrayInit(32, sizeof(SSmlSTableSchema)); // SArray<STableColumnsSchema>
  SHashObj* sname2shema = taosHashInit(32,
                                       taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);

  for (int i = 0; i < numPoint; ++i) {
    TAOS_SML_DATA_POINT* point = &points[i];
    size_t stableNameLen = strlen(point->stableName);
    SSmlSTableSchema** ppStableSchema = taosHashGet(sname2shema, point->stableName, stableNameLen);
    SSmlSTableSchema* pStableSchema = NULL;
    if (ppStableSchema) {
      pStableSchema= *ppStableSchema;
    } else {
      SSmlSTableSchema schema;
      strncpy(schema.sTableName, point->stableName, stableNameLen);
      schema.sTableName[stableNameLen] = '\0';
      schema.fields = taosArrayInit(64, sizeof(SSchema));
      schema.tags = taosArrayInit(8, sizeof(SSchema));
      schema.tagHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
      schema.fieldHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);

      pStableSchema = taosArrayPush(stableArray, &schema);
      taosHashPut(sname2shema, schema.sTableName, stableNameLen, &pStableSchema, POINTER_BYTES);
    }

    for (int j = 0; j < point->tagNum; ++j) {
      TAOS_SML_KV* tagKv = point->tags + j;
      addTaosFieldToHashAndArray(tagKv, pStableSchema->tagHash, pStableSchema->tags);
    }

    for (int j = 0; j < point->fieldNum; ++j) {
      TAOS_SML_KV* fieldKv = point->fields + j;
      addTaosFieldToHashAndArray(fieldKv, pStableSchema->fieldHash, pStableSchema->fields);
    }

    point->schema = pStableSchema;
  }

  SArray* schemaActions = taosArrayInit(32, sizeof(SSchemaAction));
  size_t numStable = taosArrayGetSize(stableArray);
  for (int i = 0; i < numStable; ++i) {
    SSmlSTableSchema* pointSchema = taosArrayGet(stableArray, i);
    SSmlSTableSchema  dbSchema = {0};
    dbSchema.tags = taosArrayInit(8, sizeof(SSchema));
    dbSchema.fields = taosArrayInit(64, sizeof(SSchema));
    dbSchema.tagHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
    dbSchema.fieldHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);

    code = loadTableMeta(taos, pointSchema->sTableName, &dbSchema);

    if (code == TSDB_CODE_MND_INVALID_TABLE_NAME) {
      SSchemaAction schemaAction = {0};
      schemaAction.action = SCHEMA_ACTION_CREATE_STABLE;
      memset(&schemaAction.createSTable, 0, sizeof(SCreateSTableActionInfo));
      memcpy(schemaAction.createSTable.sTableName, pointSchema->sTableName, TSDB_TABLE_NAME_LEN);
      schemaAction.createSTable.tags = pointSchema->tags;
      schemaAction.createSTable.fields = pointSchema->fields;
      taosArrayPush(schemaActions, &schemaAction);
    }else if (code == TSDB_CODE_SUCCESS) {
      size_t pointTagSize = taosArrayGetSize(pointSchema->tags);
      size_t pointFieldSize = taosArrayGetSize(pointSchema->fields);

      SHashObj* dbTagHash = dbSchema.tagHash;
      SHashObj* dbFieldHash = dbSchema.fieldHash;

      for (int j = 0; j < pointTagSize; ++j) {
        SSchema* pointTag = taosArrayGet(pointSchema->tags, j);
        SSchemaAction schemaAction = {0};
        bool actionNeeded = false;
        generateSchemaAction(pointTag, dbTagHash, true, pointSchema->sTableName, &schemaAction, &actionNeeded);
        if (actionNeeded) {
          taosArrayPush(schemaActions, &schemaAction);
        }
      }

      SSchema* pointColTs = taosArrayGet(pointSchema->fields, 0);
      SSchema* dbColTs = taosArrayGet(dbSchema.fields, 0);
      memcpy(pointColTs->name, dbColTs->name, TSDB_COL_NAME_LEN);

      for (int j = 1; j < pointFieldSize; ++j) {
        SSchema* pointCol = taosArrayGet(pointSchema->fields, j);
        SSchemaAction schemaAction = {0};
        bool actionNeeded = false;
        generateSchemaAction(pointCol, dbFieldHash, false, pointSchema->sTableName, &schemaAction, &actionNeeded);
        if (actionNeeded) {
          taosArrayPush(schemaActions, &schemaAction);
        }
      }
    } else {
      return code;
    }
  }

  for (int i = 0; i < taosArrayGetSize(schemaActions); ++i) {
    SSchemaAction* action = taosArrayGet(schemaActions, i);
    applySchemaAction(taos, action);
  }

  insertPoints(taos, points, numPoint);
  return code;
}


//todo: table/column length check
//todo: type check
//todo: taosmbs2ucs4 check

//=========================================================================

typedef enum {
  LP_ITEM_TAG,
  LP_ITEM_FIELD
} LPItemKind;

typedef struct  {
  SStrToken keyToken;
  SStrToken valueToken;

  char key[TSDB_COL_NAME_LEN];
  int8_t type;
  int16_t length;

  char* value;
}SLPItem;

typedef struct {
  SStrToken measToken;
  SStrToken tsToken;

  char sTableName[TSDB_TABLE_NAME_LEN];
  SArray* tags;
  SArray* fields;
  int64_t ts;

} SLPPoint;

typedef enum {
  LP_MEASUREMENT,
  LP_TAG_KEY,
  LP_TAG_VALUE,
  LP_FIELD_KEY,
  LP_FIELD_VALUE
} LPPart;

int32_t scanToCommaOrSpace(SStrToken s, int32_t start, int32_t* index, LPPart part) {
  for (int32_t i = start; i < s.n; ++i) {
    if (s.z[i] == ',' || s.z[i] == ' ') {
      *index = i;
      return 0;
    }
  }
  return -1;
}

int32_t scanToEqual(SStrToken s, int32_t start, int32_t* index) {
  for (int32_t i = start; i < s.n; ++i) {
    if (s.z[i] == '=') {
      *index = i;
      return 0;
    }
  }
  return -1;
}

int32_t setPointMeasurement(SLPPoint* point, SStrToken token) {
  point->measToken = token;
  if (point->measToken.n < TSDB_TABLE_NAME_LEN) {
    strncpy(point->sTableName, point->measToken.z, point->measToken.n);
    point->sTableName[point->measToken.n] = '\0';
  }
  return 0;
}

int32_t setItemKey(SLPItem* item, SStrToken key, LPPart part) {
  item->keyToken = key;
  if (item->keyToken.n < TSDB_COL_NAME_LEN) {
    strncpy(item->key, item->keyToken.z, item->keyToken.n);
    item->key[item->keyToken.n] = '\0';
  }
  return 0;
}

int32_t setItemValue(SLPItem* item, SStrToken value, LPPart part) {
  item->valueToken = value;
  return 0;
}

int32_t parseItemValue(SLPItem* item, LPItemKind kind) {
  char* sv = item->valueToken.z;
  char* last = item->valueToken.z + item->valueToken.n - 1;

  if (isdigit(sv[0]) || sv[0] == '-') {
    if (*last == 'i') {
      item->type = TSDB_DATA_TYPE_BIGINT;
      item->length = (int16_t)tDataTypes[item->type].bytes;
      item->value = malloc(item->length);
      char* endptr = NULL;
      *(int64_t*)(item->value) = strtoll(sv, &endptr, 10);
    } else {
      item->type = TSDB_DATA_TYPE_DOUBLE;
      item->length = (int16_t)tDataTypes[item->type].bytes;
      item->value = malloc(item->length);
      char* endptr = NULL;
      *(double*)(item->value) = strtold(sv, &endptr);
    }
  } else if ((sv[0] == 'L' && sv[1] =='"') || sv[0] == '"' ) {
    if (sv[0] == 'L') {
      item->type = TSDB_DATA_TYPE_NCHAR;
      uint32_t bytes = item->valueToken.n - 3;
      item->length = bytes;
      item->value = malloc(bytes);
      memcpy(item->value, sv+2, bytes);
    } else if (sv[0]=='"'){
      item->type = TSDB_DATA_TYPE_BINARY;
      uint32_t bytes = item->valueToken.n - 2;
      item->length = bytes;
      item->value = malloc(bytes);
      memcpy(item->value, sv+1, bytes);
    }
  } else if (sv[0] == 't' || sv[0] == 'f' || sv[0]=='T' || sv[0] == 'F') {
    item->type = TSDB_DATA_TYPE_BOOL;
    item->length = tDataTypes[item->type].bytes;
    item->value = malloc(tDataTypes[item->type].bytes);
    *(uint8_t*)(item->value) = tolower(sv[0])=='t' ? TSDB_TRUE : TSDB_FALSE;
  }
  return 0;
}

int32_t compareLPItemKey(const void* p1, const void* p2) {
  const SLPItem* t1 = p1;
  const SLPItem* t2 = p2;
  uint32_t min = (t1->keyToken.n < t2->keyToken.n) ? t1->keyToken.n : t2->keyToken.n;
  int res = strncmp(t1->keyToken.z, t2->keyToken.z, min);
  if (res != 0) {
    return res;
  } else {
    return (int)(t1->keyToken.n) - (int)(t2->keyToken.n);
  }
}

int32_t setPointTimeStamp(SLPPoint* point, SStrToken tsToken) {
  point->tsToken = tsToken;
  return 0;
}

int32_t parsePointTime(SLPPoint* point) {
  if (point->tsToken.n <= 0) {
    point->ts = taosGetTimestampNs();
  } else {
    char* endptr = NULL;
    point->ts = strtoll(point->tsToken.z, &endptr, 10);
  }
  return 0;
}

int32_t tscParseLine(SStrToken line, SLPPoint* point) {
  int32_t pos = 0;

  int32_t start = 0;
  int32_t err = scanToCommaOrSpace(line, start, &pos, LP_MEASUREMENT);
  if (err != 0) {
    tscError("a");
    return err;
  }

  SStrToken measurement = {.z = line.z+start, .n = pos-start};
  setPointMeasurement(point, measurement);
  point->tags = taosArrayInit(64, sizeof(SLPItem));
  start = pos;
  while (line.z[start] == ',') {
    SLPItem item;

    start++;
    err = scanToEqual(line, start, &pos);
    if (err != 0) {
      tscError("b");
      goto error;
    }

    SStrToken tagKey = {.z = line.z + start, .n = pos-start};
    setItemKey(&item, tagKey, LP_TAG_KEY);

    start = pos + 1;
    err = scanToCommaOrSpace(line, start, &pos, LP_TAG_VALUE);
    if (err != 0) {
      tscError("c");
      goto error;
    }

    SStrToken tagValue = {.z = line.z + start, .n = pos-start};
    setItemValue(&item, tagValue, LP_TAG_VALUE);

    parseItemValue(&item, LP_ITEM_TAG);
    taosArrayPush(point->tags, &item);

    start = pos;
  }

  taosArraySort(point->tags, compareLPItemKey);

  point->fields = taosArrayInit(64, sizeof(SLPItem));

  start++;
  do {
    SLPItem item;

    err = scanToEqual(line, start, &pos);
    if (err != 0) {
      goto error;
    }
    SStrToken fieldKey = {.z = line.z + start, .n = pos- start};
    setItemKey(&item, fieldKey, LP_FIELD_KEY);

    start = pos + 1;
    err = scanToCommaOrSpace(line, start, &pos, LP_FIELD_VALUE);
    if (err != 0) {
      goto error;
    }
    SStrToken fieldValue = {.z = line.z + start, .n = pos - start};
    setItemValue(&item, fieldValue, LP_TAG_VALUE);

    parseItemValue(&item, LP_ITEM_FIELD);
    taosArrayPush(point->fields, &item);

    start = pos + 1;
  } while (line.z[pos] == ',');

  taosArraySort(point->fields, compareLPItemKey);

  SStrToken tsToken = {.z = line.z+start, .n = line.n-start};
  setPointTimeStamp(point, tsToken);
  parsePointTime(point);

  goto done;

  error:
  // free array
  return err;
  done:
  return 0;
}


int32_t tscParseLines(char* lines[], int numLines, SArray* points, SArray* failedLines) {
  for (int32_t i = 0; i < numLines; ++i) {
    SStrToken tkLine = {.z = lines[i], .n = strlen(lines[i])+1};
    SLPPoint point;
    tscParseLine(tkLine, &point);
    taosArrayPush(points, &point);
  }
  return 0;
}

int taos_insert_by_lines(TAOS* taos, char* lines[], int numLines) {
  SArray* lpPoints = taosArrayInit(numLines, sizeof(SLPPoint));
  tscParseLines(lines, numLines, lpPoints, NULL);

  size_t numPoints = taosArrayGetSize(lpPoints);
  TAOS_SML_DATA_POINT* points = calloc(numPoints, sizeof(TAOS_SML_DATA_POINT));
  for (int i = 0; i < numPoints; ++i) {
    SLPPoint* lpPoint = taosArrayGet(lpPoints, i);
    TAOS_SML_DATA_POINT* point = points+i;
    point->stableName = calloc(1, strlen(lpPoint->sTableName)+1);
    strncpy(point->stableName, lpPoint->sTableName, strlen(lpPoint->sTableName));
    point->stableName[strlen(lpPoint->sTableName)] = '\0';

    size_t lpTagSize = taosArrayGetSize(lpPoint->tags);
    point->tags = calloc(lpTagSize, sizeof(TAOS_SML_KV));
    point->tagNum = lpTagSize;
    for (int j=0; j<lpTagSize; ++j) {
      SLPItem* lpTag = taosArrayGet(lpPoint->tags, j);
      TAOS_SML_KV* tagKv = point->tags + j;

      size_t kenLen = strlen(lpTag->key);
      tagKv->key = calloc(1, kenLen+1);
      strncpy(tagKv->key, lpTag->key, kenLen);
      tagKv->key[kenLen] = '\0';

      tagKv->type = lpTag->type;
      tagKv->length = lpTag->length;
      tagKv->value = malloc(tagKv->length);
      memcpy(tagKv->value, lpTag->value, tagKv->length);
    }

    size_t lpFieldsSize = taosArrayGetSize(lpPoint->fields);
    point->fields = calloc(lpFieldsSize + 1, sizeof(TAOS_SML_KV));
    point->fieldNum = lpFieldsSize + 1;

    TAOS_SML_KV* tsField = point->fields + 0;
    char tsKey[256];
    snprintf(tsKey, 256, "_%s_ts", point->stableName);
    size_t tsKeyLen = strlen(tsKey);
    tsField->key = calloc(1, tsKeyLen+1);
    strncpy(tsField->key, tsKey, tsKeyLen);
    tsField->key[tsKeyLen] = '\0';
    tsField->type = TSDB_DATA_TYPE_TIMESTAMP;
    tsField->length = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
    tsField->value = malloc(tsField->length);
    memcpy(tsField->value, &(lpPoint->ts), tsField->length);

    for (int j=0; j<lpFieldsSize; ++j) {
      SLPItem* lpField = taosArrayGet(lpPoint->fields, j);
      TAOS_SML_KV* fieldKv = point->fields + j + 1;

      size_t kenLen = strlen(lpField->key);
      fieldKv->key = calloc(1, kenLen+1);
      strncpy(fieldKv->key, lpField->key, kenLen);
      fieldKv->key[kenLen] = '\0';

      fieldKv->type = lpField->type;
      fieldKv->length = lpField->length;
      fieldKv->value = malloc(fieldKv->length);
      memcpy(fieldKv->value, lpField->value, fieldKv->length);
    }
  }

  taos_sml_insert(taos, points, numPoints);
  return 0;
}


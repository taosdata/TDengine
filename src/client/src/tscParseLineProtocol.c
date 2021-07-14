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

static int32_t getFieldBytesFromSmlKv(TAOS_SML_KV* kv, int32_t* bytes) {
  if (!IS_VAR_DATA_TYPE(kv->type)) {
    *bytes = tDataTypes[kv->type].bytes;
  } else {
    if (kv->type == TSDB_DATA_TYPE_NCHAR) {
      char* ucs = malloc(kv->length * TSDB_NCHAR_SIZE + 1);
      int32_t bytesNeeded = 0;
      bool succ = taosMbsToUcs4(kv->value, kv->length, ucs, kv->length * TSDB_NCHAR_SIZE, &bytesNeeded);
      if (!succ) {
        free(ucs);
        tscError("convert nchar string to UCS4_LE failed:%s", kv->value);
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

static int32_t buildSmlKvSchema(TAOS_SML_KV* smlKv, SHashObj* hash, SArray* array) {
  SSchema* pField = NULL;
  SSchema** ppField = taosHashGet(hash, smlKv->key, strlen(smlKv->key));
  int32_t code = 0;
  if (ppField) {
    pField = *ppField;

    if (pField->type != smlKv->type) {
      tscError("type mismatch. key %s, type %d. type before %d", smlKv->key, smlKv->type, pField->type);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }

    int32_t bytes = 0;
    code = getFieldBytesFromSmlKv(smlKv, &bytes);
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
    code = getFieldBytesFromSmlKv(smlKv, &bytes);
    if (code != 0) {
      return code;
    }
    field.bytes = bytes;

    pField = taosArrayPush(array, &field);
    taosHashPut(hash, field.name, tagKeyLen, &pField, POINTER_BYTES);
  }

  smlKv->schema = pField;

  return 0;
}

static int32_t buildDataPointSchemas(TAOS_SML_DATA_POINT* points, int numPoint, SArray* stableSchemas) {
  int32_t code = 0;
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

      pStableSchema = taosArrayPush(stableSchemas, &schema);
      taosHashPut(sname2shema, schema.sTableName, stableNameLen, &pStableSchema, POINTER_BYTES);
    }

    for (int j = 0; j < point->tagNum; ++j) {
      TAOS_SML_KV* tagKv = point->tags + j;
      code = buildSmlKvSchema(tagKv, pStableSchema->tagHash, pStableSchema->tags);
      if (code != 0) {
        tscError("build data point schema failed. point no.: %d, tag key: %s", i, tagKv->key);
        return code;
      }
    }

    for (int j = 0; j < point->fieldNum; ++j) {
      TAOS_SML_KV* fieldKv = point->fields + j;
      code = buildSmlKvSchema(fieldKv, pStableSchema->fieldHash, pStableSchema->fields);
      if (code != 0) {
        tscError("build data point schema failed. point no.: %d, tag key: %s", i, fieldKv->key);
        return code;
      }
    }

    point->schema = pStableSchema;
  }

  size_t numStables = taosArrayGetSize(stableSchemas);
  for (int32_t i = 0; i < numStables; ++i) {
    SSmlSTableSchema* schema = taosArrayGet(stableSchemas, i);
    taosHashCleanup(schema->tagHash);
    taosHashCleanup(schema->fieldHash);
  }
  taosHashCleanup(sname2shema);

  tscDebug("build point schema succeed. num of super table: %zu", numStables);
  for (int32_t i = 0; i < numStables; ++i) {
    SSmlSTableSchema* schema = taosArrayGet(stableSchemas, i);
    tscDebug("\ttable name: %s, tags number: %zu, fields number: %zu", schema->sTableName,
             taosArrayGetSize(schema->tags), taosArrayGetSize(schema->fields));
  }

  return 0;
}

static int32_t generateSchemaAction(SSchema* pointColField, SHashObj* dbAttrHash, bool isTag, char sTableName[],
                                       SSchemaAction* action, bool* actionNeeded) {
  SSchema** ppDbAttr = taosHashGet(dbAttrHash, pointColField->name, strlen(pointColField->name));
  if (ppDbAttr) {
    SSchema* dbAttr = *ppDbAttr;
    if (pointColField->type != dbAttr->type) {
      tscError("point type and db type mismatch. key: %s. point type: %d, db type: %d", pointColField->name,
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
  tscDebug("generate schema action. action needed: %d, action: %d", *actionNeeded, action->action);
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


static int32_t applySchemaAction(TAOS* taos, SSchemaAction* action) {
  int32_t code = 0;
  int32_t capacity = TSDB_MAX_BINARY_LEN;
  int32_t outBytes = 0;
  char *result = (char *)calloc(1, capacity);

  tscDebug("apply schema action: %d", action->action);
  switch (action->action) {
    case SCHEMA_ACTION_ADD_COLUMN: {
      int n = sprintf(result, "alter stable %s add column ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field, result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      taos_free_result(res);
      break;
    }
    case SCHEMA_ACTION_ADD_TAG: {
      int n = sprintf(result, "alter stable %s add tag ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field,
                             result+n, capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      taos_free_result(res);
      break;
    }
    case SCHEMA_ACTION_CHANGE_COLUMN_SIZE: {
      int n = sprintf(result, "alter stable %s modify column ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      taos_free_result(res);
      break;
    }
    case SCHEMA_ACTION_CHANGE_TAG_SIZE: {
      int n = sprintf(result, "alter stable %s modify tag ", action->alterSTable.sTableName);
      buildColumnDescription(action->alterSTable.field, result+n,
                             capacity-n, &outBytes);
      TAOS_RES* res = taos_query(taos, result); //TODO async doAsyncQuery
      code = taos_errno(res);
      taos_free_result(res);
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
      taos_free_result(res);
      break;
    }

    default:
      break;
  }

  free(result);
  if (code != 0) {
    tscError("apply schema action failure. %s", tstrerror(code));
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

int32_t loadTableMeta(TAOS* taos, char* tableName, SSmlSTableSchema* schema) {
  int32_t code = 0;

  STscObj *pObj = (STscObj *)taos;
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  tscDebug("load table schema. super table name: %s", tableName);

  char sql[256];
  snprintf(sql, 256, "describe %s", tableName);
  TAOS_RES* res = taos_query(taos, sql);
  code = taos_errno(res);
  if (code != 0) {
    tscError("describe table failure. %s", taos_errstr(res));
    taos_free_result(res);
    return code;
  }
  taos_free_result(res);

  SSqlObj* pSql = calloc(1, sizeof(SSqlObj));
  pSql->pTscObj = taos;
  pSql->signature = pSql;
  pSql->fp = NULL;

  SStrToken tableToken = {.z=tableName, .n=(uint32_t)strlen(tableName), .type=TK_ID};
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

  schema->tags = taosArrayInit(8, sizeof(SSchema));
  schema->fields = taosArrayInit(64, sizeof(SSchema));
  schema->tagHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);
  schema->fieldHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, false);

  uint32_t size = tscGetTableMetaMaxSize();
  STableMeta* tableMeta = calloc(1, size);
  taosHashGetClone(tscTableMetaInfo, fullTableName, strlen(fullTableName), NULL, tableMeta, -1);

  tstrncpy(schema->sTableName, tableName, strlen(tableName)+1);
  schema->precision = tableMeta->tableInfo.precision;
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
  tscDebug("load table meta succeed. %s, columns number: %d, tag number: %d, precision: %d",
           tableName, tableMeta->tableInfo.numOfColumns, tableMeta->tableInfo.numOfTags, schema->precision);
  free(tableMeta); tableMeta = NULL;
  return code;
}

static int32_t reconcileDBSchemas(TAOS* taos, SArray* stableSchemas) {
  int32_t code = 0;
  size_t numStable = taosArrayGetSize(stableSchemas);
  for (int i = 0; i < numStable; ++i) {
    SSmlSTableSchema* pointSchema = taosArrayGet(stableSchemas, i);
    SSmlSTableSchema  dbSchema;
    memset(&dbSchema, 0, sizeof(SSmlSTableSchema));

    code = loadTableMeta(taos, pointSchema->sTableName, &dbSchema);
    if (code == TSDB_CODE_MND_INVALID_TABLE_NAME) {
      SSchemaAction schemaAction = {0};
      schemaAction.action = SCHEMA_ACTION_CREATE_STABLE;
      memset(&schemaAction.createSTable, 0, sizeof(SCreateSTableActionInfo));
      memcpy(schemaAction.createSTable.sTableName, pointSchema->sTableName, TSDB_TABLE_NAME_LEN);
      schemaAction.createSTable.tags = pointSchema->tags;
      schemaAction.createSTable.fields = pointSchema->fields;
      applySchemaAction(taos, &schemaAction);
      code = loadTableMeta(taos, pointSchema->sTableName, &dbSchema);
      if (code != 0) {
        tscError("reconcile point schema failed. can not create %s", pointSchema->sTableName);
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
        generateSchemaAction(pointTag, dbTagHash, true, pointSchema->sTableName, &schemaAction, &actionNeeded);
        if (actionNeeded) {
          applySchemaAction(taos, &schemaAction);
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
          applySchemaAction(taos, &schemaAction);
        }
      }

      pointSchema->precision = dbSchema.precision;

      destroySmlSTableSchema(&dbSchema);
    } else {
      tscError("load table meta error: %s", tstrerror(code));
      return code;
    }
  }
  return 0;
}

static int32_t getChildTableName(TAOS_SML_DATA_POINT* point, char* tableName, int* tableNameLen) {
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
                           "t_%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", context.digest[0],
                           context.digest[1], context.digest[2], context.digest[3], context.digest[4], context.digest[5], context.digest[6],
                           context.digest[7], context.digest[8], context.digest[9], context.digest[10], context.digest[11],
                           context.digest[12], context.digest[13], context.digest[14], context.digest[15]);
  taosStringBuilderDestroy(&sb);
  tscDebug("child table name: %s", tableName);
  return 0;
}

static int32_t creatChildTableIfNotExists(TAOS* taos, const char* cTableName, const char* sTableName, SArray* tagsSchema, SArray* tagsBind) {
  size_t numTags = taosArrayGetSize(tagsSchema);
  char sql[TSDB_MAX_BINARY_LEN] = {0};
  int freeBytes = TSDB_MAX_BINARY_LEN;
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

  tscDebug("create table : %s", sql);

  TAOS_STMT* stmt = taos_stmt_init(taos);
  int32_t code;
  code = taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (code != 0) {
    tscError("%s", taos_stmt_errstr(stmt));
    return code;
  }

  code = taos_stmt_bind_param(stmt, TARRAY_GET_START(tagsBind));
  if (code != 0) {
    tscError("%s", taos_stmt_errstr(stmt));
    return code;
  }

  code = taos_stmt_execute(stmt);
  if (code != 0) {
    tscError("%s", taos_stmt_errstr(stmt));
    return code;
  }

  taos_stmt_close(stmt);
  return 0;
}

static int32_t insertChildTableBatch(TAOS* taos,  char* cTableName, SArray* colsSchema, SArray* rowsBind) {
  size_t numCols = taosArrayGetSize(colsSchema);
  char sql[TSDB_MAX_BINARY_LEN];
  int32_t freeBytes = TSDB_MAX_BINARY_LEN;
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

  tscDebug("insert rows %zu into child table %s. ", taosArrayGetSize(rowsBind), cTableName);

  int32_t code = 0;
  int32_t try = 0;

  TAOS_STMT* stmt = taos_stmt_init(taos);
  
  code = taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (code != 0) {
    tscError("%s", taos_stmt_errstr(stmt));
    return code;
  }

  do {

    code = taos_stmt_set_tbname(stmt, cTableName);
    if (code != 0) {
      tscError("%s", taos_stmt_errstr(stmt));
      return code;
    }

    size_t rows = taosArrayGetSize(rowsBind);
    for (int32_t i = 0; i < rows; ++i) {
      TAOS_BIND* colsBinds = taosArrayGetP(rowsBind, i);
      code = taos_stmt_bind_param(stmt, colsBinds);
      if (code != 0) {
        tscError("%s", taos_stmt_errstr(stmt));
        return code;
      }
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        tscError("%s", taos_stmt_errstr(stmt));
        return code;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      tscError("%s", taos_stmt_errstr(stmt));
    }
  } while (code == TSDB_CODE_TDB_TABLE_RECONFIGURE && try++ < TSDB_MAX_REPLICA);

  if (code != 0) {
    tscError("%s", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
  } else {
    taos_stmt_close(stmt);
  }

  return code;
}

static int32_t arrangePointsByChildTableName(TAOS_SML_DATA_POINT* points, int numPoints, SHashObj* cname2points) {
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

    for (int j = 0; j < point->tagNum; ++j) {
      TAOS_SML_KV* kv =  point->tags + j;
      if (kv->type == TSDB_DATA_TYPE_TIMESTAMP) {
        int64_t ts = *(int64_t*)(kv->value);
        ts = convertTimePrecision(ts, TSDB_TIME_PRECISION_NANO, point->schema->precision);
        *(int64_t*)(kv->value) = ts;
      }
    }

    for (int j = 0; j < point->fieldNum; ++j) {
      TAOS_SML_KV* kv =  point->fields + j;
      if (kv->type == TSDB_DATA_TYPE_TIMESTAMP) {
        int64_t ts = *(int64_t*)(kv->value);
        ts = convertTimePrecision(ts, TSDB_TIME_PRECISION_NANO, point->schema->precision);
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

static int32_t insertPoints(TAOS* taos, TAOS_SML_DATA_POINT* points, int32_t numPoints) {
  SHashObj* cname2points = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                        true, false);
  arrangePointsByChildTableName(points, numPoints, cname2points);

  int isNullColBind = TSDB_TRUE;
  SArray** pCTablePoints = taosHashIterate(cname2points, NULL);
  while (pCTablePoints) {
    SArray* cTablePoints = *pCTablePoints;

    TAOS_SML_DATA_POINT * point = taosArrayGetP(cTablePoints, 0);
    size_t numTags = taosArrayGetSize(point->schema->tags);
    size_t numCols = taosArrayGetSize(point->schema->fields);

    SArray* tagBinds = taosArrayInit(numTags, sizeof(TAOS_BIND));
    taosArraySetSize(tagBinds, numTags);
    for (int j = 0; j < numTags; ++j) {
      TAOS_BIND* bind = taosArrayGet(tagBinds, j);
      bind->is_null = &isNullColBind;
    }
    for (int j = 0; j < point->tagNum; ++j) {
      TAOS_SML_KV* kv =  point->tags + j;
      size_t idx = TARRAY_ELEM_IDX(point->schema->tags, kv->schema);
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

      TAOS_BIND* colBinds = calloc(numCols, sizeof(TAOS_BIND));
      for (int j = 0; j < numCols; ++j) {
        TAOS_BIND* bind = colBinds + j;
        bind->is_null = &isNullColBind;
      }
      for (int j = 0; j < point->fieldNum; ++j) {
        TAOS_SML_KV* kv = point->fields + j;
        size_t idx = TARRAY_ELEM_IDX(point->schema->fields, kv->schema);
        TAOS_BIND* bind = colBinds + idx;
        bind->buffer_type = kv->type;
        bind->length = malloc(sizeof(uintptr_t*));
        *bind->length = kv->length;
        bind->buffer = kv->value;
        bind->is_null = NULL;
      }
      taosArrayPush(rowsBind, &colBinds);
    }

    creatChildTableIfNotExists(taos, point->childTableName, point->stableName, point->schema->tags, tagBinds);
    for (int i = 0; i < taosArrayGetSize(tagBinds); ++i) {
      TAOS_BIND* bind = taosArrayGet(tagBinds, i);
      free(bind->length);
    }
    taosArrayDestroy(tagBinds);

    insertChildTableBatch(taos, point->childTableName, point->schema->fields, rowsBind);
    for (int i = 0; i < rows; ++i) {
      TAOS_BIND* colBinds = taosArrayGetP(rowsBind, i);
      for (int j = 0; j < numCols; ++j) {
        TAOS_BIND* bind = colBinds + j;
        free(bind->length);
      }
      free(colBinds);
    }
    taosArrayDestroy(rowsBind);
    taosArrayDestroy(cTablePoints);

    pCTablePoints = taosHashIterate(cname2points, pCTablePoints);
  }

  taosHashCleanup(cname2points);
  return 0;
}

int taos_sml_insert(TAOS* taos, TAOS_SML_DATA_POINT* points, int numPoint) {
  tscDebug("taos_sml_insert. number of points: %d", numPoint);

  int32_t code = TSDB_CODE_SUCCESS;

  SArray* stableSchemas = taosArrayInit(32, sizeof(SSmlSTableSchema)); // SArray<STableColumnsSchema>
  code = buildDataPointSchemas(points, numPoint, stableSchemas);
  if (code != 0) {
    tscError("error building data point schemas : %s", tstrerror(code));
    goto clean_up;
  }

  code = reconcileDBSchemas(taos, stableSchemas);
  if (code != 0) {
    tscError("error change db schema : %s", tstrerror(code));
    goto clean_up;
  }

  code = insertPoints(taos, points, numPoint);
  if (code != 0) {
    tscError("error insert points : %s", tstrerror(code));
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

//=========================================================================

bool is_timestamp(char *pVal, uint16_t len) {
  if ((len == 1) && pVal[0] == '0') {
    printf("Type is timestamp(%s)\n", pVal);
    return true;
  }
  if (len < 2) {
    return false;
  }
  if (pVal[len - 1] == 's') {
    switch (pVal[len - 2]) {
      case 'm':
      case 'u':
      case 'n':
        break;
      default:
        if (isdigit(pVal[len - 2])) {
          break;
        } else {
          return false;
        }
    }
    printf("Type is timestamp\n");
    return true;
  }
  return false;
}

bool is_bool(char *pVal, uint16_t len, bool *b_val) {
  if ((len == 1) &&
      (pVal[len - 1] == 't' ||
       pVal[len - 1] == 'T')) {
    printf("Type is bool(%c)\n", pVal[len - 1]);
    *b_val = true;
    return true;
  }

  if ((len == 1) &&
      (pVal[len - 1] == 'f' ||
       pVal[len - 1] == 'F')) {
    printf("Type is bool(%c)\n", pVal[len - 1]);
    *b_val = false;
    return true;
  }

  if((len == 4) &&
     (!strcmp(&pVal[len - 4], "true") ||
      !strcmp(&pVal[len - 4], "True") ||
      !strcmp(&pVal[len - 4], "TRUE"))) {
    printf("Type is bool(%s)\n", &pVal[len - 4]);
    *b_val = true;
    return true;
  }
  if((len == 5) &&
     (!strcmp(&pVal[len - 5], "false") ||
      !strcmp(&pVal[len - 5], "False") ||
      !strcmp(&pVal[len - 5], "FALSE"))) {
    printf("Type is bool(%s)\n", &pVal[len - 5]);
    *b_val = false;
    return true;
  }
  return false;
}

bool is_binary(char *pVal, uint16_t len) {
  //binary: "abc"
  if (len < 2) {
    return false;
  }
  //binary
  if (pVal[0] == '"' && pVal[len - 1] == '"') {
    printf("Type is binary(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_nchar(char *pVal, uint16_t len) {
  //nchar: L"abc"
  if (len < 3) {
    return false;
  }
  if (pVal[0] == 'L' && pVal[1] == '"' && pVal[len - 1] == '"') {
    printf("Type is nchar(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_tiny_int(char *pVal, uint16_t len) {
  if (len <= 2) {
    return false;
  }
  if (!strcmp(&pVal[len - 2], "i8")) {
    printf("Type is int8(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_tiny_uint(char *pVal, uint16_t len) {
  if (len <= 2) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  if (!strcmp(&pVal[len - 2], "u8")) {
    printf("Type is uint8(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_small_int(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (!strcmp(&pVal[len - 3], "i16")) {
    printf("Type is int16(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_small_uint(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  if (strcmp(&pVal[len - 3], "u16") == 0) {
    printf("Type is uint16(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_int(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcmp(&pVal[len - 3], "i32") == 0) {
    printf("Type is int32(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_uint(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  if (strcmp(&pVal[len - 3], "u32") == 0) {
    printf("Type is uint32(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_big_int(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcmp(&pVal[len - 3], "i64") == 0) {
    printf("Type is int64(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_big_uint(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (pVal[0] == '-') {
    return false;
  }
  if (strcmp(&pVal[len - 3], "u64") == 0) {
    printf("Type is uint64(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_float(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcmp(&pVal[len - 3], "f32") == 0) {
    printf("Type is float(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_double(char *pVal, uint16_t len) {
  if (len <= 3) {
    return false;
  }
  if (strcmp(&pVal[len - 3], "f64") == 0) {
    printf("Type is double(%s)\n", pVal);
    return true;
  }
  return false;
}

bool is_valid_integer(char *str) {
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

bool is_valid_float(char *str) {
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

bool taos_sml_timestamp_convert(TAOS_SML_KV *pVal, char *value,
                                uint16_t len) {
  if (is_timestamp(value, len)) {
    pVal->type = TSDB_DATA_TYPE_TIMESTAMP;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->value = calloc(pVal->length, 1);
    int64_t val = (int64_t)strtoll(value, NULL, 10);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  return false;
}
//len does not include '\0' from value.
bool taos_sml_type_convert(TAOS_SML_KV *pVal, char *value,
                           uint16_t len) {
  if (len <= 0) {
    return false;
  }
  //bool
  bool b_val;
  if (is_bool(value, len, &b_val)) {
    pVal->type = TSDB_DATA_TYPE_BOOL;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    pVal->value = calloc(pVal->length, 1);
    memcpy(pVal->value, &b_val, pVal->length);
    return true;
  }
  //binary
  if (is_binary(value, len)) {
    pVal->type = TSDB_DATA_TYPE_BINARY;
    pVal->length = len - 2;
    pVal->value = calloc(pVal->length, 1);
    //copy after "
    memcpy(pVal->value, value + 1, pVal->length);
    return true;
  }
  //nchar
  if (is_nchar(value, len)) {
    pVal->type = TSDB_DATA_TYPE_NCHAR;
    pVal->length = len - 3;
    pVal->value = calloc(pVal->length, 1);
    //copy after L"
    memcpy(pVal->value, value + 2, pVal->length);
    return true;
  }
  //floating number
  if (is_float(value, len)) {
    pVal->type = TSDB_DATA_TYPE_FLOAT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!is_valid_float(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    float val = (float)strtold(value, NULL);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  if (is_double(value, len)) {
    pVal->type = TSDB_DATA_TYPE_DOUBLE;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!is_valid_float(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    double val = (double)strtold(value, NULL);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  //integer number
  if (is_tiny_int(value, len)) {
    pVal->type = TSDB_DATA_TYPE_TINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 2] = '\0';
    if (!is_valid_integer(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    int8_t val = (int8_t)strtoll(value, NULL, 10);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  if (is_tiny_uint(value, len)) {
    pVal->type = TSDB_DATA_TYPE_UTINYINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 2] = '\0';
    if (!is_valid_integer(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    uint8_t val = (uint8_t)strtoul(value, NULL, 10);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  if (is_small_int(value, len)) {
    pVal->type = TSDB_DATA_TYPE_SMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!is_valid_integer(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    int16_t val = (int16_t)strtoll(value, NULL, 10);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  if (is_small_uint(value, len)) {
    pVal->type = TSDB_DATA_TYPE_USMALLINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!is_valid_integer(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    uint16_t val = (uint16_t)strtoul(value, NULL, 10);
    memcpy(pVal->value, &val, pVal->length);
    //memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  if (is_int(value, len)) {
    pVal->type = TSDB_DATA_TYPE_INT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!is_valid_integer(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    int32_t val = (int32_t)strtoll(value, NULL, 10);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  if (is_uint(value, len)) {
    pVal->type = TSDB_DATA_TYPE_UINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!is_valid_integer(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    uint32_t val = (uint32_t)strtoul(value, NULL, 10);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  if (is_big_int(value, len)) {
    pVal->type = TSDB_DATA_TYPE_BIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!is_valid_integer(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    int64_t val = (int64_t)strtoll(value, NULL, 10);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  if (is_big_uint(value, len)) {
    pVal->type = TSDB_DATA_TYPE_UBIGINT;
    pVal->length = (int16_t)tDataTypes[pVal->type].bytes;
    value[len - 3] = '\0';
    if (!is_valid_integer(value)) {
      return false;
    }
    pVal->value = calloc(pVal->length, 1);
    uint64_t val = (uint64_t)strtoul(value, NULL, 10);
    memcpy(pVal->value, &val, pVal->length);
    return true;
  }
  //TODO: handle default is float here
  return  false;
}

/*        Field                          Escape charaters
    1: measurement                        Comma,Space
    2: tag_key, tag_value, field_key  Comma,Equal Sign,Space
    3: field_value                    Double quote,Backslash
*/
void escape_special_char(uint8_t field, const char **pos) {
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

bool taos_sml_parse_measurement(TAOS_SML_DATA_POINT *pSml, const char **index, uint8_t *has_tags) {
  const char *cur = *index;
  uint16_t len = 0;

  pSml->stableName = calloc(TSDB_TABLE_NAME_LEN, 1);
  if (*cur == '_') {
    printf("Measurement field cannnot start with \'_\'\n");
    return false;
  }

  while (*cur != '\0') {
    if (len > TSDB_TABLE_NAME_LEN) {
      printf("Measurement field cannot exceeds 193 characters");
      return false;
    }
    //first unescaped comma or space identifies measurement
    //if space detected first, meaning no tag in the input
    if (*cur == ',' && *(cur - 1) != '\\') {
      *has_tags = 1;
      printf("measurement:found comma\n");
      break;
    }
    if (*cur == ' ' && *(cur - 1) != '\\') {
      printf("measurement:found space\n");
      break;
    }
    //Comma, Space, Backslash needs to be escaped if any
    if (*cur == '\\') {
      escape_special_char(1, &cur);
    }
    pSml->stableName[len] = *cur;
    cur++;
    len++;
  }
  pSml->stableName[len] = '\0';
  *index = cur + 1;
  printf("stable name:%s|len:%d\n", pSml->stableName, len);

  return true;
}


bool taos_sml_parse_key(TAOS_SML_KV *pKV, const char **index) {
  const char *cur = *index;
  char key[TSDB_COL_NAME_LEN];
  uint16_t len = 0;

  //key field cannot start with '_'
  if (*cur == '_') {
    printf("Tag key cannnot start with \'_\'\n");
    return false;
  }
  //TODO: If tag key has ID field, use corresponding
  //tag value as child table name
  while (*cur != '\0') {
    if (len > TSDB_COL_NAME_LEN) {
      printf("Key field cannot exceeds 65 characters");
      return false;
    }
    //unescaped '=' identifies a tag key
    if (*cur == '=' && *(cur - 1) != '\\') {
      printf("key: found equal sign\n");
      break;
    }
    //Escape special character
    if (*cur == '\\') {
      escape_special_char(2, &cur);
    }
    key[len] = *cur;
    cur++;
    len++;
  }
  key[len] = '\0';

  pKV->key = calloc(len + 1, 1);
  memcpy(pKV->key, key, len + 1);
  printf("key:%s|len:%d\n", pKV->key, len);
  *index = cur + 1;
  return true;
}

bool taos_sml_parse_value(TAOS_SML_KV *pKV, const char **index,
                          bool *is_last_kv) {
  const char *start, *cur;
  char *value = NULL;
  uint16_t len = 0;
  start = cur = *index;

  while (1) {
    // unescaped ',' or ' ' or '\0' identifies a value
    if ((*cur == ',' || *cur == ' ' || *cur == '\0') && *(cur - 1) != '\\') {
      value = calloc(len + 1, 1);
      memcpy(value, start, len);
      value[len] = '\0';
      if (!taos_sml_type_convert(pKV, value, len)) {
        free(value);
        return false;
      }
      //unescaped ' ' or '\0' indicates end of value
      *is_last_kv = (*cur == ' ' || *cur == '\0') ? true : false;
      break;
    }
    //Escape special character
    if (*cur == '\\') {
      escape_special_char(2, &cur);
    }
    cur++;
    len++;
  }

  if (value) {
    free(value);
  }

  *index = (*cur == '\0') ? cur : cur + 1;
  return true;
}

bool taos_sml_parse_kv_pairs(TAOS_SML_KV **pKVs, int *num_kvs, const char **index, bool isField) {
  const char *cur = *index;
  TAOS_SML_KV *pkv;
  bool is_last_kv = false;

  if (isField) {
    //leave space for timestamp
    *pKVs = calloc(2, sizeof(TAOS_SML_KV));
    pkv = *pKVs;
    pkv++;
  }
  else {
    *pKVs = calloc(1, sizeof(TAOS_SML_KV));
    pkv = *pKVs;
  }

  while (*cur != '\0') {
    if (!taos_sml_parse_key(pkv, &cur)) {
      printf("Unable to parse key field\n");
      goto error;
    }
    if (!taos_sml_parse_value(pkv, &cur, &is_last_kv)) {
      printf("Unable to parse value field\n");
      goto error;
    }
    *num_kvs += 1;

    if(is_last_kv) {
      printf("last key value field detected\n");
      goto done;
    }

    //reallocate addtional memory for more kvs
    TAOS_SML_KV *more_kvs = NULL;
    if (isField) {
      more_kvs = realloc(*pKVs, (*num_kvs + 2) * sizeof(TAOS_SML_KV));
    } else {
      more_kvs = realloc(*pKVs, (*num_kvs + 1) * sizeof(TAOS_SML_KV));
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
  free(*pKVs);
  return false;
  done:
  *index = cur;
  return true;
}

bool taos_sml_parse_timestamp(TAOS_SML_KV **pTS, const char **index) {
  const char *start, *cur;
  int len = 0;
  char key[] = "_ts";
  char *value = NULL;

  start = cur = *index;
  *pTS = calloc(1, sizeof(TAOS_SML_KV));

  if (*cur == '\0') {
    //no timestamp given, use current system time
    return true;
  }

  while(*cur != '\0') {
    cur++;
    len++;
  }
  value = calloc(len, 1);
  memcpy(value, start, len);
  if (!taos_sml_timestamp_convert(*pTS, value, len)) {
    free(*pTS);
    return false;
  }
  free(value);


  (*pTS)->key = calloc(sizeof(key), 1);
  memcpy((*pTS)->key, key, sizeof(key));
  return true;
}

bool tscParseLine(const char* sql, TAOS_SML_DATA_POINT* sml_data) {
  const char* index = sql;
  uint8_t has_tags = 0;
  TAOS_SML_KV *timestamp = NULL;


  if (!taos_sml_parse_measurement(sml_data, &index, &has_tags)) {
    printf("Unable to parse measurement\n");
    free(sml_data->stableName);
    free(sml_data);
    return false;
  }
  printf("============Parse measurement finished, has_tags:%d===============\n", has_tags);

  //Parse Tags
  if (has_tags) {
    if (!taos_sml_parse_kv_pairs(&sml_data->tags, &sml_data->tagNum, &index, false)) {
      printf("Unable to parse tag\n");
      //TODO free allocated fileds inside TAOS_SML_DATA_POINT first
      return false;
    }
  } else {
    //no tags given
  }

  printf("============Parse tags finished, num_tags:%d===============\n", sml_data->tagNum);
  //Parse fields
  if (!taos_sml_parse_kv_pairs(&sml_data->fields, &sml_data->fieldNum, &index, true)) {
    printf("Unable to parse field\n");
    //TODO free allocated fileds inside TAOS_SML_DATA_POINT first
    return false;
  }
  printf("============Parse fields finished, num_fields:%d===============\n", sml_data->fieldNum);
  //Parse timestamp
  if (!taos_sml_parse_timestamp(&timestamp, &index)) {
    printf("Unable to parse timestamp\n");

    return false;
  }

  sml_data->fieldNum = sml_data->fieldNum + 1;
  TAOS_SML_KV* tsField = sml_data->fields;
  tsField->length = timestamp->length;
  tsField->type = timestamp->type;
  tsField->value = malloc(timestamp->length);
  tsField->key = malloc(strlen(timestamp->key)+1);
  memcpy(tsField->key, timestamp->key, strlen(timestamp->key)+1);
  memcpy(tsField->value, timestamp->value, timestamp->length);

  free(timestamp->key);
  free(timestamp->value);
  free(timestamp);
  printf("============Parse timestamp finished===============\n");

  return true;
}



int32_t tscParseLines(char* lines[], int numLines, SArray* points, SArray* failedLines) {
  for (int32_t i = 0; i < numLines; ++i) {
    TAOS_SML_DATA_POINT point = {0};
    bool succ = tscParseLine(lines[i], &point);
    if (!succ) {
      tscError("data point line parse failed. line %d", i);
      return TSDB_CODE_TSC_LINE_SYNTAX_ERROR;
    } else {
      tscDebug("data point line parse success. line %d", i);
    }

    taosArrayPush(points, &point);
  }
  return 0;
}


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

int taos_insert_lines(TAOS* taos, char* lines[], int numLines) {
  int32_t code = 0;
  SArray* lpPoints = taosArrayInit(numLines, sizeof(TAOS_SML_DATA_POINT));

  code = tscParseLines(lines, numLines, lpPoints, NULL);
  if (code != 0) {
    goto cleanup;
  }

  size_t numPoints = taosArrayGetSize(lpPoints);
  TAOS_SML_DATA_POINT* points = TARRAY_GET_START(lpPoints);
  code = taos_sml_insert(taos, points, (int)numPoints);
  if (code != 0) {
    tscError("taos_sml_insert error: %s", tstrerror((code)));
  }

cleanup:
  for (int i=0; i<numPoints; ++i) {
    destroySmlDataPoint(points+i);
  }

  taosArrayDestroy(lpPoints);
  return code;
}


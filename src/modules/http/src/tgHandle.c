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

#include "tgHandle.h"
#include "shash.h"
#include "taosmsg.h"
#include "tgJson.h"
#include "tsdb.h"

/*
 * taos.telegraf.cfg formats like
 {
  "metrics": [
    {
      "name" : "system",
      "tbname" : "system_uptime",
      "fields": [
        "uptime"
      ]
    },
    {
      "name": "system",
      "tbname" : "system_uptime_format",
      "fields": [
        "uptime_format"
      ]
    },
    {
      "name": "swap",
      "tbname" : "swap_in",
      "fields": [
        "in"
      ]
    },
    {
      "name": "cpu",
      "tbname" : "cpu_usage",
      "fields": [
        "usage_active",
        "usage_guest"
      ]
    }
    ]
  }
 */

#define TG_MAX_SORT_TAG_SIZE 20

static HttpDecodeMethod tgDecodeMethod = {"telegraf", tgProcessRquest};
static HttpEncodeMethod tgQueryMethod = {tgStartQueryJson,         tgStopQueryJson, NULL,
                                         tgBuildSqlAffectRowsJson, tgInitQueryJson, tgCleanQueryJson,
                                         tgCheckFinished,          tgSetNextCmd};

static const char DEFAULT_TELEGRAF_CFG[] =
        "{\"metrics\":["
        "{\"name\":\"system\",\"tbname\":\"system_uptime\",\"fields\":[\"uptime\"]},"
        "{\"name\":\"system\",\"tbname\":\"system_uptime_format\",\"fields\":[\"uptime_format\"]},"
        "{\"name\":\"swap\",\"tbname\":\"swap_in\",\"fields\":[\"in\"]},"
        "{\"name\":\"cpu\",\"tbname\":\"cpu_usage\",\"fields\":[\"usage_guest\"]}"
        "]}";

typedef struct {
  char  *name;
  char  *tbName;
  char **fields;
  int    fieldNum;
} STgSchema;

typedef struct {
  STgSchema *schemas;
  int        size;
  int        pos;
} STgSchemas;

static STgSchemas tgSchemas = {0};

void tgFreeSchema(STgSchema *schema) {
  if (schema->name != NULL) {
    free(schema->name);
    schema->name = NULL;
  }
  if (schema->tbName != NULL) {
    free(schema->tbName);
    schema->tbName = NULL;
  }
  if (schema->fields != NULL) {
    for (int f = 0; f < schema->fieldNum; ++f) {
      if (schema->fields[f] != NULL) {
        free(schema->fields[f]);
        schema->fields[f] = NULL;
      }
    }
    free(schema->fields);
    schema->fields = NULL;
    schema->fieldNum = 0;
  }
}

void tgFreeSchemas() {
  if (tgSchemas.schemas != NULL) {
    for (int s = 0; s < tgSchemas.size; ++s) {
      tgFreeSchema(&tgSchemas.schemas[s]);
    }
    free(tgSchemas.schemas);
    tgSchemas.size = 0;
  }
}

void tgInitSchemas(int size) {
  tgFreeSchemas();
  tgSchemas.schemas = calloc(sizeof(STgSchema), size);
  tgSchemas.size = 0;
}

void tgParseSchemaMetric(cJSON *metric) {
  STgSchema  schema = {0};
  bool       parsedOk = true;

  // name
  cJSON *name = cJSON_GetObjectItem(metric, "name");
  if (name == NULL) {
    parsedOk = false;
    goto ParseEnd;
  }
  if (name->type != cJSON_String) {
    parsedOk = false;
    goto ParseEnd;
  }
  if (name->valuestring == NULL) {
    parsedOk = false;
    goto ParseEnd;
  }
  int nameLen = (int)strlen(name->valuestring);
  if (nameLen == 0) {
    parsedOk = false;
    goto ParseEnd;
  }

  schema.name = calloc(nameLen + 1, 1);
  strcpy(schema.name, name->valuestring);

  // tbname
  cJSON *tbname = cJSON_GetObjectItem(metric, "tbname");
  if (tbname == NULL) {
    parsedOk = false;
    goto ParseEnd;
  }
  if (tbname->type != cJSON_String) {
    parsedOk = false;
    goto ParseEnd;
  }
  if (tbname->valuestring == NULL) {
    parsedOk = false;
    goto ParseEnd;
  }
  int tbnameLen = (int)strlen(tbname->valuestring);
  if (tbnameLen == 0) {
    parsedOk = false;
    goto ParseEnd;
  }

  schema.tbName = calloc(tbnameLen + 1, 1);
  strcpy(schema.tbName, tbname->valuestring);

    // fields
  cJSON *fields = cJSON_GetObjectItem(metric, "fields");
  if (fields == NULL) {
    goto ParseEnd;
  }
  int fieldSize = cJSON_GetArraySize(fields);
  if (fieldSize <= 0 || fieldSize > TSDB_MAX_COLUMNS) {
    goto ParseEnd;
  }

  if (fieldSize > 0) {
    schema.fields = calloc(sizeof(STgSchema), (size_t)fieldSize);
    schema.fieldNum = fieldSize;
    for (int i = 0; i < fieldSize; i++) {
      cJSON *field = cJSON_GetArrayItem(fields, i);
      if (field == NULL) {
        parsedOk = false;
        goto ParseEnd;
      }
      if (field->valuestring == NULL) {
        parsedOk = false;
        goto ParseEnd;
      }
      int nameLen = (int)strlen(field->valuestring);
      if (nameLen == 0 || nameLen > TSDB_METER_NAME_LEN) {
        parsedOk = false;
        goto ParseEnd;
      }
      schema.fields[i] = calloc(nameLen + 1, 1);
      strcpy(schema.fields[i], field->valuestring);
    }
  }

ParseEnd:
  if (parsedOk) {
    tgSchemas.schemas[tgSchemas.size++] = schema;
  } else {
    tgFreeSchema(&schema);
  }
}

int tgParseSchema(const char *content, char*fileName) {
  cJSON *root = cJSON_Parse(content);
  if (root == NULL) {
    httpError("failed to parse telegraf schema file:%s, invalid json format, content:%s", fileName, content);
    return -1;
  }
  int size = 0;
  cJSON *metrics = cJSON_GetObjectItem(root, "metrics");
  if (metrics != NULL) {
    size = cJSON_GetArraySize(metrics);
    if (size <= 0) {
      httpError("failed to parse telegraf schema file:%s, metrics size is 0", fileName);
      cJSON_Delete(root);
      return -1;
    }

    tgInitSchemas(size);
    for (int i = 0; i < size; i++) {
      cJSON *metric = cJSON_GetArrayItem(metrics, i);
      if (metric != NULL) {
        tgParseSchemaMetric(metric);
      }
    }
  } else {
    size = 1;
    tgInitSchemas(size);
    tgParseSchemaMetric(root);
  }

  cJSON_Delete(root);
  return size;
}

int tgReadSchema(const char *fileName) {
  FILE *fp = fopen(fileName, "r");
  if (fp == NULL) {
    return -1;
  }

  httpPrint("open telegraf schema file:%s success", fileName);
  fseek(fp, 0, SEEK_END);
  size_t contentSize = (size_t)ftell(fp);
  rewind(fp);
  char *content = (char *)calloc(contentSize * sizeof(char) + 1, 1);
  size_t result = fread(content, 1, contentSize, fp);
  if (result != contentSize) {
    httpError("failed to read telegraf schema file:%s", fileName);
    return -1;
  }

  int schemaNum = tgParseSchema(content, fileName);

  free(content);
  fclose(fp);
  httpPrint("parse telegraf schema file:%s, schema size:%d", fileName, schemaNum);

  return schemaNum;
}

void tgInitHandle(HttpServer *pServer) {
  char fileName[256] = {0};
  sprintf(fileName, "%s/taos.telegraf.cfg", configDir);
  if (tgReadSchema(fileName) <= 0) {
    tgFreeSchemas();
    if (tgParseSchema(DEFAULT_TELEGRAF_CFG, "default") <= 0) {
      tgFreeSchemas();
    }
  }

  httpAddMethod(pServer, &tgDecodeMethod);
}

bool tgGetUserFromUrl(HttpContext *pContext) {
  HttpParser *pParser = &pContext->parser;
  if (pParser->path[TG_USER_URL_POS].len > TSDB_USER_LEN - 1 || pParser->path[TG_USER_URL_POS].len <= 0) {
    return false;
  }

  strcpy(pContext->user, pParser->path[TG_USER_URL_POS].pos);
  return true;
}

bool tgGetPassFromUrl(HttpContext *pContext) {
  HttpParser *pParser = &pContext->parser;
  if (pParser->path[TG_PASS_URL_POS].len > TSDB_PASSWORD_LEN - 1 || pParser->path[TG_PASS_URL_POS].len <= 0) {
    return false;
  }

  strcpy(pContext->pass, pParser->path[TG_PASS_URL_POS].pos);
  return true;
}

char *tgGetDbFromUrl(HttpContext *pContext) {
  HttpParser *pParser = &pContext->parser;
  if (pParser->path[TG_DB_URL_POS].len <= 0) {
    httpSendErrorResp(pContext, HTTP_TG_DB_NOT_INPUT);
    return NULL;
  }

  if (pParser->path[TG_DB_URL_POS].len >= TSDB_DB_NAME_LEN) {
    httpSendErrorResp(pContext, HTTP_TG_DB_TOO_LONG);
    return NULL;
  }

  return pParser->path[TG_DB_URL_POS].pos;
}

char *tgGetStableName(char *stname, cJSON *fields, int fieldsSize) {
  for (int s = 0; s < tgSchemas.size; ++s) {
    STgSchema *schema = &tgSchemas.schemas[s];
    if (strcasecmp(schema->name, stname) != 0) {
      continue;
    }

    bool schemaMatched = true;
    for (int f = 0; f < schema->fieldNum; ++f) {
      char *fieldName = schema->fields[f];
      bool fieldMatched = false;

      for (int i = 0; i < fieldsSize; i++) {
        cJSON *field = cJSON_GetArrayItem(fields, i);
        if (strcasecmp(field->string, fieldName) == 0) {
          fieldMatched = true;
          break;
        }
      }

      if (!fieldMatched) {
        schemaMatched = false;
        break;
      }
    }

    if (schemaMatched) {
      return schema->tbName;
    }
  }

  return stname;
}

/*
 * parse single metric
 {
   "fields": {
     "field_1": 30,
     "field_2": 4,
     "field_N": 59,
     "n_images": 660
   },
   "name": "docker",
   "tags": {
     "host": "raynor"
   },
   "timestamp": 1458229140
 }
 */
bool tgProcessSingleMetric(HttpContext *pContext, cJSON *metric, char *db) {
  // metric name
  cJSON *name = cJSON_GetObjectItem(metric, "name");
  if (name == NULL) {
    httpSendErrorResp(pContext, HTTP_TG_METRIC_NULL);
    return false;
  }
  if (name->type != cJSON_String) {
    httpSendErrorResp(pContext, HTTP_TG_METRIC_TYPE);
    return false;
  }
  if (name->valuestring == NULL) {
    httpSendErrorResp(pContext, HTTP_TG_METRIC_NAME_NULL);
    return false;
  }
  int nameLen = (int)strlen(name->valuestring);
  if (nameLen == 0) {
    httpSendErrorResp(pContext, HTTP_TG_METRIC_NAME_NULL);
    return false;
  }
  if (nameLen >= TSDB_METER_NAME_LEN - 7) {
    httpSendErrorResp(pContext, HTTP_TG_METRIC_NAME_LONG);
    return false;
  }

  // timestamp
  cJSON *timestamp = cJSON_GetObjectItem(metric, "timestamp");
  if (timestamp == NULL) {
    httpSendErrorResp(pContext, HTTP_TG_TIMESTAMP_NULL);
    return false;
  }
  if (timestamp->type != cJSON_Number) {
    httpSendErrorResp(pContext, HTTP_TG_TIMESTAMP_TYPE);
    return false;
  }
  if (timestamp->valueint <= 0) {
    httpSendErrorResp(pContext, HTTP_TG_TIMESTAMP_VAL_NULL);
    return false;
  }

  // tags
  cJSON *tags = cJSON_GetObjectItem(metric, "tags");
  if (tags == NULL) {
    httpSendErrorResp(pContext, HTTP_TG_TAGS_NULL);
    return false;
  }

  int tagsSize = cJSON_GetArraySize(tags);
  if (tagsSize <= 0) {
    httpSendErrorResp(pContext, HTTP_TG_TAGS_SIZE_0);
    return false;
  }

  if (tagsSize > TG_MAX_SORT_TAG_SIZE) {
    httpSendErrorResp(pContext, HTTP_TG_TAGS_SIZE_LONG);
    return false;
  }

  cJSON *host = NULL;

  for (int i = 0; i < tagsSize; i++) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    if (tag == NULL) {
      httpSendErrorResp(pContext, HTTP_TG_TAG_NULL);
      return false;
    }
    if (tag->string == NULL || strlen(tag->string) == 0) {
      httpSendErrorResp(pContext, HTTP_TG_TAG_NAME_NULL);
      return false;
    }

    /*
    * tag size may be larget than TSDB_COL_NAME_LEN
    * we keep the first TSDB_COL_NAME_LEN bytes
    */
    if (0) {
      if (strlen(tag->string) >= TSDB_COL_NAME_LEN) {
        httpSendErrorResp(pContext, HTTP_TG_TAG_NAME_SIZE);
        return false;
      }
    }

    if (tag->type != cJSON_Number && tag->type != cJSON_String) {
      httpSendErrorResp(pContext, HTTP_TG_TAG_VALUE_TYPE);
      return false;
    }

    if (tag->type == cJSON_String) {
      if (tag->valuestring == NULL || strlen(tag->valuestring) == 0) {
        httpSendErrorResp(pContext, HTTP_TG_TAG_VALUE_NULL);
        return false;
      }
    }

    if (strcasecmp(tag->string, "host") == 0) {
      host = tag;
    }
  }

  if (host == NULL) {
    httpSendErrorResp(pContext, HTTP_TG_TABLE_NULL);
    return false;
  }

  if (host->type != cJSON_String) {
    httpSendErrorResp(pContext, HTTP_TG_HOST_NOT_STRING);
    return false;
  }

  if (strlen(host->valuestring) >= TSDB_METER_NAME_LEN) {
    httpSendErrorResp(pContext, HTTP_TG_TABLE_SIZE);
    return false;
  }

  // fields
  cJSON *fields = cJSON_GetObjectItem(metric, "fields");
  if (fields == NULL) {
    httpSendErrorResp(pContext, HTTP_TG_FIELDS_NULL);
    return false;
  }

  int fieldsSize = cJSON_GetArraySize(fields);
  if (fieldsSize <= 0) {
    httpSendErrorResp(pContext, HTTP_TG_FIELDS_SIZE_0);
    return false;
  }

  if (fieldsSize > (TSDB_MAX_COLUMNS - TSDB_MAX_TAGS - 1)) {
    httpSendErrorResp(pContext, HTTP_TG_FIELDS_SIZE_LONG);
    return false;
  }

  for (int i = 0; i < fieldsSize; i++) {
    cJSON *field = cJSON_GetArrayItem(fields, i);
    if (field == NULL) {
      httpSendErrorResp(pContext, HTTP_TG_FIELD_NULL);
      return false;
    }
    if (field->string == NULL || strlen(field->string) == 0) {
      httpSendErrorResp(pContext, HTTP_TG_FIELD_NAME_NULL);
      return false;
    }
    /*
    * tag size may be larget than TSDB_COL_NAME_LEN
    * we keep the first TSDB_COL_NAME_LEN bytes
    */
    if (0) {
      if (strlen(field->string) >= TSDB_COL_NAME_LEN) {
        httpSendErrorResp(pContext, HTTP_TG_FIELD_NAME_SIZE);
        return false;
      }
    }
    if (field->type != cJSON_Number && field->type != cJSON_String) {
      httpSendErrorResp(pContext, HTTP_TG_FIELD_VALUE_TYPE);
      return false;
    }
    if (field->type == cJSON_String) {
      if (field->valuestring == NULL || strlen(field->valuestring) == 0) {
        httpSendErrorResp(pContext, HTTP_TG_FIELD_VALUE_NULL);
        return false;
      }
    }
  }

  // assembling cmds
  HttpSqlCmd *stable_cmd = httpNewSqlCmd(pContext);
  if (stable_cmd == NULL) {
    httpSendErrorResp(pContext, HTTP_NO_ENOUGH_MEMORY);
    return false;
  }
  stable_cmd->cmdType = HTTP_CMD_TYPE_CREATE_STBALE;
  stable_cmd->cmdReturnType = HTTP_CMD_RETURN_TYPE_NO_RETURN;

  HttpSqlCmd *table_cmd = httpNewSqlCmd(pContext);
  if (table_cmd == NULL) {
    httpSendErrorResp(pContext, HTTP_NO_ENOUGH_MEMORY);
    return false;
  }
  table_cmd->cmdType = HTTP_CMD_TYPE_INSERT;

  // order by tag name
  cJSON *orderedTags[TG_MAX_SORT_TAG_SIZE] = {0};
  int    orderTagsLen = 0;
  for (int i = 0; i < tagsSize; ++i) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    orderedTags[orderTagsLen++] = tag;
    for (int j = orderTagsLen - 1; j >= 1; --j) {
      cJSON *tag1 = orderedTags[j];
      cJSON *tag2 = orderedTags[j - 1];
      if (strcasecmp(tag1->string, "host") == 0 || strcmp(tag1->string, tag2->string) < 0) {
        orderedTags[j] = tag2;
        orderedTags[j - 1] = tag1;
      }
    }
  }
  orderTagsLen = orderTagsLen < TSDB_MAX_TAGS ? orderTagsLen : TSDB_MAX_TAGS;

  table_cmd->tagNum = stable_cmd->tagNum = (int8_t)orderTagsLen;
  table_cmd->timestamp = stable_cmd->timestamp = httpAddToSqlCmdBuffer(pContext, "%ld", timestamp->valueint);

  // stable name
  char *stname = tgGetStableName(name->valuestring, fields, fieldsSize);
  table_cmd->metric = stable_cmd->metric = httpAddToSqlCmdBuffer(pContext, "%s", stname);
  if (tsTelegrafUseFieldNum == 0) {
    table_cmd->stable = stable_cmd->stable = httpAddToSqlCmdBuffer(pContext, "%s", stname);
  } else {
    table_cmd->stable = stable_cmd->stable = httpAddToSqlCmdBuffer(pContext, "%s_%d_%d", stname, fieldsSize, orderTagsLen);
  }
  table_cmd->stable = stable_cmd->stable =
      httpShrinkTableName(pContext, table_cmd->stable, httpGetCmdsString(pContext, table_cmd->stable));

  // stable tag for detail
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    stable_cmd->tagNames[i] = table_cmd->tagNames[i] = httpAddToSqlCmdBuffer(pContext, tag->string);

    if (tag->type == cJSON_String)
      stable_cmd->tagValues[i] = table_cmd->tagValues[i] = httpAddToSqlCmdBuffer(pContext, "'%s'", tag->valuestring);
    else if (tag->type == cJSON_Number)
      stable_cmd->tagValues[i] = table_cmd->tagValues[i] = httpAddToSqlCmdBuffer(pContext, "%ld", tag->valueint);
    else if (tag->type == cJSON_True)
      stable_cmd->tagValues[i] = table_cmd->tagValues[i] = httpAddToSqlCmdBuffer(pContext, "1");
    else if (tag->type == cJSON_False)
      stable_cmd->tagValues[i] = table_cmd->tagValues[i] = httpAddToSqlCmdBuffer(pContext, "0");
    else
      stable_cmd->tagValues[i] = table_cmd->tagValues[i] = httpAddToSqlCmdBuffer(pContext, "NULL");
  }

  // table name
  if (tsTelegrafUseFieldNum == 0) {
    table_cmd->table = stable_cmd->table = httpAddToSqlCmdBufferNoTerminal(pContext, "%s_%s", stname, host->valuestring);
  } else {
    table_cmd->table = stable_cmd->table = httpAddToSqlCmdBufferNoTerminal(pContext, "%s_%d_%d_%s", stname, fieldsSize, orderTagsLen, host->valuestring);
  }
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    if (tag == host) continue;
    if (tag->type == cJSON_String)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_%s", tag->valuestring);
    else if (tag->type == cJSON_Number)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_%ld", tag->valueint);
    else if (tag->type == cJSON_False)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_0");
    else if (tag->type == cJSON_True)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_1");
    else
      httpAddToSqlCmdBufferNoTerminal(pContext, "_n");
  }
  httpAddToSqlCmdBuffer(pContext, "");

  table_cmd->table = stable_cmd->table =
      httpShrinkTableName(pContext, table_cmd->table, httpGetCmdsString(pContext, table_cmd->table));

  // assembling create stable sql
  stable_cmd->sql = httpAddToSqlCmdBufferNoTerminal(pContext, "create table if not exists %s.%s(ts timestamp", db,
                                                    httpGetCmdsString(pContext, table_cmd->stable));
  for (int i = 0; i < fieldsSize; ++i) {
    cJSON *field = cJSON_GetArrayItem(fields, i);
    char * field_type = "double";
    if (field->type == cJSON_String)
      field_type = "binary(32)";
    else if (field->type == cJSON_False || field->type == cJSON_True)
      field_type = "tinyint";
    else {
    }

    char *field_name = field->string;
    httpAddToSqlCmdBufferNoTerminal(pContext, ",f_%s %s", field_name, field_type);
  }
  httpAddToSqlCmdBufferNoTerminal(pContext, ") tags(");

  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    char * tag_type = "bigint";
    if (tag->type == cJSON_String)
      tag_type = "binary(32)";
    else if (tag->type == cJSON_False || tag->type == cJSON_True)
      tag_type = "tinyint";
    else {
    }

    char *tag_name = tag->string;
    if (i != orderTagsLen - 1)
      httpAddToSqlCmdBufferNoTerminal(pContext, "t_%s %s,", tag_name, tag_type);
    else
      httpAddToSqlCmdBuffer(pContext, "t_%s %s)", tag_name, tag_type);
  }

  // assembling insert sql
  table_cmd->sql = httpAddToSqlCmdBufferNoTerminal(pContext, "import into %s.%s using %s.%s tags(", db,
                                                   httpGetCmdsString(pContext, table_cmd->table), db,
                                                   httpGetCmdsString(pContext, table_cmd->stable));
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    if (i != orderTagsLen - 1) {
      if (tag->type == cJSON_Number)
        httpAddToSqlCmdBufferNoTerminal(pContext, "%ld,", tag->valueint);
      else if (tag->type == cJSON_String)
        httpAddToSqlCmdBufferNoTerminal(pContext, "'%s',", tag->valuestring);
      else if (tag->type == cJSON_False)
        httpAddToSqlCmdBufferNoTerminal(pContext, "0,");
      else if (tag->type == cJSON_True)
        httpAddToSqlCmdBufferNoTerminal(pContext, "1,");
      else {
        httpAddToSqlCmdBufferNoTerminal(pContext, "NULL,");
      }
    } else {
      if (tag->type == cJSON_Number)
        httpAddToSqlCmdBufferNoTerminal(pContext, "%ld)", tag->valueint);
      else if (tag->type == cJSON_String)
        httpAddToSqlCmdBufferNoTerminal(pContext, "'%s')", tag->valuestring);
      else if (tag->type == cJSON_False)
        httpAddToSqlCmdBufferNoTerminal(pContext, "0)");
      else if (tag->type == cJSON_True)
        httpAddToSqlCmdBufferNoTerminal(pContext, "1)");
      else {
        httpAddToSqlCmdBufferNoTerminal(pContext, "NULL)");
      }
    }
  }

  httpAddToSqlCmdBufferNoTerminal(pContext, " values(%ld,", timestamp->valueint);
  for (int i = 0; i < fieldsSize; ++i) {
    cJSON *field = cJSON_GetArrayItem(fields, i);
    if (i != fieldsSize - 1) {
      if (field->type == cJSON_Number)
        httpAddToSqlCmdBufferNoTerminal(pContext, "%lf,", field->valuedouble);
      else if (field->type == cJSON_String)
        httpAddToSqlCmdBufferNoTerminal(pContext, "'%s',", field->valuestring);
      else if (field->type == cJSON_False)
        httpAddToSqlCmdBufferNoTerminal(pContext, "0,");
      else if (field->type == cJSON_True)
        httpAddToSqlCmdBufferNoTerminal(pContext, "1,");
      else {
        httpAddToSqlCmdBufferNoTerminal(pContext, "NULL,");
      }
    } else {
      if (field->type == cJSON_Number)
        httpAddToSqlCmdBuffer(pContext, "%lf)", field->valuedouble);
      else if (field->type == cJSON_String)
        httpAddToSqlCmdBuffer(pContext, "'%s')", field->valuestring);
      else if (field->type == cJSON_False)
        httpAddToSqlCmdBuffer(pContext, "0)");
      else if (field->type == cJSON_True)
        httpAddToSqlCmdBuffer(pContext, "1)");
      else {
        httpAddToSqlCmdBuffer(pContext, "NULL)");
      }
    }
  }

  return true;
}

/**
 * request from telegraf 1.7.0
 * single request:
 {
    "fields": {
        "field_1": 30,
        "field_2": 4,
        "field_N": 59,
        "n_images": 660
    },
    "name": "docker",
    "tags": {
        "host": "raynor"
    },
    "timestamp": 1458229140
 }
 * multiple request:
 {
    "metrics": [
        {
            "fields": {
                "field_1": 30,
                "field_2": 4,
                "field_N": 59,
                "n_images": 660
            },
            "name": "docker",
            "tags": {
                "host": "raynor"
            },
            "timestamp": 1458229140
        },
        {
            "fields": {
                "field_1": 30,
                "field_2": 4,
                "field_N": 59,
                "n_images": 660
            },
            "name": "docker",
            "tags": {
                "host": "raynor"
            },orderTagsLen
            "timestamp": 1458229140
        }
    ]
 }
 */
bool tgProcessQueryRequest(HttpContext *pContext, char *db) {
  httpTrace("context:%p, fd:%d, ip:%s, process telegraf query msg", pContext, pContext->fd, pContext->ipstr);

  HttpParser *pParser = &pContext->parser;
  char *      filter = pParser->data.pos;
  if (filter == NULL) {
    httpSendErrorResp(pContext, HTTP_NO_MSG_INPUT);
    return false;
  }

  cJSON *root = cJSON_Parse(filter);
  if (root == NULL) {
    httpSendErrorResp(pContext, HTTP_TG_INVALID_JSON);
    return false;
  }

  cJSON *metrics = cJSON_GetObjectItem(root, "metrics");
  if (metrics != NULL) {
    int size = cJSON_GetArraySize(metrics);
    httpTrace("context:%p, fd:%d, ip:%s, multiple metrics:%d at one time", pContext, pContext->fd, pContext->ipstr,
              size);
    if (size <= 0) {
      httpSendErrorResp(pContext, HTTP_TG_METRICS_NULL);
      cJSON_Delete(root);
      return false;
    }

    int cmdSize = size * 2 + 1;
    if (cmdSize > HTTP_MAX_CMD_SIZE) {
      httpSendErrorResp(pContext, HTTP_TG_METRICS_SIZE);
      cJSON_Delete(root);
      return false;
    }

    if (!httpMallocMultiCmds(pContext, cmdSize, HTTP_BUFFER_SIZE)) {
      httpSendErrorResp(pContext, HTTP_NO_ENOUGH_MEMORY);
      cJSON_Delete(root);
      return false;
    }

    HttpSqlCmd *cmd = httpNewSqlCmd(pContext);
    if (cmd == NULL) {
      httpSendErrorResp(pContext, HTTP_NO_ENOUGH_MEMORY);
      cJSON_Delete(root);
      return false;
    }
    cmd->cmdType = HTTP_CMD_TYPE_CREATE_DB;
    cmd->cmdReturnType = HTTP_CMD_RETURN_TYPE_NO_RETURN;
    cmd->sql = httpAddToSqlCmdBuffer(pContext, "create database if not exists %s", db);

    for (int i = 0; i < size; i++) {
      cJSON *metric = cJSON_GetArrayItem(metrics, i);
      if (metric != NULL) {
        if (!tgProcessSingleMetric(pContext, metric, db)) {
          cJSON_Delete(root);
          return false;
        }
      }
    }
  } else {
    httpTrace("context:%p, fd:%d, ip:%s, single metric", pContext, pContext->fd, pContext->ipstr);

    if (!httpMallocMultiCmds(pContext, 3, HTTP_BUFFER_SIZE)) {
      httpSendErrorResp(pContext, HTTP_NO_ENOUGH_MEMORY);
      cJSON_Delete(root);
      return false;
    }

    HttpSqlCmd *cmd = httpNewSqlCmd(pContext);
    if (cmd == NULL) {
      httpSendErrorResp(pContext, HTTP_NO_ENOUGH_MEMORY);
      cJSON_Delete(root);
      return false;
    }
    cmd->cmdType = HTTP_CMD_TYPE_CREATE_DB;
    cmd->cmdReturnType = HTTP_CMD_RETURN_TYPE_NO_RETURN;
    cmd->sql = httpAddToSqlCmdBuffer(pContext, "create database if not exists %s", db);

    if (!tgProcessSingleMetric(pContext, root, db)) {
      cJSON_Delete(root);
      return false;
    }
  }

  cJSON_Delete(root);

  pContext->reqType = HTTP_REQTYPE_MULTI_SQL;
  pContext->encodeMethod = &tgQueryMethod;
  pContext->multiCmds->pos = 2;

  return true;
}

bool tgProcessRquest(struct HttpContext *pContext) {
  if (strlen(pContext->user) == 0 || strlen(pContext->pass) == 0) {
    httpSendErrorResp(pContext, HTTP_PARSE_USR_ERROR);
    return false;
  }

  char *db = tgGetDbFromUrl(pContext);
  if (db == NULL) {
    return false;
  }

  return tgProcessQueryRequest(pContext, db);
}

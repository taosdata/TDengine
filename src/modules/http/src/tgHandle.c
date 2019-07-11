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

#define TG_MAX_SORT_TAG_SIZE 20

static HttpDecodeMethod tgDecodeMethod = {"telegraf", tgProcessRquest};
static HttpEncodeMethod tgQueryMethod = {tgStartQueryJson,         tgStopQueryJson, NULL,
                                         tgBuildSqlAffectRowsJson, tgInitQueryJson, tgCleanQueryJson,
                                         tgCheckFinished,          tgSetNextCmd};

typedef struct {
  char *tagName;
  char *tagAlias;
  char *tagType;
} STgTag;

typedef struct {
  char *fieldName;
  char *fieldAlias;
  char *fieldType;
} STgField;

typedef struct {
  char *    stName;
  char *    stAlias;
  STgTag *  tags;
  STgField *fields;
  int16_t   tagNum;
  int16_t   fieldNum;
  char *    createSTableStr;
} STgStable;

/*
 * hash of STgStable
 */
static void *tgSchemaHash = NULL;

/*
 * formats like
 * behind the midline is an alias of field/tag/stable
  {
  "metrics": [{
    "name": "win_cpu-cpu",
    "fields": {
      "Percent_DPC_Time": "float",
      "Percent_Idle_Time": "float",
      "Percent_Interrupt_Time": "float",
      "Percent_Privileged_Time": "float",
      "Percent_Processor_Time": "float",
      "Percent_User_Time": "float"
    },
    "tags": {
      "host": "binary(32)",
      "instance": "binary(32)",
      "objectname": "binary(32)"
    }
  },
  {
    "fields": {
      "Bytes_Received_persec-f1": "float",
      "Bytes_Sent_persec-f2": "float",
      "Packets_Outbound_Discarded-f3": "float",
      "Packets_Outbound_Errors-f4": "float",
      "Packets_Received_Discarded-f5": "float",
      "Packets_Received_Errors": "float",
      "Packets_Received_persec": "float",
      "Packets_Sent_persec": "float"
    },
    "name": "win_net",
    "tags": {
      "host": "binary(32)",
      "instance": "binary(32)",
      "objectname": "binary(32)"
    },
    "timestamp": 1536219762000
  }]
  }
 */
void tgReadSchemaMetric(cJSON *metric) {
  STgStable stable = {0};
  int       createSTableStrLen = 100;
  bool      parsedOk = true;

  // stable name
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
  int aliasPos = -1;
  for (int i = 0; i < nameLen - 1; ++i) {
    if (name->valuestring[i] == '-') {
      aliasPos = i;
      break;
    }
  }
  if (aliasPos == -1) {
    stable.stName = stable.stAlias = calloc((size_t)nameLen + 1, 1);
    strcpy(stable.stName, name->valuestring);
    createSTableStrLen += nameLen;
  } else {
    stable.stName = calloc((size_t)aliasPos + 1, 1);
    stable.stAlias = calloc((size_t)(nameLen - aliasPos), 1);
    strncpy(stable.stName, name->valuestring, (size_t)aliasPos);
    strncpy(stable.stAlias, name->valuestring + aliasPos + 1, (size_t)(nameLen - aliasPos - 1));
    createSTableStrLen += (nameLen - aliasPos);
  }

  // tags
  cJSON *tags = cJSON_GetObjectItem(metric, "tags");
  if (tags == NULL) {
    parsedOk = false;
    goto ParseEnd;
  }
  int tagsSize = cJSON_GetArraySize(tags);
  if (tagsSize <= 0 || tagsSize > TSDB_MAX_TAGS) {
    parsedOk = false;
    goto ParseEnd;
  }
  stable.tags = calloc(sizeof(STgTag), (size_t)tagsSize);
  stable.tagNum = (int16_t)tagsSize;
  for (int i = 0; i < tagsSize; i++) {
    STgTag *tagSchema = &stable.tags[i];
    cJSON * tag = cJSON_GetArrayItem(tags, i);
    if (tag == NULL) {
      parsedOk = false;
      goto ParseEnd;
    }
    if (tag->string == NULL) {
      parsedOk = false;
      goto ParseEnd;
    }
    int nameLen = (int)strlen(tag->string);
    if (nameLen == 0 || nameLen > TSDB_METER_NAME_LEN) {
      parsedOk = false;
      goto ParseEnd;
    }
    int aliasPos = -1;
    for (int i = 0; i < nameLen - 1; ++i) {
      if (tag->string[i] == '-') {
        aliasPos = i;
        break;
      }
    }
    if (aliasPos == -1) {
      tagSchema->tagName = calloc((size_t)nameLen + 1, 1);
      strcpy(tagSchema->tagName, tag->string);
      tagSchema->tagAlias = calloc((size_t)nameLen + 3, 1);
      strcpy(tagSchema->tagAlias, "t_");
      strcpy(tagSchema->tagAlias + 2, tag->string);
      createSTableStrLen += (nameLen + 4);
    } else {
      tagSchema->tagName = calloc((size_t)aliasPos + 1, 1);
      tagSchema->tagAlias = calloc((size_t)(nameLen - aliasPos), 1);
      strncpy(tagSchema->tagName, tag->string, (size_t)aliasPos);
      strncpy(tagSchema->tagAlias, tag->string + aliasPos + 1, (size_t)(nameLen - aliasPos - 1));
      createSTableStrLen += (nameLen - aliasPos + 2);
    }

    if (tag->type == cJSON_String) {
      if (tag->valuestring == NULL) {
        parsedOk = false;
        goto ParseEnd;
      }
      int valueLen = (int)strlen(tag->valuestring);
      if (valueLen == 0) {
        parsedOk = false;
        goto ParseEnd;
      }
      if (strcasecmp(tag->valuestring, "timestamp") == 0 || strcasecmp(tag->valuestring, "bool") == 0 ||
          strcasecmp(tag->valuestring, "tinyint") == 0 || strcasecmp(tag->valuestring, "smallint") == 0 ||
          strcasecmp(tag->valuestring, "int") == 0 || strcasecmp(tag->valuestring, "bigint") == 0 ||
          strcasecmp(tag->valuestring, "float") == 0 || strcasecmp(tag->valuestring, "double") == 0 ||
          strncasecmp(tag->valuestring, "binary", 6) == 0 || strncasecmp(tag->valuestring, "nchar", 5) == 0) {
        tagSchema->tagType = calloc((size_t)valueLen + 1, 1);
        strcpy(tagSchema->tagType, tag->valuestring);
        createSTableStrLen += valueLen;
      } else {
        tagSchema->tagType = calloc(11, 1);
        strcpy(tagSchema->tagType, "binary(32)");
        createSTableStrLen += 12;
      }
    } else if (tag->type == cJSON_False || tag->type == cJSON_True) {
      tagSchema->tagType = calloc(8, 1);
      strcpy(tagSchema->tagType, "tinyint");
      createSTableStrLen += 10;
    } else {
      tagSchema->tagType = calloc(7, 1);
      strcpy(tagSchema->tagType, "bigint");
      createSTableStrLen += 9;
    }
  }

  // fields
  cJSON *fields = cJSON_GetObjectItem(metric, "fields");
  if (fields == NULL) {
    parsedOk = false;
    goto ParseEnd;
  }
  int fieldSize = cJSON_GetArraySize(fields);
  if (fieldSize <= 0 || fieldSize > TSDB_MAX_COLUMNS) {
    parsedOk = false;
    goto ParseEnd;
  }
  stable.fields = calloc(sizeof(STgField), (size_t)fieldSize);
  stable.fieldNum = (int16_t)fieldSize;
  for (int i = 0; i < fieldSize; i++) {
    STgField *fieldSchema = &stable.fields[i];
    cJSON *   field = cJSON_GetArrayItem(fields, i);
    if (field == NULL) {
      parsedOk = false;
      goto ParseEnd;
    }
    if (field->string == NULL) {
      parsedOk = false;
      goto ParseEnd;
    }
    int nameLen = (int)strlen(field->string);
    if (nameLen == 0 || nameLen > TSDB_METER_NAME_LEN) {
      parsedOk = false;
      goto ParseEnd;
    }
    int aliasPos = -1;
    for (int i = 0; i < nameLen - 1; ++i) {
      if (field->string[i] == '-') {
        aliasPos = i;
        break;
      }
    }
    if (aliasPos == -1) {
      fieldSchema->fieldName = calloc((size_t)nameLen + 1, 1);
      strcpy(fieldSchema->fieldName, field->string);
      fieldSchema->fieldAlias = calloc((size_t)nameLen + 3, 1);
      strcpy(fieldSchema->fieldAlias, "f_");
      strcpy(fieldSchema->fieldAlias + 2, field->string);
      createSTableStrLen += (nameLen + 4);
    } else {
      fieldSchema->fieldName = calloc((size_t)aliasPos + 1, 1);
      fieldSchema->fieldAlias = calloc((size_t)(nameLen - aliasPos), 1);
      strncpy(fieldSchema->fieldName, field->string, (size_t)aliasPos);
      strncpy(fieldSchema->fieldAlias, field->string + aliasPos + 1, (size_t)(nameLen - aliasPos - 1));
      createSTableStrLen += (nameLen - aliasPos + 2);
    }

    if (field->type == cJSON_String) {
      if (field->valuestring == NULL) {
        parsedOk = false;
        goto ParseEnd;
      }
      int valueLen = (int)strlen(field->valuestring);
      if (valueLen == 0) {
        parsedOk = false;
        goto ParseEnd;
      }
      if (strcasecmp(field->valuestring, "timestamp") == 0 || strcasecmp(field->valuestring, "bool") == 0 ||
          strcasecmp(field->valuestring, "tinyint") == 0 || strcasecmp(field->valuestring, "smallint") == 0 ||
          strcasecmp(field->valuestring, "int") == 0 || strcasecmp(field->valuestring, "bigint") == 0 ||
          strcasecmp(field->valuestring, "float") == 0 || strcasecmp(field->valuestring, "double") == 0 ||
          strncasecmp(field->valuestring, "binary", 6) == 0 || strncasecmp(field->valuestring, "nchar", 5) == 0) {
        fieldSchema->fieldType = calloc((size_t)valueLen + 1, 1);
        strcpy(fieldSchema->fieldType, field->valuestring);
        createSTableStrLen += valueLen;
      } else {
        fieldSchema->fieldType = calloc(11, 1);
        strcpy(fieldSchema->fieldType, "binary(32)");
        createSTableStrLen += 12;
      }
    } else if (field->type == cJSON_False || field->type == cJSON_True) {
      fieldSchema->fieldType = calloc(8, 1);
      strcpy(fieldSchema->fieldType, "tinyint");
      createSTableStrLen += 10;
    } else {
      fieldSchema->fieldType = calloc(7, 1);
      strcpy(fieldSchema->fieldType, "double");
      createSTableStrLen += 9;
    }
  }

  // assembling create stable sql
  stable.createSTableStr = calloc((size_t)createSTableStrLen, 1);
  strcpy(stable.createSTableStr, "create table if not exists %s.%s(ts timestamp");
  int len = (int)strlen(stable.createSTableStr);
  for (int i = 0; i < stable.fieldNum; ++i) {
    STgField *field = &stable.fields[i];
    len += sprintf(stable.createSTableStr + len, ",%s %s", field->fieldAlias, field->fieldType);
  }
  len += sprintf(stable.createSTableStr + len, ") tags(");
  for (int i = 0; i < stable.tagNum; ++i) {
    STgTag *tag = &stable.tags[i];
    if (i == 0) {
      len += sprintf(stable.createSTableStr + len, "%s %s", tag->tagAlias, tag->tagType);
    } else {
      len += sprintf(stable.createSTableStr + len, ",%s %s", tag->tagAlias, tag->tagType);
    }
  }
  sprintf(stable.createSTableStr + len, ")");

ParseEnd:

  if (parsedOk) {
    taosAddStrHash(tgSchemaHash, stable.stName, (char *)(&stable));
  } else {
    if (stable.stName != NULL) {
      free(stable.stName);
    }
    if (stable.stAlias != NULL) {
      free(stable.stName);
    }
    for (int i = 0; i < stable.tagNum; ++i) {
      if (stable.tags[i].tagName != NULL) {
        free(stable.tags[i].tagName);
      }
      if (stable.tags[i].tagAlias != NULL) {
        free(stable.tags[i].tagAlias);
      }
      if (stable.tags[i].tagType != NULL) {
        free(stable.tags[i].tagType);
      }
    }
    if (stable.tags != NULL) {
      free(stable.tags);
    }
    for (int i = 0; i < stable.fieldNum; ++i) {
      if (stable.fields[i].fieldName != NULL) {
        free(stable.fields[i].fieldName);
      }
      if (stable.fields[i].fieldAlias != NULL) {
        free(stable.fields[i].fieldAlias);
      }
      if (stable.fields[i].fieldType != NULL) {
        free(stable.fields[i].fieldType);
      }
    }
    if (stable.fields != NULL) {
      free(stable.fields);
    }
    if (stable.createSTableStr != NULL) {
      free(stable.createSTableStr);
    }
  }
}

int tgReadSchema(const char *fileName) {
  FILE *fp = fopen(fileName, "r");
  if (fp == NULL) {
    httpPrint("failed to open telegraf schema config file:%s, use default schema", fileName);
    return -1;
  }
  httpPrint("open telegraf schema config file:%s successfully", fileName);

  fseek(fp, 0, SEEK_END);
  size_t contentSize = (size_t)ftell(fp);
  rewind(fp);
  char * content = (char *)calloc(contentSize * sizeof(char) + 1, 1);
  size_t result = fread(content, 1, contentSize, fp);
  if (result != contentSize) {
    httpError("failed to read telegraf schema config file:%s, use default schema", fileName);
    return -1;
  }

  cJSON *root = cJSON_Parse(content);
  if (root == NULL) {
    httpError("failed to parse telegraf schema config file:%s, invalid json format", fileName);
    return -1;
  }

  cJSON *metrics = cJSON_GetObjectItem(root, "metrics");
  if (metrics != NULL) {
    int size = cJSON_GetArraySize(metrics);
    if (size <= 0) {
      httpError("failed to parse telegraf schema config file:%s, metrics size is 0", fileName);
      cJSON_Delete(root);
      return -1;
    }

    for (int i = 0; i < size; i++) {
      cJSON *metric = cJSON_GetArrayItem(metrics, i);
      if (metric != NULL) {
        tgReadSchemaMetric(metric);
      }
    }
  } else {
    tgReadSchemaMetric(root);
  }

  cJSON_Delete(root);
  free(content);
  fclose(fp);

  httpPrint("parse telegraf schema config file:%s successfully, stable schema size:%d", fileName);
  return 0;
}

/*
 * in case of file not exist
 * we use default schema:
 * such as:
 *    diskio
 *    mem
 *    processes
 *    procstat
 *    system
 *    disk
 *    swap
 *    kernel
 */
void tgInitHandle(HttpServer *pServer) {
  tgSchemaHash = taosInitStrHash(100, sizeof(STgStable), taosHashStringStep1);
  char fileName[256] = {0};
  sprintf(fileName, "%s/taos.telegraf.cfg", configDir);
  if (tgReadSchema(fileName) == -1) {
    taosCleanUpStrHash(tgSchemaHash);
    tgSchemaHash = NULL;
  }
  httpAddMethod(pServer, &tgDecodeMethod);
}

bool tgGetUserFromUrl(HttpContext *pContext) {
  HttpParser *pParser = &pContext->pThread->parser;
  if (pParser->path[TG_USER_URL_POS].len > TSDB_USER_LEN - 1 || pParser->path[TG_USER_URL_POS].len <= 0) {
    return false;
  }

  strcpy(pContext->user, pParser->path[TG_USER_URL_POS].pos);
  return true;
}

bool tgGetPassFromUrl(HttpContext *pContext) {
  HttpParser *pParser = &pContext->pThread->parser;
  if (pParser->path[TG_PASS_URL_POS].len > TSDB_PASSWORD_LEN - 1 || pParser->path[TG_PASS_URL_POS].len <= 0) {
    return false;
  }

  strcpy(pContext->pass, pParser->path[TG_PASS_URL_POS].pos);
  return true;
}

char *tgGetDbFromUrl(HttpContext *pContext) {
  HttpParser *pParser = &pContext->pThread->parser;
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
bool tgProcessSingleMetricUseDefaultSchema(HttpContext *pContext, cJSON *metric, char *db) {
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
  char *stname = name->valuestring;
  table_cmd->metric = stable_cmd->metric = httpAddToSqlCmdBuffer(pContext, "%s", stname);
  table_cmd->stable = stable_cmd->stable = httpAddToSqlCmdBuffer(pContext, "%s", stname);
      //httpAddToSqlCmdBuffer(pContext, "%s_%d_%d", stname, fieldsSize, orderTagsLen);
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
  table_cmd->table = stable_cmd->table = httpAddToSqlCmdBufferNoTerminal(pContext, "%s_%s", stname, host->valuestring);
      //httpAddToSqlCmdBufferNoTerminal(pContext, "%s_%d_%d_%s", stname, fieldsSize, orderTagsLen, host->valuestring);

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

bool tgProcessSingleMetricUseConfigSchema(HttpContext *pContext, cJSON *metric, char *db) {
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
  STgStable *stable = (STgStable *)taosGetStrHashData(tgSchemaHash, name->valuestring);
  if (stable == NULL) {
    httpSendErrorResp(pContext, HTTP_TG_STABLE_NOT_EXIST);
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
  table_cmd->tagNum = stable_cmd->tagNum = (int8_t)stable->tagNum;
  table_cmd->timestamp = stable_cmd->timestamp = httpAddToSqlCmdBuffer(pContext, "%ld", timestamp->valueint);

  // stable name
  char *stname = stable->stAlias;

  table_cmd->metric = stable_cmd->metric = httpAddToSqlCmdBuffer(pContext, "%s", stname);
  table_cmd->stable = stable_cmd->stable = httpAddToSqlCmdBuffer(pContext, "%s", stname);
  table_cmd->stable = stable_cmd->stable =
      httpShrinkTableName(pContext, table_cmd->stable, httpGetCmdsString(pContext, table_cmd->stable));

  // stable tag for detail
  for (int ts = 0; ts < stable->tagNum; ++ts) {
    STgTag *tagSchema = &stable->tags[ts];
    bool    tagParsed = false;
    for (int tt = 0; tt < tagsSize; ++tt) {
      cJSON *tag = cJSON_GetArrayItem(tags, tt);
      if (strcasecmp(tag->string, tagSchema->tagName) != 0) {
        continue;
      }

      stable_cmd->tagNames[ts] = table_cmd->tagNames[ts] = httpAddToSqlCmdBuffer(pContext, tagSchema->tagAlias);

      if (tag->type == cJSON_String) {
        stable_cmd->tagValues[ts] = table_cmd->tagValues[ts] =
            httpAddToSqlCmdBuffer(pContext, "'%s'", tag->valuestring);
        tagParsed = true;
      } else if (tag->type == cJSON_Number) {
        stable_cmd->tagValues[ts] = table_cmd->tagValues[ts] = httpAddToSqlCmdBuffer(pContext, "%ld", tag->valueint);
        tagParsed = true;
      } else if (tag->type == cJSON_True) {
        stable_cmd->tagValues[ts] = table_cmd->tagValues[ts] = httpAddToSqlCmdBuffer(pContext, "1");
        tagParsed = true;
      } else if (tag->type == cJSON_False) {
        stable_cmd->tagValues[ts] = table_cmd->tagValues[ts] = httpAddToSqlCmdBuffer(pContext, "0");
        tagParsed = true;
      } else {
      }

      break;
    }

    if (!tagParsed) {
      stable_cmd->tagValues[ts] = table_cmd->tagValues[ts] = httpAddToSqlCmdBuffer(pContext, "NULL");
    }
  }

  // table name
  table_cmd->table = stable_cmd->table = httpAddToSqlCmdBufferNoTerminal(pContext, "%s", stname);
  for (int ts = 0; ts < stable->tagNum; ++ts) {
    STgTag *tagSchema = &stable->tags[ts];
    bool    tagParsed = false;
    for (int tt = 0; tt < tagsSize; ++tt) {
      cJSON *tag = cJSON_GetArrayItem(tags, tt);
      if (strcasecmp(tag->string, tagSchema->tagName) != 0) {
        continue;
      }

      if (tag->type == cJSON_String) {
        httpAddToSqlCmdBufferNoTerminal(pContext, "_%s", tag->valuestring);
        tagParsed = true;
      } else if (tag->type == cJSON_Number) {
        httpAddToSqlCmdBufferNoTerminal(pContext, "_%ld", tag->valueint);
        tagParsed = true;
      } else if (tag->type == cJSON_True) {
        httpAddToSqlCmdBufferNoTerminal(pContext, "_1");
        tagParsed = true;
      } else if (tag->type == cJSON_False) {
        httpAddToSqlCmdBufferNoTerminal(pContext, "_0");
        tagParsed = true;
      } else {
      }

      break;
    }

    if (!tagParsed) {
      stable_cmd->tagValues[ts] = table_cmd->tagValues[ts] = httpAddToSqlCmdBufferNoTerminal(pContext, "_n");
    }
  }
  httpAddToSqlCmdBuffer(pContext, "");
  table_cmd->table = stable_cmd->table =
      httpShrinkTableName(pContext, table_cmd->table, httpGetCmdsString(pContext, table_cmd->table));

  // assembling create stable sql
  stable_cmd->sql =
      httpAddToSqlCmdBuffer(pContext, stable->createSTableStr, db, httpGetCmdsString(pContext, table_cmd->stable));

  // assembling insert sql
  table_cmd->sql = httpAddToSqlCmdBufferNoTerminal(pContext, "import into %s.%s using %s.%s tags(", db,
                                                   httpGetCmdsString(pContext, table_cmd->table), db,
                                                   httpGetCmdsString(pContext, table_cmd->stable));
  for (int ts = 0; ts < stable->tagNum; ++ts) {
    if (ts != 0) {
      httpAddToSqlCmdBufferNoTerminal(pContext, ",%s", httpGetCmdsString(pContext, stable_cmd->tagValues[ts]));
    } else {
      httpAddToSqlCmdBufferNoTerminal(pContext, "%s", httpGetCmdsString(pContext, stable_cmd->tagValues[ts]));
    }
  }

  httpAddToSqlCmdBufferNoTerminal(pContext, ") values(%ld", timestamp->valueint);

  // stable tag for detail
  for (int fs = 0; fs < stable->fieldNum; ++fs) {
    STgField *fieldSchema = &stable->fields[fs];
    bool      fieldParsed = false;
    for (int ff = 0; ff < fieldsSize; ++ff) {
      cJSON *field = cJSON_GetArrayItem(fields, ff);
      if (strcasecmp(field->string, fieldSchema->fieldName) != 0) {
        continue;
      }

      if (field->type == cJSON_String) {
        httpAddToSqlCmdBufferNoTerminal(pContext, ",\"%s\"", field->valuestring);
        fieldParsed = true;
      } else if (field->type == cJSON_Number) {
        httpAddToSqlCmdBufferNoTerminal(pContext, ",%lf", field->valuedouble);
        fieldParsed = true;
      } else if (field->type == cJSON_True) {
        httpAddToSqlCmdBufferNoTerminal(pContext, ",1");
        fieldParsed = true;
      } else if (field->type == cJSON_False) {
        httpAddToSqlCmdBufferNoTerminal(pContext, ",0");
        fieldParsed = true;
      } else {
      }

      break;
    }

    if (!fieldParsed) {
      httpAddToSqlCmdBufferNoTerminal(pContext, ",NULL");
    }
  }
  httpAddToSqlCmdBuffer(pContext, ")");

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

  HttpParser *pParser = &pContext->pThread->parser;
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
        if (tgSchemaHash != NULL) {
          if (!tgProcessSingleMetricUseConfigSchema(pContext, metric, db)) {
            cJSON_Delete(root);
            return false;
          }
        } else {
          if (!tgProcessSingleMetricUseDefaultSchema(pContext, metric, db)) {
            cJSON_Delete(root);
            return false;
          }
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

    if (tgSchemaHash != NULL) {
      if (!tgProcessSingleMetricUseConfigSchema(pContext, root, db)) {
        cJSON_Delete(root);
        return false;
      }
    } else {
      if (!tgProcessSingleMetricUseDefaultSchema(pContext, root, db)) {
        cJSON_Delete(root);
        return false;
      }
    }
  }

  cJSON_Delete(root);

  pContext->reqType = HTTP_REQTYPE_MULTI_SQL;
  pContext->encodeMethod = &tgQueryMethod;
  pContext->multiCmds->pos = 2;

  return true;
}

bool tgProcessRquest(struct HttpContext *pContext) {
  tgGetUserFromUrl(pContext);
  tgGetPassFromUrl(pContext);

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

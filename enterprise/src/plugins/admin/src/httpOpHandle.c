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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosmsg.h"
#include "taosdef.h"
#include "cJSON.h"
#include "httpLog.h"
#include "opJson.h"
#include "opHandle.h"

static HttpDecodeMethod opDecodeMethod = {"opentsdb", opProcessRequest};
static HttpEncodeMethod opPutSummaryMethod = {
  .startJsonFp          = NULL,         
  .stopJsonFp           = NULL, 
  .buildQueryJsonFp     = NULL,
  .buildAffectRowJsonFp = opBuildPutSummaryAffectRowsJson, 
  .initJsonFp           = opInitPutSummaryJson, 
  .cleanJsonFp          = opCleanPutSummaryJson,
  .checkFinishedFp      = opCheckPutSummaryFinished,
  .setNextCmdFp         = opSetPutSummaryNextCmd
};

static HttpEncodeMethod opPutDetailMethod = {
  .startJsonFp          = opStartPutDetailJson,         
  .stopJsonFp           = opStopPutDetailJson, 
  .buildQueryJsonFp     = NULL,
  .buildAffectRowJsonFp = opBuildPutDetailAffectRowsJson, 
  .initJsonFp           = opInitPutDetailJson, 
  .cleanJsonFp          = opCleanPutDetailJson,
  .checkFinishedFp      = opCheckPutDetailFinished,
  .setNextCmdFp         = opSetPutDetailNextCmd
};

void opInitHandle(HttpServer *pServer) {
  httpAddMethod(pServer, &opDecodeMethod);
}

bool opGetUserFromUrl(HttpContext *pContext) {
  HttpParser *pParser = &pContext->parser;
  if (pParser->path[OP_USER_URL_POS].len >= TSDB_USER_LEN || pParser->path[OP_USER_URL_POS].len <= 0) {
    return false;
  }

  strcpy(pContext->user, pParser->path[OP_USER_URL_POS].pos);
  return true;
}

bool opGetPassFromUrl(HttpContext *pContext) {
  HttpParser *pParser = &pContext->parser;
  if (pParser->path[OP_PASS_URL_POS].len > TSDB_PASSWORD_LEN || pParser->path[OP_PASS_URL_POS].len <= 0) {
    return false;
  }

  strcpy(pContext->pass, pParser->path[OP_PASS_URL_POS].pos);
  return true;
}

char *opGetDbFromUrl(HttpContext *pContext) {
  HttpParser *pParser = &pContext->parser;
  if (pParser->path[OP_DB_URL_POS].len <= 0) {
    httpSendErrorResp(pContext, HTTP_OP_DB_NOT_INPUT);
    return NULL;
  }

  if (pParser->path[OP_DB_URL_POS].len >= TSDB_DB_NAME_LEN) {
    httpSendErrorResp(pContext, HTTP_OP_DB_TOO_LONG);
    return NULL;
  }

  return pParser->path[OP_DB_URL_POS].pos;
}

bool opProcessLoginRequest(HttpContext *pContext) {
  httpDebug("context:%p, fd:%d, user:%s, process opentsdb login msg", pContext, pContext->fd, pContext->user);
  pContext->reqType = HTTP_REQTYPE_LOGIN;
  return true;
}

/*
{
  "metric": "sys_cpu",
  "timestamp": 1346846400,
  "value": 18,
  "tags": {
    "host": "web01",
    "group": "1",
    "dc": "lga"
  }
}
 */
bool opProcessPutDetailMetric(HttpContext *pContext, cJSON *metric, char *db) {
  // metric name
  cJSON *name = cJSON_GetObjectItem(metric, "metric");
  if (name == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_METRIC_NULL);
    return false;
  }
  if (name->type != cJSON_String) {
    httpSendErrorResp(pContext, HTTP_OP_METRIC_TYPE);
    return false;
  }
  if (name->valuestring == NULL || strlen(name->valuestring) == 0) {
    httpSendErrorResp(pContext, HTTP_OP_METRIC_NAME_NULL);
    return false;
  }
  if (strlen(name->valuestring) >= TSDB_TABLE_NAME_LEN - 11) {
    httpSendErrorResp(pContext, HTTP_OP_METRIC_NAME_LONG);
    return false;
  }

  // timestamp
  cJSON *timestamp = cJSON_GetObjectItem(metric, "timestamp");
  if (timestamp == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_TIMESTAMP_NULL);
    return false;
  }
  if (timestamp->type != cJSON_Number) {
    httpSendErrorResp(pContext, HTTP_OP_TIMESTAMP_TYPE);
    return false;
  }
  if (timestamp->valueint <= 0) {
    httpSendErrorResp(pContext, HTTP_OP_TIMESTAMP_VAL_NULL);
    return false;
  }

  // value
  cJSON *value = cJSON_GetObjectItem(metric, "value");
  if (value == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_VALUE_NULL);
    return false;
  }
  if (value->type != cJSON_Number && value->type != cJSON_String && value->type != cJSON_False &&
      value->type != cJSON_True) {
    httpSendErrorResp(pContext, HTTP_OP_VALUE_TYPE);
    return false;
  }

  // tags
  cJSON *tags = cJSON_GetObjectItem(metric, "tags");
  if (tags == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_TAGS_NULL);
    return false;
  }

  int tagsSize = cJSON_GetArraySize(tags);
  if (tagsSize <= 0) {
    httpSendErrorResp(pContext, HTTP_OP_TAGS_SIZE_0);
    return false;
  }

  if (tagsSize > TSDB_MAX_TAGS) {
    httpSendErrorResp(pContext, HTTP_OP_TAGS_SIZE_LONG);
    return false;
  }

  // tags detail
  for (int i = 0; i < tagsSize; i++) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    if (tag == NULL) {
      httpSendErrorResp(pContext, HTTP_OP_TAG_NULL);
      return false;
    }
    if (tag->string == NULL || strlen(tag->string) == 0) {
      httpSendErrorResp(pContext, HTTP_OP_TAG_NAME_NULL);
      return false;
    }
    if (strlen(tag->string) >= TSDB_COL_NAME_LEN - 1) {
      httpSendErrorResp(pContext, HTTP_OP_TAG_NAME_SIZE);
      return false;
    }

    if (tag->type != cJSON_Number && tag->type != cJSON_String && tag->type != cJSON_False && tag->type != cJSON_True) {
      httpSendErrorResp(pContext, HTTP_OP_TAG_VALUE_TYPE);
      return false;
    }

    if (tag->type == cJSON_String) {
      if (tag->valuestring == NULL) {
        httpSendErrorResp(pContext, HTTP_OP_TAG_VALUE_NULL);
        return false;
      }
      int len = (int)strlen(tag->valuestring);
      if (len == 0) {
        httpSendErrorResp(pContext, HTTP_OP_TAG_VALUE_NULL);
        return false;
      }
      if (len > 64) {
        httpSendErrorResp(pContext, HTTP_OP_TAG_VALUE_TOO_LONG);
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
  cJSON *orderedTags[12] = {0};
  int    orderTagsLen = 0;
  tagsSize = MIN(tagsSize, 12);
  for (int t1 = 0; t1 < tagsSize; ++t1) {
    cJSON *tag = cJSON_GetArrayItem(tags, t1);
    orderedTags[orderTagsLen++] = tag;
    for (int t2 = orderTagsLen - 1; t2 >= 1; --t2) {
      cJSON *tag1 = orderedTags[t2];
      cJSON *tag2 = orderedTags[t2 - 1];
      if (strcmp(tag1->string, tag2->string) < 0) {
        orderedTags[t2] = tag2;
        orderedTags[t2 - 1] = tag1;
      }
    }
  }

  table_cmd->tagNum = stable_cmd->tagNum = (int8_t)tagsSize;
  table_cmd->timestamp = stable_cmd->timestamp = httpAddToSqlCmdBuffer(pContext, "%" PRId64, timestamp->valueint);
  table_cmd->metric = stable_cmd->metric = httpAddToSqlCmdBuffer(pContext, "%s", name->valuestring);

  // stable name
  table_cmd->stable = stable_cmd->stable = httpAddToSqlCmdBufferNoTerminal(pContext, "%s_", name->valuestring);
  if (value->type == cJSON_String)
    httpAddToSqlCmdBufferNoTerminal(pContext, "b");
  else if (value->type == cJSON_Number)
    httpAddToSqlCmdBufferNoTerminal(pContext, "d");
  else if (value->type == cJSON_False || value->type == cJSON_True)
    httpAddToSqlCmdBufferNoTerminal(pContext, "t");
  else
    httpAddToSqlCmdBufferNoTerminal(pContext, "n");
  httpAddToSqlCmdBufferNoTerminal(pContext, "_");

  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    if (tag->type == cJSON_String)
      httpAddToSqlCmdBufferNoTerminal(pContext, "b");
    else if (tag->type == cJSON_Number)
      httpAddToSqlCmdBufferNoTerminal(pContext, "d");
    else if (tag->type == cJSON_False || tag->type == cJSON_True)
      httpAddToSqlCmdBufferNoTerminal(pContext, "t");
    else
      httpAddToSqlCmdBufferNoTerminal(pContext, "n");
  }
  httpAddToSqlCmdBuffer(pContext, "");
  table_cmd->stable = stable_cmd->stable =
      httpShrinkTableName(pContext, table_cmd->stable, httpGetCmdsString(pContext, table_cmd->stable));

  // stable tag for detail
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    stable_cmd->tagNames[i] = table_cmd->tagNames[i] = httpAddToSqlCmdBuffer(pContext, tag->string);

    if (tag->type == cJSON_String)
      stable_cmd->tagValues[i] = table_cmd->tagValues[i] = httpAddToSqlCmdBuffer(pContext, "\"%s\"", tag->valuestring);
    else if (tag->type == cJSON_Number)
      stable_cmd->tagValues[i] = table_cmd->tagValues[i] = httpAddToSqlCmdBuffer(pContext, "%" PRId64, tag->valueint);
    else if (tag->type == cJSON_False || tag->type == cJSON_True)
      stable_cmd->tagValues[i] = table_cmd->tagValues[i] = httpAddToSqlCmdBuffer(pContext, "\"true\"");
    else
      stable_cmd->tagValues[i] = table_cmd->tagValues[i] = httpAddToSqlCmdBuffer(pContext, "\"false\"");
  }

  // table name
  table_cmd->table = stable_cmd->table =
      httpAddToSqlCmdBufferNoTerminal(pContext, "%s", httpGetCmdsString(pContext, table_cmd->stable));
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    if (tag->type == cJSON_String)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_%s", tag->valuestring);
    else if (tag->type == cJSON_Number)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_%" PRId64, tag->valueint);
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
  char *field_type = "double";
  if (value->type == cJSON_String)
    field_type = "binary(64)";
  else if (value->type == cJSON_False || value->type == cJSON_True)
    field_type = "tinyint";
  else {
  }
  httpAddToSqlCmdBufferNoTerminal(pContext, ",%s %s) tags(", value->string, field_type);
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    char * tag_type = "double";
    if (tag->type == cJSON_String)
      tag_type = "binary(64)";
    else if (tag->type == cJSON_False || tag->type == cJSON_True)
      tag_type = "tinyint";
    else {
    }

    if (i != tagsSize - 1)
      httpAddToSqlCmdBufferNoTerminal(pContext, "%s %s,", tag->string, tag_type);
    else
      httpAddToSqlCmdBuffer(pContext, "%s %s)", tag->string, tag_type);
  }

  // assembling insert sql
  table_cmd->sql = httpAddToSqlCmdBufferNoTerminal(pContext, "import into %s.%s using %s.%s tags(", db,
                                                   httpGetCmdsString(pContext, table_cmd->table), db,
                                                   httpGetCmdsString(pContext, table_cmd->stable));
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    if (i != tagsSize - 1) {
      if (tag->type == cJSON_Number)
        httpAddToSqlCmdBufferNoTerminal(pContext, "%lf,", tag->valuedouble);
      else if (tag->type == cJSON_String)
        httpAddToSqlCmdBufferNoTerminal(pContext, "'%s',", tag->valuestring);
      else if (tag->type == cJSON_False)
        httpAddToSqlCmdBufferNoTerminal(pContext, "0,");
      else if (tag->type == cJSON_True)
        httpAddToSqlCmdBufferNoTerminal(pContext, "1,");
      else {
      }
    } else {
      if (tag->type == cJSON_Number)
        httpAddToSqlCmdBufferNoTerminal(pContext, "%lf)", tag->valuedouble);
      else if (tag->type == cJSON_String)
        httpAddToSqlCmdBufferNoTerminal(pContext, "'%s')", tag->valuestring);
      else if (tag->type == cJSON_False)
        httpAddToSqlCmdBufferNoTerminal(pContext, "0)");
      else if (tag->type == cJSON_True)
        httpAddToSqlCmdBufferNoTerminal(pContext, "1)");
      else {
      }
    }
  }

  httpAddToSqlCmdBufferNoTerminal(pContext, " values(%" PRId64 ",", timestamp->valueint);
  if (value->type == cJSON_Number) {
    httpAddToSqlCmdBuffer(pContext, "%lf)", value->valuedouble);
    table_cmd->values = stable_cmd->values = httpAddToSqlCmdBuffer(pContext, "%lf", value->valuedouble);
  } else if (value->type == cJSON_String) {
    httpAddToSqlCmdBuffer(pContext, "'%s')", value->valuestring);
    table_cmd->values = stable_cmd->values = httpAddToSqlCmdBuffer(pContext, "\"%s\"", value->valuestring);
  } else if (value->type == cJSON_False) {
    httpAddToSqlCmdBuffer(pContext, "0)");
    table_cmd->values = stable_cmd->values = httpAddToSqlCmdBuffer(pContext, "\"false\"");
  } else if (value->type == cJSON_True) {
    httpAddToSqlCmdBuffer(pContext, "1)");
    table_cmd->values = stable_cmd->values = httpAddToSqlCmdBuffer(pContext, "\"true\"");
  } else {
  }

  return true;
}

/**
request from opentsdb
[
    {
        "metric": "sys_cpu",
        "timestamp": 1346846400,
        "value": 18,
        "tags": {
           "host": "web01",
           "group": "1",
           "dc": "lga"
        }
    },
    {
        "metric": "sys_cpu",
        "timestamp": 1346846400,
        "value": 9,
        "tags": {
           "host": "web02",
           "group": "1",
           "dc": "lga"
        }
    },
    {
        "metric": "sys_status",
        "timestamp": 1346846400,
        "value": "High CPU Load",
        "tags": {
           "host": "web03",
           "group": "2",
           "dc": "lga"
        }
    },
    {
        "metric": "sys_running",
        "timestamp": 1346846400,
        "value": true,
        "tags": {
           "host": "web01",
           "group": "1",
           "dc": "lga"
        }
    }
]
*/
bool opProcessPutDetailRequest(HttpContext *pContext, char *db) {
  httpDebug("context:%p, fd:%d, process opentsdb put detail msg", pContext, pContext->fd);

  HttpParser *pParser = &pContext->parser;
  char *      filter = pParser->data.pos;
  if (filter == NULL) {
    httpSendErrorResp(pContext, HTTP_NO_MSG_INPUT);
    return false;
  }

  cJSON *root = cJSON_Parse(filter);
  if (root == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_INVALID_JSON);
    return false;
  }

  int size = cJSON_GetArraySize(root);
  httpDebug("context:%p, fd:%d, metrics:%d at one time", pContext, pContext->fd, size);
  if (size <= 0) {
    httpSendErrorResp(pContext, HTTP_OP_METRICS_NULL);
    cJSON_Delete(root);
    return false;
  }

  int cmdSize = size * 2 + 1;
  if (cmdSize > HTTP_MAX_CMD_SIZE) {
    httpSendErrorResp(pContext, HTTP_OP_METRICS_SIZE);
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
    cJSON *metric = cJSON_GetArrayItem(root, i);
    if (metric != NULL) {
      if (!opProcessPutDetailMetric(pContext, metric, db)) {
        cJSON_Delete(root);
        return false;
      }
    }
  }

  cJSON_Delete(root);

  pContext->reqType = HTTP_REQTYPE_MULTI_SQL;
  pContext->encodeMethod = &opPutDetailMethod;
  pContext->multiCmds->pos = 2;

  return true;
}

// summary make stable sql
bool opProcessPutSummaryMetric(HttpContext *pContext, cJSON *metric, char *db) {
  // metric name
  cJSON *name = cJSON_GetObjectItem(metric, "metric");
  if (name == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_METRIC_NULL);
    return false;
  }
  if (name->type != cJSON_String) {
    httpSendErrorResp(pContext, HTTP_OP_METRIC_TYPE);
    return false;
  }
  if (name->valuestring == NULL || strlen(name->valuestring) == 0) {
    httpSendErrorResp(pContext, HTTP_OP_METRIC_NAME_NULL);
    return false;
  }
  if (strlen(name->valuestring) >= TSDB_TABLE_NAME_LEN - 11) {
    httpSendErrorResp(pContext, HTTP_OP_METRIC_NAME_LONG);
    return false;
  }

  // timestamp
  cJSON *timestamp = cJSON_GetObjectItem(metric, "timestamp");
  if (timestamp == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_TIMESTAMP_NULL);
    return false;
  }
  if (timestamp->type != cJSON_Number) {
    httpSendErrorResp(pContext, HTTP_OP_TIMESTAMP_TYPE);
    return false;
  }
  if (timestamp->valueint <= 0) {
    httpSendErrorResp(pContext, HTTP_OP_TIMESTAMP_VAL_NULL);
    return false;
  }

  // value
  cJSON *value = cJSON_GetObjectItem(metric, "value");
  if (value == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_VALUE_NULL);
    return false;
  }
  if (value->type != cJSON_Number && value->type != cJSON_String && value->type != cJSON_False &&
      value->type != cJSON_True) {
    httpSendErrorResp(pContext, HTTP_OP_VALUE_TYPE);
    return false;
  }

  // tags
  cJSON *tags = cJSON_GetObjectItem(metric, "tags");
  if (tags == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_TAGS_NULL);
    return false;
  }

  int tagsSize = cJSON_GetArraySize(tags);
  if (tagsSize <= 0) {
    httpSendErrorResp(pContext, HTTP_OP_TAGS_SIZE_0);
    return false;
  }

  if (tagsSize > TSDB_MAX_TAGS) {
    httpSendErrorResp(pContext, HTTP_OP_TAGS_SIZE_LONG);
    return false;
  }

  // tags detail
  for (int i = 0; i < tagsSize; i++) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    if (tag == NULL) {
      httpSendErrorResp(pContext, HTTP_OP_TAG_NULL);
      return false;
    }
    if (tag->string == NULL || strlen(tag->string) == 0) {
      httpSendErrorResp(pContext, HTTP_OP_TAG_NAME_NULL);
      return false;
    }
    if (strlen(tag->string) >= TSDB_COL_NAME_LEN - 1) {
      httpSendErrorResp(pContext, HTTP_OP_TAG_NAME_SIZE);
      return false;
    }

    if (tag->type != cJSON_Number && tag->type != cJSON_String && tag->type != cJSON_False && tag->type != cJSON_True) {
      httpSendErrorResp(pContext, HTTP_OP_TAG_VALUE_TYPE);
      return false;
    }

    if (tag->type == cJSON_String) {
      if (tag->valuestring == NULL) {
        httpSendErrorResp(pContext, HTTP_OP_TAG_VALUE_NULL);
        return false;
      }
      int len = (int)strlen(tag->valuestring);
      if (len == 0) {
        httpSendErrorResp(pContext, HTTP_OP_TAG_VALUE_NULL);
        return false;
      }
      if (len > 64) {
        httpSendErrorResp(pContext, HTTP_OP_TAG_VALUE_TOO_LONG);
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

  // order by tag name
  cJSON *orderedTags[6] = {0};
  int    orderTagsLen = 0;
  for (int i = 0; i < tagsSize; ++i) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    orderedTags[orderTagsLen++] = tag;
    for (int j = orderTagsLen - 1; j >= 1; --j) {
      cJSON *tag1 = orderedTags[j];
      cJSON *tag2 = orderedTags[j - 1];
      if (strcmp(tag1->string, tag2->string) < 0) {
        orderedTags[j] = tag2;
        orderedTags[j - 1] = tag1;
      }
    }
  }

  // stable name
  stable_cmd->stable = httpAddToSqlCmdBufferNoTerminal(pContext, "%s_", name->valuestring);
  if (value->type == cJSON_String)
    httpAddToSqlCmdBufferNoTerminal(pContext, "b");
  else if (value->type == cJSON_Number)
    httpAddToSqlCmdBufferNoTerminal(pContext, "d");
  else if (value->type == cJSON_False || value->type == cJSON_True)
    httpAddToSqlCmdBufferNoTerminal(pContext, "t");
  else
    httpAddToSqlCmdBufferNoTerminal(pContext, "n");
  httpAddToSqlCmdBufferNoTerminal(pContext, "_");

  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    if (tag->type == cJSON_String)
      httpAddToSqlCmdBufferNoTerminal(pContext, "b");
    else if (tag->type == cJSON_Number)
      httpAddToSqlCmdBufferNoTerminal(pContext, "d");
    else if (tag->type == cJSON_False || tag->type == cJSON_True)
      httpAddToSqlCmdBufferNoTerminal(pContext, "t");
    else
      httpAddToSqlCmdBufferNoTerminal(pContext, "n");
  }
  httpAddToSqlCmdBuffer(pContext, "");
  stable_cmd->stable =
      httpShrinkTableName(pContext, stable_cmd->stable, httpGetCmdsString(pContext, stable_cmd->stable));

  // table name
  stable_cmd->table = httpAddToSqlCmdBufferNoTerminal(pContext, "%s", httpGetCmdsString(pContext, stable_cmd->stable));
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    if (tag->type == cJSON_String)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_%s", tag->valuestring);
    else if (tag->type == cJSON_Number)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_%" PRId64, tag->valueint);
    else if (tag->type == cJSON_False)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_0");
    else if (tag->type == cJSON_True)
      httpAddToSqlCmdBufferNoTerminal(pContext, "_1");
    else
      httpAddToSqlCmdBufferNoTerminal(pContext, "_n");
  }
  httpAddToSqlCmdBuffer(pContext, "");
  stable_cmd->table = httpShrinkTableName(pContext, stable_cmd->table, httpGetCmdsString(pContext, stable_cmd->table));

  // assembling create stable sql
  stable_cmd->sql = httpAddToSqlCmdBufferNoTerminal(pContext, "create table if not exists %s.%s(ts timestamp", db,
                                                    httpGetCmdsString(pContext, stable_cmd->stable));
  char *field_type = "double";
  if (value->type == cJSON_String)
    field_type = "binary(64)";
  else if (value->type == cJSON_False || value->type == cJSON_True)
    field_type = "tinyint";
  else {
  }

  httpAddToSqlCmdBufferNoTerminal(pContext, ",%s %s) tags(", value->string, field_type);
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    char * tag_type = "double";
    if (tag->type == cJSON_String)
      tag_type = "binary(64)";
    else if (tag->type == cJSON_False || tag->type == cJSON_True)
      tag_type = "tinyint";
    else {
    }

    if (i != tagsSize - 1)
      httpAddToSqlCmdBufferNoTerminal(pContext, "%s %s,", tag->string, tag_type);
    else
      httpAddToSqlCmdBuffer(pContext, "%s %s)", tag->string, tag_type);
  }

  return true;
}

// summary make insert sql
bool opProcessPutSummaryMetricValues(HttpContext *pContext, cJSON *metric, char *db, HttpSqlCmd *table_cmd) {
  // timestamp
  cJSON *timestamp = cJSON_GetObjectItem(metric, "timestamp");

  // value
  cJSON *value = cJSON_GetObjectItem(metric, "value");

  // tags
  cJSON *tags = cJSON_GetObjectItem(metric, "tags");
  int    tagsSize = cJSON_GetArraySize(tags);

  // order by tag name
  cJSON *orderedTags[6] = {0};
  int    orderTagsLen = 0;
  for (int i = 0; i < tagsSize; ++i) {
    cJSON *tag = cJSON_GetArrayItem(tags, i);
    orderedTags[orderTagsLen++] = tag;
    for (int j = orderTagsLen - 1; j >= 1; --j) {
      cJSON *tag1 = orderedTags[j];
      cJSON *tag2 = orderedTags[j - 1];
      if (strcmp(tag1->string, tag2->string) < 0) {
        orderedTags[j] = tag2;
        orderedTags[j - 1] = tag1;
      }
    }
  }

  // assembling insert sql
  httpAddToSqlCmdBufferNoTerminal(pContext, " %s.%s using %s.%s tags(", db,
                                  httpGetCmdsString(pContext, table_cmd->table), db,
                                  httpGetCmdsString(pContext, table_cmd->stable));
  for (int i = 0; i < orderTagsLen; ++i) {
    cJSON *tag = orderedTags[i];
    if (i != tagsSize - 1) {
      if (tag->type == cJSON_Number)
        httpAddToSqlCmdBufferNoTerminal(pContext, "%lf,", tag->valuedouble);
      else if (tag->type == cJSON_String)
        httpAddToSqlCmdBufferNoTerminal(pContext, "'%s',", tag->valuestring);
      else if (tag->type == cJSON_False)
        httpAddToSqlCmdBufferNoTerminal(pContext, "0,");
      else if (tag->type == cJSON_True)
        httpAddToSqlCmdBufferNoTerminal(pContext, "1,");
      else {
      }
    } else {
      if (tag->type == cJSON_Number)
        httpAddToSqlCmdBufferNoTerminal(pContext, "%lf)", tag->valuedouble);
      else if (tag->type == cJSON_String)
        httpAddToSqlCmdBufferNoTerminal(pContext, "'%s')", tag->valuestring);
      else if (tag->type == cJSON_False)
        httpAddToSqlCmdBufferNoTerminal(pContext, "0)");
      else if (tag->type == cJSON_True)
        httpAddToSqlCmdBufferNoTerminal(pContext, "1)");
      else {
      }
    }
  }

  httpAddToSqlCmdBufferNoTerminal(pContext, " values(%" PRId64 ",", timestamp->valueint);
  if (value->type == cJSON_Number) {
    httpAddToSqlCmdBufferNoTerminal(pContext, "%lf)", value->valuedouble);
  } else if (value->type == cJSON_String) {
    httpAddToSqlCmdBufferNoTerminal(pContext, "'%s')", value->valuestring);
  } else if (value->type == cJSON_False) {
    httpAddToSqlCmdBufferNoTerminal(pContext, "0)");
  } else if (value->type == cJSON_True) {
    httpAddToSqlCmdBufferNoTerminal(pContext, "1)");
  } else {
  }

  return true;
}

// summary parse
bool opProcessPutSummaryRequest(HttpContext *pContext, char *db) {
  httpDebug("context:%p, fd:%d, process opentsdb put summary msg", pContext, pContext->fd);

  HttpParser *pParser = &pContext->parser;
  char *      filter = pParser->data.pos;
  if (filter == NULL) {
    httpSendErrorResp(pContext, HTTP_NO_MSG_INPUT);
    return false;
  }

  cJSON *root = cJSON_Parse(filter);
  if (root == NULL) {
    httpSendErrorResp(pContext, HTTP_OP_INVALID_JSON);
    return false;
  }

  int size = cJSON_GetArraySize(root);
  httpDebug("context:%p, fd:%d, metrics:%d at one time", pContext, pContext->fd, size);
  if (size <= 0) {
    httpSendErrorResp(pContext, HTTP_OP_METRICS_NULL);
    cJSON_Delete(root);
    return false;
  }

  int cmdSize = size + 5;
  if (cmdSize > HTTP_MAX_CMD_SIZE) {
    httpSendErrorResp(pContext, HTTP_OP_METRICS_SIZE);
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
    cJSON *metric = cJSON_GetArrayItem(root, i);
    if (metric != NULL) {
      if (!opProcessPutSummaryMetric(pContext, metric, db)) {
        cJSON_Delete(root);
        return false;
      }
    }
  }

  // batch insert sql
  cmd = httpNewSqlCmd(pContext);
  if (cmd == NULL) {
    httpSendErrorResp(pContext, HTTP_NO_ENOUGH_MEMORY);
    cJSON_Delete(root);
    return false;
  }
  cmd->cmdType = HTTP_CMD_TYPE_INSERT;
  cmd->sql = httpAddToSqlCmdBufferNoTerminal(pContext, "import into", db);

  for (int i = 0; i < size; i++) {
    cJSON *metric = cJSON_GetArrayItem(root, i);
    if (metric != NULL) {
      int sqlLen = pContext->multiCmds->bufferPos - cmd->sql;
      if (sqlLen > 50000) {
        httpAddToSqlCmdBuffer(pContext, "");
        cmd = httpNewSqlCmd(pContext);
        if (cmd == NULL) {
          httpSendErrorResp(pContext, HTTP_NO_ENOUGH_MEMORY);
          cJSON_Delete(root);
          return false;
        }
        cmd->cmdType = HTTP_CMD_TYPE_INSERT;
        cmd->sql = httpAddToSqlCmdBufferNoTerminal(pContext, "import into", db);
      }

      HttpSqlCmd *stable_cmd = &pContext->multiCmds->cmds[i + 1];
      cmd->timestamp = stable_cmd->timestamp;
      cmd->metric = stable_cmd->metric;
      cmd->stable = stable_cmd->stable;
      cmd->table = stable_cmd->table;
      cmd->values = stable_cmd->values;
      if (!opProcessPutSummaryMetricValues(pContext, metric, db, cmd)) {
        cJSON_Delete(root);
        return false;
      }
    }
  }

  httpAddToSqlCmdBuffer(pContext, "");
  cJSON_Delete(root);

  pContext->reqType = HTTP_REQTYPE_MULTI_SQL;
  pContext->encodeMethod = &opPutSummaryMethod;
  pContext->multiCmds->pos = (int16_t)(1 + size);

  return true;
}

bool opProcessRequest(struct HttpContext *pContext) {
  if (httpUrlMatch(pContext, OP_ACTION_URL_POS, "login")) {
    opGetUserFromUrl(pContext);
    opGetPassFromUrl(pContext);
  }

  if (strlen(pContext->user) == 0 || strlen(pContext->pass) == 0) {
    httpSendErrorResp(pContext, HTTP_PARSE_USR_ERROR);
    return false;
  }

  char *db = opGetDbFromUrl(pContext);
  if (db == NULL) {
    return false;
  }

  if (httpUrlMatch(pContext, OP_ACTION_URL_POS, "put") ||
      httpUrlMatch(pContext, OP_ACTION_URL_POS, "put?details=true")) {
    return opProcessPutDetailRequest(pContext, db);
  }
  if (httpUrlMatch(pContext, OP_ACTION_URL_POS, "put?details=false")) {
    return opProcessPutSummaryRequest(pContext, db);
  } else if (httpUrlMatch(pContext, OP_ACTION_URL_POS, "login")) {
    return opProcessLoginRequest(pContext);
  } else {
  }

  httpSendErrorResp(pContext, HTTP_PARSE_URL_ERROR);
  return false;
}

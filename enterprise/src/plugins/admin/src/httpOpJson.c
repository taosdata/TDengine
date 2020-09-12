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
#include "httpLog.h"
#include "httpJson.h"
#include "httpResp.h"
#include "opHandle.h"
#include "opJson.h"

void opInitPutDetailJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  httpInitJsonBuf(jsonBuf, pContext);
  httpWriteJsonBufHead(jsonBuf);

  // obj begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonObjStt);

  httpJsonPairHead(jsonBuf, "errors", 6);

  // array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);
}

void opCleanPutDetailJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // array end
  httpJsonToken(jsonBuf, JsonArrEnd);

  int successed = 0;
  int failed = 0;
  int affected_rows = 0;
  for (int i = 0; i < pContext->multiCmds->size; ++i) {
    HttpSqlCmd *cmd = &pContext->multiCmds->cmds[i];
    if (cmd->cmdType == HTTP_CMD_TYPE_INSERT) {
      if (cmd->code == 0)
        successed++;
      else
        failed++;
      affected_rows += cmd->numOfRows;
    }
  }

  httpJsonItemToken(jsonBuf);
  httpJsonPairIntVal(jsonBuf, "failed", 6, failed);

  httpJsonItemToken(jsonBuf);
  httpJsonPairIntVal(jsonBuf, "success", 7, successed);

  httpJsonItemToken(jsonBuf);
  httpJsonPairIntVal(jsonBuf, "affected_rows", 13, affected_rows);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);

  httpWriteJsonBufEnd(jsonBuf);
}

void opStartPutDetailJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // datapoint begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonObjStt);
  httpJsonPairHead(jsonBuf, "datapoint", 9);

  // object begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonObjStt);

  // data
  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, "metric", 6, httpGetCmdsString(pContext, cmd->stable),
               (int)strlen(httpGetCmdsString(pContext, cmd->metric)));

  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, "stable", 6, httpGetCmdsString(pContext, cmd->stable),
               (int)strlen(httpGetCmdsString(pContext, cmd->stable)));

  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, "table", 5, httpGetCmdsString(pContext, cmd->table),
               (int)strlen(httpGetCmdsString(pContext, cmd->table)));

  httpJsonItemToken(jsonBuf);
  httpJsonPairOriginString(jsonBuf, "timestamp", 9, httpGetCmdsString(pContext, cmd->timestamp),
                           (int)strlen(httpGetCmdsString(pContext, cmd->timestamp)));

  httpJsonItemToken(jsonBuf);
  httpJsonPairOriginString(jsonBuf, "value", 5, httpGetCmdsString(pContext, cmd->values),
                           (int)strlen(httpGetCmdsString(pContext, cmd->values)));

  // tag begin
  httpJsonItemToken(jsonBuf);
  httpJsonPairHead(jsonBuf, "tags", 4);
  httpJsonToken(jsonBuf, JsonObjStt);

  for (int i = 0; i < cmd->tagNum; ++i) {
    httpJsonItemToken(jsonBuf);
    char *tagName = httpGetCmdsString(pContext, cmd->tagNames[i]);
    char *tagValue = httpGetCmdsString(pContext, cmd->tagValues[i]);

    httpJsonItemToken(jsonBuf);
    httpJsonPairOriginString(jsonBuf, tagName, (int)strlen(tagName), tagValue, (int)strlen(tagValue));
  }

  // tag end
  httpJsonToken(jsonBuf, JsonObjEnd);
}

void opStopPutDetailJson(HttpContext *pContext, HttpSqlCmd *cmd) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // data
  httpJsonItemToken(jsonBuf);
  httpJsonPairStatus(jsonBuf, cmd->code);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);

  // datapoint end
  httpJsonToken(jsonBuf, JsonObjEnd);
}

void opBuildPutDetailAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int affect_rows) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // data
  httpJsonPairIntVal(jsonBuf, "affected_rows", 13, affect_rows);
  cmd->numOfRows += affect_rows;
}

bool opCheckPutDetailFinished(struct HttpContext *pContext, HttpSqlCmd *cmd, int code) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  httpDebug("context:%p, fd:%d, check opentsdb command, code:%s, state:%d, type:%d, rettype:%d, tags:%d", pContext,
            pContext->fd, tstrerror(code), cmd->cmdState, cmd->cmdType, cmd->cmdReturnType, cmd->tagNum);

  if (cmd->cmdType == HTTP_CMD_TYPE_INSERT) {
    if (cmd->cmdState == HTTP_CMD_STATE_NOT_RUN_YET) {
      if (code == TSDB_CODE_MND_DB_NOT_SELECTED || code == TSDB_CODE_MND_INVALID_DB) {
        cmd->cmdState = HTTP_CMD_STATE_RUN_FINISHED;
        if (multiCmds->cmds[0].cmdState == HTTP_CMD_STATE_NOT_RUN_YET) {
          multiCmds->pos = (int16_t)-1;
          httpDebug("context:%p, fd:%d, import failed, try create database", pContext, pContext->fd);
          return false;
        }
      } else if (code == TSDB_CODE_MND_INVALID_TABLE_NAME) {
        cmd->cmdState = HTTP_CMD_STATE_RUN_FINISHED;
        if (multiCmds->cmds[multiCmds->pos - 1].cmdState == HTTP_CMD_STATE_NOT_RUN_YET) {
          multiCmds->pos = (int16_t)(multiCmds->pos - 2);
          httpDebug("context:%p, fd:%d, import failed, try create stable", pContext, pContext->fd);
          return false;
        }
      } else {
      }
    } else {
    }
  } else if (cmd->cmdType == HTTP_CMD_TYPE_CREATE_DB) {
    cmd->cmdState = HTTP_CMD_STATE_RUN_FINISHED;
    httpDebug("context:%p, fd:%d, code:%s, create database failed", pContext, pContext->fd, tstrerror(code));
  } else if (cmd->cmdType == HTTP_CMD_TYPE_CREATE_STBALE) {
    cmd->cmdState = HTTP_CMD_STATE_RUN_FINISHED;
    httpDebug("context:%p, fd:%d, code:%s, create stable failed", pContext, pContext->fd, tstrerror(code));
  } else {
  }

  return true;
}

void opSetPutDetailNextCmd(struct HttpContext *pContext, HttpSqlCmd *cmd, int code) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  httpDebug(
      "context:%p, fd:%d, get opentsdb detail next command, pos:%d, "
      "code:%s, state:%d, type:%d, rettype:%d, tags:%d",
      pContext, pContext->fd, multiCmds->pos, tstrerror(code), cmd->cmdState, cmd->cmdType, cmd->cmdReturnType,
      cmd->tagNum);

  if (cmd->cmdType == HTTP_CMD_TYPE_INSERT) {
    multiCmds->pos = (int16_t)(multiCmds->pos + 2);
  } else if (cmd->cmdType == HTTP_CMD_TYPE_CREATE_DB) {
    multiCmds->pos++;
  } else if (cmd->cmdType == HTTP_CMD_TYPE_CREATE_STBALE) {
    multiCmds->pos++;
  } else {
    multiCmds->pos++;
  }
}

void opInitPutSummaryJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  httpInitJsonBuf(jsonBuf, pContext);
  httpWriteJsonBufHead(jsonBuf);

  // obj begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonObjStt);
}

void opCleanPutSummaryJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  int total = 0;
  int successed = 0;
  for (int i = 0; i < pContext->multiCmds->size; ++i) {
    HttpSqlCmd *cmd = &pContext->multiCmds->cmds[i];
    if (cmd->cmdType == HTTP_CMD_TYPE_CREATE_STBALE) {
      total++;
    } else if (cmd->cmdType == HTTP_CMD_TYPE_INSERT) {
      successed += cmd->numOfRows;
    }
  }

  httpJsonItemToken(jsonBuf);
  httpJsonPairIntVal(jsonBuf, "failed", 6, total - successed);

  httpJsonItemToken(jsonBuf);
  httpJsonPairIntVal(jsonBuf, "success", 7, successed);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);

  httpWriteJsonBufEnd(jsonBuf);
}

void opBuildPutSummaryAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int affect_rows) {
  cmd->numOfRows += affect_rows;
}

bool opCheckPutSummaryFinished(struct HttpContext *pContext, HttpSqlCmd *cmd, int code) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  httpDebug(
      "context:%p, fd:%d, check opentsdb summary command, code:%s, "
      "state:%d, type:%d, rettype:%d, tags:%d",
      pContext, pContext->fd, tstrerror(code), cmd->cmdState, cmd->cmdType, cmd->cmdReturnType, cmd->tagNum);

  if (cmd->cmdType == HTTP_CMD_TYPE_INSERT) {
    if (cmd->cmdState == HTTP_CMD_STATE_NOT_RUN_YET) {
      if (code == TSDB_CODE_MND_DB_NOT_SELECTED || code == TSDB_CODE_MND_INVALID_DB) {
        cmd->cmdState = HTTP_CMD_STATE_RUN_FINISHED;
        if (multiCmds->cmds[0].cmdState == HTTP_CMD_STATE_NOT_RUN_YET) {
          multiCmds->pos = -1;
          httpDebug("context:%p, fd:%d, import failed, try create database", pContext, pContext->fd);
          return false;
        }
      } else if (code == TSDB_CODE_MND_INVALID_TABLE_NAME) {
        cmd->cmdState = HTTP_CMD_STATE_RUN_FINISHED;
        if (multiCmds->cmds[multiCmds->pos - 1].cmdState == HTTP_CMD_STATE_NOT_RUN_YET) {
          multiCmds->pos = 0;
          httpDebug("context:%p, fd:%d, import failed, try create stable", pContext, pContext->fd);
          return false;
        }
      } else {
      }
    } else {
    }
  } else if (cmd->cmdType == HTTP_CMD_TYPE_CREATE_DB) {
    cmd->cmdState = HTTP_CMD_STATE_RUN_FINISHED;
    httpDebug("context:%p, fd:%d, code:%s, create database failed", pContext, pContext->fd, tstrerror(code));
  } else if (cmd->cmdType == HTTP_CMD_TYPE_CREATE_STBALE) {
    cmd->cmdState = HTTP_CMD_STATE_RUN_FINISHED;
    httpDebug("context:%p, fd:%d, code:%s, create stable failed", pContext, pContext->fd, tstrerror(code));
  } else {
  }

  return true;
}

void opSetPutSummaryNextCmd(struct HttpContext *pContext, HttpSqlCmd *cmd, int code) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  httpDebug(
      "context:%p, fd:%d, get opentsdb summary next command, pos:%d, "
      "code:%s, state:%d, type:%d, rettype:%d, tags:%d",
      pContext, pContext->fd, multiCmds->pos, tstrerror(code), cmd->cmdState, cmd->cmdType, cmd->cmdReturnType,
      cmd->tagNum);

  if (cmd->cmdType == HTTP_CMD_TYPE_INSERT) {
    multiCmds->pos++;
  } else if (cmd->cmdType == HTTP_CMD_TYPE_CREATE_DB) {
    multiCmds->pos++;
  } else if (cmd->cmdType == HTTP_CMD_TYPE_CREATE_STBALE) {
    multiCmds->pos++;
    for (int16_t i = multiCmds->pos; i < multiCmds->size; ++i) {
      if (multiCmds->cmds[multiCmds->pos].cmdType == HTTP_CMD_TYPE_INSERT) {
        multiCmds->pos = i;
        break;
      }
    }
  } else {
    multiCmds->pos++;
  }
}

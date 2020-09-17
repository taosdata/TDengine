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
#include "httpTgHandle.h"
#include "httpTgJson.h"

void tgInitQueryJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  httpInitJsonBuf(jsonBuf, pContext);
  httpWriteJsonBufHead(jsonBuf);

  // array begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonObjStt);

  httpJsonPairHead(jsonBuf, "metrics", 7);

  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonArrStt);
}

void tgCleanQueryJson(HttpContext *pContext) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // array end
  httpJsonToken(jsonBuf, JsonArrEnd);
  httpJsonToken(jsonBuf, JsonObjEnd);

  httpWriteJsonBufEnd(jsonBuf);
}

void tgStartQueryJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // object begin
  httpJsonItemToken(jsonBuf);
  httpJsonToken(jsonBuf, JsonObjStt);

  // data
  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, "metric", 6, httpGetCmdsString(pContext, cmd->stable),
               (int32_t)strlen(httpGetCmdsString(pContext, cmd->metric)));

  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, "stable", 6, httpGetCmdsString(pContext, cmd->stable),
               (int32_t)strlen(httpGetCmdsString(pContext, cmd->stable)));

  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, "table", 5, httpGetCmdsString(pContext, cmd->table),
               (int32_t)strlen(httpGetCmdsString(pContext, cmd->table)));

  httpJsonItemToken(jsonBuf);
  httpJsonPair(jsonBuf, "timestamp", 9, httpGetCmdsString(pContext, cmd->timestamp),
               (int32_t)strlen(httpGetCmdsString(pContext, cmd->timestamp)));  // hack way
}

void tgStopQueryJson(HttpContext *pContext, HttpSqlCmd *cmd) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // data
  httpJsonItemToken(jsonBuf);
  httpJsonPairStatus(jsonBuf, cmd->code);

  // object end
  httpJsonToken(jsonBuf, JsonObjEnd);
}

void tgBuildSqlAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int32_t affect_rows) {
  JsonBuf *jsonBuf = httpMallocJsonBuf(pContext);
  if (jsonBuf == NULL) return;

  // data
  httpJsonPairIntVal(jsonBuf, "affected_rows", 13, affect_rows);
}

bool tgCheckFinished(struct HttpContext *pContext, HttpSqlCmd *cmd, int32_t code) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  httpDebug("context:%p, fd:%d, check telegraf command, code:%s, state:%d, type:%d, rettype:%d, tags:%d", pContext,
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

void tgSetNextCmd(struct HttpContext *pContext, HttpSqlCmd *cmd, int32_t code) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  httpDebug("context:%p, fd:%d, get telegraf next command, pos:%d, code:%s, state:%d, type:%d, rettype:%d, tags:%d",
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

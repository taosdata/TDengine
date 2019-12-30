/*******************************************************************
 *           Copyright (c) 2001 by TAOS Networks, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Networks, Inc.
 *  No part of this file may be reproduced, stored, transmitted,
 *  disclosed or used in any form or by any means other than as
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "os.h"
#include "sim.h"
#include "taos.h"
#include "tglobalcfg.h"
#include "tsclient.h"
#include "tutil.h"

void simLogSql(char *sql) {
  static FILE *fp = NULL;
  if (fp == NULL) {
    fp = fopen("bug.sql", "w");
    if (fp == NULL) {
      fprintf(stderr, "ERROR: failed to open file: bug.sql\n");
      return;
    }
  }
  fprintf(fp, "%s;\n", sql);
}

char *simGetVariable(SScript *script, char *varName, int varLen) {
  if (strncmp(varName, "error", varLen) == 0) return script->error;

  if (strncmp(varName, "rows", varLen) == 0) return script->rows;

  if (strncmp(varName, "system_exit", varLen) == 0)
    return script->system_exit_code;

  if (strncmp(varName, "system_content", varLen) == 0)
    return script->system_ret_content;

  // variable like data2_192.168.0.1
  if (strncmp(varName, "data", 4) == 0) {
    if (varLen < 6) {
      return "null";
    }

    if (varName[5] == '_') {
      int col = varName[4] - '0';
      if (col < 0 || col >= MAX_QUERY_COL_NUM) {
        return "null";
      }

      char *keyName;
      int keyLen;
      paGetToken(varName + 6, &keyName, &keyLen);

      for (int i = 0; i < MAX_QUERY_ROW_NUM; ++i) {
        if (strncmp(keyName, script->data[i][0], keyLen) == 0) {
          simTrace("script:%s, keyName:%s, keyValue:%s", script->fileName,
                   script->data[i][0], script->data[i][col]);
          return script->data[i][col];
        }
      }
      return "null";
    } else {
      int row = varName[4] - '0';
      int col = varName[5] - '0';
      if (row < 0 || row >= MAX_QUERY_ROW_NUM) {
        return "null";
      }
      if (col < 0 || col >= MAX_QUERY_COL_NUM) {
        return "null";
      }

      simTrace("script:%s, data[%d][%d]=%s", script->fileName, row, col,
               script->data[row][col]);
      return script->data[row][col];
    }
  }

  for (int i = 0; i < script->varLen; ++i) {
    SVariable *var = &script->variables[i];
    if (var->varNameLen != varLen) {
      continue;
    }
    if (strncmp(varName, var->varName, varLen) == 0) {
      // if (strlen(var->varValue) != 0)
      //  simTrace("script:%s, var:%s, value:%s", script->fileName,
      //  var->varName, var->varValue);
      return var->varValue;
    }
  }

  if (script->varLen >= MAX_VAR_LEN) {
    simError("script:%s, too many varialbes:%d", script->fileName,
             script->varLen);
    exit(0);
  }

  SVariable *var = &script->variables[script->varLen];
  script->varLen++;
  strncpy(var->varName, varName, varLen);
  var->varNameLen = varLen;
  var->varValue[0] = 0;
  return var->varValue;
}

int simExecuteExpression(SScript *script, char *exp) {
  char *op1, *op2, *var1, *var2, *var3, *rest;
  int op1Len, op2Len, var1Len, var2Len, var3Len, val0, val1;
  char t0[512], t1[512], t2[512], t3[512];
  int result;

  rest = paGetToken(exp, &var1, &var1Len);
  rest = paGetToken(rest, &op1, &op1Len);
  rest = paGetToken(rest, &var2, &var2Len);
  rest = paGetToken(rest, &op2, &op2Len);

  if (var1[0] == '$')
    strcpy(t0, simGetVariable(script, var1 + 1, var1Len - 1));
  else {
    memcpy(t0, var1, var1Len);
    t0[var1Len] = 0;
  }

  if (var2[0] == '$')
    strcpy(t1, simGetVariable(script, var2 + 1, var2Len - 1));
  else {
    memcpy(t1, var2, var2Len);
    t1[var2Len] = 0;
  }

  if (op2Len != 0) {
    rest = paGetToken(rest, &var3, &var3Len);

    if (var3[0] == '$')
      strcpy(t2, simGetVariable(script, var3 + 1, var3Len - 1));
    else {
      memcpy(t2, var3, var3Len);
      t2[var3Len] = 0;
    }

    if (op2[0] == '+') {
      sprintf(t3, "%lld", atoll(t1) + atoll(t2));
    } else if (op2[0] == '-') {
      sprintf(t3, "%lld", atoll(t1) - atoll(t2));
    } else if (op2[0] == '*') {
      sprintf(t3, "%lld", atoll(t1) * atoll(t2));
    } else if (op2[0] == '/') {
      sprintf(t3, "%lld", atoll(t1) / atoll(t2));
    } else if (op2[0] == '.') {
      sprintf(t3, "%s%s", t1, t2);
    }
  } else {
    strcpy(t3, t1);
  }

  result = 0;

  if (op1Len == 1) {
    if (op1[0] == '=') {
      strcpy(simGetVariable(script, var1 + 1, var1Len - 1), t3);
    } else if (op1[0] == '<') {
      val0 = atoi(t0);
      val1 = atoi(t3);
      if (val0 >= val1) result = -1;
    } else if (op1[0] == '>') {
      val0 = atoi(t0);
      val1 = atoi(t3);
      if (val0 <= val1) result = -1;
    }
  } else {
    if (op1[0] == '=' && op1[1] == '=') {
      if (strcmp(t0, t3) != 0) result = -1;
    } else if (op1[0] == '!' && op1[1] == '=') {
      if (strcmp(t0, t3) == 0) result = -1;
    } else if (op1[0] == '<' && op1[1] == '=') {
      val0 = atoi(t0);
      val1 = atoi(t3);
      if (val0 > val1) result = -1;
    } else if (op1[0] == '>' && op1[1] == '=') {
      val0 = atoi(t0);
      val1 = atoi(t3);
      if (val0 < val1) result = -1;
    }
  }

  return result;
}

bool simExecuteExpCmd(SScript *script, char *option) {
  simExecuteExpression(script, option);
  script->linePos++;
  return true;
}

bool simExecuteTestCmd(SScript *script, char *option) {
  int result;
  result = simExecuteExpression(script, option);

  if (result >= 0)
    script->linePos++;
  else
    script->linePos = script->lines[script->linePos].jump;

  return true;
}

bool simExecuteGotoCmd(SScript *script, char *option) {
  script->linePos = script->lines[script->linePos].jump;
  return true;
}

bool simExecuteRunCmd(SScript *script, char *option) {
  char *fileName = option;
  if (fileName == NULL || strlen(fileName) == 0) {
    sprintf(script->error, "lineNum:%d. script file is null",
            script->lines[script->linePos].lineNum);
    return false;
  }

  SScript *newScript = simParseScript(option);
  if (newScript == NULL) {
    sprintf(script->error, "lineNum:%d. parse file:%s error",
            script->lines[script->linePos].lineNum, fileName);
    return false;
  }

  simPrint("script:%s, start to execute", newScript->fileName);

  newScript->type = SIM_SCRIPT_TYPE_MAIN;
  simScriptPos++;
  simScriptList[simScriptPos] = newScript;

  script->linePos++;
  return true;
}

bool simExecuteRunBackCmd(SScript *script, char *option) {
  char *fileName = option;
  if (fileName == NULL || strlen(fileName) == 0) {
    sprintf(script->error, "lineNum:%d. script file is null",
            script->lines[script->linePos].lineNum);
    return false;
  }

  SScript *newScript = simParseScript(option);
  if (newScript == NULL) {
    sprintf(script->error, "lineNum:%d. parse file:%s error",
            script->lines[script->linePos].lineNum, fileName);
    return false;
  }
  simPrint("script:%s, start to execute in background", newScript->fileName);

  newScript->type = SIM_SCRIPT_TYPE_BACKGROUND;
  script->bgScripts[script->bgScriptLen++] = newScript;

  pthread_t pid;
  if (pthread_create(&pid, NULL, simExecuteScript, (void *)newScript) != 0) {
    sprintf(script->error, "lineNum:%d. create background thread failed",
            script->lines[script->linePos].lineNum);
    return false;
  }

  script->linePos++;
  return true;
}

bool simExecuteSystemCmd(SScript *script, char *option) {
  char buf[1024] = {0};

  sprintf(buf, "cd %s; ", scriptDir);
  simVisuallizeOption(script, option, buf + strlen(buf));

  sprintf(script->system_exit_code, "%d", system(buf));

  script->linePos++;
  return true;
}

void simStoreSystemContentResult(SScript *script, char *filename) {
  memset(script->system_ret_content, 0, MAX_SYSTEM_RESULT_LEN);

  FILE *fd;
  if ((fd = fopen(filename, "r")) != NULL) {
    fread(script->system_ret_content, 1, MAX_SYSTEM_RESULT_LEN - 1, fd);
    fclose(fd);
    char rmCmd[MAX_FILE_NAME_LEN] = {0};
    sprintf(rmCmd, "rm -f %s", filename);
    system(rmCmd);
  }
}

bool simExecuteSystemContentCmd(SScript *script, char *option) {
  char buf[1024] = {0};
  char filename[100] = {0};
  sprintf(filename, "%s/%s.tmp", scriptDir, script->fileName);

  sprintf(buf, "cd %s; ", scriptDir);
  simVisuallizeOption(script, option, buf + strlen(buf));
  sprintf(buf, "%s > %s 2>/dev/null", buf, filename);

  sprintf(script->system_exit_code, "%d", system(buf));
  simStoreSystemContentResult(script, filename);

  script->linePos++;
  return true;
}

bool simExecutePrintCmd(SScript *script, char *rest) {
  char buf[1024];

  simVisuallizeOption(script, rest, buf);
  rest = buf;

  simPrint("script:%s, %s", script->fileName, rest);
  script->linePos++;
  return true;
}

bool simExecuteSleepCmd(SScript *script, char *option) {
  int delta;
  char buf[1024];

  simVisuallizeOption(script, option, buf);
  option = buf;

  delta = atoi(option);
  if (delta <= 0) delta = 5;

  simPrint("script:%s, sleep %dms begin", script->fileName, delta);
  taosMsleep(delta);
  simPrint("script:%s, sleep %dms finished", script->fileName, delta);

  script->linePos++;
  return true;
}

bool simExecuteReturnCmd(SScript *script, char *option) {
  char buf[1024];

  simVisuallizeOption(script, option, buf);
  option = buf;

  int ret = 1;
  if (option && option[0] != 0) ret = atoi(option);

  if (ret < 0) {
    sprintf(script->error, "lineNum:%d. error return %s",
            script->lines[script->linePos].lineNum, option);
    return false;
  } else {
    simPrint("script:%s, return cmd execute with:%d", script->fileName, ret);
    script->linePos = script->numOfLines;
  }

  script->linePos++;
  return true;
}

void simVisuallizeOption(SScript *script, char *src, char *dst) {
  char *var, *token, *value;
  int dstLen, srcLen, tokenLen;

  dst[0] = 0, dstLen = 0;

  while (1) {
    var = strchr(src, '$');
    if (var == NULL) break;
    if (var && ((var - src - 1) > 0) && *(var - 1) == '\\') {
      srcLen = var - src - 1;
      memcpy(dst + dstLen, src, srcLen);
      dstLen += srcLen;
      src = var;
      break;
    }

    srcLen = var - src;
    memcpy(dst + dstLen, src, srcLen);
    dstLen += srcLen;

    src = paGetToken(var + 1, &token, &tokenLen);
    value = simGetVariable(script, token, tokenLen);

    strcpy(dst + dstLen, value);
    dstLen += strlen(value);
  }

  strcpy(dst + dstLen, src);
}

void simCloseTaosdConnect(SScript *script) {
  if (script->taos == NULL) return;

  simTrace("script:%s, taos:%p closed", script->fileName, script->taos);
  taos_close(script->taos);

#ifdef CLUSTER
  tscMgmtIpList.numOfIps = 2;
  strcpy(tscMgmtIpList.ipstr[0], tsMasterIp);
  tscMgmtIpList.ip[0] = inet_addr(tsMasterIp);

  strcpy(tscMgmtIpList.ipstr[1], tsMasterIp);
  tscMgmtIpList.ip[1] = inet_addr(tsMasterIp);

  if (tsSecondIp[0]) {
    tscMgmtIpList.numOfIps = 3;
    strcpy(tscMgmtIpList.ipstr[2], tsSecondIp);
    tscMgmtIpList.ip[2] = inet_addr(tsSecondIp);
  }
#endif

  script->taos = NULL;
}

bool simCreateTaosdConnect(SScript *script, char *rest) {
  simCloseTaosdConnect(script);

  char *user = tsDefaultUser;
  char *token;
  int tokenLen;
  rest = paGetToken(rest, &token, &tokenLen);
  rest = paGetToken(rest, &token, &tokenLen);
  if (tokenLen != 0) {
    user = token;
  }

  void *taos = NULL;
  for (int attempt = 0; attempt < 10; ++attempt) {
    taos = taos_connect(NULL, user, tsDefaultPass, NULL, tsMgmtShellPort);
    if (taos == NULL) {
      simTrace("script:%s, user:%s connect taosd failed:%s, attempt:%d",
               script->fileName, user, taos_errstr(NULL), attempt);
      taosMsleep(1000);
    } else {
      simTrace("script:%s, user:%s connect taosd successed, attempt:%d",
               script->fileName, user, attempt);
      break;
    }
  }

  if (taos == NULL) {
    sprintf(script->error, "lineNum:%d. connect taosd failed:%s",
            script->lines[script->linePos].lineNum, taos_errstr(NULL));
    return false;
  }

  script->taos = taos;
  simTrace("script:%s, connect taosd successed, taos:%p", script->fileName,
           taos);

  return true;
}

bool simExecuteSqlImpCmd(SScript *script, char *rest, bool isSlow) {
  char buf[3000];
  char timeStr[30] = {0};
  time_t tt;
  struct tm *tp;
  SCmdLine *line = &script->lines[script->linePos];

  simVisuallizeOption(script, rest, buf);
  rest = buf;

  simTrace("script:%s, exec:%s", script->fileName, rest);
  strcpy(script->rows, "-1");
  for (int row = 0; row < MAX_QUERY_ROW_NUM; ++row) {
    for (int col = 0; col < MAX_QUERY_COL_NUM; ++col) {
      strcpy(script->data[row][col], "null");
    }
  }

  if (strncmp(rest, "connect", 7) == 0) {
    if (!simCreateTaosdConnect(script, rest)) {
      return false;
    }
    script->linePos++;
    return true;
  }

  if (script->taos == NULL) {
    if (!simCreateTaosdConnect(script, "connect root")) {
      if (line->errorJump == SQL_JUMP_TRUE) {
        script->linePos = line->jump;
        return true;
      }
      return false;
    }
  }

  if (strncmp(rest, "close", 5) == 0) {
    simCloseTaosdConnect(script);
    script->linePos++;
    return true;
  }

  int ret = -1;
  for (int attempt = 0; attempt < 10; ++attempt) {
    simLogSql(rest);
    ret = taos_query(script->taos, rest);
    if (ret == TSDB_CODE_TABLE_ALREADY_EXIST ||
        ret == TSDB_CODE_DB_ALREADY_EXIST) {
      simTrace("script:%s, taos:%p, %s success, ret:%d:%s", script->fileName,
               script->taos, rest, ret, tsError[ret]);
      ret = 0;
      break;
    } else if (ret != 0) {
      simTrace("script:%s, taos:%p, %s failed, ret:%d:%s, error:%s",
               script->fileName, script->taos, rest, ret, tsError[ret],
               taos_errstr(script->taos));

      if (line->errorJump == SQL_JUMP_TRUE) {
        script->linePos = line->jump;
        return true;
      }
      taosMsleep(1000);
    } else {
      break;
    }
  }

  if (ret) {
    sprintf(script->error, "lineNum:%d. sql:%s failed, ret:%d:%s",
            line->lineNum, rest, ret, tsError[ret]);
    return false;
  }

  int numOfRows = 0;
  int num_fields = taos_field_count(script->taos);
  if (num_fields != 0) {
    TAOS_RES *result = taos_use_result(script->taos);
    if (result == NULL) {
      simTrace("script:%s, taos:%p, %s failed, result is null",
               script->fileName, script->taos, rest);
      if (line->errorJump == SQL_JUMP_TRUE) {
        script->linePos = line->jump;
        return true;
      }

      sprintf(script->error, "lineNum:%d. result set null, sql:%s",
              line->lineNum, rest);
      return false;
    }

    TAOS_ROW row;

    while ((row = taos_fetch_row(result))) {
      if (numOfRows < MAX_QUERY_ROW_NUM) {
        TAOS_FIELD *fields = taos_fetch_fields(result);
        for (int i = 0; i < num_fields; i++) {
          char *value = NULL;
          if (i < MAX_QUERY_COL_NUM) {
            value = script->data[numOfRows][i];
          }
          if (value == NULL) {
            continue;
          }

          if (row[i] == 0) {
            strcpy(value, "NULL");
            continue;
          }

          switch (fields[i].type) {
            case TSDB_DATA_TYPE_BOOL:
              sprintf(value, "%s",
                      ((((int)(*((char *)row[i]))) == 1) ? "true" : "false"));
              break;
            case TSDB_DATA_TYPE_TINYINT:
              sprintf(value, "%d", (int)(*((char *)row[i])));
              break;
            case TSDB_DATA_TYPE_SMALLINT:
              sprintf(value, "%d", (int)(*((short *)row[i])));
              break;
            case TSDB_DATA_TYPE_INT:
              sprintf(value, "%d", *((int *)row[i]));
              break;
            case TSDB_DATA_TYPE_BIGINT:
              #ifdef _TD_ARM_32_
                sprintf(value, "%lld", *((int64_t *)row[i]));
              #else
                sprintf(value, "%ld", *((int64_t *)row[i]));
              #endif
              break;
            case TSDB_DATA_TYPE_FLOAT:{
#ifdef _TD_ARM_32_
              float fv = 0;
              *(int32_t*)(&fv) = *(int32_t*)row[i];              
              sprintf(value, "%.4f", fv);
#else
	      sprintf(value, "%.4f", *((float *)row[i]));
#endif
              }
              break;
            case TSDB_DATA_TYPE_DOUBLE: {
#ifdef _TD_ARM_32_
              double dv = 0;
              *(int64_t*)(&dv) = *(int64_t*)row[i];              
              sprintf(value, "%.9lf", dv);
#else
	      sprintf(value, "%.9lf", *((double *)row[i]));
#endif
              }
              break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
              memcpy(value, row[i], fields[i].bytes);
              value[fields[i].bytes] = 0;
              // snprintf(value, fields[i].bytes, "%s", (char *)row[i]);
              break;
            case TSDB_DATA_TYPE_TIMESTAMP:
              tt = *(int64_t *)row[i] / 1000;
              tp = localtime(&tt);
              strftime(timeStr, 64, "%y-%m-%d %H:%M:%S", tp);
              sprintf(value, "%s.%03d", timeStr,
                      (int)(*((int64_t *)row[i]) % 1000));
              break;
            default:
              break;
          }  // end of switch
        }    // end of for
      }      // end of if
      numOfRows++;
      if (isSlow && numOfRows % 100 == 0) {
        taosMsleep(200);
      }
      if (numOfRows > 2000000000) {
        simError("script:%s, too many rows return from query",
                 script->fileName);
        break;
      }
    }

    taos_free_result(result);
  } else {
    numOfRows = taos_affected_rows(script->taos);
  }

  sprintf(script->rows, "%d", numOfRows);

  script->linePos++;
  return true;
}

bool simExecuteSqlCmd(SScript *script, char *rest) {
  bool isSlow = false;
  return simExecuteSqlImpCmd(script, rest, isSlow);
}

bool simExecuteSqlSlowCmd(SScript *script, char *rest) {
  bool isSlow = true;
  return simExecuteSqlImpCmd(script, rest, isSlow);
}

bool simExecuteSqlErrorCmd(SScript *script, char *rest) {
  char buf[3000];
  SCmdLine *line = &script->lines[script->linePos];

  simVisuallizeOption(script, rest, buf);
  rest = buf;

  simTrace("script:%s, exec:%s", script->fileName, rest);
  strcpy(script->rows, "-1");
  for (int row = 0; row < MAX_QUERY_ROW_NUM; ++row) {
    for (int col = 0; col < MAX_QUERY_COL_NUM; ++col) {
      strcpy(script->data[row][col], "null");
    }
  }

  if (strncmp(rest, "connect", 7) == 0) {
    if (!simCreateTaosdConnect(script, rest)) {
      return false;
    }
    script->linePos++;
    return true;
  }

  if (script->taos == NULL) {
    if (!simCreateTaosdConnect(script, "connect root")) {
      if (line->errorJump == SQL_JUMP_TRUE) {
        script->linePos = line->jump;
        return true;
      }
      return false;
    }
  }

  if (strncmp(rest, "close", 5) == 0) {
    simCloseTaosdConnect(script);
    script->linePos++;
    return true;
  }

  int ret = taos_query(script->taos, rest);
  if (ret != TSDB_CODE_SUCCESS) {
    simTrace("script:%s, taos:%p, %s execute, expect failed, so success, ret:%d:%s",
             script->fileName, script->taos, rest, ret, tsError[ret]);
    script->linePos++;
    return true;
  }

  sprintf(script->error, "lineNum:%d. sql:%s expect failed, but success, ret:%d:%s",
          line->lineNum, rest, ret, tsError[ret]);
  return false;
}

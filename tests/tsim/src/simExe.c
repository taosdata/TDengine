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

#include "os.h"
#include "sim.h"
#include "taos.h"
#include "taoserror.h"
#include "tglobal.h"
#include "tutil.h"
#include "cJSON.h"

void simLogSql(char *sql) {
  static FILE *fp = NULL;
  char filename[256];
  sprintf(filename, "%s/sim.sql", tsScriptDir);
  if (fp == NULL) {
    fp = fopen(filename, "w");
    if (fp == NULL) {
      fprintf(stderr, "ERROR: failed to open file: %s\n", filename);
      return;
    }
  }
  fprintf(fp, "%s;\n", sql);
  fflush(fp);
}

char *simParseArbitratorName(char *varName);
char *simParseHostName(char *varName);
char *simGetVariable(SScript *script, char *varName, int varLen) {
  if (strncmp(varName, "hostname", 8) == 0) {
    return simParseHostName(varName);
  }

  if (strncmp(varName, "arbitrator", 10) == 0) {
      return simParseArbitratorName(varName);
  }

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
          simTrace("script:%s, keyName:%s, keyValue:%s", script->fileName, script->data[i][0], script->data[i][col]);
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

      simTrace("script:%s, data[%d][%d]=%s", script->fileName, row, col, script->data[row][col]);
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
    simError("script:%s, too many varialbes:%d", script->fileName, script->varLen);
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
  char t0[512], t1[512], t2[512], t3[1024];
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
    sprintf(script->error, "lineNum:%d. script file is null", script->lines[script->linePos].lineNum);
    return false;
  }

  SScript *newScript = simParseScript(option);
  if (newScript == NULL) {
    sprintf(script->error, "lineNum:%d. parse file:%s error", script->lines[script->linePos].lineNum, fileName);
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
    sprintf(script->error, "lineNum:%d. script file is null", script->lines[script->linePos].lineNum);
    return false;
  }

  SScript *newScript = simParseScript(option);
  if (newScript == NULL) {
    sprintf(script->error, "lineNum:%d. parse file:%s error", script->lines[script->linePos].lineNum, fileName);
    return false;
  }
  simPrint("script:%s, start to execute in background", newScript->fileName);

  newScript->type = SIM_SCRIPT_TYPE_BACKGROUND;
  script->bgScripts[script->bgScriptLen++] = newScript;

  pthread_t pid;
  if (pthread_create(&pid, NULL, simExecuteScript, (void *)newScript) != 0) {
    sprintf(script->error, "lineNum:%d. create background thread failed", script->lines[script->linePos].lineNum);
    return false;
  }

  script->linePos++;
  return true;
}

bool simExecuteSystemCmd(SScript *script, char *option) {
  char buf[4096] = {0};

  sprintf(buf, "cd %s; ", tsScriptDir);
  simVisuallizeOption(script, option, buf + strlen(buf));

  int code = system(buf);
  int repeatTimes = 0;
  while (code < 0) {
    simError("script:%s, failed to execute %s , code %d, errno:%d %s, repeatTimes:%d",
        script->fileName, buf, code, errno, strerror(errno), repeatTimes);
    taosMsleep(1000);
    signal(SIGCHLD, SIG_DFL);
    if (repeatTimes++ >= 10) {
      exit(0);
    }
  }

  sprintf(script->system_exit_code, "%d", code);
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
  char buf[4096] = {0};
  char buf1[4096 + 512] = {0};
  char filename[400] = {0};
  sprintf(filename, "%s/%s.tmp", tsScriptDir, script->fileName);

  sprintf(buf, "cd %s; ", tsScriptDir);
  simVisuallizeOption(script, option, buf + strlen(buf));
  sprintf(buf1, "%s > %s 2>/dev/null", buf, filename);

  sprintf(script->system_exit_code, "%d", system(buf1));
  simStoreSystemContentResult(script, filename);

  script->linePos++;
  return true;
}

bool simExecutePrintCmd(SScript *script, char *rest) {
  char buf[65536];

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
    sprintf(script->error, "lineNum:%d. error return %s", script->lines[script->linePos].lineNum, option);
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

void simCloseRestFulConnect(SScript *script) {
  memset(script->auth, 0, sizeof(script->auth));
}

void simCloseNativeConnect(SScript *script) {
  if (script->taos == NULL) return;

  simTrace("script:%s, taos:%p closed", script->fileName, script->taos);
  taos_close(script->taos);
  taosMsleep(1200);

  script->taos = NULL;
}

void simCloseTaosdConnect(SScript *script) {
  if (simAsyncQuery) {
    return simCloseRestFulConnect(script);
  } else {
    return simCloseNativeConnect(script);
  }
}
//  {"status":"succ","code":0,"desc":"/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04"}
//  {"status":"succ","head":["affected_rows"],"data":[[1]],"rows":1}
//  {"status":"succ","head":["ts","i"],"data":[["2017-12-25 21:28:41.022",1],["2017-12-25 21:28:42.022",2],["2017-12-25 21:28:43.022",3],["2017-12-25 21:28:44.022",4],["2017-12-25 21:28:45.022",5],["2017-12-25 21:28:46.022",6],["2017-12-25 21:28:47.022",7],["2017-12-25 21:28:48.022",8],["2017-12-25 21:28:49.022",9],["2017-12-25 21:28:50.022",10]],"rows":10}
int simParseHttpCommandResult(SScript *script, char *command) {
  cJSON* root = cJSON_Parse(command);
  if (root == NULL) {
    simError("script:%s, failed to parse json, response:%s", script->fileName, command);
    return -1;
  }

  cJSON *status = cJSON_GetObjectItem(root, "status");
  if (status == NULL) {
    simError("script:%s, failed to parse json, status is null, response:%s", script->fileName, command);
    cJSON_Delete(root);
    return -1;
  }

  if (status->valuestring == NULL || strlen(status->valuestring) == 0) {
    simError("script:%s, failed to parse json, status value is null, response:%s", script->fileName, command);
    cJSON_Delete(root);
    return -1;
  }

  if (strcmp(status->valuestring, "succ") != 0) {
    cJSON *code = cJSON_GetObjectItem(root, "code");
    if (code == NULL) {
      simError("script:%s, failed to parse json, code is null, response:%s", script->fileName, command);
      cJSON_Delete(root);
      return -1;
    }
    int retcode = (int)code->valueint;
    if (retcode != 1017) {
      simError("script:%s, json:status:%s not equal to succ, response:%s", script->fileName, status->valuestring, command);
      cJSON_Delete(root);
      return retcode;
    } else {
      simTrace("script:%s, json:status:%s not equal to succ, but code is %d, response:%s", script->fileName,
          status->valuestring, retcode, command);
      cJSON_Delete(root);
      return 0;
    }
  }

  cJSON *desc = cJSON_GetObjectItem(root, "desc");
  if (desc != NULL) {
    if (desc->valuestring == NULL || strlen(desc->valuestring) == 0) {
      simError("script:%s, failed to parse json, desc value is null, response:%s", script->fileName, command);
      cJSON_Delete(root);
      return -1;
    }
    strcpy(script->auth, desc->valuestring);
    cJSON_Delete(root);
    return 0;
  }

  cJSON *data = cJSON_GetObjectItem(root, "data");
  if (data == NULL) {
    simError("script:%s, failed to parse json, data is null, response:%s", script->fileName, command);
    cJSON_Delete(root);
    return -1;
  }

  int rowsize = cJSON_GetArraySize(data);
  if (rowsize < 0) {
    simError("script:%s, failed to parse json:data, data size %d, response:%s", script->fileName, rowsize, command);
    cJSON_Delete(root);
    return -1;
  }

  int rowIndex = 0;
  sprintf(script->rows, "%d", rowsize);
  for (int r = 0; r < rowsize; ++r) {
    cJSON *row = cJSON_GetArrayItem(data, r);
    if (row == NULL) continue;
    if (rowIndex++ >= 10) break;

    int colsize = cJSON_GetArraySize(row);
    if (colsize < 0) {
      break;
    }

    colsize = MIN(10, colsize);
    for (int c = 0; c < colsize; ++c) {
      cJSON *col = cJSON_GetArrayItem(row, c);
      if (col->valuestring != NULL) {
        strcpy(script->data[r][c], col->valuestring);
      } else {
        if (col->numberstring[0] == 0) {
          strcpy(script->data[r][c], "null");
        } else {
          strcpy(script->data[r][c], col->numberstring);
        }
      }
    }
  }

  return 0;
}

int simExecuteRestFulCommand(SScript *script, char *command) {
  char buf[5000] = {0};
  sprintf(buf, "%s 2>/dev/null", command);

  FILE *fp = popen(buf, "r");
  if (fp == NULL) {
    simError("failed to execute %s", buf);
    return TSDB_CODE_OTHERS;
  }

  int mallocSize = 2000;
  int alreadyReadSize = 0;
  char* content = malloc(mallocSize);

  while (!feof(fp)) {
    int availSize = mallocSize - alreadyReadSize;
    int len = fread(content + alreadyReadSize, 1, availSize, fp);
    if (len >= availSize) {
      alreadyReadSize += len;
      mallocSize *= 2;
      content = realloc(content, mallocSize);
    }
  }

  pclose(fp);

  return simParseHttpCommandResult(script, content);
}

bool simCreateRestFulConnect(SScript *script, char *user, char *pass) {
  char command[4096];
  sprintf(command, "curl 127.0.0.1:6020/rest/login/%s/%s", user, pass);

  bool success = false;
  for (int attempt = 0; attempt < 10; ++attempt) {
    success = simExecuteRestFulCommand(script, command) == 0;
    if (!success) {
      simTrace("script:%s, user:%s connect taosd failed:%s, attempt:%d", script->fileName, user, taos_errstr(NULL), attempt);
      taosMsleep(1000);
    } else {
      simTrace("script:%s, user:%s connect taosd successed, attempt:%d", script->fileName, user, attempt);
      break;
    }
  }

  if (!success) {
    sprintf(script->error, "lineNum:%d. connect taosd failed:%s", script->lines[script->linePos].lineNum, taos_errstr(NULL));
    return false;
  }

  simTrace("script:%s, connect taosd successed, auth:%p", script->fileName, script->auth);
  return true;
}

bool simCreateNativeConnect(SScript *script, char *user, char *pass) {
  simCloseTaosdConnect(script);
  void *taos = NULL;
  taosMsleep(2000);
  for (int attempt = 0; attempt < 10; ++attempt) {
    taos = taos_connect(NULL, user, pass, NULL, tsDnodeShellPort);
    if (taos == NULL) {
      simTrace("script:%s, user:%s connect taosd failed:%s, attempt:%d", script->fileName, user, taos_errstr(NULL), attempt);
      taosMsleep(1000);
    } else {
      simTrace("script:%s, user:%s connect taosd successed, attempt:%d", script->fileName, user, attempt);
      break;
    }
  }

  if (taos == NULL) {
    sprintf(script->error, "lineNum:%d. connect taosd failed:%s", script->lines[script->linePos].lineNum, taos_errstr(NULL));
    return false;
  }

  script->taos = taos;
  simTrace("script:%s, connect taosd successed, taos:%p", script->fileName, taos);

  return true;
}

bool simCreateTaosdConnect(SScript *script, char *rest) {
  char *user = tsDefaultUser;
  char *token;
  int tokenLen;
  rest = paGetToken(rest, &token, &tokenLen);
  rest = paGetToken(rest, &token, &tokenLen);
  if (tokenLen != 0) {
    user = token;
  }

  if (simAsyncQuery) {
    return simCreateRestFulConnect(script, user, tsDefaultPass);
  } else {
    return simCreateNativeConnect(script, user, tsDefaultPass);
  }
}

bool simExecuteNativeSqlCommand(SScript *script, char *rest, bool isSlow) {
  char timeStr[30] = {0};
  time_t tt;
  struct tm *tp;
  SCmdLine *line = &script->lines[script->linePos];
  int ret = -1;

  for (int attempt = 0; attempt < 3; ++attempt) {
    simLogSql(rest);
    ret = taos_query(script->taos, rest);
    if (ret == TSDB_CODE_TABLE_ALREADY_EXIST ||
        ret == TSDB_CODE_DB_ALREADY_EXIST) {
      simTrace("script:%s, taos:%p, %s success, ret:%d:%s", script->fileName, script->taos, rest, ret, tstrerror(ret));
      ret = 0;
      break;
    } else if (ret != 0) {
      simTrace("script:%s, taos:%p, %s failed, ret:%d:%s, error:%s",
               script->fileName, script->taos, rest, ret, tstrerror(ret), taos_errstr(script->taos));

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
    sprintf(script->error, "lineNum:%d. sql:%s failed, ret:%d:%s", line->lineNum, rest, ret, tstrerror(ret));
    return false;
  }

  int numOfRows = 0;
  int num_fields = taos_field_count(script->taos);
  if (num_fields != 0) {
    TAOS_RES *result = taos_use_result(script->taos);
    if (result == NULL) {
      simTrace("script:%s, taos:%p, %s failed, result is null", script->fileName, script->taos, rest);
      if (line->errorJump == SQL_JUMP_TRUE) {
        script->linePos = line->jump;
        return true;
      }

      sprintf(script->error, "lineNum:%d. result set null, sql:%s", line->lineNum, rest);
      return false;
    }

    TAOS_ROW row;

    while ((row = taos_fetch_row(result))) {
      if (numOfRows < MAX_QUERY_ROW_NUM) {
        TAOS_FIELD *fields = taos_fetch_fields(result);
        int* length = taos_fetch_lengths(result);
        
        for (int i = 0; i < num_fields; i++) {
          char *value = NULL;
          if (i < MAX_QUERY_COL_NUM) {
            value = script->data[numOfRows][i];
          }
          if (value == NULL) {
            continue;
          }

          if (row[i] == 0) {
            strcpy(value, TSDB_DATA_NULL_STR);
            continue;
          }

          switch (fields[i].type) {
            case TSDB_DATA_TYPE_BOOL:
              sprintf(value, "%s",
                      ((((int)(*((char *)row[i]))) == 1) ? "1" : "0"));
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
              sprintf(value, "%.5f", fv);
#else
              sprintf(value, "%.5f", *((float *)row[i]));
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
              memset(value, 0, MAX_QUERY_VALUE_LEN);
              memcpy(value, row[i], length[i]);
              value[length[i]] = 0;
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
        simError("script:%s, too many rows return from query", script->fileName);
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

bool simExecuteRestFulSqlCommand(SScript *script, char *rest) {
  SCmdLine *line = &script->lines[script->linePos];
  char command[4096];
  sprintf(command, "curl -H 'Authorization: Taosd %s' -d \"%s\" 127.0.0.1:6020/rest/sql", script->auth, rest);

  int ret = -1;
  for (int attempt = 0; attempt < 10; ++attempt) {
    ret = simExecuteRestFulCommand(script, command);
    if (ret == TSDB_CODE_TABLE_ALREADY_EXIST ||
        ret == TSDB_CODE_DB_ALREADY_EXIST) {
      simTrace("script:%s, taos:%p, %s success, ret:%d:%s", script->fileName, script->taos, rest, ret, tstrerror(ret));
      ret = 0;
      break;
    } else if (ret != 0) {
      simTrace("script:%s, taos:%p, %s failed, ret:%d",
               script->fileName, script->taos, rest, ret);

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
    sprintf(script->error, "lineNum:%d. sql:%s failed, ret:%d", line->lineNum, rest, ret);
    return false;
  }

  script->linePos++;
  return true;
}

bool simExecuteSqlImpCmd(SScript *script, char *rest, bool isSlow) {
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

  if ((!simAsyncQuery && script->taos == NULL) || (simAsyncQuery && script->auth[0] == 0)) {
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

  if (simAsyncQuery) {
    return simExecuteRestFulSqlCommand(script, rest);
  } else {
    return simExecuteNativeSqlCommand(script, rest, isSlow);
  }
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

  if ((!simAsyncQuery && script->taos == NULL) || (simAsyncQuery && script->auth[0] == 0)) {
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

  int ret;
  if (simAsyncQuery) {
    char command[4096];
    sprintf(command, "curl -H 'Authorization: Taosd %s' -d '%s' 127.0.0.1:6020/rest/sql", script->auth, rest);
    ret = simExecuteRestFulCommand(script, command);
  }
  else {
    ret = taos_query(script->taos, rest);
  }

  if (ret != TSDB_CODE_SUCCESS) {
    simTrace("script:%s, taos:%p, %s execute, expect failed, so success, ret:%d:%s",
        script->fileName, script->taos, rest, ret, tstrerror(ret));
    script->linePos++;
    return true;
  }
  sprintf(script->error, "lineNum:%d. sql:%s expect failed, but success, ret:%d:%s", line->lineNum, rest, ret, tstrerror(ret));

  return false;
}

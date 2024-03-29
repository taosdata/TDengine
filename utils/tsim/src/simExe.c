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
#include "simInt.h"

void simLogSql(char *sql, bool useSharp) {
  static TdFilePtr pFile = NULL;
  char             filename[256];
  sprintf(filename, "%s/sim.sql", simScriptDir);
  if (pFile == NULL) {
    pFile = taosOpenFile(filename, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);
  }

  if (pFile != NULL) {
    if (useSharp) {
      taosFprintfFile(pFile, "# %s;\n", sql);
    } else {
      taosFprintfFile(pFile, "%s;\n", sql);
    }

    UNUSED(taosFsyncFile(pFile));
  }
}

char *simParseHostName(char *varName) {
  static char hostName[140];
//#ifdef WINDOWS
//  hostName[0] = '\"';
//  taosGetFqdn(&hostName[1]);
//  int strEndIndex = strlen(hostName);
//  hostName[strEndIndex] = '\"';
//  hostName[strEndIndex + 1] = '\0';
//#else
  sprintf(hostName, "%s", "localhost");
//#endif
  return hostName;
}

static void simFindFirstNum(const char *begin, int32_t beginLen, int32_t *num) {
  *num = 0;

  if (beginLen > 5) {
    *num = atoi(begin + 5);
  }
}

static void simFindSecondNum(const char *begin, int32_t beginLen, int32_t *num) {
  *num = 0;

  const char *number = strstr(begin, "][");
  if (number != NULL) {
    *num = atoi(number + 2);
  }
}

static void simFindFirstKeyVal(const char *begin, int32_t beginLen, char *key, int32_t keyLen) {
  key[0] = 0;
  for (int32_t i = 5; i < beginLen && i - 5 < keyLen; ++i) {
    if (begin[i] != 0 && begin[i] != ']' && begin[i] != ')') {
      key[i - 5] = begin[i];
    }
  }
}

static void simFindSecondKeyNum(const char *begin, int32_t beginLen, int32_t *num) {
  *num = 0;

  const char *number = strstr(begin, ")[");
  if (number != NULL) {
    *num = atoi(number + 2);
  }
}

char *simGetVariable(SScript *script, char *varName, int32_t varLen) {
  if (strncmp(varName, "hostname", 8) == 0) {
    return simParseHostName(varName);
  }

  if (strncmp(varName, "error", varLen) == 0) return script->error;

  if (strncmp(varName, "rows", varLen) == 0) return script->rows;

  if (strncmp(varName, "cols", varLen) == 0) return script->cols;

  if (strncmp(varName, "system_exit", varLen) == 0) return script->system_exit_code;

  if (strncmp(varName, "system_content", varLen) == 0) return script->system_ret_content;

  if (strncmp(varName, "data", 4) == 0) {
    if (varLen < 6) {
      return "null";
    }

    int32_t row = 0;
    int32_t col = 0;
    char    keyVal[1024] = {0};
    int32_t keyLen = 1024;

    if (varName[4] == '[') {
      // $data[0][1]
      simFindFirstNum(varName, varLen, &row);
      simFindSecondNum(varName, varLen, &col);
      if (row < 0 || row >= MAX_QUERY_ROW_NUM) {
        return "null";
      }
      if (col < 0 || col >= MAX_QUERY_COL_NUM) {
        return "null";
      }
      simDebug("script:%s, data[%d][%d]=%s", script->fileName, row, col, script->data[row][col]);
      return script->data[row][col];
    } else if (varName[4] == '(') {
      // $data(db)[0]
      simFindFirstKeyVal(varName, varLen, keyVal, keyLen);
      simFindSecondKeyNum(varName, varLen, &col);
      for (int32_t i = 0; i < MAX_QUERY_ROW_NUM; ++i) {
        if (strncmp(keyVal, script->data[i][0], keyLen) == 0) {
          simDebug("script:%s, keyName:%s, keyValue:%s", script->fileName, script->data[i][0], script->data[i][col]);
          return script->data[i][col];
        }
      }
    } else if (varName[5] == '_') {
      // data2_db
      int32_t col = varName[4] - '0';
      col = col % MAX_QUERY_COL_NUM;

      char   *keyName;
      int32_t keyLen;
      paGetToken(varName + 6, &keyName, &keyLen);

      for (int32_t i = 0; i < MAX_QUERY_ROW_NUM; ++i) {
        if (strncmp(keyName, script->data[i][0], keyLen) == 0) {
          simDebug("script:%s, keyName:%s, keyValue:%s", script->fileName, script->data[i][0], script->data[i][col]);
          return script->data[i][col];
        }
      }
      return "null";
    } else if (varName[6] == '_') {
      // data21_db
      int32_t col = (varName[4] - '0') * 10 + (varName[5] - '0');
      col = col % MAX_QUERY_COL_NUM;

      char   *keyName;
      int32_t keyLen;
      paGetToken(varName + 7, &keyName, &keyLen);

      for (int32_t i = 0; i < MAX_QUERY_ROW_NUM; ++i) {
        if (strncmp(keyName, script->data[i][0], keyLen) == 0) {
          simTrace("script:%s, keyName:%s, keyValue:%s", script->fileName, script->data[i][0], script->data[i][col]);
          return script->data[i][col];
        }
      }
    } else {
      // $data00
      int32_t row = varName[4] - '0';
      int32_t col = varName[5] - '0';
      row = row % MAX_QUERY_ROW_NUM;
      col = col % MAX_QUERY_COL_NUM;

      simDebug("script:%s, data[%d][%d]=%s", script->fileName, row, col, script->data[row][col]);
      return script->data[row][col];
    }

    return "null";
  }

  for (int32_t i = 0; i < script->varLen; ++i) {
    SVariable *var = &script->variables[i];
    if (var->varNameLen != varLen) {
      continue;
    }
    if (strncmp(varName, var->varName, varLen) == 0) {
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

int32_t simExecuteExpression(SScript *script, char *exp) {
  char   *op1 = NULL;
  char   *op2 = NULL;
  char   *var1 = NULL;
  char   *var2 = NULL;
  char   *var3 = NULL;
  char   *rest = NULL;
  int32_t op1Len = 0;
  int32_t op2Len = 0;
  int32_t var1Len = 0;
  int32_t var2Len = 0;
  int32_t var3Len = 0;
  int32_t val0 = 0;
  int32_t val1 = 0;
  char    t0[2048] = {0};
  char    t1[2048] = {0};
  char    t2[2048] = {0};
  char    t3[2048] = {0};
  int32_t result = 0;

  rest = paGetToken(exp, &var1, &var1Len);
  rest = paGetToken(rest, &op1, &op1Len);
  rest = paGetToken(rest, &var2, &var2Len);
  rest = paGetToken(rest, &op2, &op2Len);

  if (var1[0] == '$')
    tstrncpy(t0, simGetVariable(script, var1 + 1, var1Len - 1), sizeof(t0));
  else {
    tstrncpy(t0, var1, var1Len);
    t0[var1Len] = 0;
  }

  if (var2[0] == '$') {
    tstrncpy(t1, simGetVariable(script, var2 + 1, var2Len - 1), sizeof(t1));
  } else {
    memcpy(t1, var2, var2Len);
    t1[var2Len] = 0;
  }

  if (op2Len != 0) {
    rest = paGetToken(rest, &var3, &var3Len);

    if (var3[0] == '$')
      tstrncpy(t2, simGetVariable(script, var3 + 1, var3Len - 1), sizeof(t2));
    else {
      memcpy(t2, var3, var3Len);
      t2[var3Len] = 0;
    }

    int64_t t1l = atoll(t1);
    int64_t t2l = atoll(t2);

    if (op2[0] == '+') {
      sprintf(t3, "%" PRId64, t1l + t2l);
    } else if (op2[0] == '-') {
      sprintf(t3, "%" PRId64, t1l - t2l);
    } else if (op2[0] == '*') {
      sprintf(t3, "%" PRId64, t1l * t2l);
    } else if (op2[0] == '/') {
      if (t2l == 0) {
        sprintf(t3, "%" PRId64, INT64_MAX);
      } else {
        sprintf(t3, "%" PRId64, t1l / t2l);
      }
    } else if (op2[0] == '.') {
      snprintf(t3, sizeof(t3), "%s%s", t1, t2);
    }
  } else {
    tstrncpy(t3, t1, sizeof(t3));
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
  int32_t result;
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

  simInfo("script:%s, start to execute", newScript->fileName);

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

  newScript->type = SIM_SCRIPT_TYPE_BACKGROUND;
  script->bgScripts[script->bgScriptLen++] = newScript;
  simInfo("script:%s, start to execute in background,", newScript->fileName);

  if (taosThreadCreate(&newScript->bgPid, NULL, simExecuteScript, (void *)newScript) != 0) {
    sprintf(script->error, "lineNum:%d. create background thread failed", script->lines[script->linePos].lineNum);
    return false;
  } else {
    simDebug("script:%s, background thread:0x%08" PRIx64 " is created", newScript->fileName,
             taosGetPthreadId(newScript->bgPid));
  }

  script->linePos++;
  return true;
}

void simReplaceDirSep (char *buf){
#ifdef WINDOWS
  int i=0;
  while(buf[i] != '\0')
  {
      if(buf[i] == '/')
      {
          buf[i] = '\\';
      }
      i++;
  }
#endif
}

bool simReplaceStr(char *buf, char *src, char *dst) {
  bool  replaced = false;
  char *begin = strstr(buf, src);
  if (begin != NULL) {
    int32_t srcLen = (int32_t)strlen(src);
    int32_t dstLen = (int32_t)strlen(dst);
    int32_t interval = (dstLen - srcLen);
    int32_t remainLen = (int32_t)strlen(buf);
    char   *end = buf + remainLen;
    *(end + interval) = 0;

    for (char *p = end; p >= begin; p--) {
      *(p + interval) = *p;
    }

    memcpy(begin, dst, dstLen);
    replaced = true;
  }

  simInfo("system cmd is %s", buf);
  return replaced;
}

bool simExecuteSystemCmd(SScript *script, char *option) {
  char buf[4096] = {0};
  bool replaced = false;

#ifndef WINDOWS
  sprintf(buf, "cd %s; ", simScriptDir);
  simVisuallizeOption(script, option, buf + strlen(buf));
#else
  sprintf(buf, "cd %s && ", simScriptDir);
  simVisuallizeOption(script, option, buf + strlen(buf));
  simReplaceStr(buf, ".sh", ".bat");
  simReplaceDirSep(buf);
#endif

  if (useValgrind) {
    replaced = simReplaceStr(buf, "exec.sh", "exec.sh -v");
  }

  simLogSql(buf, true);
  int32_t code = system(buf);
  int32_t repeatTimes = 0;
  while (code != 0) {
    simError("script:%s, failed to execute %s , code %d, errno:%d %s, repeatTimes:%d", script->fileName, buf, code,
             errno, strerror(errno), repeatTimes);
    taosMsleep(1000);
    taosDflSignal(SIGCHLD);
    if (repeatTimes++ >= 10) {
      exit(0);
    }
  }

  sprintf(script->system_exit_code, "%d", code);
  script->linePos++;
  if (replaced && strstr(buf, "start") != NULL) {
    simInfo("====> startup is slow in valgrind mode, so sleep 5 seconds after exec.sh -s start");
    taosMsleep(5000);
  }

  return true;
}

void simStoreSystemContentResult(SScript *script, char *filename) {
  memset(script->system_ret_content, 0, MAX_SYSTEM_RESULT_LEN);

  TdFilePtr pFile;
  // if ((fd = fopen(filename, "r")) != NULL) {
  if ((pFile = taosOpenFile(filename, TD_FILE_READ)) != NULL) {
    taosReadFile(pFile, script->system_ret_content, MAX_SYSTEM_RESULT_LEN - 1);
    int32_t len = strlen(script->system_ret_content);
    for (int32_t i = 0; i < len; ++i) {
      if (script->system_ret_content[i] == '\n' || script->system_ret_content[i] == '\r') {
        script->system_ret_content[i] = 0;
      }
    }
    taosCloseFile(&pFile);
    char rmCmd[MAX_FILE_NAME_LEN] = {0};
    sprintf(rmCmd, "rm -f %s", filename);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    system(rmCmd);
#pragma GCC diagnostic pop
  }
}

bool simExecuteSystemContentCmd(SScript *script, char *option) {
  char buf[4096] = {0};
  char buf1[4096 + 512] = {0};
  char filename[400] = {0};
  sprintf(filename, "%s" TD_DIRSEP "%s.tmp", simScriptDir, script->fileName);

#ifdef WINDOWS
  sprintf(buf, "cd %s && ", simScriptDir);
  simVisuallizeOption(script, option, buf + strlen(buf));
  simReplaceStr(buf, ".sh", ".bat");
  simReplaceDirSep(buf);
  sprintf(buf1, "%s > %s 2>nul", buf, filename);
#else
  sprintf(buf, "cd %s; ", simScriptDir);
  simVisuallizeOption(script, option, buf + strlen(buf));
  sprintf(buf1, "%s > %s 2>/dev/null", buf, filename);
#endif

  sprintf(script->system_exit_code, "%d", system(buf1));
  simStoreSystemContentResult(script, filename);

  script->linePos++;
  return true;
}

bool simExecuteSetBIModeCmd(SScript *script, char *option) {
  char    buf[1024];

  simVisuallizeOption(script, option, buf);
  option = buf;

  int32_t mode = atoi(option);

  simInfo("script:%s, set bi mode %d", script->fileName, mode);

  if (mode != 0) {
    taos_set_conn_mode(script->taos, TAOS_CONN_MODE_BI, 1);
  } else {
    taos_set_conn_mode(script->taos, TAOS_CONN_MODE_BI, 0);
  }

  script->linePos++;
  return true;
}

bool simExecutePrintCmd(SScript *script, char *rest) {
  char buf[65536];

  simVisuallizeOption(script, rest, buf);
  rest = buf;

  simInfo("script:%s, %s", script->fileName, rest);
  script->linePos++;
  return true;
}

bool simExecuteSleepCmd(SScript *script, char *option) {
  int32_t delta;
  char    buf[1024];

  simVisuallizeOption(script, option, buf);
  option = buf;

  delta = atoi(option);
  if (delta <= 0) delta = 5;

  simInfo("script:%s, sleep %dms begin", script->fileName, delta);
  taosMsleep(delta);
  simInfo("script:%s, sleep %dms finished", script->fileName, delta);

  char sleepStr[32] = {0};
  sprintf(sleepStr, "sleep %d", delta);
  simLogSql(sleepStr, true);

  script->linePos++;
  return true;
}

bool simExecuteReturnCmd(SScript *script, char *option) {
  char buf[1024];

  simVisuallizeOption(script, option, buf);
  option = buf;

  int32_t ret = 1;
  if (option && option[0] != 0) ret = atoi(option);

  if (ret < 0) {
    sprintf(script->error, "lineNum:%d. error return %s", script->lines[script->linePos].lineNum, option);
    return false;
  } else {
    simInfo("script:%s, return cmd execute with:%d", script->fileName, ret);
    script->linePos = script->numOfLines;
  }

  script->linePos++;
  return true;
}

void simVisuallizeOption(SScript *script, char *src, char *dst) {
  char   *var, *token, *value;
  int32_t dstLen, srcLen, tokenLen;

  dst[0] = 0, dstLen = 0;

  while (1) {
    var = strchr(src, '$');
    if (var == NULL) break;

#if 0
    if (var && ((var - src - 1) > 0) && *(var - 1) == '\\') {
      srcLen = (int32_t)(var - src - 1);
      memcpy(dst + dstLen, src, srcLen);
      dstLen += srcLen;
      src = var;
      break;
    }
#endif

    srcLen = (int32_t)(var - src);
    memcpy(dst + dstLen, src, srcLen);
    dstLen += srcLen;

    src = paGetToken(var + 1, &token, &tokenLen);
    value = simGetVariable(script, token, tokenLen);

    strcpy(dst + dstLen, value);
    dstLen += (int32_t)strlen(value);
  }

  strcpy(dst + dstLen, src);
}

void simCloseNativeConnect(SScript *script) {
  if (script->taos == NULL) return;

  simDebug("script:%s, taos:%p closed", script->fileName, script->taos);
  taos_close(script->taos);

  script->taos = NULL;
}

void simCloseTaosdConnect(SScript *script) { simCloseNativeConnect(script); }

bool simCreateNativeConnect(SScript *script, char *user, char *pass) {
  simCloseTaosdConnect(script);
  void *taos = NULL;
  for (int32_t attempt = 0; attempt < 10; ++attempt) {
    if (abortExecution) {
      script->killed = true;
      return false;
    }

    taos = taos_connect(NULL, user, pass, NULL, 0);
    if (taos == NULL) {
      simDebug("script:%s, user:%s connect taosd failed:%s, attempt:%d", script->fileName, user, taos_errstr(NULL),
               attempt);
      taosMsleep(1000);
    } else {
      simDebug("script:%s, user:%s connect taosd successed, attempt:%d", script->fileName, user, attempt);
      break;
    }
  }

  if (taos == NULL) {
    sprintf(script->error, "lineNum:%d. connect taosd failed:%s", script->lines[script->linePos].lineNum,
            taos_errstr(NULL));
    return false;
  }

  script->taos = taos;
  simDebug("script:%s, connect taosd successed, taos:%p", script->fileName, taos);

  return true;
}

bool simCreateTaosdConnect(SScript *script, char *rest) {
  char   *user = TSDB_DEFAULT_USER;
  char   *token;
  int32_t tokenLen;
  rest = paGetToken(rest, &token, &tokenLen);
  rest = paGetToken(rest, &token, &tokenLen);
  if (tokenLen != 0) {
    user = token;
  }

  return simCreateNativeConnect(script, user, TSDB_DEFAULT_PASS);
}

bool simExecuteNativeSqlCommand(SScript *script, char *rest, bool isSlow) {
  char      timeStr[80] = {0};
  time_t    tt;
  struct tm tp;
  SCmdLine *line = &script->lines[script->linePos];
  int32_t   ret = -1;

  TAOS_RES *pSql = NULL;

  for (int32_t attempt = 0; attempt < 10; ++attempt) {
    if (abortExecution) {
      script->killed = true;
      return false;
    }

    simLogSql(rest, false);
    pSql = taos_query(script->taos, rest);
    ret = taos_errno(pSql);

    if (ret == TSDB_CODE_MND_STB_ALREADY_EXIST || ret == TSDB_CODE_MND_DB_ALREADY_EXIST) {
      simDebug("script:%s, taos:%p, %s success, ret:%d:%s", script->fileName, script->taos, rest, ret & 0XFFFF,
               tstrerror(ret));
      ret = 0;
      break;
    } else if (ret != 0) {
      simDebug("script:%s, taos:%p, %s failed, ret:%d:%s", script->fileName, script->taos, rest, ret & 0XFFFF,
               tstrerror(ret));

      if (line->errorJump == SQL_JUMP_TRUE) {
        script->linePos = line->jump;
        taos_free_result(pSql);
        return true;
      }
      taosMsleep(1000);
    } else {
      break;
    }

    taos_free_result(pSql);
  }

  if (ret) {
    sprintf(script->error, "lineNum:%d. sql:%s failed, ret:%d:%s", line->lineNum, rest, ret & 0XFFFF, tstrerror(ret));
    return false;
  }

  int32_t numOfRows = 0;
  int32_t num_fields = taos_field_count(pSql);
  if (num_fields != 0) {
    TAOS_ROW row;

    while ((row = taos_fetch_row(pSql))) {
      if (numOfRows < MAX_QUERY_ROW_NUM) {
        TAOS_FIELD *fields = taos_fetch_fields(pSql);
        int32_t    *length = taos_fetch_lengths(pSql);

        for (int32_t i = 0; i < num_fields; i++) {
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
              sprintf(value, "%s", ((((int32_t)(*((char *)row[i]))) == 1) ? "1" : "0"));
              break;
            case TSDB_DATA_TYPE_TINYINT:
              sprintf(value, "%d", *((int8_t *)row[i]));
              break;
            case TSDB_DATA_TYPE_UTINYINT:
              sprintf(value, "%u", *((uint8_t *)row[i]));
              break;
            case TSDB_DATA_TYPE_SMALLINT:
              sprintf(value, "%d", *((int16_t *)row[i]));
              break;
            case TSDB_DATA_TYPE_USMALLINT:
              sprintf(value, "%u", *((uint16_t *)row[i]));
              break;
            case TSDB_DATA_TYPE_INT:
              sprintf(value, "%d", *((int32_t *)row[i]));
              break;
            case TSDB_DATA_TYPE_UINT:
              sprintf(value, "%u", *((uint32_t *)row[i]));
              break;
            case TSDB_DATA_TYPE_BIGINT:
              sprintf(value, "%" PRId64, *((int64_t *)row[i]));
              break;
            case TSDB_DATA_TYPE_UBIGINT:
              sprintf(value, "%" PRIu64, *((uint64_t *)row[i]));
              break;
            case TSDB_DATA_TYPE_FLOAT:
              sprintf(value, "%.5f", GET_FLOAT_VAL(row[i]));
              break;
            case TSDB_DATA_TYPE_DOUBLE:
              sprintf(value, "%.9lf", GET_DOUBLE_VAL(row[i]));
              break;
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_GEOMETRY:
              if (length[i] < 0 || length[i] > 1 << 20) {
                fprintf(stderr, "Invalid length(%d) of BINARY or NCHAR\n", length[i]);
                exit(-1);
              }

              memset(value, 0, MAX_QUERY_VALUE_LEN);
              memcpy(value, row[i], length[i]);
              value[length[i]] = 0;
              // snprintf(value, fields[i].bytes, "%s", (char *)row[i]);
              break;
            case TSDB_DATA_TYPE_TIMESTAMP: {
              int32_t precision = taos_result_precision(pSql);
              if (precision == TSDB_TIME_PRECISION_MILLI) {
                tt = (*(int64_t *)row[i]) / 1000;
              } else if (precision == TSDB_TIME_PRECISION_MICRO) {
                tt = (*(int64_t *)row[i]) / 1000000;
              } else {
                tt = (*(int64_t *)row[i]) / 1000000000;
              }

              if (taosLocalTime(&tt, &tp, timeStr) == NULL) {
                break;
              }
              strftime(timeStr, 64, "%y-%m-%d %H:%M:%S", &tp);
              if (precision == TSDB_TIME_PRECISION_MILLI) {
                sprintf(value, "%s.%03d", timeStr, (int32_t)(*((int64_t *)row[i]) % 1000));
              } else if (precision == TSDB_TIME_PRECISION_MICRO) {
                sprintf(value, "%s.%06d", timeStr, (int32_t)(*((int64_t *)row[i]) % 1000000));
              } else {
                sprintf(value, "%s.%09d", timeStr, (int32_t)(*((int64_t *)row[i]) % 1000000000));
              }

              break;
            }
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

  } else {
    numOfRows = taos_affected_rows(pSql);
  }

  taos_free_result(pSql);
  sprintf(script->rows, "%d", numOfRows);
  sprintf(script->cols, "%d", num_fields);

  script->linePos++;
  return true;
}

bool simExecuteSqlImpCmd(SScript *script, char *rest, bool isSlow) {
  char      buf[3000];
  SCmdLine *line = &script->lines[script->linePos];

  simVisuallizeOption(script, rest, buf);
  rest = buf;

  simDebug("script:%s, exec:%s", script->fileName, rest);
  strcpy(script->rows, "-1");
  strcpy(script->cols, "-1");
  for (int32_t row = 0; row < MAX_QUERY_ROW_NUM; ++row) {
    for (int32_t col = 0; col < MAX_QUERY_COL_NUM; ++col) {
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

  return simExecuteNativeSqlCommand(script, rest, isSlow);
}

bool simExecuteSqlCmd(SScript *script, char *rest) {
  bool isSlow = false;
  return simExecuteSqlImpCmd(script, rest, isSlow);
}

bool simExecuteSqlSlowCmd(SScript *script, char *rest) {
  bool isSlow = true;
  return simExecuteSqlImpCmd(script, rest, isSlow);
}

#if 0
bool simExecuteRestfulCmd(SScript *script, char *rest) {
  TdFilePtr pFile = NULL;
  char      filename[256];
  sprintf(filename, "%s/tmp.sql", simScriptDir);
  // fp = fopen(filename, "w");
  pFile = taosOpenFile(filename, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);
  if (pFile == NULL) {
    fprintf(stderr, "ERROR: failed to open file: %s\n", filename);
    return false;
  }

  char    db[64] = {0};
  char    tb[64] = {0};
  char    gzip[32] = {0};
  int32_t ts;
  int32_t times;
  sscanf(rest, "%s %s %d %d %s", db, tb, &ts, &times, gzip);

  taosFprintfFile(pFile, "insert into %s.%s values ", db, tb);
  for (int32_t i = 0; i < times; ++i) {
    taosFprintfFile(pFile, "(%d000, %d)", ts + i, ts);
  }
  taosFprintfFile(pFile, "  \n");
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);

  char cmd[1024] = {0};
  if (strcmp(gzip, "gzip") == 0) {
    sprintf(cmd,
            "curl -H 'Authorization: Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04' --header "
            "--compressed --data-ascii @%s 127.0.0.1:7111/rest/sql",
            filename);
  } else {
    sprintf(cmd,
            "curl -H 'Authorization: Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04' --header "
            "'Transfer-Encoding: chunked' --data-ascii @%s 127.0.0.1:7111/rest/sql",
            filename);
  }

  return simExecuteSystemCmd(script, cmd);
}
#endif

bool simExecuteSqlErrorCmd(SScript *script, char *rest) {
  char      buf[3000];
  SCmdLine *line = &script->lines[script->linePos];

  simVisuallizeOption(script, rest, buf);
  rest = buf;

  simDebug("script:%s, exec:%s", script->fileName, rest);
  strcpy(script->rows, "-1");
  strcpy(script->cols, "-1");
  for (int32_t row = 0; row < MAX_QUERY_ROW_NUM; ++row) {
    for (int32_t col = 0; col < MAX_QUERY_COL_NUM; ++col) {
      strcpy(script->data[row][col], "null");
    }
  }

  TAOS_RES *pSql = taos_query(script->taos, rest);
  int32_t   ret = taos_errno(pSql);
  taos_free_result(pSql);

  if (ret != TSDB_CODE_SUCCESS) {
    simDebug("script:%s, taos:%p, %s execute, expect failed, so success, ret:%d:%s", script->fileName, script->taos,
             rest, ret & 0XFFFF, tstrerror(ret));
    script->linePos++;
    return true;
  }

  sprintf(script->error, "lineNum:%d. sql:%s expect failed, but success, ret:%d:%s", line->lineNum, rest, ret & 0XFFFF,
          tstrerror(ret));

  return false;
}

#if 0
bool simExecuteLineInsertCmd(SScript *script, char *rest) {
  char buf[TSDB_MAX_BINARY_LEN] = {0};

  simVisuallizeOption(script, rest, buf);
  rest = buf;

  SCmdLine *line = &script->lines[script->linePos];

  simInfo("script:%s, %s", script->fileName, rest);
  simLogSql(buf, true);
  char *lines[] = {rest};
#if 0
  int32_t ret = taos_insert_lines(script->taos, lines, 1);
    if (ret == TSDB_CODE_SUCCESS) {
    simDebug("script:%s, taos:%p, %s executed. success.", script->fileName, script->taos, rest);
    script->linePos++;
    return true;
  } else {
    sprintf(script->error, "lineNum: %d. line: %s failed, ret:%d:%s", line->lineNum, rest, ret & 0XFFFF,
            tstrerror(ret));
    return false;
  }
#else
  simDebug("script:%s, taos:%p, %s executed. success.", script->fileName, script->taos, rest);
  script->linePos++;
  return true;
#endif
}

bool simExecuteLineInsertErrorCmd(SScript *script, char *rest) {
  char buf[TSDB_MAX_BINARY_LEN];

  simVisuallizeOption(script, rest, buf);
  rest = buf;

  SCmdLine *line = &script->lines[script->linePos];

  simInfo("script:%s, %s", script->fileName, rest);
  simLogSql(buf, true);
  char *lines[] = {rest};
#if 0
  int32_t ret = taos_insert_lines(script->taos, lines, 1);
#else
  int32_t ret = 0;
#endif
  if (ret == TSDB_CODE_SUCCESS) {
    sprintf(script->error, "script:%s, taos:%p, %s executed. expect failed, but success.", script->fileName,
            script->taos, rest);
    script->linePos++;
    return false;
  } else {
    simDebug("lineNum: %d. line: %s failed, ret:%d:%s. Expect failed, so success", line->lineNum, rest, ret & 0XFFFF,
             tstrerror(ret));
    return true;
  }
}
#endif

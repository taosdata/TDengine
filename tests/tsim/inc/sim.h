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

#ifndef __SIM_H__
#define __SIM_H__

#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>

#include "taos.h"
#include "tidpool.h"
#include "tlog.h"
#include "tutil.h"

#define MAX_MAIN_SCRIPT_NUM 10
#define MAX_BACKGROUND_SCRIPT_NUM 10
#define MAX_FILE_NAME_LEN 256
#define MAX_ERROR_LEN 1024
#define MAX_QUERY_VALUE_LEN 40
#define MAX_QUERY_COL_NUM 10
#define MAX_QUERY_ROW_NUM 10
#define MAX_SYSTEM_RESULT_LEN 2048
#define MAX_VAR_LEN 100
#define MAX_VAR_NAME_LEN 32
#define MAX_VAR_VAL_LEN 80
#define MAX_OPT_NAME_LEN 32
#define MAX_SIM_CMD_NAME_LEN 40

#ifdef LINUX
#define SUCCESS_PREFIX "\033[44;32;1m"
#define SUCCESS_POSTFIX "\033[0m"
#define FAILED_PREFIX "\033[44;31;1m"
#define FAILED_POSTFIX "\033[0m"
#else
#define SUCCESS_PREFIX ""
#define SUCCESS_POSTFIX ""
#define FAILED_PREFIX ""
#define FAILED_POSTFIX ""
#endif

#define simError(...)                        \
  if (simDebugFlag & DEBUG_ERROR) {          \
    taosPrintLog("ERROR SIM ", 255, __VA_ARGS__); \
  }
#define simWarn(...)                                 \
  if (simDebugFlag & DEBUG_WARN) {                   \
    taosPrintLog("WARN SIM ", simDebugFlag, __VA_ARGS__); \
  }
#define simTrace(...)                           \
  if (simDebugFlag & DEBUG_TRACE) {             \
    taosPrintLog("SIM ", simDebugFlag, __VA_ARGS__); \
  }
#define simDump(x, y)              \
  if (simDebugFlag & DEBUG_DUMP) { \
    taosDumpData(x, y);            \
  }
#define simPrint(...) \
  { taosPrintLog("SIM ", 255, __VA_ARGS__); }

enum { SIM_SCRIPT_TYPE_MAIN, SIM_SCRIPT_TYPE_BACKGROUND };

enum {
  SIM_CMD_EXP,
  SIM_CMD_IF,
  SIM_CMD_ELIF,
  SIM_CMD_ELSE,
  SIM_CMD_ENDI,
  SIM_CMD_WHILE,
  SIM_CMD_ENDW,
  SIM_CMD_SWITCH,
  SIM_CMD_CASE,
  SIM_CMD_DEFAULT,
  SIM_CMD_CONTINUE,
  SIM_CMD_BREAK,
  SIM_CMD_ENDS,
  SIM_CMD_SLEEP,
  SIM_CMD_GOTO,
  SIM_CMD_RUN,
  SIM_CMD_RUN_BACK,
  SIM_CMD_PRINT,
  SIM_CMD_SYSTEM,
  SIM_CMD_SYSTEM_CONTENT,
  SIM_CMD_SQL,
  SIM_CMD_SQL_ERROR,
  SIM_CMD_SQL_SLOW,
  SIM_CMD_TEST,
  SIM_CMD_RETURN,
  SIM_CMD_END
};

enum {
  SQL_JUMP_FALSE,
  SQL_JUMP_TRUE,
};

struct _script_t;
typedef struct _cmd_t {
  short cmdno;
  short nlen;
  char name[MAX_SIM_CMD_NAME_LEN];
  bool (*parseCmd)(char *, struct _cmd_t *, int);
  bool (*executeCmd)(struct _script_t *script, char *option);
  struct _cmd_t *next;
} SCommand;

typedef struct {
  short cmdno;
  short jump;        // jump position
  short errorJump;   // sql jump flag, while '-x' exist in sql cmd, this flag
                     // will be SQL_JUMP_TRUE, otherwise is SQL_JUMP_FALSE */
  short lineNum;     // correspodning line number in original file
  int optionOffset;  // relative option offset
} SCmdLine;

typedef struct _var_t {
  char varName[MAX_VAR_NAME_LEN];
  char varValue[MAX_VAR_VAL_LEN];
  char varNameLen;
} SVariable;

typedef struct _script_t {
  int type;
  bool killed;

  void *taos;
  char rows[12];  // number of rows data retrieved
  char data[MAX_QUERY_ROW_NUM][MAX_QUERY_COL_NUM]
           [MAX_QUERY_VALUE_LEN];  // query results
  char system_exit_code[12];
  char system_ret_content[MAX_SYSTEM_RESULT_LEN];

  int varLen;
  int linePos;     // current cmd position
  int numOfLines;  // number of lines in the script
  int bgScriptLen;
  char fileName[MAX_FILE_NAME_LEN];  // script file name
  char error[MAX_ERROR_LEN];
  char *optionBuffer;
  SCmdLine *lines;  // command list
  SVariable variables[MAX_VAR_LEN];
  struct _script_t *bgScripts[MAX_BACKGROUND_SCRIPT_NUM];
  char auth[128];
} SScript;

extern SScript *simScriptList[MAX_MAIN_SCRIPT_NUM];
extern SCommand simCmdList[];
extern int simScriptPos;
extern int simScriptSucced;
extern int simDebugFlag;
extern char tsScriptDir[];
extern bool simAsyncQuery;

SScript *simParseScript(char *fileName);

SScript *simProcessCallOver(SScript *script);
void *simExecuteScript(void *script);
void simInitsimCmdList();
bool simSystemInit();
void simSystemCleanUp();
char *simGetVariable(SScript *script, char *varName, int varLen);
bool simExecuteExpCmd(SScript *script, char *option);
bool simExecuteTestCmd(SScript *script, char *option);
bool simExecuteGotoCmd(SScript *script, char *option);
bool simExecuteRunCmd(SScript *script, char *option);
bool simExecuteRunBackCmd(SScript *script, char *option);
bool simExecuteSystemCmd(SScript *script, char *option);
bool simExecuteSystemContentCmd(SScript *script, char *option);
bool simExecutePrintCmd(SScript *script, char *option);
bool simExecuteSleepCmd(SScript *script, char *option);
bool simExecuteReturnCmd(SScript *script, char *option);
bool simExecuteSqlCmd(SScript *script, char *option);
bool simExecuteSqlErrorCmd(SScript *script, char *rest);
bool simExecuteSqlSlowCmd(SScript *script, char *option);
void simVisuallizeOption(SScript *script, char *src, char *dst);

#endif
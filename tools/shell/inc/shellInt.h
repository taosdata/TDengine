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

#ifndef _TD_SHELL_INT_H_
#define _TD_SHELL_INT_H_

#include "os.h"
#include "taos.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tconfig.h"
#include "tglobal.h"
#include "trpc.h"
#include "ttypes.h"
#include "tutil.h"

#ifdef WEBSOCKET
#include "taosws.h"

#define SHELL_WS_TIMEOUT                        30
#define SHELL_WS_DSN_BUFF                       256
#define SHELL_WS_DSN_MASK                       10
#endif

#define SHELL_MAX_HISTORY_SIZE                 1000
#define SHELL_MAX_COMMAND_SIZE                 1048586
#define SHELL_HISTORY_FILE                     ".taos_history"
#define SHELL_DEFAULT_RES_SHOW_NUM             100
#define SHELL_DEFAULT_MAX_BINARY_DISPLAY_WIDTH 30
#define SHELL_MAX_PKG_LEN                      2 * 1024 * 1024
#define SHELL_MIN_PKG_LEN                      1
#define SHELL_DEF_PKG_LEN                      1024
#define SHELL_MAX_PKG_NUM                      1 * 1024 * 1024
#define SHELL_MIN_PKG_NUM                      1
#define SHELL_DEF_PKG_NUM                      100
#define SHELL_FLOAT_WIDTH                      20
#define SHELL_DOUBLE_WIDTH                     25

typedef struct {
  char*   hist[SHELL_MAX_HISTORY_SIZE];
  char    file[TSDB_FILENAME_LEN];
  int32_t hstart;
  int32_t hend;
} SShellHistory;

typedef struct {
  const char* host;
  const char* user;
  const char* auth;
  const char* database;
  const char* cfgdir;
  const char* commands;
  const char* netrole;
  char        file[PATH_MAX];
  char        password[TSDB_USET_PASSWORD_LEN];
  bool        is_gen_auth;
  bool        is_bi_mode;
  bool        is_raw_time;
  bool        is_version;
  bool        is_dump_config;
  bool        is_check;
  bool        is_startup;
  bool        is_help;
  int32_t     port;
  int32_t     pktLen;
  int32_t     pktNum;
  int32_t     displayWidth;
  int32_t     abort;
#ifdef WEBSOCKET
  bool        restful;
  bool        cloud;
  bool        local;
  char*       dsn;
  int32_t     timeout;
#endif
} SShellArgs;

typedef struct {
  const char *clientVersion;
  char cusName[32];
  char promptHeader[32];
  char promptContinue[32];
  const char* osname;
  int32_t     promptSize;
  char        programVersion[256];
} SShellOsDetails;

typedef struct {
  SShellArgs      args;
  SShellHistory   history;
  SShellOsDetails info;
  TAOS*           conn;
  TdThread        pid;
  tsem_t          cancelSem;
  bool            exit;
#ifdef WEBSOCKET
  WS_TAOS*        ws_conn;
  bool            stop_query;
#endif
} SShellObj;

typedef struct {
  char    *buffer;
  char    *command;
  uint32_t commandSize;
  uint32_t bufferSize;
  uint32_t cursorOffset;
  uint32_t screenOffset;
  uint32_t endOffset;
} SShellCmd;

// shellArguments.c
int32_t shellParseArgs(int32_t argc, char* argv[]);

// shellCommand.c
int32_t shellReadCommand(char* command);

// shellEngine.c
int32_t shellExecute();
int32_t shellCalcColWidth(TAOS_FIELD *field, int32_t precision);
void    shellPrintHeader(TAOS_FIELD *fields, int32_t *width, int32_t num_fields);
void    shellPrintField(const char *val, TAOS_FIELD *field, int32_t width, int32_t length, int32_t precision);
void    shellDumpFieldToFile(TdFilePtr pFile, const char *val, TAOS_FIELD *field, int32_t length, int32_t precision); 
// shellUtil.c
int32_t shellCheckIntSize();
void    shellPrintVersion();
void    shellPrintHelp();
void    shellGenerateAuth();
void    shellDumpConfig();
void    shellCheckServerStatus();
bool    shellRegexMatch(const char* s, const char* reg, int32_t cflags);
void    shellExit();

// shellNettest.c
void shellTestNetWork();

#ifdef WEBSOCKET
void shellCheckConnectMode();
// shellWebsocket.c
int shell_conn_ws_server(bool first);
int32_t shell_run_websocket();
void shellRunSingleCommandWebsocketImp(char *command); 
#endif

// shellMain.c
extern SShellObj shell;
extern void tscWriteCrashInfo(int signum, void *sigInfo, void *context);

#endif /*_TD_SHELL_INT_H_*/

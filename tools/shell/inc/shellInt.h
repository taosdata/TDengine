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
#include "ttypes.h"
#include "tutil.h"

#define SHELL_MAX_HISTORY_SIZE                 1000
#define SHELL_MAX_COMMAND_SIZE                 1048586
#define SHELL_HISTORY_FILE                     ".taos_history"
#define SHELL_DEFAULT_RES_SHOW_NUM             100
#define SHELL_DEFAULT_MAX_BINARY_DISPLAY_WIDTH 30

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
  bool        is_raw_time;
  bool        is_version;
  bool        is_dump_config;
  bool        is_check;
  bool        is_startup;
  bool        is_help;
  uint16_t    port;
  int32_t     pktLen;
  int32_t     pktNum;
  int32_t     displayWidth;
  int32_t     abort;
} SShellArgs;

typedef struct {
  const char* clientVersion;
  const char* promptHeader;
  const char* promptContinue;
  const char* osname;
  int32_t     promptSize;
  char        programVersion[32];
} SShellOsDetails;

typedef struct {
  SShellArgs      args;
  SShellHistory   history;
  SShellOsDetails info;
  TAOS*           conn;
  TdThread        pid;
  tsem_t          cancelSem;
  int64_t         result;
} SShellObj;

// shellArguments.c
int32_t shellParseArgs(int32_t argc, char* argv[]);

// shellCommand.c
int32_t shellReadCommand(char* command);

// shellEngine.c
int32_t shellExecute();

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

// shellMain.c
extern SShellObj shell;
extern void      taos_init();

#endif /*_TD_SHELL_INT_H_*/

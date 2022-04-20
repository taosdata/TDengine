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

#include <regex.h>
#include <wordexp.h>

#define MAX_HISTORY_SIZE     1000
#define MAX_COMMAND_SIZE     1048586
#define HISTORY_FILE         ".taos_history"
#define DEFAULT_RES_SHOW_NUM 100

typedef struct {
  char*   hist[MAX_HISTORY_SIZE];
  int32_t hstart;
  int32_t hend;
} SShellHistory;

typedef struct {
  const char* host;
  const char* password;
  const char* user;
  const char* auth;
  const char* database;
  const char* file;
  const char* cfgdir;
  const char* commands;
  bool        is_gen_auth;
  bool        is_raw_time;
  bool        is_client;
  bool        is_server;
  bool        is_version;
  bool        is_dump_config;
  bool        is_check;
  bool        is_startup;
  bool        is_help;
  uint16_t    port;
  int32_t     pktLen;
  int32_t     pktNum;
  int32_t     abort;
} SShellArgs;

typedef struct {
  SShellArgs    args;
  SShellHistory history;
  TAOS*         conn;
  int64_t       result;
} SShellObj;

int32_t shellParseArgs(int32_t argc, char* argv[]);
int32_t shellInit();
void    shellCleanup(void* arg);
void    shellExit();

void*   shellThreadLoop(void* arg);
void    shellPrintError(TAOS_RES* tres, int64_t st);
int32_t shellRegexMatch(const char* s, const char* reg, int32_t cflags);
void    shellGetGrantInfo();
void    shellReadHistory();
void    shellWriteHistory();
void    shellHistoryPath(char* history);

int32_t shellReadCommand(char command[]);
int32_t shellRunCommand(char* command);
void    shellRunCommandImp(char command[]);
void    shellSourceFile(TAOS* con, char* fptr);
int32_t shellDumpResult(TAOS_RES* con, char* fname, int32_t* error_no, bool printMode);

extern char      PROMPT_HEADER[];
extern char      CONTINUE_PROMPT[];
extern int32_t   prompt_size;
extern SShellObj shell;

#endif /*_TD_SHELL_INT_H_*/

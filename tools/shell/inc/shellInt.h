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

#ifndef _TD_SHELL_H_
#define _TD_SHELL_H_

#include "os.h"

#include "taos.h"
#include "taosdef.h"

#define MAX_HISTORY_SIZE     1000
#define MAX_COMMAND_SIZE     1048586
#define HISTORY_FILE         ".taos_history"
#define DEFAULT_RES_SHOW_NUM 100

typedef struct SShellHistory {
  char* hist[MAX_HISTORY_SIZE];
  int   hstart;
  int   hend;
} SShellHistory;

typedef struct SShellArguments {
  char* host;
  char* password;
  char* user;
  char* auth;
  char* database;
  char* timezone;
  bool  is_raw_time;
  bool  is_use_passwd;
  bool  dump_config;
  char  file[TSDB_FILENAME_LEN];
  char  dir[TSDB_FILENAME_LEN];
  int   threadNum;
  int   check;
  bool  status;
  bool  verbose;
  char* commands;
  int   abort;
  int   port;
  int   pktLen;
  int   pktNum;
  char* pktType;
  char* netTestRole;
} SShellArguments;

/**************** Function declarations ****************/
extern void shellParseArgument(int argc, char* argv[], SShellArguments* arguments);
extern TAOS* shellInit(SShellArguments* args);
extern void* shellLoopQuery(void* arg);
extern void taos_error(TAOS_RES* tres, int64_t st);
extern int regex_match(const char* s, const char* reg, int cflags);
int32_t shellReadCommand(TAOS* con, char command[]);
int32_t shellRunCommand(TAOS* con, char* command);
void shellRunCommandOnServer(TAOS* con, char command[]);
void read_history();
void write_history();
void source_file(TAOS* con, char* fptr);
void source_dir(TAOS* con, SShellArguments* args);
void get_history_path(char* history);
void shellCheck(TAOS* con, SShellArguments* args);
void cleanup_handler(void* arg);
void exitShell();
int shellDumpResult(TAOS_RES* con, char* fname, int* error_no, bool printMode);
void shellGetGrantInfo(void *con);
int isCommentLine(char *line);

/**************** Global variable declarations ****************/
extern char           PROMPT_HEADER[];
extern char           CONTINUE_PROMPT[];
extern int            prompt_size;
extern SShellHistory  history;
extern SShellArguments args;
extern int64_t         result;

#endif

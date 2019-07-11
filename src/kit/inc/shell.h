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

#ifndef __SHELL__
#define __SHELL__

#include "stdbool.h"
#include "taos.h"
#include "tlog.h"
#include "tsdb.h"

#define MAX_USERNAME_SIZE      64
#define MAX_DBNAME_SIZE        64
#define MAX_IP_SIZE            20
#define MAX_PASSWORD_SIZE      20
#define MAX_HISTORY_SIZE       1000
#define MAX_COMMAND_SIZE       65536
#define HISTORY_FILE           ".taos_history"

#define BOOL_OUTPUT_LENGTH     6
#define TINYINT_OUTPUT_LENGTH  6
#define SMALLINT_OUTPUT_LENGTH 7
#define INT_OUTPUT_LENGTH      11
#define BIGINT_OUTPUT_LENGTH   21
#define FLOAT_OUTPUT_LENGTH    20
#define DOUBLE_OUTPUT_LENGTH   25
#define BINARY_OUTPUT_LENGTH   20

// dynamic config timestamp width according to maximum time precision
extern int32_t TIMESTAMP_OUTPUT_LENGTH;

typedef struct History History;
struct History {
  char* hist[MAX_HISTORY_SIZE];
  int   hstart;
  int   hend;
};

struct arguments {
  char* host;
  char* password;
  char* user;
  char* database;
  char* timezone;
  bool  is_raw_time;
  bool  is_use_passwd;
  char  file[TSDB_FILENAME_LEN];
  char* commands;
  int   abort;
};

/**************** Function declarations ****************/
extern void shellParseArgument(int argc, char* argv[], struct arguments* arguments);
extern TAOS* shellInit(struct arguments* args);
extern void* shellLoopQuery(void* arg);
extern void taos_error(TAOS* con);
extern int regex_match(const char* s, const char* reg, int cflags);
void shellReadCommand(TAOS* con, char command[]);
void shellRunCommand(TAOS* con, char* command);
void shellRunCommandOnServer(TAOS* con, char command[]);
void read_history();
void write_history();
void source_file(TAOS* con, char* fptr);
void get_history_path(char* history);
void cleanup_handler(void* arg);
void exitShell();
int shellDumpResult(TAOS* con, char* fname, int* error_no);
void shellPrintNChar(char* str, int width);
#define max(a, b) ((int)(a) < (int)(b) ? (int)(b) : (int)(a))

/**************** Global variable declarations ****************/
extern char           PROMPT_HEADER[];
extern char           CONTINUE_PROMPT[];
extern int            prompt_size;
extern History        history;
extern struct termios oldtio;
extern void           set_terminal_mode();
extern int get_old_terminal_mode(struct termios* tio);
extern void             reset_terminal_mode();
extern struct arguments args;
extern TAOS_RES*        result;

#endif

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
#if !(defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32))
#include <sys/socket.h>
#else
#include <winsock2.h>
#pragma comment(lib,"ws2_32.lib")
#endif
#include "cJSON.h"
#include "stdbool.h"
#include "taos.h"
#include "taosdef.h"
#include "tsclient.h"

#define MAX_USERNAME_SIZE      64
#define MAX_DBNAME_SIZE        64
#define MAX_IP_SIZE            20
#define MAX_HISTORY_SIZE       1000
#define MAX_COMMAND_SIZE       1048586
#define HISTORY_FILE           ".taos_history"
#define DEFAULT_RES_SHOW_NUM   100
#define TEMP_RECV_BUF          1024

typedef struct SShellHistory {
  char* hist[MAX_HISTORY_SIZE];
  int   hstart;
  int   hend;
} SShellHistory;

typedef enum enumWebSocketFrameType {
  TEXT_FRAME = 0x81,
  PING_FRAME = 0x89,
  PONG_FRAME = 0x8A,
  BINARY_FRAME = 0x82
} WebSocketFrameType;

typedef struct SWSParser {
  int offset;
  int payload_length;
  WebSocketFrameType frame;
} SWSParser;

typedef struct SShellArguments {
  char* host;
  char* password;
  char* user;
  char* auth;
  char* database;
  char* timezone;
  bool  restful;
#ifdef WINDOWS
  SOCKET socket;
#else
  int socket;
#endif
  TAOS* con;
  bool  is_raw_time;
  bool  is_use_passwd;
  bool  dump_config;
  char  file[TSDB_FILENAME_LEN];
  char  dir[TSDB_FILENAME_LEN];
  int   threadNum;
  int   check;
  char* commands;
  int   abort;
  int   port;
  int   pktLen;
  int   pktNum;
  char* pktType;
  char* netTestRole;
  char* cloudDsn;
  bool  cloud;
  char* cloudHost;
  char* cloudPort;
  char* cloudToken;
  int64_t id;
  int64_t st;
  char * response_buffer;
  TAOS_FIELD *fields;
} SShellArguments;

typedef enum WS_ACTION_TYPE_S {
  WS_CONN,
  WS_QUERY,
  WS_FETCH,
  WS_FETCH_BLOCK,
  WS_CLOSE,
} WS_ACTION_TYPE;

typedef enum {
 CANCELED,
 DISCONNECTED,
 TCP_CONNECTED,
 WS_CONNECTED,
 RECV_ERROR,
 SEND_ERROR,
} WS_STATUS;

typedef struct {
 char *version;
 char *code;
 char *desc;
 char *body;
 int bodySize;
} HttpResponse;

typedef struct {
 WS_STATUS status;
 int64_t   reqId;
} WebSocketClient;

/**************** Function declarations ****************/
extern void shellParseArgument(int argc, char* argv[], SShellArguments* arguments);
extern void  shellInit(SShellArguments* args);
extern void* shellLoopQuery(void* arg);
extern void taos_error(TAOS_RES* tres, int64_t st);
extern int regex_match(const char* s, const char* reg, int cflags);
int32_t shellReadCommand(TAOS* con, char command[]);
int32_t shellRunCommand(TAOS* con, char* command);
void shellRunCommandOnServer(TAOS* con, char command[]);
void shellRunCommandOnWebsocket(char command[]);
int wsclient_check(cJSON *root);
void read_history();
void write_history();
void source_file(TAOS* con, char* fptr);
void source_dir(TAOS* con, SShellArguments* args);
void shellCheck(TAOS* con, SShellArguments* args);
void get_history_path(char* history);
void shellCheck(TAOS* con, SShellArguments* args);
void cleanup_handler(void* arg);
void* pingHandler(void *arg);
void exitShell();
int shellDumpResult(TAOS_RES* con, char* fname, int* error_no, bool printMode);
int wsclientDumpResult(cJSON* query, char *fname, int *error_no, bool vertical, uint64_t limit);
void* recvHandler(void *arg);
void shellGetGrantInfo(void* con);
int isCommentLine(char* line);
int wsclient_handshake(bool printMsg);
int wsclient_conn();
cJSON* wsclient_query(char* command);
int wsclient_send_sql(char *command, WS_ACTION_TYPE type);
int tcpConnect(char* host, int port);
int parse_cloud_dsn();
void wsclient_parse_frame(SWSParser * parser, uint8_t * recv_buffer);
int wsclient_send(char *strdata, WebSocketFrameType frame);

/**************** Global variable declarations ****************/
extern char           PROMPT_HEADER[];
extern char           CONTINUE_PROMPT[];
extern int            prompt_size;
extern SShellHistory  history;
extern struct termios oldtio;
extern void           set_terminal_mode();
extern int get_old_terminal_mode(struct termios* tio);
extern void            reset_terminal_mode();
extern SShellArguments args;
extern int64_t         result;
extern WebSocketClient wsclient;
extern pthread_t       rpid;
extern pthread_t       ppid;
#endif

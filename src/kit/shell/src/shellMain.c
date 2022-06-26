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
#include "shell.h"
#include "tconfig.h"
#include "tnettest.h"

pthread_t pid;
pthread_t rpid;
pthread_t ppid;
static tsem_t cancelSem;
WebSocketClient wsclient;

void shellQueryInterruptHandler(int32_t signum, void *sigInfo, void *context) {
  tsem_post(&cancelSem);
}

void shellRestfulSendInterruptHandler(int32_t signum, void *sigInfo, void *context) {}

void* pingHandler(void *arg) {
  while (1) {
    uint8_t recv_buffer[TEMP_RECV_BUF];
    recv(args.socket, recv_buffer, TEMP_RECV_BUF - 1, 0);
    SWSParser parser;
    wsclient_parse_frame(&parser, recv_buffer);
    if (parser.frame == PING_FRAME) {
      wsclient_send("pong", PONG_FRAME);
    }
  }
  return NULL;
}

void* recvHandler(void *arg) {
START:
 pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
 uint8_t recv_buffer[TEMP_RECV_BUF]= {0};
 int   received = 0;
 SWSParser parser;
 int bytes = recv(args.socket, recv_buffer + received, TEMP_RECV_BUF - 1, 0);
 if (bytes == 0) {
   wsclient.status = DISCONNECTED;
   return NULL;
 }
 if (bytes < 0) {
   wsclient.status = RECV_ERROR;
   return NULL;
 }
 wsclient_parse_frame(&parser, recv_buffer);
 if (parser.frame == PING_FRAME) {
   if (wsclient_send("pong", PONG_FRAME)) {
     wsclient.status = SEND_ERROR;
     return NULL;
   }
   if (bytes > 2) {
     int i;
     for (i = 0; i < bytes - 2; ++i) {
       recv_buffer[i] = recv_buffer[i + 2];
     }
     recv_buffer[i] = '\0';
     if ((char)(recv_buffer[0]) == '{') {
       tfree(args.response_buffer);
       args.response_buffer = calloc(1, bytes -1);
       memcpy(args.response_buffer, recv_buffer, bytes - 2);
       args.response_buffer[bytes - 2] = '\0';
       return NULL;
     }
     wsclient_parse_frame(&parser, recv_buffer);
   } else {
     goto START;
   }
 }
 tfree(args.response_buffer);
 args.response_buffer = calloc(1, parser.payload_length + 1);
 int pos = bytes - parser.offset;
 memcpy(args.response_buffer, recv_buffer + parser.offset, pos);
 while (pos < parser.payload_length) {
   bytes = recv(args.socket, args.response_buffer + pos, parser.payload_length - pos, 0);
   if (bytes == 0) {
     wsclient.status = DISCONNECTED;
     return NULL;
   }
   if (bytes < 0) {
     wsclient.status = RECV_ERROR;
     return NULL;
   }
   pos += bytes;
 }
 args.response_buffer[pos] = '\0';
 return NULL;
}

void *cancelHandler(void *arg) {
  setThreadName("cancelHandler");

 while(1) {
   if (tsem_wait(&cancelSem) != 0) {
     taosMsleep(10);
     continue;
   }

   if (args.restful || args.cloud) {
     wsclient.status = CANCELED;
     pthread_cancel(rpid);
     close(args.socket);
     tfree(args.response_buffer);
     tfree(args.fields);
   } else {
#ifdef LINUX
    int64_t rid = atomic_val_compare_exchange_64(&result, result, 0);
    SSqlObj* pSql = taosAcquireRef(tscObjRef, rid);
    taos_stop_query(pSql);
    taosReleaseRef(tscObjRef, rid);
#else
    printf("\nReceive ctrl+c or other signal, quit shell.\n");
    exit(0);
#endif
   }
 }
 return NULL;
}

int checkVersion() {
  if (sizeof(int8_t) != 1) {
    printf("taos int8 size is %d(!= 1)", (int)sizeof(int8_t));
    return 0;
  }
  if (sizeof(int16_t) != 2) {
    printf("taos int16 size is %d(!= 2)", (int)sizeof(int16_t));
    return 0;
  }
  if (sizeof(int32_t) != 4) {
    printf("taos int32 size is %d(!= 4)", (int)sizeof(int32_t));
    return 0;
  }
  if (sizeof(int64_t) != 8) {
    printf("taos int64 size is %d(!= 8)", (int)sizeof(int64_t));
    return 0;
  }
  return 1;
}

// Global configurations
SShellArguments args = {.host = NULL,
#ifndef TD_WINDOWS
  .password = NULL,
#endif
  .user = NULL,
  .database = NULL,
  .timezone = NULL,
  .restful = false,
  .is_raw_time = false,
  .is_use_passwd = false,
  .dump_config = false,
  .file = "\0",
  .dir = "\0",
  .threadNum = 5,
  .commands = NULL,
  .pktLen = 1000,
  .pktNum = 100,
  .pktType = "TCP",
  .netTestRole = NULL,
  .cloudDsn = NULL,
  .cloud = true,
  .cloudHost = NULL,
  .cloudPort = NULL,
  .cloudToken = NULL,
  .fields = NULL,
  .st = 0,
  };

/*
 * Main function.
 */
int main(int argc, char* argv[]) {
  /*setlocale(LC_ALL, "en_US.UTF-8"); */
#ifdef WINDOWS
  SetConsoleOutputCP(CP_UTF8);
#endif

  if (!checkVersion()) {
    exit(EXIT_FAILURE);
  }

  shellParseArgument(argc, argv, &args);

  if (args.dump_config) {
    taosInitGlobalCfg();
    taosReadGlobalLogCfg();

    if (!taosReadGlobalCfg()) {
      printf("TDengine read global config failed");
      exit(EXIT_FAILURE);
    }

    taosDumpGlobalCfg();
    exit(0);
  }

  if (args.netTestRole && args.netTestRole[0] != 0) {
    if (taos_init()) {
      printf("Failed to init taos");
      exit(EXIT_FAILURE);
    }
    taosNetTest(args.netTestRole, args.host, args.port, args.pktLen, args.pktNum, args.pktType);
    exit(0);
  }

  if (args.cloud) {
      if (parse_cloud_dsn()) {
          exit(EXIT_FAILURE);
      }
      if (tcpConnect(args.cloudHost, atoi(args.cloudPort))) {
          exit(EXIT_FAILURE);
      }
  } else if (args.restful) {
      if (tcpConnect(args.host, args.port)) {
          exit(EXIT_FAILURE);
      }
  }

  /* Initialize the shell */
  shellInit(&args);

  if (tsem_init(&cancelSem, 0, 0) != 0) {
    printf("failed to create cancel semphore\n");
    exit(EXIT_FAILURE);
  }

  pthread_t spid;
  pthread_create(&spid, NULL, cancelHandler, NULL);

  /* Interrupt handler. */
  taosSetSignal(SIGTERM, shellQueryInterruptHandler);
  taosSetSignal(SIGINT, shellQueryInterruptHandler);
  taosSetSignal(SIGHUP, shellQueryInterruptHandler);
  taosSetSignal(SIGABRT, shellQueryInterruptHandler);
  if (args.restful || args.cloud) {
#ifdef LINUX
    taosSetSignal(SIGPIPE, shellRestfulSendInterruptHandler);
#endif
    wsclient.reqId = 0;
  }

  /* Get grant information */
  shellGetGrantInfo(args.con);

  /* Loop to query the input. */
  while (1) {
    pthread_create(&pid, NULL, shellLoopQuery, args.con);
    pthread_join(pid, NULL);
  }
}

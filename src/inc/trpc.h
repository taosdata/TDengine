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
#ifndef TDENGINE_TRPC_H
#define TDENGINE_TRPC_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taosmsg.h"
#include "tsched.h"

#define TAOS_CONN_UDPS     0
#define TAOS_CONN_UDPC     1
#define TAOS_CONN_UDP      1
#define TAOS_CONN_TCPS     2
#define TAOS_CONN_TCPC     3
#define TAOS_CONN_HTTPS    4
#define TAOS_CONN_HTTPC    5

#define TAOS_ID_ASSIGNED   0
#define TAOS_ID_FREE       1
#define TAOS_ID_REALLOCATE 2

#define taosSendMsgToPeer(x, y, z) taosSendMsgToPeerH(x, y, z, NULL)
#define taosBuildReqMsg(x, y) taosBuildReqMsgWithSize(x, y, 512)
#define taosBuildRspMsg(x, y) taosBuildRspMsgWithSize(x, y, 512)

typedef struct {
  char *localIp;                        // local IP used
  short localPort;                      // local port
  char *label;                          // for debug purpose
  int   numOfThreads;                   // number of threads to handle connections
  void *(*fp)(char *, void *, void *);  // function to process the incoming msg
  void *qhandle;                        // queue handle
  int   bits;                           // number of bits for sessionId
  int   numOfChanns;                    // number of channels
  int   sessionsPerChann;               // number of sessions per channel
  int   idMgmt;                         // TAOS_ID_ASSIGNED, TAOS_ID_FREE
  int   connType;                       // TAOS_CONN_UDP, TAOS_CONN_TCPC, TAOS_CONN_TCPS
  int   idleTime;                       // milliseconds, 0 means idle timer is disabled
  int   noFree;                         // not free buffer
  void (*efp)(int cid);                 // call back function to process not activated chann
  int (*afp)(char *meterId, char *spi, char *encrypt, uint8_t *secret,
             uint8_t *ckey);  // call back to retrieve auth info
} SRpcInit;

typedef struct {
  int      cid;       // channel ID
  int      sid;       // session ID
  char *   meterId;   // meter ID
  uint32_t peerId;    // peer link ID
  void *   shandle;   // pointer returned by taosOpenRpc
  void *   ahandle;   // handle provided by app
  char *   peerIp;    // peer IP string
  short    peerPort;  // peer port
  char     spi;       // security parameter index
  char     encrypt;   // encrypt algorithm
  char *   secret;    // key for authentication
  char *   ckey;      // ciphering key
} SRpcConnInit;

extern int taosDebugFlag;
extern int tsRpcHeadSize;

void *taosOpenRpc(SRpcInit *pRpc);

void taosCloseRpc(void *);

int taosOpenRpcChann(void *handle, int cid, int sessions);

void taosCloseRpcChann(void *handle, int cid);

void *taosOpenRpcConn(SRpcConnInit *pInit, uint8_t *code);

void taosCloseRpcConn(void *thandle);

void taosStopRpcConn(void *thandle);

int taosSendMsgToPeerH(void *thandle, char *pCont, int contLen, void *ahandle);

char *taosBuildReqHeader(void *param, char type, char *msg);

char *taosBuildReqMsgWithSize(void *, char type, int size);

char *taosBuildRspMsgWithSize(void *, char type, int size);

int taosSendSimpleRsp(void *thandle, char rsptype, char code);

int taosSetSecurityInfo(int cid, int sid, char *id, int spi, int encrypt, char *secret, char *ckey);

void taosGetRpcConnInfo(void *thandle, uint32_t *peerId, uint32_t *peerIp, short *peerPort, int *cid, int *sid);

int taosGetOutType(void *thandle);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TRPC_H

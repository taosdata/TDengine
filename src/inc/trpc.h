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

#include <stdbool.h>
#include <stdint.h>

#define TAOS_CONN_UDPS     0
#define TAOS_CONN_UDPC     1
#define TAOS_CONN_TCPS     2
#define TAOS_CONN_TCPC     3
#define TAOS_CONN_HTTPS    4
#define TAOS_CONN_HTTPC    5

#define TAOS_SOCKET_TYPE_NAME_TCP  "tcp"
#define TAOS_SOCKET_TYPE_NAME_UDP  "udp"

#define TAOS_CONN_SOCKET_TYPE_S()  ((strcasecmp(tsSocketType, TAOS_SOCKET_TYPE_NAME_UDP) == 0)? TAOS_CONN_UDPS:TAOS_CONN_TCPS)
#define TAOS_CONN_SOCKET_TYPE_C()  ((strcasecmp(tsSocketType, TAOS_SOCKET_TYPE_NAME_UDP) == 0)? TAOS_CONN_UDPC:TAOS_CONN_TCPC)

extern int tsRpcHeadSize;

typedef struct {
  char *localIp;                        // local IP used
  uint16_t localPort;                   // local port
  char *label;                          // for debug purpose
  int   numOfThreads;                   // number of threads to handle connections
  void *(*fp)(int8_t type, void *pCont, int32_t contLen, void *handle, int32_t index);  // function to process the incoming msg
  int   sessions;                       // number of sessions allowed
  int   connType;                       // TAOS_CONN_UDP, TAOS_CONN_TCPC, TAOS_CONN_TCPS
  int   idleTime;                       // milliseconds, 0 means idle timer is disabled
  char *meterId;   // meter ID
  char  spi;       // security parameter index
  char  encrypt;   // encrypt algorithm
  char *secret;    // key for authentication
  char *ckey;      // ciphering key
  int   (*afp) (char *meterId, char *spi, char *encrypt, uint8_t *secret, uint8_t *ckey); // call back to retrieve auth info
} SRpcInit;

typedef struct {
  int16_t   index; 
  int16_t   numOfIps;
  uint16_t  port;
  uint32_t  ip[TSDB_MAX_MPEERS];
  char      ipStr[TSDB_MAX_MPEERS][TSDB_IPv4ADDR_LEN];
} SRpcIpSet;

void *rpcOpen(SRpcInit *pRpc);
void  rpcClose(void *);
void *rpcMallocCont(int contLen);
void  rpcFreeCont(void *pCont);
void  rpcSendRequest(void *thandle, SRpcIpSet ipSet, char msgType, void *pCont, int contLen, void *ahandle);
void  rpcSendResponse(void *pConn, void *pCont, int contLen);
void  rpcSendSimpleRsp(void *pConn, int32_t code);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TRPC_H

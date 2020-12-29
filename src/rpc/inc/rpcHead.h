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

#ifndef TDENGINE_RPCHEAD_H
#define TDENGINE_RPCHEAD_H

#ifdef __cplusplus
extern "C" {
#endif

#define RPC_CONN_TCP    2

extern int tsRpcOverhead;

typedef struct {
  void    *msg;
  int      msgLen;
  uint32_t ip; 
  uint16_t port;
  int      connType;
  void    *shandle;
  void    *thandle;
  void    *chandle;
} SRecvInfo;

#pragma pack(push, 1)

typedef struct {
  char     version:4; // RPC version
  char     comp:4;    // compression algorithm, 0:no compression 1:lz4
  char     resflag:2; // reserved bits
  char     spi:3;     // security parameter index
  char     encrypt:3; // encrypt algorithm, 0: no encryption
  uint16_t tranId;    // transcation ID
  uint32_t linkUid;   // for unique connection ID assigned by client
  uint64_t ahandle;   // ahandle assigned by client 
  uint32_t sourceId;  // source ID, an index for connection list  
  uint32_t destId;    // destination ID, an index for connection list
  uint32_t destIp;    // destination IP address, for NAT scenario
  char     user[TSDB_UNI_LEN]; // user ID 
  uint16_t port;      // for UDP only, port may be changed
  char     empty[1];  // reserved
  uint8_t  msgType;   // message type  
  int32_t  msgLen;    // message length including the header iteslf
  uint32_t msgVer;
  int32_t  code;      // code in response message
  uint8_t  content[0]; // message body starts from here
} SRpcHead;

typedef struct {
  int32_t  reserved;
  int32_t  contLen;
} SRpcComp;

typedef struct {
  uint32_t timeStamp;
  uint8_t  auth[TSDB_AUTH_LEN];
} SRpcDigest;

#pragma pack(pop)


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_RPCHEAD_H


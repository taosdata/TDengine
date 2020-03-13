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

#ifndef TDENGINE_SYNC_H
#define TDENGINE_SYNC_H

#ifdef __cplusplus
extern "C" {
#endif

#define TAOS_SYNC_NAME_LEN    128 
#define TAOS_SYNC_MAX_REPLICA 5

typedef enum _TAOS_SYNC_STATUS {
  TAOS_SYNC_STATUS_OFFLINE,
  TAOS_SYNC_STATUS_UNSYNCED,
  TAOS_SYNC_STATUS_FILE,
  TAOS_SYNC_STATUS_CACHE,
  TAOS_SYNC_STATUS_SLAVE,
  TAOS_SYNC_STATUS_MASTER,
} ESyncStatus;

typedef struct {
  uint64_t  version;
  int32_t   len;
  uint32_t  cksum;
  char      cont[];
} SWalHead;

typedef struct {
  uint32_t  nodeId;    // node ID assigned by TDengine
  uint32_t  nodeIp;    // node IP address
  char      name[TAOS_SYNC_NAME_LEN];  // external node name 
} SNodeInfo;

typedef struct {
  int       selfIndex;
  uint32_t  nodeId[TAOS_SYNC_MAX_REPLICA];
  int       status[TAOS_SYNC_MAX_REPLICA];  
} SSyncStatus;
  
typedef struct {
  char       label[20]; // for debug purpose
  int8_t     replica;   // number of replications
  int8_t     quorum; 
  int32_t    vgId;      // vgroup ID
  void      *ahandle;   // handle provided by APP 

  // if name is null, get the file from index or after, used by master
  // if name is provided, get the named file at the specified index, used by unsynced node
  // it returns the file magic number, if file not there, magic shall be 0.
  uint32_t   (*getFileInfo)(char *name, int *index, int *size); 

  // get the wal file from index or after
  // return value, -1: error, 0: last wal, 1:more wal files
  int        (*getWalInfo)(char *name, int *index); 

  int        (*writeToCache)(void *ahandle, uint64_t version, void *cont, int len);
  void       (*confirmFwd)(void *ahandle, int64_t version);
  void       (*notifyStatus)(void *ahandle, int8_t status);
  SNodeInfo  nodeInfo[TAOS_SYNC_MAX_REPLICA];
} SSyncInfo;

typedef void* tsync_h;

tsync_h syncStart(SSyncInfo *);
void    syncStop(tsync_h );
int     syncReconfig(tsync_h, SSyncInfo *);
int     syncForwardToPeer(void *param, uint64_t version, char *cont, int contLen);
void    syncRecover(tsync_h );      // recover from other nodes:
int     syncGetStatus(tsync_h, SSyncStatus *);

extern  char syncStatus[];

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SYNC_H

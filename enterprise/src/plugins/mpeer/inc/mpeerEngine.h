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

#ifndef TDENGINE_PLUGIN_MPEER_ENGINE_H
#define TDENGINE_PLUGIN_MPEER_ENGINE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <pthread.h>

/*
 * Interface functions
 */

int32_t mpeerInitMnodes(char *directory);
void    mpeerCleanUpMnodes();

int32_t mpeerAddMnode(uint32_t privateIp, uint32_t publicIp);
int32_t mpeerRemoveMnode(uint32_t privateIp);

int32_t mpeerForwardRequest(char type, void *cont, int32_t contLen);

/*
 * Internal definitions
 */

#define MPEER_MAX_QUEUE_SIZE       2000
#define MPEER_MAX_TRY_WAIT_TIMES   2000
#define MPEER_TRY_WAIT_TIME_IN_MS  1
#define MPEER_MAX_MNODES           100
#define MPEER_DEFAULT_ZONE         "root"

typedef struct {
  char *          buffer;
  char *          offset;
  int             trans;
  int             bufferSize;
  pthread_mutex_t qmutex;
} STranQueue;

typedef struct {
  char     status;
  char     role;
  char     numOfMnodes;
  uint64_t dbVersion;
  uint32_t numOfDnodes;
  uint32_t publicIp;
} SMpeerStatusMsg, SMpeerStatusRsp;

typedef struct {
  uint8_t  msgType;
  int32_t  msgLen;
  uint8_t  content[0];
} SSchedFordwardMsg;

typedef struct {
  char     numOfTables;
  uint64_t version[];
} SSdbSync;


#ifdef __cplusplus
}
#endif

#endif
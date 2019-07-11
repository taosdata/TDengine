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

#ifndef TDENGINE_TMODULE_H
#define TDENGINE_TMODULE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <semaphore.h>

typedef struct _msg_header {
  int mid; /* message ID */
  int cid; /* call ID */
  int tid; /* transaction ID */
  //  int   len;           /* length of msg */
  char *msg; /* content holder */
} msg_header_t, msg_t;

typedef struct {
  char *          name;   /* module name */
  pthread_t       thread; /* thread ID */
  sem_t           emptySem;
  sem_t           fullSem;
  int             fullSlot;
  int             emptySlot;
  int             debugFlag;
  int             queueSize;
  int             msgSize;
  pthread_mutex_t queueMutex;
  pthread_mutex_t stmMutex;
  msg_t *         queue;

  int (*processMsg)(msg_t *);

  int (*init)();

  void (*cleanUp)();
} module_t;

typedef struct {
  short         len;
  unsigned char data[0];
} sim_data_t;

extern int      maxCid;
extern module_t moduleObj[];
extern char *   msgName[];

extern int taosSendMsgToModule(module_t *mod_p, int cid, int mid, int tid, char *msg);

extern char *taosDisplayModuleStatus(int moduleNum);

extern int taosInitModule(module_t *);

extern void taosCleanUpModule(module_t *);

#ifdef __cplusplus
}
#endif

#endif

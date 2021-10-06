/*
 * Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef _TD_DNODE_TELEMETRY_H_
#define _TD_DNODE_TELEMETRY_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dnodeInt.h"

/*
 * sem_timedwait is NOT implemented on MacOSX
 * thus we use pthread_mutex_t/pthread_cond_t to simulate
 */
typedef struct DnTelem {
  bool             enable;
  pthread_mutex_t  lock;
  pthread_cond_t   cond;
  volatile int32_t exit;
  pthread_t        thread;
  char             email[TSDB_FQDN_LEN];
} DnTelem;

int32_t dnodeInitTelem(DnTelem **telem);
void    dnodeCleanupTelem(DnTelem **telem);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_TELEMETRY_H_*/

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

#ifndef _TD_DNODE_EPS_H_
#define _TD_DNODE_EPS_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "hash.h"
#include "dnodeInt.h"

typedef struct SDnEps {
  int32_t         dnodeId;
  int32_t         dnodeNum;
  SDnodeEp *      dnodeList;
  SHashObj *      dnodeHash;
  char            file[PATH_MAX + 20];
  pthread_mutex_t mutex;
} SDnEps;

int32_t dnodeInitEps(SDnEps **eps);
void    dnodeCleanupEps(SDnEps **eps);
void    dnodeUpdateEps(SDnEps *eps, SDnodeEps *data);
bool    dnodeIsDnodeEpChanged(SDnEps *eps, int32_t dnodeId, char *epstr);
void    dnodeGetDnodeEp(int32_t dnodeId, char *epstr, char *fqdn, uint16_t *port);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_EPS_H_*/
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

#ifndef _TD_DNODE_CFG_H_
#define _TD_DNODE_CFG_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dnodeInt.h"

typedef struct SDnCfg {
  int32_t         dnodeId;
  int32_t         dropped;
  char            clusterId[TSDB_CLUSTER_ID_LEN];
  char            file[PATH_MAX + 20];
  pthread_mutex_t mutex;
} SDnCfg;

int32_t dnodeInitCfg(SDnCfg **cfg);
void    dnodeCleanupCfg(SDnCfg **cfg);
void    dnodeUpdateCfg(SDnCfg *cfg, SDnodeCfg *data);
int32_t dnodeGetDnodeId(SDnCfg *cfg);
void    dnodeGetClusterId(SDnCfg *cfg, char *clusterId);
void    dnodeGetCfg(SDnCfg *cfg, int32_t *dnodeId, char *clusterId);
void    dnodeSetDropped(SDnCfg *cfg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_CFG_H_*/

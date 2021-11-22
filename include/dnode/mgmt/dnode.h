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

#ifndef _TD_DNODE_H_
#define _TD_DNODE_H_

#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SDnode SDnode;

typedef struct {
  int32_t  sver;
  int32_t  numOfCores;
  float    numOfThreadsPerCore;
  float    ratioOfQueryCores;
  int32_t  maxShellConns;
  int32_t  shellActivityTimer;
  int32_t  statusInterval;
  uint16_t serverPort;
  char     dataDir[PATH_MAX];
  char     localEp[TSDB_EP_LEN];
  char     localFqdn[TSDB_FQDN_LEN];
  char     firstEp[TSDB_EP_LEN];
  char     timezone[TSDB_TIMEZONE_LEN];
  char     locale[TSDB_LOCALE_LEN];
  char     charset[TSDB_LOCALE_LEN];
} SDnodeOpt;

/* ------------------------ SDnode ------------------------ */
/**
 * @brief Initialize and start the dnode.
 *
 * @param cfgPath Config file path.
 * @return SDnode* The dnode object.
 */
SDnode *dndInit(SDnodeOpt *pOptions);

/**
 * @brief Stop and cleanup dnode.
 *
 * @param pDnd The dnode object to close.
 */
void dndCleanup(SDnode *pDnd);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_H_*/

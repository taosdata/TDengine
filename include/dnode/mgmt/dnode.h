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
  int16_t  numOfCores;
  int16_t  numOfSupportVnodes;
  int16_t  numOfCommitThreads;
  int8_t   enableTelem;
  int32_t  statusInterval;
  float    numOfThreadsPerCore;
  float    ratioOfQueryCores;
  int32_t  maxShellConns;
  int32_t  shellActivityTimer;
  uint16_t serverPort;
  char     dataDir[TSDB_FILENAME_LEN];
  char     localEp[TSDB_EP_LEN];
  char     localFqdn[TSDB_FQDN_LEN];
  char     firstEp[TSDB_EP_LEN];
  char     timezone[TSDB_TIMEZONE_LEN];
  char     locale[TSDB_LOCALE_LEN];
  char     charset[TSDB_LOCALE_LEN];
  char     buildinfo[64];
  char     gitinfo[48];
} SDnodeOpt;

/* ------------------------ SDnode ------------------------ */
/**
 * @brief Initialize and start the dnode.
 *
 * @param pOption Option of the dnode.
 * @return SDnode* The dnode object.
 */
SDnode *dndInit(SDnodeOpt *pOption);

/**
 * @brief Stop and cleanup the dnode.
 *
 * @param pDnode The dnode object to close.
 */
void dndCleanup(SDnode *pDnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_H_*/

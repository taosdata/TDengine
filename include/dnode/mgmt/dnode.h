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
  /**
   * @brief software version of the program.
   *
   */
  int32_t sver;

  /**
   * @brief num of CPU cores.
   *
   */
  int32_t numOfCores;

  /**
   * @brief number of threads per CPU core.
   *
   */
  float numOfThreadsPerCore;

  /**
   * @brief the proportion of total CPU cores available for query processing.
   *
   */
  float ratioOfQueryCores;

  /**
   * @brief max number of connections allowed in dnode.
   *
   */
  int32_t maxShellConns;

  /**
   * @brief time interval of heart beat from shell to dnode, seconds.
   *
   */
  int32_t shellActivityTimer;

  /**
   * @brief time interval of dnode status reporting to mnode, seconds, for cluster only.
   *
   */
  int32_t statusInterval;

  /**
   * @brief first port number for the connection (12 continuous UDP/TCP port number are used).
   *
   */
  uint16_t serverPort;

  /**
   * @brief data file's directory.
   *
   */
  char dataDir[TSDB_FILENAME_LEN];

  /**
   * @brief local endpoint.
   *
   */
  char localEp[TSDB_EP_LEN];

  /**
   * @brieflocal fully qualified domain name (FQDN).
   *
   */
  char localFqdn[TSDB_FQDN_LEN];

  /**
   * @brief first fully qualified domain name (FQDN) for TDengine system.
   *
   */
  char firstEp[TSDB_EP_LEN];

  /**
   * @brief system time zone.
   *
   */
  char timezone[TSDB_TIMEZONE_LEN];

  /**
   * @brief system locale.
   *
   */
  char locale[TSDB_LOCALE_LEN];

  /**
   * @briefdefault system charset.
   *
   */
  char charset[TSDB_LOCALE_LEN];
} SDnodeOpt;

/* ------------------------ SDnode ------------------------ */
/**
 * @brief Initialize and start the dnode.
 *
 * @param pOptions Options of the dnode.
 * @return SDnode* The dnode object.
 */
SDnode *dndInit(SDnodeOpt *pOptions);

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

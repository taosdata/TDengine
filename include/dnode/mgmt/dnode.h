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

/* ------------------------ TYPES EXPOSED ---------------- */
typedef struct SDnode SDnode;

/**
 * @brief Initialize the environment
 *
 * @return int32_t 0 for success and -1 for failure
 */
int32_t dndInit();

/**
 * @brief Clear the environment
 */
void dndCleanup();

/* ------------------------ SDnode ----------------------- */
typedef struct {
  int32_t   numOfSupportVnodes;
  uint16_t  serverPort;
  char      dataDir[PATH_MAX];
  char      localEp[TSDB_EP_LEN];
  char      localFqdn[TSDB_FQDN_LEN];
  char      firstEp[TSDB_EP_LEN];
  char      secondEp[TSDB_EP_LEN];
  SDiskCfg *pDisks;
  int32_t   numOfDisks;
} SDnodeOpt;

typedef enum { DND_EVENT_START, DND_EVENT_STOP = 1, DND_EVENT_RELOAD } EDndEvent;

/**
 * @brief Initialize and start the dnode.
 *
 * @param pOption Option of the dnode.
 * @return SDnode* The dnode object.
 */
SDnode *dndCreate(const SDnodeOpt *pOption);

/**
 * @brief Stop and cleanup the dnode.
 *
 * @param pDnode The dnode object to close.
 */
void dndClose(SDnode *pDnode);

/**
 * @brief Run dnode until specific event is receive.
 *
 * @param pDnode The dnode object to run.
 */
int32_t dndRun(SDnode *pDnode);

/**
 * @brief Handle event in the dnode.
 *
 * @param pDnode The dnode object to close.
 * @param event The event to handle.
 */
void dndHandleEvent(SDnode *pDnode, EDndEvent event);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_H_*/

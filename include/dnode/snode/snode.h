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

#ifndef _TD_SNODE_H_
#define _TD_SNODE_H_

#include "tmsg.h"
#include "tmsgcb.h"
#include "trpc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SSnode SSnode;

typedef struct {
  int32_t reserved;
} SSnodeLoad;

typedef struct {
  SMsgCb msgCb;
} SSnodeOpt;

/* ------------------------ SSnode ------------------------ */
/**
 * @brief Start one Snode in Dnode.
 *
 * @param path Path of the snode.
 * @param pOption Option of the snode.
 * @return SSnode* The snode object.
 */
SSnode *sndOpen(const char *path, const SSnodeOpt *pOption);

int32_t sndInit(SSnode * pSnode);
/**
 * @brief Stop Snode in Dnode.
 *
 * @param pSnode The snode object to close.
 */
void sndClose(SSnode *pSnode);

/**
 * @brief Get the statistical information of Snode
 *
 * @param pSnode The snode object.
 * @param pLoad Statistics of the snode.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t sndGetLoad(SSnode *pSnode, SSnodeLoad *pLoad);

/**
 * @brief Process a query message.
 *
 * @param pSnode The snode object.
 * @param pMsg The request message
 * @param pRsp The response message
 */
int32_t sndProcessWriteMsg(SSnode *pSnode, SRpcMsg *pMsg, SRpcMsg *pRsp);
int32_t sndProcessStreamMsg(SSnode *pSnode, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SNODE_H_*/

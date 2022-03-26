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

#ifndef _TD_DND_INT_H_
#define _TD_DND_INT_H_

#include "dnd.h"

#include "bm.h"
#include "dm.h"
#include "mm.h"
#include "qm.h"
#include "sm.h"
#include "vm.h"

#ifdef __cplusplus
extern "C" {
#endif

// dndInt.c
int32_t     dndInit();
void        dndCleanup();
const char *dndStatStr(EDndStatus stat);
void        dndGetStartup(SDnode *pDnode, SStartupReq *pStartup);
TdFilePtr   dndCheckRunning(const char *dataDir);
void        dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pMsg);

// dndMsg.c
void dndProcessRpcMsg(SMgmtWrapper *pWrapper, SRpcMsg *pMsg, SEpSet *pEpSet);

// dndExec.c
int32_t dndOpenNode(SMgmtWrapper *pWrapper);
void    dndCloseNode(SMgmtWrapper *pWrapper);
int32_t dndRun(SDnode *pDnode);

// dndObj.c
SDnode *dndCreate(const SDnodeOpt *pOption);
void    dndClose(SDnode *pDnode);
void    dndHandleEvent(SDnode *pDnode, EDndEvent event);

// dndTransport.c
int32_t dndInitServer(SDnode *pDnode);
void    dndCleanupServer(SDnode *pDnode);
int32_t dndInitClient(SDnode *pDnode);
void    dndCleanupClient(SDnode *pDnode);
int32_t dndInitMsgHandle(SDnode *pDnode);
void    dndSendRpcRsp(SMgmtWrapper *pWrapper, SRpcMsg *pRsp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_INT_H_*/
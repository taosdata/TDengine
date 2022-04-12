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

#include "dndDef.h"

#ifdef __cplusplus
extern "C" {
#endif

// dndInt.c
const char   *dndStatName(EDndRunStatus stat);
const char   *dndLogName(EDndNodeType ntype);
const char   *dndProcName(EDndNodeType ntype);
const char   *dndEventName(EDndEvent ev);
SMgmtWrapper *dndAcquireWrapper(SDnode *pDnode, EDndNodeType nType);
int32_t       dndMarkWrapper(SMgmtWrapper *pWrapper);
void          dndReleaseWrapper(SMgmtWrapper *pWrapper);
EDndRunStatus dndGetStatus(SDnode *pDnode);
void          dndSetStatus(SDnode *pDnode, EDndRunStatus stat);
void          dndSetEvent(SDnode *pDnode, EDndEvent event);
void          dndSetMsgHandle(SMgmtWrapper *pWrapper, tmsg_t msgType, NodeMsgFp nodeMsgFp, int8_t vgId);
void          dndReportStartup(SDnode *pDnode, const char *pName, const char *pDesc);
void          dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pMsg);

// dndFile.c
int32_t   dndReadFile(SMgmtWrapper *pWrapper, bool *pDeployed);
int32_t   dndWriteFile(SMgmtWrapper *pWrapper, bool deployed);
TdFilePtr dndCheckRunning(const char *dataDir);
int32_t   dndReadShmFile(SDnode *pDnode);
int32_t   dndWriteShmFile(SDnode *pDnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_INT_H_*/
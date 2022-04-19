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

#ifndef _TD_DM_INT_H_
#define _TD_DM_INT_H_

#include "dmDef.h"

#ifdef __cplusplus
extern "C" {
#endif

// dmInt.c
SMgmtWrapper *dmAcquireWrapper(SDnode *pDnode, EDndNodeType nType);
int32_t       dmMarkWrapper(SMgmtWrapper *pWrapper);
void          dmReleaseWrapper(SMgmtWrapper *pWrapper);
const char   *dmStatName(EDndRunStatus stat);
const char   *dmLogName(EDndNodeType ntype);
const char   *dmProcName(EDndNodeType ntype);
const char   *dmEventName(EDndEvent ev);

void   dmSetStatus(SDnode *pDnode, EDndRunStatus stat);
void   dmSetEvent(SDnode *pDnode, EDndEvent event);
void   dmSetMsgHandle(SMgmtWrapper *pWrapper, tmsg_t msgType, NodeMsgFp nodeMsgFp, int8_t vgId);
void   dmReportStartup(SDnode *pDnode, const char *pName, const char *pDesc, bool finished);
void   dmProcessServerStatusReq(SDnode *pDnode, SRpcMsg *pMsg);
void   dmGetMonitorSysInfo(SMonSysInfo *pInfo);

// dmFile.c
int32_t   dmReadFile(SMgmtWrapper *pWrapper, bool *pDeployed);
int32_t   dmWriteFile(SMgmtWrapper *pWrapper, bool deployed);
TdFilePtr dmCheckRunning(const char *dataDir);
int32_t   dmReadShmFile(SMgmtWrapper *pWrapper);
int32_t   dmWriteShmFile(SMgmtWrapper *pWrapper);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DM_INT_H_*/
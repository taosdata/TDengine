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

#ifndef TDENGINE_MGMT_BALANCE_H
#define TDENGINE_MGMT_BALANCE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "mgmt.h"
#include "tglobalcfg.h"
#include "vnodeStatus.h"
#include "ttime.h"

extern void    (*mgmtStartBalanceTimer)(int64_t mseconds);
extern int32_t (*mgmtInitBalance)();
extern void    (*mgmtCleanupBalance)();
extern int32_t (*mgmtAllocVnodes)(SVgObj *pVgroup);
extern bool    (*mgmtCheckModuleInDnode)(SDnodeObj *pDnode, int moduleType);
extern char*   (*mgmtGetVnodeStatus)(SVgObj *pVgroup, SVnodeGid *pVnode);
extern bool    (*mgmtCheckVnodeReady)(SDnodeObj *pDnode, SVgObj *pVgroup, SVnodeGid *pVnode);
extern void    (*mgmtUpdateDnodeState)(SDnodeObj *pDnode, int lbStatus);
extern void    (*mgmtUpdateVgroupState)(SVgObj *pVgroup, int lbStatus, int srcIp);
extern bool    (*mgmtAddVnode)(SVgObj *pVgroup, SDnodeObj *pSrcDnode, SDnodeObj *pDestDnode);

#ifdef __cplusplus
}
#endif

#endif

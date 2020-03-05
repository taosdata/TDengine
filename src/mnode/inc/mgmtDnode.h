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

#ifndef TDENGINE_MGMT_DNODE_H
#define TDENGINE_MGMT_DNODE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "mnode.h"

void    mgmtSetDnodeVgid(SVnodeGid vnodeGid[], int32_t numOfVnodes, int32_t vgId);
void    mgmtUnSetDnodeVgid(SVnodeGid vnodeGid[], int32_t numOfVnodes);
int32_t mgmtGetDnodeMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);
int32_t mgmtSendCfgDnodeMsg(char *cont);
void    mgmtSetDnodeMaxVnodes(SDnodeObj *pDnode);

int32_t mgmtGetConfigMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveConfigs(SShowObj *pShow, char *data, int32_t rows, void *pConn);

bool    mgmtCheckModuleInDnode(SDnodeObj *pDnode, int32_t moduleType);
int32_t mgmtGetModuleMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn);

int32_t mgmtGetVnodeMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveVnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);

int32_t mgmtGetScoresMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn);

int32_t mgmtInitDnodes();
void    mgmtCleanUpDnodes();
int32_t mgmtGetDnodesNum();
int32_t mgmtUpdateDnode(SDnodeObj *pDnode);
void*   mgmtGetNextDnode(SShowObj *pShow, SDnodeObj **pDnode);
bool    mgmtCheckConfigShow(SGlobalConfig *cfg);
bool    mgmtCheckDnodeInRemoveState(SDnodeObj *pDnode);
bool    mgmtCheckDnodeInOfflineState(SDnodeObj *pDnode);
void    mgmtSetDnodeUnRemove(SDnodeObj *pDnode);
SDnodeObj* mgmtGetDnode(uint32_t ip);

extern  int32_t (*mgmtCreateDnodeFp)(uint32_t ip);
extern  int32_t (*mgmtDropDnodeByIpFp)(uint32_t ip);

#ifdef __cplusplus
}
#endif

#endif

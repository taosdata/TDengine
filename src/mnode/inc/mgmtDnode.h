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

int32_t mgmtCreateDnode(uint32_t ip);
int32_t mgmtDropDnode(SDnodeObj *pDnode);
int32_t mgmtDropDnodeByIp(uint32_t ip);
int32_t mgmtGetNextVnode(SVnodeGid *pVnodeGid);
void    mgmtSetDnodeVgid(SVnodeGid vnodeGid[], int32_t numOfVnodes, int32_t vgId);
void    mgmtUnSetDnodeVgid(SVnodeGid vnodeGid[], int32_t numOfVnodes);
int32_t mgmtGetDnodeMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);
int32_t mgmtSendCfgDnodeMsg(char *cont);
void    mgmtSetDnodeMaxVnodes(SDnodeObj *pDnode);

int32_t mgmtGetConfigMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveConfigs(SShowObj *pShow, char *data, int32_t rows, void *pConn);

int32_t mgmtGetModuleMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn);

int32_t mgmtGetVnodeMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t mgmtRetrieveVnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);

extern int32_t    (*mgmtInitDnodes)();
extern void       (*mgmtCleanUpDnodes)();
extern SDnodeObj* (*mgmtGetDnode)(uint32_t ip);
extern int32_t    (*mgmtGetDnodesNum)();
extern void*      (*mgmtGetNextDnode)(SShowObj *pShow, SDnodeObj **pDnode);
extern int32_t    (*mgmtUpdateDnode)(SDnodeObj *pDnode);
extern void       (*mgmtSetDnodeUnRemove)(SDnodeObj *pDnode);
extern int32_t    (*mgmtGetScoresMeta)(STableMeta *pMeta, SShowObj *pShow, void *pConn);
extern int32_t    (*mgmtRetrieveScores)(SShowObj *pShow, char *data, int32_t rows, void *pConn);
extern bool       (*mgmtCheckConfigShow)(SGlobalConfig *cfg);

extern SDnodeObj tsDnodeObj;

#ifdef __cplusplus
}
#endif

#endif

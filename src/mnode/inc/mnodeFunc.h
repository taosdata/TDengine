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

#ifndef TDENGINE_MNODE_FUNC_H
#define TDENGINE_MNODE_FUNC_H

#ifdef __cplusplus
extern "C" {
#endif
#include "mnodeDef.h"

int32_t   mnodeInitFuncs();
void      mnodeCleanupFuncs();

SFuncObj *mnodeGetFunc(char *name);
void *    mnodeGetNextFunc(void *pIter, SFuncObj **pFunc);
void      mnodeCancelGetNextFunc(void *pIter);

void      mnodeIncFuncRef(SFuncObj *pFunc);
void      mnodeDecFuncRef(SFuncObj *pFunc);

int32_t   mnodeCreateFunc(SAcctObj *pAcct, char *name, int32_t codeLen, char *code, char *path, SMnodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif

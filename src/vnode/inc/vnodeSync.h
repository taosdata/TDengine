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

#ifndef TDENGINE_VNODE_SYNC_H
#define TDENGINE_VNODE_SYNC_H

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"

uint32_t vnodeGetFileInfo(int32_t vgId, char *name, uint32_t *index, uint32_t eindex, int64_t *size, uint64_t *fver);
int32_t  vnodeGetWalInfo(int32_t vgId, char *fileName, int64_t *fileId);
void     vnodeNotifyRole(int32_t vgId, int8_t role);
void     vnodeCtrlFlow(int32_t vgId, int32_t level);
void     vnodeStartSyncFile(int32_t vgId);
void     vnodeStopSyncFile(int32_t vgId, uint64_t fversion);
void     vnodeConfirmForard(int32_t vgId, void *wparam, int32_t code);
int32_t  vnodeWriteToCache(int32_t vgId, void *wparam, int32_t qtype, void *rparam);
int32_t  vnodeGetVersion(int32_t vgId, uint64_t *fver, uint64_t *wver);
int32_t  vnodeResetVersion(int32_t vgId, uint64_t fver);

void     vnodeConfirmForward(void *pVnode, uint64_t version, int32_t code);

#ifdef __cplusplus
}
#endif

#endif
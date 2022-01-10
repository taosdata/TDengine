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

#ifndef _TD_TDISK_MGR_H_
#define _TD_TDISK_MGR_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "tkvDef.h"

typedef struct STkvDiskMgr STkvDiskMgr;

int    tdmOpen(STkvDiskMgr **ppDiskMgr, const char *fname, uint16_t pgsize);
int    tdmClose(STkvDiskMgr *pDiskMgr);
int    tdmReadPage(STkvDiskMgr *pDiskMgr, pgid_t pgid, void *pData);
int    tdmWritePage(STkvDiskMgr *pDiskMgr, pgid_t pgid, const void *pData);
int    tdmFlush(STkvDiskMgr *pDiskMgr);
pgid_t tdmAllocPage(STkvDiskMgr *pDiskMgr);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDISK_MGR_H_*/
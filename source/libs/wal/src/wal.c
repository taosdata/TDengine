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

#include "wal.h"

int32_t walInit() { return 0; }

void walCleanUp() {}

SWal *walOpen(char *path, SWalCfg *pCfg) { return NULL; }

int32_t walAlter(SWal *pWal, SWalCfg *pCfg) { return 0; }

void walClose(SWal *pWal) {}

void walFsync(SWal *pWal, bool force) {}

int64_t walWrite(SWal *pWal, int64_t index, void *body, int32_t bodyLen) {
  return 0;
}

int32_t walCommit(SWal *pWal, int64_t ver) { return 0; }

int32_t walRollback(SWal *pWal, int64_t ver) { return 0; }

int32_t walPrune(SWal *pWal, int64_t ver) { return 0; }


int32_t walRead(SWal *, SWalHead **, int64_t ver);
int32_t walReadWithFp(SWal *, FWalWrite writeFp, int64_t verStart, int32_t readNum);

int64_t walGetFirstVer(SWal *);
int64_t walGetSnapshotVer(SWal *);
int64_t walGetLastVer(SWal *);

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

#include "tsdbint.h"

#if 0
#ifndef _TSDB_PLUGINS

int tsdbScanFGroup(STsdbScanHandle* pScanHandle, char* rootDir, int fid) { return 0; }

STsdbScanHandle* tsdbNewScanHandle() { return NULL; }

void tsdbSetScanLogStream(STsdbScanHandle* pScanHandle, FILE* fLogStream) {}

int tsdbSetAndOpenScanFile(STsdbScanHandle* pScanHandle, char* rootDir, int fid) { return 0; }

int tsdbScanSBlockIdx(STsdbScanHandle* pScanHandle) { return 0; }

int tsdbScanSBlock(STsdbScanHandle* pScanHandle, int idx) { return 0; }

int tsdbCloseScanFile(STsdbScanHandle* pScanHandle) { return 0; }

void tsdbFreeScanHandle(STsdbScanHandle* pScanHandle) {}

#endif
#endif
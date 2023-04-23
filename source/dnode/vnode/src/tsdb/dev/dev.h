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

#ifndef _TSDB_DEV_H
#define _TSDB_DEV_H

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SFDataPtr {
  int64_t offset;
  int64_t size;
} SFDataPtr;

typedef struct {
  int64_t   prevFooter;
  SFDataPtr dict[4];  // 0:bloom filter, 1:SSttBlk, 2:STbStatisBlk, 3:SDelBlk
  uint8_t   reserved[24];
} SFSttFooter;

#include "tsdbUtil.h"

#include "tsdbFile.h"

#include "tsdbFSet.h"

#include "tsdbFS.h"

#include "tsdbSttFWriter.h"

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_DEV_H*/
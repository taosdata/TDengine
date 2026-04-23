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

#ifndef _TD_DM_REPAIR_COPY_H_
#define _TD_DM_REPAIR_COPY_H_

#include "os.h"
#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SRepairCopyOpts {
  bool    enabled;
  char    modeStr[32];
  char    nodeType[32];
  char    sourceHost[256];
  char    sourceCfg[PATH_MAX];
  SArray *vnodeIds;  // sorted ascending, deduplicated array of int32_t
} SRepairCopyOpts;

// Parse a vnode ID list string like "2-5,8,3,2" into a sorted, deduplicated
// SArray of int32_t. Caller must call taosArrayDestroy() on the result.
// Returns NULL on parse error.
SArray *dmParseVnodeIds(const char *str);

// Execute copy-mode repair. Returns exit code: 0=all ok, 1=bad args,
// 2=SSH fail, 3=partial failure, 4=all failed.
int32_t dmRepairCopyMode(const SRepairCopyOpts *pOpts);

#ifdef __cplusplus
}
#endif

#endif /* _TD_DM_REPAIR_COPY_H_ */

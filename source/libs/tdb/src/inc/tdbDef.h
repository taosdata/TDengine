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

#ifndef _TD_TDB_DEF_H_
#define _TD_TDB_DEF_H_

#include "os.h"
#include "tlist.h"

#ifdef __cplusplus
extern "C" {
#endif

// pgid_t
typedef int32_t pgid_t;
#define TDB_IVLD_PGID ((pgid_t)-1)

// framd_id_t
typedef int32_t frame_id_t;

// pgsize_t
typedef int32_t pgsize_t;
#define TDB_MIN_PGSIZE 512
#define TDB_MAX_PGSIZE 16384
#define TDB_DEFAULT_PGSIZE 4096
#define TDB_IS_PGSIZE_VLD(s) (((s) >= TKV_MIN_PGSIZE) && (TKV_MAX_PGSIZE <= TKV_MAX_PGSIZE))

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_DEF_H_*/
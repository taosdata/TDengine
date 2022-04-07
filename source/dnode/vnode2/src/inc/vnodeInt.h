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

#ifndef _TD_VNODE_INT_H_
#define _TD_VNODE_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "vnode.h"

typedef int8_t   i8;
typedef int16_t  i16;
typedef int32_t  i32;
typedef int64_t  i64;
typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef struct SMeta SMeta;
typedef struct STsdb STsdb;
typedef struct STQ   STQ;

struct SVnode {
  i32    vid;
  char  *path;
  SMeta *pMeta;
  STsdb *pTsdb;
  STQ   *pTq;
};

#include "vnodeMeta.h"

#include "vnodeTsdb.h"

#include "vnodeTq.h"

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_INT_H_*/
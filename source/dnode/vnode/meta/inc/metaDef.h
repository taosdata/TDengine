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

#ifndef _TD_META_DEF_H_
#define _TD_META_DEF_H_

#include "mallocator.h"

#include "meta.h"
#include "metaCache.h"
#include "metaDB.h"
#include "metaIdx.h"
#include "metaOptions.h"
#include "metaTbOptions.h"
#include "metaTbTag.h"
#include "metaTbUid.h"

#ifdef __cplusplus
extern "C" {
#endif

struct SMeta {
  char*                 path;
  SMetaOptions          options;
  meta_db_t*            pDB;
  meta_index_t*         pIdx;
  meta_cache_t*         pCache;
  STbUidGenerator       uidGnrt;
  SMemAllocatorFactory* pmaf;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_META_DEF_H_*/
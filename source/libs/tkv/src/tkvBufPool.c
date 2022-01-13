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

#include "thash.h"
#include "tlist.h"

#include "tkvBufPool.h"
#include "tkvDiskMgr.h"
#include "tkvPage.h"

struct SFrameIdWrapper {
  TD_SLIST_NODE(SFrameIdWrapper);
  frame_id_t id;
};

struct STkvBufPool {
  STkvPage*    pages;
  STkvDiskMgr* pDiskMgr;
  SHashObj*    pgTb;  // page_id_t --> frame_id_t
  TD_SLIST(SFrameIdWrapper) freeList;
  pthread_mutex_t mutex;
};

typedef struct STkvLRUReplacer {
} STkvLRUReplacer;

typedef struct STkvLFUReplacer {
} STkvLFUReplacer;

typedef struct STkvCLKReplacer {
} STkvCLKReplacer;

typedef enum { TKV_LRU_REPLACER = 0, TKV_LFU_REPLACER, TVK_CLK_REPLACER } tkv_replacer_t;

typedef struct STkvReplacer {
  tkv_replacer_t type;
  union {
    STkvLRUReplacer lruRep;
    STkvLFUReplacer lfuRep;
    STkvCLKReplacer clkRep;
  };
} STkvReplacer;
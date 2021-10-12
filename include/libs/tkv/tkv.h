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

#ifndef _TD_TKV_H_
#define _TD_TKV_H_

#ifdef __cplusplus
extern "C" {
#endif

// Types exported
typedef struct STkvDb      STkvDb;
typedef struct STkvOptions STkvOptions;
typedef struct STkvCache   STkvCache;

// DB operations
STkvDb *tkvOpen(const STkvOptions *options, const char *path);
void    tkvClose(STkvDb *db);
void    tkvPut(STkvDb *db, void * /*TODO*/);

// DB options
STkvOptions *tkvOptionsCreate();
void         tkvOptionsDestroy(STkvOptions *);
void         tkvOptionsSetCache(STkvOptions *, STkvCache *);

// DB cache
typedef enum { TKV_LRU_CACHE = 0, TKV_LFU_CACHE = 1 } ETkvCacheType;
STkvCache *tkvCacheCreate(size_t capacity, ETkvCacheType type);
void       tkvCacheDestroy(STkvCache *);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TKV_H_*/
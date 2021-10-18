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

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types exported
typedef struct STkvDb        STkvDb;
typedef struct STkvOpts      STkvOpts;
typedef struct STkvCache     STkvCache;
typedef struct STkvReadOpts  STkvReadOpts;
typedef struct STkvWriteOpts STkvWriteOpts;

// DB operations
STkvDb *tkvOpen(const STkvOpts *options, const char *path);
void    tkvClose(STkvDb *db);
void    tkvPut(STkvDb *db, const STkvWriteOpts *, const char *key, size_t keylen, const char *val, size_t vallen);
char *  tkvGet(STkvDb *db, const STkvReadOpts *, const char *key, size_t keylen, size_t *vallen);
void    tkvCommit(STkvDb *db);

// DB options
STkvOpts *tkvOptsCreate();
void      tkvOptsDestroy(STkvOpts *);
void      tkvOptionsSetCache(STkvOpts *, STkvCache *);
void      tkvOptsSetCreateIfMissing(STkvOpts *, unsigned char);

// DB cache
typedef enum { TKV_LRU_CACHE = 0, TKV_LFU_CACHE = 1 } ETkvCacheType;
STkvCache *tkvCacheCreate(size_t capacity, ETkvCacheType type);
void       tkvCacheDestroy(STkvCache *);

// STkvReadOpts
STkvReadOpts *tkvReadOptsCreate();
void          tkvReadOptsDestroy(STkvReadOpts *);

// STkvWriteOpts
STkvWriteOpts *tkvWriteOptsCreate();
void           tkvWriteOptsDestroy(STkvWriteOpts *);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TKV_H_*/
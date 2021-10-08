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

#ifndef _TD_META_H_
#define _TD_META_H_

#include "taosMsg.h"

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t tuid_t;

// Types exported
typedef struct SMeta             SMeta;
typedef struct SMetaOptions      SMetaOptions;
typedef struct SMetaQueryHandle  SMetaQueryHandle;
typedef struct SMetaQueryOptions SMetaQueryOptions;

// SMeta operations
int    metaCreate(const char *path);
int    metaDestroy(const char *path);
SMeta *metaOpen(SMetaOptions *);
void   metaClose(SMeta *);
int    metaCreateTable(SMeta *, void *);
int    metaDropTable(SMeta *, uint64_t tuid_t);
int    metaAlterTable(SMeta *, void *);
int    metaCommit(SMeta *);

// Options
SMetaOptions *metaOptionsCreate();
void          metaOptionsDestroy(SMetaOptions *);
void          metaOptionsSetCache(SMetaOptions *, size_t capacity);

// SMetaQueryHandle
SMetaQueryHandle *metaQueryHandleCreate(SMetaQueryOptions *);
void              metaQueryHandleDestroy(SMetaQueryHandle *);

// SMetaQueryOptions
SMetaQueryOptions *metaQueryOptionsCreate();
void               metaQueryOptionsDestroy(SMetaQueryOptions *);

#ifdef __cplusplus
}
#endif

#endif /*_TD_META_H_*/
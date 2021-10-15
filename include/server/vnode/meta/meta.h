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

#include "taosmsg.h"

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t tuid_t;

// Types exported
typedef struct SMeta            SMeta;
typedef struct SMetaOpts        SMetaOpts;
typedef struct SMetaQueryHandle SMetaQueryHandle;
typedef struct SMetaQueryOpts   SMetaQueryOpts;
typedef struct STableOpts       STableOpts;

// SMeta operations
int    metaCreate(const char *path);
void   metaDestroy(const char *path);
SMeta *metaOpen(SMetaOpts *);
void   metaClose(SMeta *);
int    metaCreateTable(SMeta *, const STableOpts *);
int    metaDropTable(SMeta *, uint64_t tuid_t);
int    metaAlterTable(SMeta *, void *);
int    metaCommit(SMeta *);

// Options
SMetaOpts *metaOptionsCreate();
void       metaOptionsDestroy(SMetaOpts *);
void       metaOptionsSetCache(SMetaOpts *, size_t capacity);

// SMetaQueryHandle
SMetaQueryHandle *metaQueryHandleCreate(SMetaQueryOpts *);
void              metaQueryHandleDestroy(SMetaQueryHandle *);

// SMetaQueryOpts
SMetaQueryOpts *metaQueryOptionsCreate();
void            metaQueryOptionsDestroy(SMetaQueryOpts *);

// STableOpts
void metaTableOptsInit(STableOpts *, int8_t type, const char *name, const STSchema *pSchema);

#ifdef __cplusplus
}
#endif

#endif /*_TD_META_H_*/

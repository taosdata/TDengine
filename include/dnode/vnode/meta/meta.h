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

#include "impl/metaImpl.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types exported
typedef struct SMeta        SMeta;
typedef struct SMetaOptions SMetaOptions;
typedef struct STbOptions   STbOptions;

// SMeta operations
SMeta *metaOpen(const char *path, const SMetaOptions *pOptions);
void   metaClose(SMeta *pMeta);
void   metaRemove(const char *path);
int    metaCreateTable(SMeta *pMeta, const STbOptions *pTbOptions);
int    metaDropTable(SMeta *pMeta, tb_uid_t uid);
int    metaCommit(SMeta *pMeta);

// Options
void metaOptionsInit(SMetaOptions *pOptions);
void metaOptionsClear(SMetaOptions *pOptions);

// STableOpts
#define META_TABLE_OPTS_DECLARE(name) STableOpts name = {0}
void metaNormalTableOptsInit(STbOptions *pTbOptions, const char *name, const STSchema *pSchema);
void metaSuperTableOptsInit(STbOptions *pTbOptions, const char *name, tb_uid_t uid, const STSchema *pSchema,
                            const STSchema *pTagSchema);
void metaChildTableOptsInit(STbOptions *pTbOptions, const char *name, tb_uid_t suid, const SKVRow tags);
void metaTableOptsClear(STbOptions *pTbOptions);

#ifdef __cplusplus
}
#endif

#endif /*_TD_META_H_*/

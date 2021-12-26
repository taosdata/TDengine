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

#include "mallocator.h"
#include "os.h"
#include "trow.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types exported
typedef struct SMeta SMeta;

#define META_SUPER_TABLE 0
#define META_CHILD_TABLE 1
#define META_NORMAL_TABLE 2

typedef struct SMetaCfg {
  /// LRU cache size
  uint64_t lruSize;
} SMetaCfg;

typedef SVCreateTbReq STbCfg;

// SMeta operations
SMeta *metaOpen(const char *path, const SMetaCfg *pMetaCfg, SMemAllocatorFactory *pMAF);
void   metaClose(SMeta *pMeta);
void   metaRemove(const char *path);
int    metaCreateTable(SMeta *pMeta, STbCfg *pTbCfg);
int    metaDropTable(SMeta *pMeta, tb_uid_t uid);
int    metaCommit(SMeta *pMeta);

// Options
void metaOptionsInit(SMetaCfg *pMetaCfg);
void metaOptionsClear(SMetaCfg *pMetaCfg);

// STbCfg
#define META_INIT_STB_CFG(NAME, TTL, KEEP, SUID, PSCHEMA, PTAGSCHEMA)                   \
  {                                                                                     \
    .name = (NAME), .ttl = (TTL), .keep = (KEEP), .type = META_SUPER_TABLE, .stbCfg = { \
      .suid = (SUID),                                                                   \
      .pSchema = (PSCHEMA),                                                             \
      .pTagSchema = (PTAGSCHEMA)                                                        \
    }                                                                                   \
  }

#define META_INIT_CTB_CFG(NAME, TTL, KEEP, SUID, PTAG)                                                                \
  {                                                                                                                   \
    .name = (NAME), .ttl = (TTL), .keep = (KEEP), .type = META_CHILD_TABLE, .ctbCfg = {.suid = (SUID), .pTag = PTAG } \
  }

#define META_INIT_NTB_CFG(NAME, TTL, KEEP, SUID, PSCHEMA)                                                      \
  {                                                                                                            \
    .name = (NAME), .ttl = (TTL), .keep = (KEEP), .type = META_NORMAL_TABLE, .ntbCfg = {.pSchema = (PSCHEMA) } \
  }

#define META_CLEAR_TB_CFG(pTbCfg)

int   metaEncodeTbCfg(void **pBuf, STbCfg *pTbCfg);
void *metaDecodeTbCfg(void *pBuf, STbCfg *pTbCfg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_META_H_*/

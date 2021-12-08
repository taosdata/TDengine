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

#include "os.h"
#include "trow.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types exported
typedef uint64_t     tb_uid_t;
typedef struct SMeta SMeta;

#define META_SUPER_TABLE 0
#define META_CHILD_TABLE 1
#define META_NORMAL_TABLE 2

typedef struct SMetaCfg {
  /// LRU cache size
  uint64_t lruSize;
} SMetaCfg;

typedef struct STbCfg {
  /// name of the table
  char *name;
  /// time to live of the table
  uint32_t ttl;
  /// keep time of this table
  uint32_t keep;
  /// type of table
  uint8_t type;
  union {
    /// super table configurations
    struct {
      /// super table UID
      tb_uid_t suid;
      /// row schema
      STSchema *pSchema;
      /// tag schema
      STSchema *pTagSchema;
    } stbCfg;

    /// normal table configuration
    struct {
      /// row schema
      STSchema *pSchema;
    } ntbCfg;
    /// child table configuration
    struct {
      /// super table UID
      tb_uid_t suid;
      SKVRow   pTag;
    } ctbCfg;
  };
} STbCfg;

// SMeta operations
SMeta *metaOpen(const char *path, const SMetaCfg *pOptions);
void   metaClose(SMeta *pMeta);
void   metaRemove(const char *path);
int    metaCreateTable(SMeta *pMeta, STbCfg *pTbCfg);
int    metaDropTable(SMeta *pMeta, tb_uid_t uid);
int    metaCommit(SMeta *pMeta);

// Options
void metaOptionsInit(SMetaCfg *pOptions);
void metaOptionsClear(SMetaCfg *pOptions);

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

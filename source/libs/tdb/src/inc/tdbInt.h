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

#ifndef _TD_TDB_INTERNAL_H_
#define _TD_TDB_INTERNAL_H_

#include "tlist.h"
#include "tlockfree.h"

#include "tdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SPgFile SPgFile;

// pgno_t
typedef int32_t pgno_t;
#define TDB_IVLD_PGNO ((pgno_t)0)

// fileid
#define TDB_FILE_ID_LEN 24

// pgid_t
typedef struct {
  uint8_t fileid[TDB_FILE_ID_LEN];
  pgno_t  pgno;
} pgid_t;

#define TDB_IVLD_PGID (pgid_t){0, TDB_IVLD_PGNO};

static FORCE_INLINE int tdbCmprPgId(const void *p1, const void *p2) {
  pgid_t *pgid1 = (pgid_t *)p1;
  pgid_t *pgid2 = (pgid_t *)p2;
  int     rcode;

  rcode = memcmp(pgid1->fileid, pgid2->fileid, TDB_FILE_ID_LEN);
  if (rcode) {
    return rcode;
  } else {
    if (pgid1->pgno > pgid2->pgno) {
      return 1;
    } else if (pgid1->pgno < pgid2->pgno) {
      return -1;
    } else {
      return 0;
    }
  }
}

// framd_id_t
typedef int32_t frame_id_t;

// pgsz_t
#define TDB_MIN_PGSIZE 512
#define TDB_MAX_PGSIZE 65536
#define TDB_DEFAULT_PGSIZE 4096
#define TDB_IS_PGSIZE_VLD(s) (((s) >= TDB_MIN_PGSIZE) && ((s) <= TDB_MAX_PGSIZE))

// pgoff_t
typedef pgsz_t pgoff_t;

// cache
#define TDB_DEFAULT_CACHE_SIZE (256 * 4096)  // 1M

// dbname
#define TDB_MAX_DBNAME_LEN 24

// tdb_log
#define tdbError(var)

typedef TD_DLIST(STDb) STDbList;
typedef TD_DLIST(SPgFile) SPgFileList;
typedef TD_DLIST_NODE(SPgFile) SPgFileListNode;

#define TERR_A(val, op, flag)  \
  do {                         \
    if (((val) = (op)) != 0) { \
      goto flag;               \
    }                          \
  } while (0)

#define TERR_B(val, op, flag)     \
  do {                            \
    if (((val) = (op)) == NULL) { \
      goto flag;                  \
    }                             \
  } while (0)

#define TDB_VARIANT_LEN (int)-1

// page payload format
// <keyLen> + <valLen> + [key] + [value]
#define TDB_DECODE_PAYLOAD(pPayload, keyLen, pKey, valLen, pVal) \
  do {                                                           \
    if ((keyLen) == TDB_VARIANT_LEN) {                           \
      /* TODO: decode the keyLen */                              \
    }                                                            \
    if ((valLen) == TDB_VARIANT_LEN) {                           \
      /* TODO: decode the valLen */                              \
    }                                                            \
    /* TODO */                                                   \
  } while (0)

#define TDB_JOURNAL_NAME "tdb.journal"

#include "tdbUtil.h"

#include "tdbBtree.h"

#include "tdbPgCache.h"

#include "tdbPgFile.h"

#include "tdbEnv.h"

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_INTERNAL_H_*/

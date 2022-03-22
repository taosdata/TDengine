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

// #include "tdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int8_t   i8;
typedef int16_t  i16;
typedef int32_t  i32;
typedef int64_t  i64;
typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

// p must be u8 *
#define TDB_GET_U24(p) ((p)[0] * 65536 + *(u16 *)((p) + 1))
#define TDB_PUT_U24(p, v)       \
  do {                          \
    int tv = (v);               \
    (p)[2] = tv & 0xff;         \
    (p)[1] = (tv >> 8) & 0xff;  \
    (p)[0] = (tv >> 16) & 0xff; \
  } while (0)

// SPgno
typedef u32 SPgno;
#define TDB_IVLD_PGNO ((pgno_t)0)

// fileid
#define TDB_FILE_ID_LEN 24

// pgid_t
typedef struct {
  uint8_t fileid[TDB_FILE_ID_LEN];
  SPgno   pgno;
} pgid_t, SPgid;

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

#define TDB_IS_SAME_PAGE(pPgid1, pPgid2) (tdbCmprPgId(pPgid1, pPgid2) == 0)

// pgsz_t
#define TDB_MIN_PGSIZE       512       // 512B
#define TDB_MAX_PGSIZE       16777216  // 16M
#define TDB_DEFAULT_PGSIZE   4096
#define TDB_IS_PGSIZE_VLD(s) (((s) >= TDB_MIN_PGSIZE) && ((s) <= TDB_MAX_PGSIZE))

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

#define TDB_VARIANT_LEN ((int)-1)

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

typedef int (*FKeyComparator)(const void *pKey1, int kLen1, const void *pKey2, int kLen2);

#define TDB_JOURNAL_NAME "tdb.journal"

#define TDB_FILENAME_LEN 128

#define TDB_DEFAULT_FANOUT 6

#define BTREE_MAX_DEPTH 20

#define TDB_FLAG_IS(flags, flag)     ((flags) == (flag))
#define TDB_FLAG_HAS(flags, flag)    (((flags) & (flag)) != 0)
#define TDB_FLAG_NO(flags, flag)     ((flags) & (flag) == 0)
#define TDB_FLAG_ADD(flags, flag)    ((flags) |= (flag))
#define TDB_FLAG_REMOVE(flags, flag) ((flags) &= (~(flag)))

typedef struct SPager  SPager;
typedef struct SPCache SPCache;
typedef struct SPage   SPage;

#include "tdbUtil.h"

#include "tdbPCache.h"

#include "tdbPager.h"

#include "tdbBtree.h"

#include "tdbEnv.h"

#include "tdbDb.h"

#include "tdbPage.h"

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_INTERNAL_H_*/

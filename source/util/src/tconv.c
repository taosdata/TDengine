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

#define ALLOW_FORBID_FUNC

#include "tconv.h"
#include "thash.h"
#include "osString.h"

#define CONV_MAX_NUM 512

// Upper bound on the number of distinct charsets cached in gConvInfo. The cache
// never evicts (converters are borrowed concurrently via taosAcquireConv, so
// runtime eviction would be use-after-free) and each entry pins CONV_MAX_NUM*2
// iconv handles until taosConvDestroy. Client-supplied charset names reach here
// via nodesDecodeCharsetName, so without a cap a client could grow the cache
// without bound. Real deployments use one charset per client; 10 is generous.
#define CONV_MAX_CHARSETS 10

SHashObj *gConvInfo = NULL;

// Serializes cache creation, lookup, insertion, and teardown. It prevents
// taosConvDestroy from freeing the hash while taosConvInit accesses it; it does
// not make teardown safe while callers hold or use a converter context.
static int32_t gConvLock = 0;

// M2C: Mbs --> Ucs4
// C2M: Ucs4--> Mbs

static void taosConvDestroyInner(void *arg) {
#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  SConvInfo *info = (SConvInfo *)arg;
  if (info == NULL) {
    return;
  }
  // This also runs on taosConvInit's FAILED path, where an iconv_open (or
  // the pool calloc itself) failed partway through populating the pool.
  // gConvMaxNum[type] there still holds the *requested* pool size, not how
  // many slots were actually opened: the untouched tail is calloc-zeroed
  // (skip via the NULL check), and the slot that failed and triggered
  // FAILED holds the (iconv_t)-1 sentinel instead -- skip that too, the
  // same as the call site's own (iconv_t)-1 check. Passing -1 to
  // iconv_close is undefined: glibc tolerates it, but win-iconv
  // dereferences it and crashes. On the fully-initialized teardown path
  // every slot is a real non-NULL, non-(-1) handle, so neither check
  // changes anything there.
  if (info->gConv[M2C] != NULL) {
    for (int32_t i = 0; i < info->gConvMaxNum[M2C]; ++i) {
      iconv_t c = info->gConv[M2C][i].conv;
      if (c != NULL && c != (iconv_t)-1) {
        (void)iconv_close(c);
      }
    }
  }
  if (info->gConv[C2M] != NULL) {
    for (int32_t i = 0; i < info->gConvMaxNum[C2M]; ++i) {
      iconv_t c = info->gConv[C2M][i].conv;
      if (c != NULL && c != (iconv_t)-1) {
        (void)iconv_close(c);
      }
    }
  }
  taosMemoryFreeClear(info->gConv[M2C]);
  taosMemoryFreeClear(info->gConv[C2M]);

  info->gConvMaxNum[M2C] = -1;
  info->gConvMaxNum[C2M] = -1;
#endif
}

static void taosConvLock(void) {
  for (int32_t i = 1; atomic_val_compare_exchange_32(&gConvLock, 0, 1) != 0;
       ++i) {
    if (i % 1000 == 0) {
      uInfo("haven't acquired converter cache lock after spinning %d times", i);
      (void)sched_yield();
    }
  }
}

static void taosConvUnlock(void) { atomic_store_32(&gConvLock, 0); }

void* taosConvInit(const char* charset) {
#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  if (charset == NULL){
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  // Reject rather than silently truncate: tstrncpy force-terminates within
  // TD_CHARSET_LEN, so an overlong name would otherwise resolve (and cache)
  // under a shortened key the caller never asked for.
  if (strlen(charset) >= TD_CHARSET_LEN) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  void *conv = NULL;

  // Normalize to upper case so that case variants of the same encoding (iconv
  // matches case-insensitively) collapse to one cache entry instead of each
  // spelling pinning its own converter pool. Charset names are ASCII.
  char csKey[TD_CHARSET_LEN] = {0};
  tstrncpy(csKey, charset, sizeof(csKey));
  for (char *p = csKey; *p != '\0'; ++p) {
    if (*p >= 'a' && *p <= 'z') *p -= 'a' - 'A';
  }

  taosConvLock();

  if (atomic_load_ptr(&gConvInfo) == NULL){
    SHashObj *pNew = taosHashInit(0, MurmurHash3_32, false, HASH_ENTRY_LOCK);
    if (pNew == NULL) {
      goto END;
    }
    taosHashSetFreeFp(pNew, taosConvDestroyInner);
    atomic_store_ptr(&gConvInfo, pNew);
  }

  conv = taosHashGet(gConvInfo, csKey, strlen(csKey));
  if (conv != NULL){
    goto END;
  }

  // Reject once the cache is full rather than evict a possibly in-use entry.
  if (taosHashGetSize(gConvInfo) >= CONV_MAX_CHARSETS) {
    uError("charset context cache full (%d), reject charset:%s",
           CONV_MAX_CHARSETS, csKey);
    terrno = TSDB_CODE_CHARSET_CONVERT_CACHE_FULL;
    goto END;
  }

  SConvInfo info = {0};
  info.gConvMaxNum[M2C] = CONV_MAX_NUM;
  info.gConvMaxNum[C2M] = CONV_MAX_NUM;
  tstrncpy(info.charset, csKey, sizeof(info.charset));

  info.gConv[M2C] = taosMemoryCalloc(info.gConvMaxNum[M2C], sizeof(SConv));
  if (info.gConv[M2C] == NULL) {
    goto FAILED;
  }

  info.gConv[C2M] = taosMemoryCalloc(info.gConvMaxNum[C2M], sizeof(SConv));
  if (info.gConv[C2M] == NULL) {
    goto FAILED;
  }

  for (int32_t i = 0; i < info.gConvMaxNum[M2C]; ++i) {
    info.gConv[M2C][i].conv = iconv_open(DEFAULT_UNICODE_ENCODEC, csKey);
    if ((iconv_t)-1 == info.gConv[M2C][i].conv) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      goto FAILED;
    }
  }
  for (int32_t i = 0; i < info.gConvMaxNum[C2M]; ++i) {
    info.gConv[C2M][i].conv = iconv_open(csKey, DEFAULT_UNICODE_ENCODEC);
    if ((iconv_t)-1 == info.gConv[C2M][i].conv) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      goto FAILED;
    }
  }

  int32_t code = taosHashPut(gConvInfo, csKey, strlen(csKey), &info, sizeof(info));
  if (code != 0){
    goto FAILED;
  }
  conv = taosHashGet(gConvInfo, csKey, strlen(csKey));
  goto END;

FAILED:
  taosConvDestroyInner(&info);

END:
  taosConvUnlock();
  return conv;
#else
  return NULL;
#endif
}

void taosConvDestroy() {
#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  // The caller must quiesce all conversion users and charset-context owners
  // before cleanup. This lock only serializes teardown with taosConvInit.
  taosConvLock();
  SHashObj *pOld = atomic_load_ptr(&gConvInfo);
  atomic_store_ptr(&gConvInfo, NULL);
  taosHashCleanup(pOld);
  taosConvUnlock();
#endif
}

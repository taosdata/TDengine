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
SHashObj *gConvInfo = NULL;

// M2C: Mbs --> Ucs4
// C2M: Ucs4--> Mbs

static void taosConvDestroyInner(void *arg) {
#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  SConvInfo *info = (SConvInfo *)arg;
  if (info == NULL) {
    return;
  }
  for (int32_t i = 0; i < info->gConvMaxNum[M2C]; ++i) {
    (void)iconv_close(info->gConv[M2C][i].conv);
  }
  for (int32_t i = 0; i < info->gConvMaxNum[C2M]; ++i) {
    (void)iconv_close(info->gConv[C2M][i].conv);
  }
  taosMemoryFreeClear(info->gConv[M2C]);
  taosMemoryFreeClear(info->gConv[C2M]);

  info->gConvMaxNum[M2C] = -1;
  info->gConvMaxNum[C2M] = -1;
#endif
}

void* taosConvInit(const char* charset) {
#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  if (charset == NULL){
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  void*  conv = NULL;
  static int32_t lock_c = 0;

  for (int i = 1; atomic_val_compare_exchange_32(&lock_c, 0, 1) != 0; ++i) {
    if (i % 1000 == 0) {
      uInfo("haven't acquire lock after spin %d times.", i);
      (void)sched_yield();
    }
  }

  if (gConvInfo == NULL){
    gConvInfo = taosHashInit(0, MurmurHash3_32, false, HASH_ENTRY_LOCK);
    if (gConvInfo == NULL) {
      atomic_store_32(&lock_c, 0);
      goto END;
    }
    taosHashSetFreeFp(gConvInfo, taosConvDestroyInner);
  }

  conv = taosHashGet(gConvInfo, charset, strlen(charset));
  if (conv != NULL){
    goto END;
  }

  SConvInfo info = {0};
  info.gConvMaxNum[M2C] = CONV_MAX_NUM;
  info.gConvMaxNum[C2M] = CONV_MAX_NUM;
  tstrncpy(info.charset, charset, sizeof(info.charset));

  info.gConv[M2C] = taosMemoryCalloc(info.gConvMaxNum[M2C], sizeof(SConv));
  if (info.gConv[M2C] == NULL) {
    goto FAILED;
  }

  info.gConv[C2M] = taosMemoryCalloc(info.gConvMaxNum[C2M], sizeof(SConv));
  if (info.gConv[C2M] == NULL) {
    goto FAILED;
  }

  for (int32_t i = 0; i < info.gConvMaxNum[M2C]; ++i) {
    info.gConv[M2C][i].conv = iconv_open(DEFAULT_UNICODE_ENCODEC, charset);
    if ((iconv_t)-1 == info.gConv[M2C][i].conv) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      goto FAILED;
    }
  }
  for (int32_t i = 0; i < info.gConvMaxNum[C2M]; ++i) {
    info.gConv[C2M][i].conv = iconv_open(charset, DEFAULT_UNICODE_ENCODEC);
    if ((iconv_t)-1 == info.gConv[C2M][i].conv) {
      terrno = TAOS_SYSTEM_ERROR(ERRNO);
      goto FAILED;
    }
  }

  int32_t code = taosHashPut(gConvInfo, charset, strlen(charset), &info, sizeof(info));
  if (code != 0){
    goto FAILED;
  }
  conv = taosHashGet(gConvInfo, charset, strlen(charset));
  goto END;

FAILED:
  taosConvDestroyInner(&info);

END:
  atomic_store_32(&lock_c, 0);
  return conv;
#else
  return NULL;
#endif
}

void taosConvDestroy() {
#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  taosHashCleanup(gConvInfo);
  gConvInfo = NULL;  
#endif
}

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

#ifndef _TD_UTIL_TREALLOC_H_
#define _TD_UTIL_TREALLOC_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

static FORCE_INLINE int32_t tRealloc(uint8_t **ppBuf, int64_t size) {
  int32_t  code = 0;
  int64_t  bsize = 0;
  uint8_t *pBuf = NULL;

  if (*ppBuf) {
    pBuf = (*ppBuf) - sizeof(int64_t);
    bsize = *(int64_t *)pBuf;
  }

  if (bsize >= size) goto _exit;

  if (bsize == 0) bsize = 64;
  while (bsize < size) {
    bsize *= 2;
  }

  pBuf = (uint8_t *)taosMemoryRealloc(pBuf, bsize + sizeof(int64_t));
  if (pBuf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  *(int64_t *)pBuf = bsize;
  *ppBuf = pBuf + sizeof(int64_t);

_exit:
  return code;
}

#define tFree(BUF)                                        \
  do {                                                    \
    if (BUF) {                                            \
      taosMemoryFree((uint8_t *)(BUF) - sizeof(int64_t)); \
      (BUF) = NULL;                                       \
    }                                                     \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TREALLOC_H_*/

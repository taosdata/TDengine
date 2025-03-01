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

#ifndef _TD_UTIL_CHECKSUM_H_
#define _TD_UTIL_CHECKSUM_H_

#include "tcrc32c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t TSCKSUM;

static FORCE_INLINE TSCKSUM taosCalcChecksum(TSCKSUM csi, const uint8_t *stream, uint32_t ssize) {
  return (*crc32c)(csi, stream, (size_t)ssize);
}

static FORCE_INLINE int32_t taosCalcChecksumAppend(TSCKSUM csi, uint8_t *stream, uint32_t ssize) {
  if (ssize < sizeof(TSCKSUM)) return TSDB_CODE_INVALID_PARA;

  *((TSCKSUM *)(stream + ssize - sizeof(TSCKSUM))) = (*crc32c)(csi, stream, (size_t)(ssize - sizeof(TSCKSUM)));

  return 0;
}

static FORCE_INLINE int32_t taosCheckChecksum(const uint8_t *stream, uint32_t ssize, TSCKSUM checksum) {
  return (checksum != (*crc32c)(0, stream, (size_t)ssize));
}

static FORCE_INLINE int32_t taosCheckChecksumWhole(const uint8_t *stream, uint32_t ssize) {
  if (ssize < sizeof(TSCKSUM)) return 0;

#if (_WIN64)
  return 1;
#else
  return *((TSCKSUM *)(stream + ssize - sizeof(TSCKSUM))) == (*crc32c)(0, stream, (size_t)(ssize - sizeof(TSCKSUM)));
#endif
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_CHECKSUM_H_*/

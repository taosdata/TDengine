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

#ifndef TDENGINE_TCONV_H
#define TDENGINE_TCONV_H

#ifdef __cplusplus
extern "C" {
#endif

//#include "osString.h"
//
//bool    taosValidateEncodec(const char *encodec);
//int32_t taosUcs4len(TdUcs4 *ucs4);
void*   taosConvInit(const char* charset);
void    taosConvDestroy();
//iconv_t taosAcquireConv(int32_t *idx, ConvType type, void* charsetCxt);
//void    taosReleaseConv(int32_t idx, iconv_t conv, ConvType type, void* charsetCxt);
//int32_t taosUcs4ToMbs(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs, void* charsetCxt);
//int32_t taosUcs4ToMbsEx(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs, iconv_t conv);
//bool    taosMbsToUcs4(const char *mbs, size_t mbs_len, TdUcs4 *ucs4, int32_t ucs4_max_len, int32_t *len, void* charsetCxt);
//int32_t taosUcs4Compare(TdUcs4 *f1_ucs4, TdUcs4 *f2_ucs4, int32_t bytes);
//int32_t taosUcs4Copy(TdUcs4 *target_ucs4, TdUcs4 *source_ucs4, int32_t len_ucs4);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCONV_H

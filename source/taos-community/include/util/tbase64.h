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

#ifndef _TD_UTIL_BASE64_H_
#define _TD_UTIL_BASE64_H_

#include "os.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t base64_decode(const char *value, int32_t inlen, int32_t *outlen, uint8_t **result);
int32_t base64_encode(const uint8_t *value, int32_t vlen, char **result);

void     tbase64_encode(uint8_t *out, const uint8_t *input, size_t in_len, VarDataLenT out_len);
int32_t  tbase64_decode(uint8_t *out, const uint8_t *input, size_t in_len, VarDataLenT *out_len);
uint32_t tbase64_encode_len(size_t in_len);
uint32_t tbase64_decode_len(size_t in_len);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_BASE64_H_*/

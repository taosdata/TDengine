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

#include <stdint.h>

#ifndef _TD_ENCRYPT_
#define _TD_ENCRYPT_

#ifdef __cplusplus
extern "C" {
#endif

int32_t tAesEncrypt(char *plainText, int32_t plainTextLen, char *cipherText, int32_t cipherTextLen, char *key,
                    int32_t keyLen);
int32_t tAesDecrypt(char *cipherText, int32_t cipherTextLen, char *plainText, int32_t plainTextLen, char *key,
                    int32_t keyLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_ENCRYPT_*/
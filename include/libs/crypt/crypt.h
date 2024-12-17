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

#ifndef _CRYPT_H_
#define _CRYPT_H_
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SCryptOpts {
  int32_t len;
  char*   source;
  char*   result;
  int32_t unitLen;
  char    key[ENCRYPT_KEY_LEN + 1];
} SCryptOpts;

int32_t CBC_Decrypt(SCryptOpts* opts);
int32_t CBC_Encrypt(SCryptOpts* opts);

#ifdef __cplusplus
}
#endif

#endif  // _CRYPT_H_
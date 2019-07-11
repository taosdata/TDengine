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

#ifndef _TAOS_KEY_H_
#define _TAOS_KEY_H_

#include <stdbool.h>
#include <stdint.h>
#include "tlog.h"
#include "tmd5.h"
#include "tutil.h"

unsigned char *base64_decode(const char *value, int inlen, int *outlen);
char *base64_encode(const unsigned char *value, int vlen);
char *taosDesEncode(int64_t key, char *src, int len);
char *taosDesDecode(int64_t key, char *src, int len);

#endif

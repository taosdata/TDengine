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
#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <string.h>

// formatTotp formats the TOTP code with leading zeros to match the specified digit length.
int taosFormatTotp(int32_t totp, int digits, char *buffer, size_t size) {
  return snprintf(buffer, size, "%0*d", digits, totp);
}

#ifndef TD_ENTERPRISE

int32_t taosGenerateTotpCode(const uint8_t *secret, size_t secretLen, int digits) {
  return 0;
}

int taosVerifyTotpCode(const uint8_t *secret, size_t secretLen, int32_t userCode, int digits, int window) {
  return 1;
}

int taosGenerateTotpSecret(const char *seed, size_t seedLen, uint8_t *secret, size_t secretLen) {
  memset(secret, 0, secretLen);
  return secretLen;
}

#endif // TD_ENTERPRISE

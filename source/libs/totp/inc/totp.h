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


#ifndef _TD_TOTP_H_
#define _TD_TOTP_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


// format the TOTP code with leading zeros to match the specified digit length.
// return the number of characters written, excluding the null terminator.
int taosFormatTotp(int32_t totp, int digits, char *buffer, size_t size);

// generate TOTP code for given secret at current time.
// secret is a byte array, not a base32 string.
// return the generated TOTP code, or -1 on error.
int32_t taosGenerateTotpCode(const uint8_t *secret, size_t secretLen, int digits);

// verify TOTP code for given secret at current time with allowed window.
// secret is a byte array, not a base32 string.
// return 1 if the code is correct, 0 otherwise.
int taosVerifyTotpCode(const uint8_t *secret, size_t secretLen, int32_t userCode, int digits, int window);

// generate TOTP secret from seed
// if seedLen is 0, the seed is treated as a null-terminated string and its length is determined using strlen.
// secret is a byte array, not a base32 string.
// return the length of the generated secret, or -1 on error.
int taosGenerateTotpSecret(const char *seed, size_t seedLen, uint8_t *secret, size_t secretLen);


#ifdef __cplusplus
}
#endif

#endif /*_TD_TOTP_H_*/

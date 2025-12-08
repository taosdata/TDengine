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
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// OpenSSL HMAC 函数
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include "tmd5.h"

// doGenerateTotp generates a TOTP code based on the provided secret and time.
// secret is a byte array, not a base32 string
static int32_t doGenerateTotp(const uint8_t *secret, size_t secretLen, uint64_t tm, int digits) {
  if (secretLen < 16) {
    return -1;  // secret is too short
  }

  // convert timestamp to big-endian if system is little-endian
  uint32_t endianness = 0xdeadbeef;
  if ((*(const uint8_t *)&endianness) == 0xef) {
    tm = ((tm & 0x00000000ffffffff) << 32) | ((tm & 0xffffffff00000000) >> 32);
    tm = ((tm & 0x0000ffff0000ffff) << 16) | ((tm & 0xffff0000ffff0000) >> 16);
    tm = ((tm & 0x00ff00ff00ff00ff) <<  8) | ((tm & 0xff00ff00ff00ff00) >>  8);
  };

  // calculate HMAC-SHA1
  uint8_t      hmacResult[EVP_MAX_MD_SIZE];
  unsigned int hmacLen;
  HMAC(EVP_sha1(), secret, secretLen, (const uint8_t *)&tm, sizeof(tm), hmacResult, &hmacLen);

  // use the lower 4 bits of the last byte as offset
  int offset = hmacResult[hmacLen - 1] & 0x0F;

  int32_t binary = (hmacResult[offset] & 0x7F) << 24;
  binary |= (hmacResult[offset + 1] & 0xFF) << 16;
  binary |= (hmacResult[offset + 2] & 0xFF) << 8;
  binary |= hmacResult[offset + 3] & 0xFF;

  return binary % (int32_t)pow(10, digits);
}

// formatTotp formats the TOTP code with leading zeros to match the specified digit length.
int taosFormatTotp(int32_t totp, int digits, char *buffer, size_t size) {
  return snprintf(buffer, size, "%0*d", digits, totp);
}

// generate TOTP code for given secret at current time
// secret is a byte array, not a base32 string
int32_t taosGenerateTotpCode(const uint8_t *secret, size_t secretLen, int digits) {
  if (secretLen < 16) {
    return -1;  // secret is too short
  }

  uint64_t now = taosGetTimestampSec() / 30;
  return doGenerateTotp(secret, secretLen, now, digits);
}

// verify TOTP code for given secret at current time with allowed window
// secret is a byte array, not a base32 string
int taosVerifyTotpCode(const char *secret, size_t secretLen, int32_t userCode, int digits, int window) {
  if (secretLen < 16) {
    return 0;  // secret is too short
  }

  uint64_t now = taosGetTimestampSec() / 30;
  for (int i = -window; i <= window; i++) {
    if (doGenerateTotp(secret, secretLen, now + i, digits) == userCode) {
      return 1;
    }
  }

  return 0;
}

// generate TOTP secret from seed
// secret is a byte array, not a base32 string
int taosGenerateTotpSecret(const char *seed, size_t seedLen, uint8_t *secret, size_t secretLen) {
  // TOTP secret requires at least 16 characters for adequate security
  if (secretLen < 16) {
    return 0;
  }

  if (seedLen == 0) {
    seedLen = strlen(seed);
  }

  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, (uint8_t*)seed, seedLen);
  tMD5Final(&context);
  (void)memcpy(secret, context.digest, sizeof(context.digest));

  return sizeof(context.digest);
}

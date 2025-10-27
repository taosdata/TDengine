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
#include "tbase64.h"

static char basis_64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

int32_t base64_encode(const uint8_t *value, int32_t vlen, char **result) {
  uint8_t oval = 0;
  *result = (char *)taosMemoryMalloc((size_t)(vlen * 4) / 3 + 10);
  if (*result == NULL) {
    return terrno;
  }
  char *out = *result;
  while (vlen >= 3) {
    *out++ = basis_64[value[0] >> 2];
    *out++ = basis_64[((value[0] << 4) & 0x30) | (value[1] >> 4)];
    *out++ = basis_64[((value[1] << 2) & 0x3C) | (value[2] >> 6)];
    *out++ = basis_64[value[2] & 0x3F];
    value += 3;
    vlen -= 3;
  }
  if (vlen > 0) {
    *out++ = basis_64[value[0] >> 2];
    oval = (value[0] << 4) & 0x30;
    if (vlen > 1) oval |= value[1] >> 4;
    *out++ = basis_64[oval];
    *out++ = (vlen < 2) ? '=' : basis_64[(value[1] << 2) & 0x3C];
    *out++ = '=';
  }
  *out = '\0';
  return 0;
}

#define CHAR64(c) (((c) < 0 || (c) > 127) ? -1 : index_64[(c)])

static signed char index_64[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55,
    56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
    13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32,
    33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1};

int32_t base64_decode(const char *value, int32_t inlen, int32_t *outlen, uint8_t **result) {
  int32_t c1, c2, c3, c4;
  *result = (uint8_t *)taosMemoryMalloc((size_t)(inlen * 3) / 4 + 1);
  if (*result == NULL) {
    return terrno;
  }
  uint8_t *out = *result;

  *outlen = 0;

  while (1) {
    if (value[0] == 0) {
      *out = '\0';
      return 0;
    }

    // skip \r\n
    if (value[0] == '\n' || value[0] == '\r') {
      value += 1;
      continue;
    }

    c1 = value[0];
    if (CHAR64(c1) == -1) goto base64_decode_error;
    c2 = value[1];
    if (CHAR64(c2) == -1) goto base64_decode_error;
    c3 = value[2];
    if ((c3 != '=') && (CHAR64(c3) == -1)) goto base64_decode_error;
    c4 = value[3];
    if ((c4 != '=') && (CHAR64(c4) == -1)) goto base64_decode_error;

    value += 4;
    *out++ = (uint8_t)((CHAR64(c1) << 2) | (CHAR64(c2) >> 4));
    *outlen += 1;
    if (c3 != '=') {
      *out++ = (uint8_t)(((CHAR64(c2) << 4) & 0xf0) | (CHAR64(c3) >> 2));
      *outlen += 1;
      if (c4 != '=') {
        *out++ = (uint8_t)(((CHAR64(c3) << 6) & 0xc0) | CHAR64(c4));
        *outlen += 1;
      }
    }
  }

base64_decode_error:
  taosMemoryFree(*result);
  *result = 0;
  *outlen = 0;

  return TSDB_CODE_INVALID_DATA_FMT;
}

static char tbase64_encoding_table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

void tbase64_encode(uint8_t *out, const uint8_t *input, size_t in_len, VarDataLenT out_len) {
  for (size_t i = 0, j = 0; i < in_len;) {
    unsigned int octet_a = i < in_len ? input[i++] : 0;
    unsigned int octet_b = i < in_len ? input[i++] : 0;
    unsigned int octet_c = i < in_len ? input[i++] : 0;

    unsigned int triple = (octet_a << 16) | (octet_b << 8) | octet_c;

    out[j++] = tbase64_encoding_table[(triple >> 18) & 0x3F];
    out[j++] = tbase64_encoding_table[(triple >> 12) & 0x3F];
    out[j++] = tbase64_encoding_table[(triple >> 6) & 0x3F];
    out[j++] = tbase64_encoding_table[triple & 0x3F];
  }

  for (int k = 0; k < (3 - (in_len % 3)) % 3; k++) {
    out[out_len - k - 1] = '=';
  }
}

static TdThreadOnce tbase64_decoding_table_building = PTHREAD_ONCE_INIT;

static char tbase64_decoding_table[256] = {0};

static void tbase64_build_decoding_table() {
  for (int i = 0; i < 64; i++) {
    tbase64_decoding_table[(unsigned char)tbase64_encoding_table[i]] = i;
  }
}

void tbase64_decode(uint8_t *out, const uint8_t *input, size_t in_len, VarDataLenT out_len) {
  (void)taosThreadOnce(&tbase64_decoding_table_building, tbase64_build_decoding_table);

  if (in_len % 4 != 0) {
    out[0] = 0;
    return;
  }

  if (input[in_len - 1] == '=') out_len--;
  if (input[in_len - 2] == '=') out_len--;

  for (int i = 0, j = 0; i < in_len;) {
    uint32_t sextet_a = input[i] == '=' ? 0 & i++ : tbase64_decoding_table[input[i++]];
    uint32_t sextet_b = input[i] == '=' ? 0 & i++ : tbase64_decoding_table[input[i++]];
    uint32_t sextet_c = input[i] == '=' ? 0 & i++ : tbase64_decoding_table[input[i++]];
    uint32_t sextet_d = input[i] == '=' ? 0 & i++ : tbase64_decoding_table[input[i++]];

    uint32_t triple = (sextet_a << 3 * 6) + (sextet_b << 2 * 6) + (sextet_c << 1 * 6) + (sextet_d << 0 * 6);

    if (j < out_len) out[j++] = (triple >> 2 * 8) & 0xFF;
    if (j < out_len) out[j++] = (triple >> 1 * 8) & 0xFF;
    if (j < out_len) out[j++] = (triple >> 0 * 8) & 0xFF;
  }
}

uint32_t tbase64_encode_len(size_t in_len) { return 4 * ((in_len + 2) / 3); }

uint32_t tbase64_decode_len(size_t in_len) { return in_len / 4 * 3; }

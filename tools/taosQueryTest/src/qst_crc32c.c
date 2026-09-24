/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#include "qst_crc32c.h"

#include <stddef.h>
#include <string.h>

#if defined(__i386__) || defined(__x86_64__)
#include <nmmintrin.h>

__attribute__((target("sse4.2")))
static uint32_t qstCrc32cSse42(uint32_t crc, const uint8_t *bytes,
                               uint64_t length) {
  uint64_t value64 = 0;
  uint32_t value = ~crc;

#if defined(__x86_64__)
  while (length >= sizeof(value64)) {
    memcpy(&value64, bytes, sizeof(value64));
    value = (uint32_t)_mm_crc32_u64(value, value64);
    bytes += sizeof(value64);
    length -= sizeof(value64);
  }
#endif
  while (length != 0) {
    value = _mm_crc32_u8(value, *bytes++);
    --length;
  }
  return ~value;
}

static int32_t qstCrc32cHasSse42(void) {
  __builtin_cpu_init();
  return __builtin_cpu_supports("sse4.2");
}
#endif

uint32_t qstCrc32c(uint32_t crc, const void *data, uint64_t length) {
  const uint8_t *bytes = data;
  uint32_t value = ~crc;

  if (NULL == data && length != 0) {
    return 0;
  }

#if defined(__i386__) || defined(__x86_64__)
  if (qstCrc32cHasSse42()) {
    return qstCrc32cSse42(crc, bytes, length);
  }
#endif

  for (uint64_t index = 0; index < length; ++index) {
    value ^= bytes[index];
    for (uint32_t bit = 0; bit < 8; ++bit) {
      value = (value >> 1) ^ ((value & 1U) ? 0x82F63B78U : 0U);
    }
  }

  return ~value;
}

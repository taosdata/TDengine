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

#ifndef _TD_UTIL_ENCODE_H_
#define _TD_UTIL_ENCODE_H_

#include "tcoding.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  td_endian_t endian;
  uint8_t*    data;
  int64_t     size;
  int64_t     pos;
} SEncoder;

typedef struct {
  td_endian_t    endian;
  const uint8_t* data;
  int64_t        size;
  int16_t        pos;
} SDecoder;

#define tPut(TYPE, BUF, VAL) ((TYPE*)(BUF))[0] = (VAL)
#define tGet(TYPE, BUF, VAL) (VAL) = ((TYPE*)(BUF))[0]

#define tRPut16(PDEST, PSRC)                      \
  ((uint8_t*)(PDEST))[0] = ((uint8_t*)(PSRC))[1]; \
  ((uint8_t*)(PDEST))[1] = ((uint8_t*)(PSRC))[0];

#define tRPut32(PDEST, PSRC)                      \
  ((uint8_t*)(PDEST))[0] = ((uint8_t*)(PSRC))[3]; \
  ((uint8_t*)(PDEST))[1] = ((uint8_t*)(PSRC))[2]; \
  ((uint8_t*)(PDEST))[2] = ((uint8_t*)(PSRC))[1]; \
  ((uint8_t*)(PDEST))[3] = ((uint8_t*)(PSRC))[0];

#define tRPut64(PDEST, PSRC)                      \
  ((uint8_t*)(PDEST))[0] = ((uint8_t*)(PSRC))[7]; \
  ((uint8_t*)(PDEST))[1] = ((uint8_t*)(PSRC))[6]; \
  ((uint8_t*)(PDEST))[2] = ((uint8_t*)(PSRC))[5]; \
  ((uint8_t*)(PDEST))[3] = ((uint8_t*)(PSRC))[4]; \
  ((uint8_t*)(PDEST))[4] = ((uint8_t*)(PSRC))[3]; \
  ((uint8_t*)(PDEST))[5] = ((uint8_t*)(PSRC))[2]; \
  ((uint8_t*)(PDEST))[6] = ((uint8_t*)(PSRC))[1]; \
  ((uint8_t*)(PDEST))[7] = ((uint8_t*)(PSRC))[0];

#define tRGet16 tRPut16
#define tRGet32 tRPut32
#define tRGet64 tRPut64

#define TD_CODER_CURRENT(CODER) ((CODER)->data + (CODER)->pos)
#define TD_CODER_MOVE_POS(CODER, MOVE) ((CODER)->pos += (MOVE))
#define TD_CHECK_CODER_CAPACITY_FAILED(CODER, EXPSIZE) (((CODER)->size - (CODER)->pos) < (EXPSIZE))

/* ------------------------ FOR ENCODER ------------------------ */
static FORCE_INLINE void tInitEncoder(SEncoder* pEncoder, td_endian_t endian, uint8_t* data, int64_t size) {
  pEncoder->endian = endian;
  pEncoder->data = data;
  pEncoder->size = (data) ? size : 0;
  pEncoder->pos = 0;
}

// 8
static FORCE_INLINE int tEncodeU8(SEncoder* pEncoder, uint8_t val) {
  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(val))) return -1;
    tPut(uint8_t, TD_CODER_CURRENT(pEncoder), val);
  }
  TD_CODER_MOVE_POS(pEncoder, sizeof(val));
  return 0;
}

static FORCE_INLINE int tEncodeI8(SEncoder* pEncoder, int8_t val) {
  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(val))) return -1;
    tPut(int8_t, TD_CODER_CURRENT(pEncoder), val);
  }
  TD_CODER_MOVE_POS(pEncoder, sizeof(val));
  return 0;
}

// 16
static FORCE_INLINE int tEncodeU16(SEncoder* pEncoder, uint16_t val) {
  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(val))) return -1;
    if (TD_RT_ENDIAN() == pEncoder->endian) {
      tPut(uint16_t, TD_CODER_CURRENT(pEncoder), val);
    } else {
      tRPut16(TD_CODER_CURRENT(pEncoder), &val);
    }
  }
  TD_CODER_MOVE_POS(pEncoder, sizeof(val));
  return 0;
}

static FORCE_INLINE int tEncodeI16(SEncoder* pEncoder, int16_t val) {
  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(val))) return -1;
    if (TD_RT_ENDIAN() == pEncoder->endian) {
      tPut(int16_t, TD_CODER_CURRENT(pEncoder), val);
    } else {
      tRPut16(TD_CODER_CURRENT(pEncoder), &val);
    }
  }
  TD_CODER_MOVE_POS(pEncoder, sizeof(val));
  return 0;
}

// 32
static FORCE_INLINE int tEncodeU32(SEncoder* pEncoder, uint32_t val) {
  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(val))) return -1;
    if (TD_RT_ENDIAN() == pEncoder->endian) {
      tPut(uint32_t, TD_CODER_CURRENT(pEncoder), val);
    } else {
      tRPut32(TD_CODER_CURRENT(pEncoder), &val);
    }
  }
  TD_CODER_MOVE_POS(pEncoder, sizeof(val));
  return 0;
}

static FORCE_INLINE int tEncodeI32(SEncoder* pEncoder, int32_t val) {
  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(val))) return -1;
    if (TD_RT_ENDIAN() == pEncoder->endian) {
      tPut(int32_t, TD_CODER_CURRENT(pEncoder), val);
    } else {
      tRPut32(TD_CODER_CURRENT(pEncoder), &val);
    }
  }
  TD_CODER_MOVE_POS(pEncoder, sizeof(val));
  return 0;
}

// 64
static FORCE_INLINE int tEncodeU64(SEncoder* pEncoder, uint64_t val) {
  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(val))) return -1;
    if (TD_RT_ENDIAN() == pEncoder->endian) {
      tPut(uint64_t, TD_CODER_CURRENT(pEncoder), val);
    } else {
      tRPut64(TD_CODER_CURRENT(pEncoder), &val);
    }
  }
  TD_CODER_MOVE_POS(pEncoder, sizeof(val));
  return 0;
}

static FORCE_INLINE int tEncodeI64(SEncoder* pEncoder, int64_t val) {
  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(val))) return -1;
    if (TD_RT_ENDIAN() == pEncoder->endian) {
      tPut(int64_t, TD_CODER_CURRENT(pEncoder), val);
    } else {
      tRPut64(TD_CODER_CURRENT(pEncoder), &val);
    }
  }
  TD_CODER_MOVE_POS(pEncoder, sizeof(val));
  return 0;
}

static FORCE_INLINE int tEncodeU16v(SEncoder* pEncoder, uint16_t val) {
  int64_t i = 0;
  while (val >= ENCODE_LIMIT) {
    if (pEncoder->data) {
      if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, 1)) return -1;
      TD_CODER_CURRENT(pEncoder)[i] = (val | ENCODE_LIMIT) & 0xff;
    }

    val >>= 7;
    i++;
  }

  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, 1)) return -1;
    TD_CODER_CURRENT(pEncoder)[i] = (uint8_t)val;
  }

  TD_CODER_MOVE_POS(pEncoder, i + 1);

  return 0;
}

static FORCE_INLINE int tEncodeI16v(SEncoder* pEncoder, int16_t val) { return tEncodeU16v(pEncoder, ZIGZAGE(val)); }

static FORCE_INLINE int tEncodeU32v(SEncoder* pEncoder, uint32_t val) {
  int64_t i = 0;
  while (val >= ENCODE_LIMIT) {
    if (pEncoder->data) {
      if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, 1)) return -1;
      TD_CODER_CURRENT(pEncoder)[i] = (val | ENCODE_LIMIT) & 0xff;
    }

    val >>= 7;
    i++;
  }

  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, 1)) return -1;
    TD_CODER_CURRENT(pEncoder)[i] = (uint8_t)val;
  }

  TD_CODER_MOVE_POS(pEncoder, i + 1);

  return 0;
}

static FORCE_INLINE int tEncodeI32v(SEncoder* pEncoder, int32_t val) { return tEncodeU32v(pEncoder, ZIGZAGE(val)); }

static FORCE_INLINE int tEncodeU64v(SEncoder* pEncoder, uint64_t val) {
  int64_t i = 0;
  while (val >= ENCODE_LIMIT) {
    if (pEncoder->data) {
      if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, 1)) return -1;
      TD_CODER_CURRENT(pEncoder)[i] = (val | ENCODE_LIMIT) & 0xff;
    }

    val >>= 7;
    i++;
  }

  if (pEncoder->data) {
    if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, 1)) return -1;
    TD_CODER_CURRENT(pEncoder)[i] = (uint8_t)val;
  }

  TD_CODER_MOVE_POS(pEncoder, i + 1);

  return 0;
}

static FORCE_INLINE int tEncodeI64v(SEncoder* pEncoder, int64_t val) { return tEncodeU64v(pEncoder, ZIGZAGE(val)); }

/* ------------------------ FOR DECODER ------------------------ */
static FORCE_INLINE void tInitDecoder(SDecoder* pDecoder, td_endian_t endian, const uint8_t* data, int64_t size) {
  ASSERT(!TD_IS_NULL(data));
  pDecoder->endian = endian;
  pDecoder->data = data;
  pDecoder->size = size;
  pDecoder->pos = 0;
}

// 8
static FORCER_INLINE int tDecodeU8(SDecoder* pDecoder, uint8_t* val) {
  if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(*val))) return -1;
  tGet(uint8_t, TD_CODER_CURRENT(pDecoder), *val);
  TD_CODER_MOVE_POS(pDecoder, sizeof(*val));
  return 0;
}

static FORCER_INLINE int tDecodeI8(SDecoder* pDecoder, int8_t* val) {
  if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(*val))) return -1;
  tGet(int8_t, TD_CODER_CURRENT(pDecoder), *val);
  TD_CODER_MOVE_POS(pDecoder, sizeof(*val));
  return 0;
}

// 16
static FORCER_INLINE int tDecodeU16(SDecoder* pDecoder, uint16_t* val) {
  if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(*val))) return -1;
  if (TD_RT_ENDIAN() == pDecoder->endian) {
    tGet(uint16_t, TD_CODER_CURRENT(pDecoder), *val);
  } else {
    tRGet16(val, TD_CODER_CURRENT(pDecoder));
  }

  TD_CODER_MOVE_POS(pDecoder, sizeof(*val));
  return 0;
}

static FORCER_INLINE int tDecodeI16(SDecoder* pDecoder, int16_t* val) {
  if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(*val))) return -1;
  if (TD_RT_ENDIAN() == pDecoder->endian) {
    tGet(int16_t, TD_CODER_CURRENT(pDecoder), *val);
  } else {
    tRGet16(val, TD_CODER_CURRENT(pDecoder));
  }

  TD_CODER_MOVE_POS(pDecoder, sizeof(*val));
  return 0;
}

// 32
static FORCER_INLINE int tDecodeU32(SDecoder* pDecoder, uint32_t* val) {
  if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(*val))) return -1;
  if (TD_RT_ENDIAN() == pDecoder->endian) {
    tGet(uint32_t, TD_CODER_CURRENT(pDecoder), *val);
  } else {
    tRGet32(val, TD_CODER_CURRENT(pDecoder));
  }

  TD_CODER_MOVE_POS(pDecoder, sizeof(*val));
  return 0;
}

static FORCER_INLINE int tDecodeI32(SDecoder* pDecoder, int32_t* val) {
  if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(*val))) return -1;
  if (TD_RT_ENDIAN() == pDecoder->endian) {
    tGet(int32_t, TD_CODER_CURRENT(pDecoder), *val);
  } else {
    tRGet32(val, TD_CODER_CURRENT(pDecoder));
  }

  TD_CODER_MOVE_POS(pDecoder, sizeof(*val));
  return 0;
}

// 64
static FORCER_INLINE int tDecodeU64(SDecoder* pDecoder, uint64_t* val) {
  if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(*val))) return -1;
  if (TD_RT_ENDIAN() == pDecoder->endian) {
    tGet(uint64_t, TD_CODER_CURRENT(pDecoder), *val);
  } else {
    tRGet64(val, TD_CODER_CURRENT(pDecoder));
  }

  TD_CODER_MOVE_POS(pDecoder, sizeof(*val));
  return 0;
}

static FORCER_INLINE int tDecodeI64(SDecoder* pDecoder, int64_t* val) {
  if (TD_CHECK_CODER_CAPACITY_FAILED(pEncoder, sizeof(*val))) return -1;
  if (TD_RT_ENDIAN() == pDecoder->endian) {
    tGet(int64_t, TD_CODER_CURRENT(pDecoder), *val);
  } else {
    tRGet64(val, TD_CODER_CURRENT(pDecoder));
  }

  TD_CODER_MOVE_POS(pDecoder, sizeof(*val));
  return 0;
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_ENCODE_H_*/
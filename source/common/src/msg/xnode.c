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
#include "taoserror.h"
#include "tglobal.h"
#include "tmsg.h"

// #define DECODESQL()                                                               \
//   do {                                                                            \
//     if (!tDecodeIsEnd(&decoder)) {                                                \
//       TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->sqlLen));                       \
//       if (pReq->sqlLen > 0) {                                                     \
//         TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->sql, NULL)); \
//       }                                                                           \
//     }                                                                             \
//   } while (0)

// #define ENCODESQL()                                                                       \
//   do {                                                                                    \
//     TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->sqlLen));                                  \
//     if (pReq->sqlLen > 0) {                                                               \
//       TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->sql, pReq->sqlLen)); \
//     }                                                                                     \
//   } while (0)

// #define FREESQL()                \
//   do {                           \
//     if (pReq->sql != NULL) {     \
//       taosMemoryFree(pReq->sql); \
//     }                            \
//     pReq->sql = NULL;            \
//   } while (0)

int32_t tSerializeSMCreateXnodeReq(void *buf, int32_t bufLen, SMCreateXnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  ENCODESQL();

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->urlLen));
  if (pReq->urlLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->url, pReq->urlLen));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->userLen));
  if (pReq->userLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->user, pReq->userLen));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->passLen));
  if (pReq->passLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->pass, pReq->passLen));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->passIsMd5));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateXnodeReq(void *buf, int32_t bufLen, SMCreateXnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  DECODESQL();

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->urlLen));
  if (pReq->urlLen > 0) {
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->url, NULL));
  }
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->userLen));
  if (pReq->userLen > 0) {
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->user, NULL));
  }
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->passLen));
  if (pReq->passLen > 0) {
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->pass, NULL));
  }
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->passIsMd5));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMCreateXnodeReq(SMCreateXnodeReq *pReq) {
  taosMemoryFreeClear(pReq->url);
  taosMemoryFreeClear(pReq->user);
  taosMemoryFreeClear(pReq->pass);
  FREESQL();
}

int32_t tSerializeSMCreateXnodeTaskReq(void *buf, int32_t bufLen, SMCreateXnodeTaskReq *pReq) {
  printf("serializeCreateXnodeTask: %s\n", pReq->name);
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  ENCODESQL();

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->nameLen));
  if (pReq->nameLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->name, pReq->nameLen));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSMCreateXnodeTaskReq(void *buf, int32_t bufLen, SMCreateXnodeTaskReq *pReq) {
  printf("deserializeCreateXnodeTask: %s\n", pReq->name);
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  DECODESQL();

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->nameLen));
  if (pReq->nameLen > 0) {
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->name, NULL));
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}
void tFreeSMCreateXnodeTaskReq(SMCreateXnodeTaskReq *pReq) {
  printf("freeCreateXnodeTask: %s\n", pReq->name);
  taosMemoryFreeClear(pReq->name);
  FREESQL();
}
int32_t tSerializeSMDropXnodeTaskReq(void *buf, int32_t bufLen, SMDropXnodeTaskReq *pReq) {
  printf("serializeDropXnodeTask: %d\n", pReq->tid);
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  // 0. sql
  ENCODESQL();

  // 1. tid.
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->tid));
  // 2. config
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->nameLen));
  if (pReq->nameLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->name, pReq->nameLen));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSMDropXnodeTaskReq(void *buf, int32_t bufLen, SMDropXnodeTaskReq *pReq) {
  printf("deserializeDropXnodeTask: %d\n", pReq->tid);
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  DECODESQL();
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->tid));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->nameLen));
  if (pReq->nameLen > 0) {
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->name, NULL));
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}
void tFreeSMDropXnodeTaskReq(SMDropXnodeTaskReq *pReq) { printf("freeDropXnodeTask: %d\n", pReq->tid); }

int32_t tSerializeSMUpdateXnodeTaskReq(void *buf, int32_t bufLen, SMUpdateXnodeTaskReq *pReq) {
  printf("SerializeUpdateXnodeTask: %d\n", pReq->tid);
  return TSDB_CODE_MND_XNODE_INVALID_VERSION;
}
int32_t tDeserializeSMUpdateXnodeTaskReq(void *buf, int32_t bufLen, SMUpdateXnodeTaskReq *pReq) {
  printf("DeserializeUpdateXnodeTask: %d\n", pReq->tid);
  return TSDB_CODE_MND_XNODE_INVALID_VERSION;
}

void tFreeSMUpdateXnodeTaskReq(SMUpdateXnodeTaskReq *pReq) {
  printf("freeUpdateXnodeTask: %d\n", pReq->tid);
  FREESQL();
}

int32_t tSerializeSMDropXnodeReq(void *buf, int32_t bufLen, SMDropXnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->xnodeId));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropXnodeReq(void *buf, int32_t bufLen, SMDropXnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->xnodeId));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMDropXnodeReq(SMDropXnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSMUpdateXnodeReq(void *buf, int32_t bufLen, SMUpdateXnodeReq *pReq) {
  return tSerializeSMDropXnodeReq(buf, bufLen, pReq);
}

int32_t tDeserializeSMUpdateXnodeReq(void *buf, int32_t bufLen, SMUpdateXnodeReq *pReq) {
  return tDeserializeSMDropXnodeReq(buf, bufLen, pReq);
}

void tFreeSMUpdateXnodeReq(SMUpdateXnodeReq *pReq) { tFreeSMDropXnodeReq(pReq); }

int32_t tSerializeSMCreateXnodeJobReq(void *buf, int32_t bufLen, SMCreateXnodeJobReq *pReq) {
  printf("serializeCreateXnodeTaskJob: %d\n", pReq->tid);
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  // 0. sql
  ENCODESQL();

  // 1. tid.
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->tid));
  // 2. config
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->configLen));
  if (pReq->configLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->config, pReq->configLen));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateXnodeJobReq(void *buf, int32_t bufLen, SMCreateXnodeJobReq *pReq) {
  printf("deserializeCreateXnodeTaskJob: %d\n", pReq->tid);
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  DECODESQL();
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->tid));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->configLen));
  if (pReq->configLen > 0) {
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->config, NULL));
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}
void tFreeSMCreateXnodeJobReq(SMCreateXnodeJobReq *pReq) {
  printf("freeCreateXnodeTaskJob: %d\n", pReq->tid);
  FREESQL();
  taosMemoryFreeClear(pReq->config);
}

int32_t tSerializeSMDropXnodeJobReq(void *buf, int32_t bufLen, SMDropXnodeJobReq *pReq) {
  printf("serializeDropXnodeTaskJob with jid:%d, tid:%d\n", pReq->jid, pReq->tid);
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  // 0. sql
  ENCODESQL();
  // 1. jid
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->jid));
  // 2. tid.
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->tid));

  tEndEncode(&encoder);
_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSMDropXnodeJobReq(void *buf, int32_t bufLen, SMDropXnodeJobReq *pReq) {
  printf("deserializeDropXnodeTaskJob: %d\n", pReq->tid);
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  DECODESQL();
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->jid));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->tid));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}
void tFreeSMDropXnodeJobReq(SMDropXnodeJobReq *pReq) {
  printf("tFreeSMDropXnodeJobReq: %d\n", pReq->tid);
  FREESQL();
}

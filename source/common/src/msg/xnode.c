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
#include "osMemPool.h"
#include "osMemory.h"
#include "taoserror.h"
#include "tencode.h"
#include "tmsg.h"
#include "tutil.h"

CowStr xCreateCowStr(int32_t len, const char *ptr, bool shouldClone) {
  CowStr cow;
  if (shouldClone) {
    cow.len = len;
    cow.ptr = taosStrndupi(ptr, (int64_t)len);
    cow.shouldFree = true;
  } else {
    cow.len = len;
    cow.ptr = ptr;
    cow.shouldFree = false;
  }
  return cow;
}
void xSetCowStr(CowStr *cow, int32_t len, const char *ptr, bool shouldFree) {
  if (cow == NULL) {
    printf("Set CowStr with NULL pointer\n");
    return;
  }
  cow->len = len;
  cow->ptr = taosStrndupi(ptr, (int64_t)len);
  cow->shouldFree = shouldFree;
}
CowStr xCloneRefCowStr(CowStr *cow) {
  CowStr ref = {0};
  if (cow == NULL) {
    return ref;
  }
  ref.len = cow->len;
  ref.ptr = cow->ptr;
  ref.shouldFree = false;
  return ref;
}
void xFreeCowStr(CowStr *cow) {
  // printf("Free CowStr: cow=%p, ptr=%p, len=%d, shouldFree=%d\n", cow, cow->ptr, cow->len, cow->shouldFree);
  if (cow == NULL) {
    return;
  }
  if (cow->shouldFree && cow->ptr != NULL && cow->len > 0) {
    taosMemoryFreeClear(cow->ptr);
    cow->ptr = NULL;
  }
  cow->len = 0;
  cow->ptr = NULL;
  cow->shouldFree = false;
}
int32_t xEncodeCowStr(SEncoder *encoder, CowStr *cow) { return tEncodeCStrWithLen(encoder, cow->ptr, cow->len); }
int32_t xDecodeCowStr(SDecoder *decoder, CowStr *cow, bool shouldClone) {
  if (decoder == NULL || cow == NULL) {
    return TSDB_CODE_MND_XNODE_INVALID_MSG;
  }
  int32_t code = 0;
  int32_t lino;

  if (shouldClone) {
    uint64_t len = 0;
    TAOS_CHECK_EXIT(tDecodeCStrAndLenAlloc(decoder, (char **)&cow->ptr, (uint64_t *)&len));
    cow->len = (int32_t)len;
    cow->shouldFree = true;
  } else {
    TAOS_CHECK_EXIT(tDecodeCStrAndLen(decoder, (char **)&cow->ptr, (uint32_t *)&cow->len));
    cow->shouldFree = false;
  }
_exit:
  return code;
}
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
void xFreeTaskSource(xTaskSource *source) {
  if (source == NULL) {
    return;
  }
  xFreeCowStr(&source->cstr);
}
xTaskSource xCreateClonedTaskSource(ENodeXTaskSourceType sourceType, int32_t len, char *source) {
  printf("clone task source from %p\n", source);
  xTaskSource taskSource = {0};
  taskSource.type = sourceType;
  taskSource.cstr = xCreateCowStr(len, source, true);
  return taskSource;
}
xTaskSource xCreateTaskSource(ENodeXTaskSourceType sourceType, int32_t len, char *ptr) {
   xTaskSource taskSource = {0};
  taskSource.type = sourceType;
  taskSource.cstr = xCreateCowStr(len, ptr, false);
   return taskSource;
}
xTaskSource xCloneTaskSourceRef(xTaskSource *source) {
  xTaskSource taskSource = {0};
  if (source == NULL) {
    return taskSource;
  }
  taskSource.type = source->type;
  taskSource.cstr = xCloneRefCowStr(&source->cstr);
  return taskSource;
}

const char *xGetTaskSourceTypeAsStr(xTaskSource *source) {
  switch (source->type) {
    case XNODE_TASK_SOURCE_DATABASE:
      return "database";
    case XNODE_TASK_SOURCE_TOPIC:
      return "topic";
    case XNODE_TASK_SOURCE_DSN:
      return "dsn";
    default:
      return "unknown";
  }
}
const char *xGetTaskSourceStr(xTaskSource *source) { return source->cstr.ptr; }
int32_t     xSerializeTaskSource(SEncoder *encoder, xTaskSource *source) {
  int32_t code = 0;
  int32_t lino;
  TAOS_CHECK_EXIT(tEncodeI32(encoder, source->type));
  TAOS_CHECK_EXIT(xEncodeCowStr(encoder, &source->cstr));
_exit:
  return code;
}
int32_t xDeserializeTaskSource(SDecoder *decoder, xTaskSource *source) {
  if (decoder == NULL || source == NULL) {
    return TSDB_CODE_MND_XNODE_INVALID_MSG;
  }
  int32_t code = 0;
  int32_t lino;
  int32_t type;
  TAOS_CHECK_EXIT(tDecodeI32(decoder, &type));
  source->type = type;
  TAOS_CHECK_EXIT(xDecodeCowStr(decoder, &source->cstr, true));
_exit:
  return code;
}

void xFreeTaskSink(xTaskSink *sink) {
  if (sink == NULL) {
    return;
  }
  xFreeCowStr(&sink->cstr);
}
xTaskSink xCreateClonedTaskSink(ENodeXTaskSinkType sinkType, int32_t len, char *sink) {
  xTaskSink taskSink = {0};
  taskSink.type = sinkType;
  taskSink.cstr = xCreateCowStr(len, sink, false);
  return taskSink;
}
xTaskSink xCreateTaskSink(ENodeXTaskSinkType sinkType, int32_t len, char *ptr) {
  xTaskSink taskSink = {0};
  taskSink.type = sinkType;
  taskSink.cstr = xCreateCowStr(len, ptr, false);
  return taskSink;
}
xTaskSink   xCloneTaskSinkRef(xTaskSink* sink) {
  xTaskSink taskSink = {0};
  if (sink == NULL) {
    return taskSink;
  }
  taskSink.type = sink->type;
  taskSink.cstr = xCloneRefCowStr(&sink->cstr);
  return taskSink;
}
const char *xGetTaskSinkTypeAsStr(xTaskSink *sink) {
  switch (sink->type) {
    case XNODE_TASK_SINK_DATABASE:
      return "database";
    case XNODE_TASK_SINK_DSN:
      return "dsn";
    default:
      return "unknown";
  }
}
const char *xGetTaskSinkStr(xTaskSink *sink) { return sink->cstr.ptr; }

int32_t xSerializeTaskSink(SEncoder *encoder, xTaskSink *sink) {
  int32_t code = 0;
  int32_t lino;
  TAOS_CHECK_EXIT(tEncodeI32(encoder, sink->type));
  TAOS_CHECK_EXIT(xEncodeCowStr(encoder, &sink->cstr));
_exit:
  return code;
}
int32_t xDeserializeTaskSink(SDecoder *decoder, xTaskSink *sink) {
  if (decoder == NULL || sink == NULL) {
    return TSDB_CODE_MND_XNODE_INVALID_MSG;
  }
  int32_t code = 0;
  int32_t lino;
  int32_t type;
  TAOS_CHECK_EXIT(tDecodeI32(decoder, &type));
  switch (type) {
    case XNODE_TASK_SINK_DSN:
    case XNODE_TASK_SINK_DATABASE:
      sink->type = type;
      break;
    default:
      TAOS_RETURN(TSDB_CODE_MND_XNODE_INVALID_MSG);
  }
  TAOS_CHECK_EXIT(xDecodeCowStr(decoder, &sink->cstr, true));
_exit:
  return code;
}

void xFreeTaskOptions(xTaskOptions *options) {
  if (options == NULL) {
    return;
  }
  options->via = -1;
  xFreeCowStr(&options->trigger);
  xFreeCowStr(&options->parser);
  xFreeCowStr(&options->health);
  for (int i = 0; i < TSDB_XNODE_TASK_OPTIONS_MAX_NUM; i++) {
    xFreeCowStr(&options->options[i]);
  }
}
void printXnodeTaskOptions(xTaskOptions *options) {
  printf("Xnode Task Options:\n");
  printf("  trigger: %s\n", options->trigger.ptr);
  printf("  parser: %s\n", options->parser.ptr);
  printf("  health: %s\n", options->health.ptr);
  if (options->via > 0) {
    printf("  via: %d\n", options->via);
  } else {
    printf("  via: nil\n");
  }
  for (int i = 0; i < options->optionsNum; ++i) {
    printf("  option[%d]: %s\n", i, options->options[i].ptr);
  }
}
int32_t xSerializeTaskOptions(SEncoder *encoder, xTaskOptions *options) {
  int32_t code = 0;
  int32_t lino;
  TAOS_CHECK_EXIT(tEncodeI32(encoder, options->via));
  TAOS_CHECK_EXIT(xEncodeCowStr(encoder, &options->parser));
  TAOS_CHECK_EXIT(xEncodeCowStr(encoder, &options->trigger));
  TAOS_CHECK_EXIT(xEncodeCowStr(encoder, &options->health));
  TAOS_CHECK_EXIT(tEncodeI32(encoder, options->optionsNum));
  for (int i = 0; i < options->optionsNum && i < TSDB_XNODE_TASK_OPTIONS_MAX_NUM; i++) {
    TAOS_CHECK_EXIT(xEncodeCowStr(encoder, &options->options[i]));
  }
_exit:
  return code;
}
int32_t xDeserializeTaskOptions(SDecoder *decoder, xTaskOptions *options) {
  if (decoder == NULL || options == NULL) {
    return TSDB_CODE_MND_XNODE_INVALID_MSG;
  }
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(decoder, &options->via));
  TAOS_CHECK_EXIT(xDecodeCowStr(decoder, &options->parser, true));
  TAOS_CHECK_EXIT(xDecodeCowStr(decoder, &options->trigger, true));
  TAOS_CHECK_EXIT(xDecodeCowStr(decoder, &options->health, true));
  TAOS_CHECK_EXIT(tDecodeI32(decoder, &options->optionsNum));
  for (int i = 0; i < options->optionsNum && i < TSDB_XNODE_TASK_OPTIONS_MAX_NUM; i++) {
    TAOS_CHECK_EXIT(xDecodeCowStr(decoder, &options->options[i], true));
  }
_exit:
  return code;
}
int32_t tSerializeXnodeTaskSource(SEncoder *encoder, xTaskSource *source) {
  int32_t code = 0;
  int32_t lino;
  int32_t tlen;
  TAOS_CHECK_EXIT(tEncodeI32(encoder, source->type));
  TAOS_CHECK_EXIT(xEncodeCowStr(encoder, &source->cstr));
_exit:
  return code;
}

int32_t tDeserializeXnodeTaskSource(SDecoder *decoder, xTaskSource *source) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(decoder, (int32_t *)&source->type));
  TAOS_CHECK_EXIT(xDecodeCowStr(decoder, &source->cstr, true));
_exit:
  return code;
}
int32_t tSerializeSMCreateXnodeTaskReq(void *buf, int32_t bufLen, SMCreateXnodeTaskReq *pReq) {
  printf("serializeCreateXnodeTask: %s\n", pReq->name.ptr);
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  ENCODESQL();

  TAOS_CHECK_EXIT(xEncodeCowStr(&encoder, &pReq->name));
  TAOS_CHECK_EXIT(xSerializeTaskSource(&encoder, &pReq->source));
  TAOS_CHECK_EXIT(xSerializeTaskSink(&encoder, &pReq->sink));
  TAOS_CHECK_EXIT(xSerializeTaskOptions(&encoder, &pReq->options));
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
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  DECODESQL();

  TAOS_CHECK_EXIT(xDecodeCowStr(&decoder, &pReq->name, true));
  printf("deserializeCreateXnodeTask: %s\n", pReq->name.ptr);
  printf("Deserialized name: %s\n", pReq->name.ptr);
  TAOS_CHECK_EXIT(xDeserializeTaskSource(&decoder, &pReq->source));
  printf("Deserialized source: %s:%s\n", xGetTaskSourceTypeAsStr(&pReq->source), xGetTaskSourceStr(&pReq->source));
  TAOS_CHECK_EXIT(xDeserializeTaskSink(&decoder, &pReq->sink));
  printf("Deserialized sink: %s:%s\n", xGetTaskSinkTypeAsStr(&pReq->sink), xGetTaskSinkStr(&pReq->sink));
  TAOS_CHECK_EXIT(xDeserializeTaskOptions(&decoder, &pReq->options));
  printXnodeTaskOptions(&pReq->options);
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}
void tFreeSMCreateXnodeTaskReq(SMCreateXnodeTaskReq *pReq) {
  printf("freeCreateXnodeTask: %s\n", pReq->name.ptr);
  FREESQL();
  xFreeCowStr(&pReq->name);
  xFreeTaskSource(&pReq->source);
  xFreeTaskSink(&pReq->sink);
  xFreeTaskOptions(&pReq->options);
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

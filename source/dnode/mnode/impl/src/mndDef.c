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

#include "mndDef.h"

int32_t tEncodeSStreamObj(SCoder *pEncoder, const SStreamObj *pObj) {
  int32_t sz = 0;
  int32_t outputNameSz = 0;
  if (tEncodeCStr(pEncoder, pObj->name) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->db) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->createTime) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->updateTime) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->uid) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->dbUid) < 0) return -1;
  if (tEncodeI32(pEncoder, pObj->version) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->status) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->sql) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->logicalPlan) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->physicalPlan) < 0) return -1;
  // TODO encode tasks
  if (pObj->tasks) {
    sz = taosArrayGetSize(pObj->tasks);
  }
  if (tEncodeI32(pEncoder, sz) < 0) return -1;

  for (int32_t i = 0; i < sz; i++) {
    SArray *pArray = taosArrayGet(pObj->tasks, i);
    int32_t innerSz = taosArrayGetSize(pArray);
    if (tEncodeI32(pEncoder, innerSz) < 0) return -1;
    for (int32_t j = 0; j < innerSz; j++) {
      SStreamTask *pTask = taosArrayGet(pArray, j);
      if (tEncodeSStreamTask(pEncoder, pTask) < 0) return -1;
    }
  }

  if (pObj->ColAlias != NULL) {
    outputNameSz = taosArrayGetSize(pObj->ColAlias);
  }
  if (tEncodeI32(pEncoder, outputNameSz) < 0) return -1;
  for (int32_t i = 0; i < outputNameSz; i++) {
    char *name = taosArrayGetP(pObj->ColAlias, i);
    if (tEncodeCStr(pEncoder, name) < 0) return -1;
  }
  return pEncoder->pos;
}

int32_t tDecodeSStreamObj(SCoder *pDecoder, SStreamObj *pObj) {
  if (tDecodeCStrTo(pDecoder, pObj->name) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->db) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->createTime) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->updateTime) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->uid) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->dbUid) < 0) return -1;
  if (tDecodeI32(pDecoder, &pObj->version) < 0) return -1;
  if (tDecodeI8(pDecoder, &pObj->status) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pObj->sql) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pObj->logicalPlan) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pObj->physicalPlan) < 0) return -1;
  pObj->tasks = NULL;
  int32_t sz;
  if (tDecodeI32(pDecoder, &sz) < 0) return -1;
  if (sz != 0) {
    pObj->tasks = taosArrayInit(sz, sizeof(SArray));
    for (int32_t i = 0; i < sz; i++) {
      int32_t innerSz;
      if (tDecodeI32(pDecoder, &innerSz) < 0) return -1;
      SArray *pArray = taosArrayInit(innerSz, sizeof(SStreamTask));
      for (int32_t j = 0; j < innerSz; j++) {
        SStreamTask task;
        if (tDecodeSStreamTask(pDecoder, &task) < 0) return -1;
        taosArrayPush(pArray, &task);
      }
      taosArrayPush(pObj->tasks, pArray);
    }
  }
  int32_t outputNameSz;
  if (tDecodeI32(pDecoder, &outputNameSz) < 0) return -1;
  if (outputNameSz != 0) {
    pObj->ColAlias = taosArrayInit(outputNameSz, sizeof(void *));
    if (pObj->ColAlias == NULL) {
      return -1;
    }
  }
  for (int32_t i = 0; i < outputNameSz; i++) {
    char *name;
    if (tDecodeCStrAlloc(pDecoder, &name) < 0) return -1;
    taosArrayPush(pObj->ColAlias, &name);
  }
  return 0;
}

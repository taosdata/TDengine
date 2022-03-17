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
  return 0;
}

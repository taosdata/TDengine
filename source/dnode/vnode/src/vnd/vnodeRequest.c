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

#include "vnodeDef.h"

#if 0

static int   vnodeBuildCreateTableReq(void **buf, const SVCreateTableReq *pReq);
static void *vnodeParseCreateTableReq(void *buf, SVCreateTableReq *pReq);

int vnodeBuildReq(void **buf, const SVnodeReq *pReq, tmsg_t type) {
  int tsize = 0;

  tsize += taosEncodeFixedU64(buf, pReq->ver);
  switch (type) {
    case TDMT_VND_CREATE_STB:
      tsize += vnodeBuildCreateTableReq(buf, &(pReq->ctReq));
      break;
    case TDMT_VND_SUBMIT:
      /* code */
      break;
    default:
      break;
  }
  /* TODO */
  return tsize;
}

void *vnodeParseReq(void *buf, SVnodeReq *pReq, tmsg_t type) {
  buf = taosDecodeFixedU64(buf, &(pReq->ver));

  switch (type) {
    case TDMT_VND_CREATE_STB:
      buf = vnodeParseCreateTableReq(buf, &(pReq->ctReq));
      break;

    default:
      break;
  }

  // TODO
  return buf;
}

static int vnodeBuildCreateTableReq(void **buf, const SVCreateTableReq *pReq) {
  int tsize = 0;

  tsize += taosEncodeString(buf, pReq->name);
  tsize += taosEncodeFixedU32(buf, pReq->ttl);
  tsize += taosEncodeFixedU32(buf, pReq->keep);
  tsize += taosEncodeFixedU8(buf, pReq->type);

  switch (pReq->type) {
    case META_SUPER_TABLE:
      tsize += taosEncodeFixedU64(buf, pReq->stbCfg.suid);
      tsize += tdEncodeSchema(buf, pReq->stbCfg.pSchema);
      tsize += tdEncodeSchema(buf, pReq->stbCfg.pTagSchema);
      break;
    case META_CHILD_TABLE:
      tsize += taosEncodeFixedU64(buf, pReq->ctbCfg.suid);
      tsize += tdEncodeKVRow(buf, pReq->ctbCfg.pTag);
      break;
    case META_NORMAL_TABLE:
      tsize += tdEncodeSchema(buf, pReq->ntbCfg.pSchema);
      break;
    default:
      break;
  }

  return tsize;
}

static void *vnodeParseCreateTableReq(void *buf, SVCreateTableReq *pReq) {
  buf = taosDecodeString(buf, &(pReq->name));
  buf = taosDecodeFixedU32(buf, &(pReq->ttl));
  buf = taosDecodeFixedU32(buf, &(pReq->keep));
  buf = taosDecodeFixedU8(buf, &(pReq->type));

  switch (pReq->type) {
    case META_SUPER_TABLE:
      buf = taosDecodeFixedU64(buf, &(pReq->stbCfg.suid));
      buf = tdDecodeSchema(buf, &(pReq->stbCfg.pSchema));
      buf = tdDecodeSchema(buf, &(pReq->stbCfg.pTagSchema));
      break;
    case META_CHILD_TABLE:
      buf = taosDecodeFixedU64(buf, &(pReq->ctbCfg.suid));
      buf = tdDecodeKVRow(buf, &(pReq->ctbCfg.pTag));
      break;
    case META_NORMAL_TABLE:
      buf = tdDecodeSchema(buf, &(pReq->ntbCfg.pSchema));
      break;
    default:
      break;
  }

  return buf;
}

int vnodeBuildDropTableReq(void **buf, const SVDropTbReq *pReq) {
  // TODO
  return 0;
}

void *vnodeParseDropTableReq(void *buf, SVDropTbReq *pReq) {
  // TODO
}
#endif
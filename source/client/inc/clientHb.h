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

#include "os.h"
#include "tarray.h"
#include "thash.h"
#include "tmsg.h"

typedef enum {
  mq = 0,
  // type can be added here
  //
  HEARTBEAT_TYPE_MAX
} EHbType;

typedef struct SKlv {
  int32_t keyLen;
  int32_t valueLen;
  void*   key;
  void*   value;
} SKlv;

typedef struct SClientHbKey {
  int32_t connId;
  int32_t hbType;
} SClientHbKey;

typedef struct SClientHbReq {
  SClientHbKey connKey;
  SHashObj*    info;  // hash<Slv.key, Sklv>
} SClientHbReq;

typedef struct SClientHbBatchReq {
  int64_t reqId;
  SArray* reqs;  // SArray<SClientHbReq>
} SClientHbBatchReq;

typedef struct SClientHbHandleResult {
} SClientHbHandleResult;

typedef struct SClientHbRsp {
  SClientHbKey connKey;
  int32_t status;
  int32_t bodyLen;
  void*   body;
} SClientHbRsp;

typedef struct SClientHbBatchRsp {
  int64_t reqId;
  int64_t rspId;
  SArray* rsps;  // SArray<SClientHbRsp>
} SClientHbBatchRsp;

typedef int32_t (*FHbRspHandle)(SClientHbRsp* pReq);
typedef int32_t (*FGetConnInfo)(SClientHbKey connKey, void* param);

typedef struct SClientHbMgr {
  int8_t       inited;
  int32_t      reportInterval;  // unit ms
  int32_t      stats;
  SRWLatch     lock;
  SHashObj*    activeInfo;    // hash<SClientHbKey, SClientHbReq>
  SHashObj*    getInfoFuncs;  // hash<SClientHbKey, FGetConnInfo>
  FHbRspHandle handle[HEARTBEAT_TYPE_MAX];
  // input queue
} SClientHbMgr;

static SClientHbMgr clientHbMgr = {0};

int  hbMgrInit();
void hbMgrCleanUp();

int hbHandleRsp(void* hbMsg);

int hbRegisterConn(SClientHbKey connKey, FGetConnInfo func);

int hbAddConnInfo(SClientHbKey connKey, void* key, void* value, int32_t keyLen, int32_t valueLen);

static FORCE_INLINE int taosEncodeSKlv(void** buf, const SKlv* pKlv) {
  int tlen = 0;
  tlen += taosEncodeFixedI32(buf, pKlv->keyLen);
  tlen += taosEncodeFixedI32(buf, pKlv->valueLen);
  tlen += taosEncodeBinary(buf, pKlv->key, pKlv->keyLen);
  tlen += taosEncodeBinary(buf, pKlv->value, pKlv->valueLen);
  return tlen;
}

static FORCE_INLINE void* taosDecodeSKlv(void* buf, SKlv* pKlv) {
  buf = taosDecodeFixedI32(buf, &pKlv->keyLen);
  buf = taosDecodeFixedI32(buf, &pKlv->valueLen);
  buf = taosDecodeBinary(buf, &pKlv->key, pKlv->keyLen);
  buf = taosDecodeBinary(buf, &pKlv->value, pKlv->valueLen);
  return buf;
}

static FORCE_INLINE int taosEncodeSClientHbKey(void** buf, const SClientHbKey* pKey) {
  int tlen = 0;
  tlen += taosEncodeFixedI32(buf, pKey->connId);
  tlen += taosEncodeFixedI32(buf, pKey->hbType);
  return tlen;
}

static FORCE_INLINE void* taosDecodeSClientHbKey(void* buf, SClientHbKey* pKey) {
  buf = taosDecodeFixedI32(buf, &pKey->connId);
  buf = taosDecodeFixedI32(buf, &pKey->hbType);
  return buf;
}

static FORCE_INLINE int tSerializeSClientHbReq(void** buf, const SClientHbReq* pReq) {
  int tlen = 0;
  tlen += taosEncodeSClientHbKey(buf, &pReq->connKey);

  void* pIter = NULL;
  void* data;
  SKlv  klv;
  data = taosHashIterate(pReq->info, pIter);
  while (data != NULL) {
    taosHashGetKey(data, &klv.key, (size_t*)&klv.keyLen);
    klv.valueLen = taosHashGetDataLen(data);
    klv.value = data;
    taosEncodeSKlv(buf, &klv);

    data = taosHashIterate(pReq->info, pIter);
  }
  return tlen;
}

static FORCE_INLINE void* tDeserializeClientHbReq(void* buf, SClientHbReq* pReq) {
  ASSERT(pReq->info != NULL);
  buf = taosDecodeSClientHbKey(buf, &pReq->connKey);

  // TODO: error handling
  if (pReq->info == NULL) {
    pReq->info = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }
  SKlv klv;
  buf = taosDecodeSKlv(buf, &klv);
  taosHashPut(pReq->info, klv.key, klv.keyLen, klv.value, klv.valueLen);

  return buf;
}

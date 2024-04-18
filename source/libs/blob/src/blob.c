#include "blob.h"
#include "blobInt.h"
#include "tdataformat.h"
#include "tmsg.h"

int32_t tBlobEntrySize(SBlobEntry *pEntry) {
  int32_t size = sizeof(SBlobEntry);
  if (pEntry->flag == EBLOB_FLAG_DATA) {
    size = MAX(size, offsetof(SBlobEntry, varData.data) + pEntry->varData.len);
  }
  return size;
}

int32_t blReWriteBlobData(SColVal *pVal, TSKEY ts, SBlobData **ppBlob) {
  ASSERT(pVal && pVal->value.type == TSDB_DATA_TYPE_BLOB);

  SBlobEntry *pEntry = (void *)pVal->value.pData;
  if (pEntry->flag != EBLOB_FLAG_DATA) {
    terrno = TSDB_CODE_INVALID_DATA_FMT;
    return terrno;
  }

  if (pEntry->varData.len == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SBlobData *pBlob = NULL;
  int32_t    code = blSeparateBlobData(pEntry, &pBlob);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  ASSERT(pBlob != NULL);
  pBlob->ts = ts;

  void *ptr = taosMemoryRealloc(pVal->value.pData, sizeof(SBlobEntry));
  if (ptr == NULL) {
    blFreeBlobDataImpl(pBlob);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  pEntry = ptr;
  pEntry->flag = EBLOB_FLAG_OID;
  pEntry->oid = BLOB_OID_INVALID;

  pVal->value.pData = ptr;
  pVal->value.nData = sizeof(SBlobEntry);
  *ppBlob = pBlob;
  return TSDB_CODE_SUCCESS;
}

int32_t blCreateBlobChunk(SBlobChunk *pChunk, uint32_t offset, uint32_t length) {
  pChunk->info.offset = offset;
  pChunk->info.length = length;
  pChunk->pData = taosMemoryMalloc(length);
  if (pChunk->pData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  return 0;
}

void blDestroyBlobChunk(void *ptr) {
  SBlobChunk *pChunk = ptr;
  if (pChunk->pData != NULL) {
    taosMemoryFree(pChunk->pData);
    pChunk->pData = NULL;
  }
}

SBlobData *blCreateBlobData() {
  SBlobData *pBlob = (SBlobData *)taosMemoryCalloc(1, sizeof(SBlobData));
  if (pBlob == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  SArray *pChunks = taosArrayInit(16, sizeof(SBlobChunk));
  if (pChunks == NULL) {
    taosMemoryFree(pBlob);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pBlob->hdr.chunkLvl = BLOB_CHUNK_LVL;
  pBlob->pChunks = pChunks;
  return pBlob;
}

void blFreeBlobDataImpl(void *ptr) {
  SBlobData *pBlob = ptr;
  if (pBlob == NULL) {
    return;
  }

  taosArrayDestroyEx(pBlob->pChunks, blDestroyBlobChunk);
  taosMemoryFree(pBlob);
}

int32_t blChopDataIntoChunks(SBlobData *pBlob, const void *pData, int32_t length) {
  uint32_t chunkSize = tBlobChunkSize(pBlob->hdr.chunkLvl);
  uint32_t aSize = tBlobChunkNum(length, chunkSize);
  uint32_t offset = 0;
  for (int32_t i = 0; i < aSize; i++) {
    SBlobChunk *pChunk = taosArrayReserve(pBlob->pChunks, 1);
    if (pChunk == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return terrno;
    }
    uint32_t len = MIN(length - offset, chunkSize);
    ASSERT(len < (1 << 16));
    int32_t code = blCreateBlobChunk(pChunk, offset, len);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pChunk->info.offset = offset;
    pChunk->info.length = len;
    memcpy(pChunk->pData, pData + offset, len);
    offset += len;
  }
  pBlob->hdr.length = length;
  return TSDB_CODE_SUCCESS;
}

int32_t blSeparateBlobData(const SBlobEntry *pEntry, SBlobData **ppBlob) {
  STATIC_ASSERT(10 <= BLOB_CHUNK_LVL && BLOB_CHUNK_LVL < 16, "BLOB_CHUNK_LVL should be within [10, 16)");
  if (pEntry->flag != EBLOB_FLAG_DATA) {
    terrno = TSDB_CODE_INVALID_DATA_FMT;
    return terrno;
  }

  SBlobData *pBlob = blCreateBlobData();
  if (pBlob == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  int32_t code = TSDB_CODE_SUCCESS;

  if ((code = blChopDataIntoChunks(pBlob, pEntry->varData.data, pEntry->varData.len)) != TSDB_CODE_SUCCESS) {
    blFreeBlobData(pBlob);
    return code;
  }
  *ppBlob = pBlob;
  return TSDB_CODE_SUCCESS;
}

int32_t tPutBlobHdr(uint8_t *p, const SBlobDataHdr *pHdr) {
  STATIC_ASSERT(sizeof(TdOidT) == sizeof(SBlobOidOffset), "TdOidT should be the same size as SBlobOidOffset");
  int32_t n = 0;
  n += tPutI64(p ? p + n : p, pHdr->oid.v);
  n += tPutI64(p ? p + n : p, pHdr->version);
  n += tPutU32(p ? p + n : p, pHdr->length);
  n += tPutU8(p ? p + n : p, pHdr->cmprAlg);
  n += tPutU8(p ? p + n : p, pHdr->chunkLvl);
  n += tPutU16(p ? p + n : p, pHdr->reserved);
  n += tPutU32(p ? p + n : p, pHdr->bodyLen);
  return n;
}

int32_t tGetBlobHdr(uint8_t *p, SBlobDataHdr *pHdr) {
  int32_t n = 0;
  n += tGetI64(p + n, &pHdr->oid.v);
  n += tGetI64(p + n, &pHdr->version);
  n += tGetU32(p + n, &pHdr->length);
  n += tGetU8(p + n, &pHdr->cmprAlg);
  n += tGetU8(p + n, &pHdr->chunkLvl);
  n += tGetU16(p + n, &pHdr->reserved);
  n += tGetU32(p + n, &pHdr->bodyLen);
  return n;
}

int32_t tPutBlobChunkOffset(uint8_t *p, const SBlobChunkOffset *pInfo) {
  int32_t n = 0;
  n += tPutU32(p ? p + n : p, pInfo->offset);
  n += tPutU16(p ? p + n : p, pInfo->length);
  return n;
}

int32_t tGetBlobChunkOffset(uint8_t *p, SBlobChunkOffset *pInfo) {
  int32_t n = 0;
  n += tGetU32(p + n, &pInfo->offset);
  n += tGetU16(p + n, &pInfo->length);
  return n;
}

int32_t tPutBlobTsKey(uint8_t *p, TSKEY ts) {
  STATIC_ASSERT(sizeof(TSKEY) == sizeof(int64_t), "TSKEY should be the same size as int64_t");
  int32_t n = 0;
  n += tPutI64(p ? p + n : p, ts);
  return n;
}

int32_t tGetBlobTsKey(uint8_t *p, TSKEY *pTs) {
  int32_t n = 0;
  n += tGetI64(p + n, pTs);
  return n;
}

int32_t tPutBlobOidKey(uint8_t *p, TdOidT oid) {
  STATIC_ASSERT(sizeof(TdOidT) == sizeof(int64_t), "TdOidT should be the same size as int64_t");
  int32_t n = 0;
  n += tPutI64(p ? p + n : p, oid);
  return n;
}

int32_t tGetBlobOidKey(uint8_t *p, TdOidT *pOid) {
  int32_t n = 0;
  n += tGetI64(p + n, pOid);
  return n;
}

int32_t tPutBlobData(uint8_t *p, const SBlobData *pBlob) {
  int32_t n = 0;
  n += tPutBlobTsKey(p ? p + n : p, pBlob->ts);
  n += tPutBlobHdr(p ? p + n : p, &pBlob->hdr);

  const int32_t     chunkSize = tBlobChunkSize(pBlob->hdr.chunkLvl);
  const int32_t     aSize = tBlobChunkNum(pBlob->hdr.length, chunkSize);
  const SBlobChunk *aChunks = TARRAY_DATA(pBlob->pChunks);

  for (int32_t j = 0; j < aSize; j++) {
    n += tPutBlobChunkOffset(p ? p + n : p, &aChunks[j].info);
  }
  for (int32_t j = 0; j < aSize; j++) {
    if (p) {
      memcpy(p + n, aChunks[j].pData, aChunks[j].info.length);
    }
    n += aChunks[j].info.length;
  }
  return n;
}

int32_t tPutSubmitBlobData(uint8_t *p, const SSubmitBlobData *pSubmitBlobData) {
  int32_t n = 0;
  int32_t size = taosArrayGetSize(pSubmitBlobData->aBlobs);
  n += tPutI32(p ? p + n : p, size);

  for (int32_t i = 0; i < size; i++) {
    const SBlobData **ppBlob = taosArrayGet(pSubmitBlobData->aBlobs, i);
    n += tPutBlobData(p ? p + n : p, *ppBlob);
  }
  return n;
}

int32_t tPutSubmitBlobDataArr(uint8_t *p, const SArray *aSubmitBlobData) {
  int32_t n = 0;
  int32_t nSubmitBlobData = taosArrayGetSize(aSubmitBlobData);
  n += tPutI32(p ? p + n : p, nSubmitBlobData);
  for (size_t i = 0; i < nSubmitBlobData; i++) {
    SSubmitBlobData *pBlobData = taosArrayGet(aSubmitBlobData, i);
    n += tPutSubmitBlobData(p ? p + n : p, pBlobData);
  }
  return n;
}

int32_t tGetBlobData(uint8_t *p, SBlobData *pBlob) {
  int32_t code = TSDB_CODE_INVALID_MSG;
  int32_t n = 0;

  n += tGetBlobTsKey(p + n, &pBlob->ts);
  n += tGetBlobHdr(p + n, &pBlob->hdr);

  const int32_t chunkSize = tBlobChunkSize(pBlob->hdr.chunkLvl);
  const int32_t aSize = tBlobChunkNum(pBlob->hdr.length, chunkSize);
  ASSERT(pBlob->pChunks != NULL);

  for (int32_t j = 0; j < aSize; j++) {
    SBlobChunk *pChunk = taosArrayReserve(pBlob->pChunks, 1);
    n += tGetBlobChunkOffset(p + n, &pChunk->info);
  }

  for (int32_t j = 0; j < aSize; j++) {
    SBlobChunk *pChunk = taosArrayGet(pBlob->pChunks, j);
    pChunk->pData = taosMemoryMalloc(pChunk->info.length);
    ASSERT(pChunk->pData != NULL);

    memcpy(pChunk->pData, p + n, pChunk->info.length);
    n += pChunk->info.length;
  }
  return n;
}

int32_t tGetSubmitBlobData(uint8_t *p, SSubmitBlobData *pSubmitBlobData) {
  int32_t code = TSDB_CODE_INVALID_MSG;
  int32_t size = 0;
  int32_t n = 0;
  n += tGetI32(p + n, &size);
  ASSERT(size >= 0);

  pSubmitBlobData->aBlobs = taosArrayInit(size, sizeof(SBlobData *));
  ASSERT(pSubmitBlobData->aBlobs != NULL);

  for (int32_t i = 0; i < size; i++) {
    SBlobData **ppBlob = taosArrayReserve(pSubmitBlobData->aBlobs, 1);
    SBlobData  *pBlob = blCreateBlobData();
    ASSERT(pBlob != NULL);
    *ppBlob = pBlob;
    n += tGetBlobData(p + n, pBlob);
  }
  return n;
}

int32_t tGetSubmitBlobDataArr(uint8_t *p, SArray *aSubmitBlobData) {
  int32_t n = 0;
  int32_t nSubmitBlobData;
  n += tGetI32(p + n, &nSubmitBlobData);
  taosArrayEnsureCap(aSubmitBlobData, nSubmitBlobData);

  for (int32_t i = 0; i < nSubmitBlobData; i++) {
    SSubmitBlobData *pBlobData = taosArrayReserve(aSubmitBlobData, 1);
    n += tGetSubmitBlobData(p + n, pBlobData);
  }
  return n;
}

int32_t tEncodeSubmitOidData(SEncoder *pCoder, const SSubmitOidData *pOidData) {
  if (tStartEncode(pCoder) < 0) return -1;

  int32_t nOids = taosArrayGetSize(pOidData->aOids);
  if (tEncodeU64v(pCoder, nOids) < 0) return -1;
  for (uint64_t j = 0; j < nOids; j++) {
    SBlobOidInfo *pOid = taosArrayGet(pOidData->aOids, j);
    if (tEncodeI64(pCoder, pOid->ts) < 0) return -1;
    if (tEncodeI64(pCoder, pOid->oid) < 0) return -1;
    if (tEncodeI32(pCoder, pOid->info.idx) < 0) return -1;
    if (tEncodeI32(pCoder, pOid->info.offset) < 0) return -1;
  }
  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSubmitOidData(SDecoder *pCoder, SSubmitOidData *pOidData) {
  int32_t code = TSDB_CODE_INVALID_MSG;
  if (tStartDecode(pCoder) < 0) goto _EXIT;

  uint64_t nOids = 0;
  if (tDecodeU64v(pCoder, &nOids) < 0) goto _EXIT;
  for (uint64_t j = 0; j < nOids; j++) {
    SBlobOidInfo sOid = {0};
    if (tDecodeI64(pCoder, &sOid.ts) < 0) goto _EXIT;
    if (tDecodeI64(pCoder, &sOid.oid) < 0) goto _EXIT;
    if (tDecodeI32(pCoder, &sOid.info.idx) < 0) goto _EXIT;
    if (tDecodeI32(pCoder, &sOid.info.offset) < 0) goto _EXIT;
    taosArrayPush(pOidData->aOids, &sOid);
  }

  tEndDecode(pCoder);
  code = TSDB_CODE_SUCCESS;
_EXIT:
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayClear(pOidData->aOids);
  }
  return code;
}

int32_t blApplyOids(const SSubmitOidData *pOidData, SSubmitTbData *pTbData) {
  int32_t  code = TSDB_CODE_SUCCESS;
  uint64_t nOids = taosArrayGetSize(pOidData->aOids);
  for (uint64_t j = 0; j < nOids; j++) {
    SBlobOidInfo *pOid = taosArrayGet(pOidData->aOids, j);
    if (pTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      if (pOid->info.idx >= taosArrayGetSize(pTbData->aCol)) {
        code = TSDB_CODE_INVALID_MSG;
        break;
      }
      SColData *pColData = taosArrayGet(pTbData->aCol, pOid->info.idx);
      if (pColData == NULL || pColData->type != TSDB_DATA_TYPE_BLOB) {
        code = TSDB_CODE_INVALID_MSG;
        break;
      }
      SBlobEntry *pBlob = container_of(pColData->pData + pOid->info.offset, SBlobEntry, oid);
      if (pBlob->flag != EBLOB_FLAG_OID) {
        code = TSDB_CODE_INVALID_MSG;
        break;
      }
      ASSERT(pBlob->oid == BLOB_OID_INVALID || pBlob->oid == pOid->oid);
      pBlob->oid = pOid->oid;
    } else {
      if (pOid->info.idx >= taosArrayGetSize(pTbData->aRowP)) {
        code = TSDB_CODE_INVALID_MSG;
        break;
      }
      SRow *pRow = taosArrayGet(pTbData->aRowP, pOid->info.idx);
      if (pRow == NULL) {
        code = TSDB_CODE_INVALID_MSG;
        break;
      }
      SBlobEntry *pBlob = container_of(pRow->data + pOid->info.offset, SBlobEntry, oid);
      if (pBlob->flag != EBLOB_FLAG_OID) {
        code = TSDB_CODE_INVALID_MSG;
        break;
      }
      ASSERT(pBlob->oid == BLOB_OID_INVALID || pBlob->oid == pOid->oid);
      pBlob->oid = pOid->oid;
    }
  }
  return code;
}

int32_t blApplySubmitOidData(SSubmitReq2 *pReq) {
  int32_t nSubmitOids = taosArrayGetSize(pReq->aSubmitOidData);
  for (int32_t i = 0; i < nSubmitOids; i++) {
    SSubmitOidData *pOidData = taosArrayGet(pReq->aSubmitOidData, i);
    SSubmitTbData  *pTbData = taosArrayGet(pReq->aSubmitTbData, i);
    int32_t         code = blApplyOids(pOidData, pTbData);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

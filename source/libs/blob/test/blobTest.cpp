#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include "blobInt.h"

TEST(SBlobData, createAndFree) {
  SBlobData *pBlob = blCreateBlobData();
  blFreeBlobData(pBlob);
}

TEST(SBlobData, putAndGet) {
  char buf[4086] = "hello world";
  int  len = sizeof(buf);

  SBlobData *pBlob = blCreateBlobData();
  pBlob->ts = 1234567890;
  pBlob->hdr.oid.i.idx = 321;
  pBlob->hdr.oid.i.offset = 123;
  pBlob->hdr.version = 1;
  pBlob->hdr.cmprAlg = 12;
  pBlob->hdr.reserved = 0;
  pBlob->hdr.bodyLen = 1000;

  int32_t code = 0;
  code = blChopDataIntoChunks(pBlob, buf, len);
  EXPECT_EQ(0, code);

  int32_t  size = tPutBlobData(NULL, pBlob);
  uint8_t *ptr = (uint8_t *)taosMemoryMalloc(size);
  int32_t  n = tPutBlobData(ptr, pBlob);
  EXPECT_EQ(size, n);

  SBlobData *pBlob2 = blCreateBlobData();
  n = tGetBlobData(ptr, pBlob2);
  EXPECT_EQ(size, n);
  EXPECT_EQ(pBlob->ts, pBlob2->ts);
  EXPECT_EQ(pBlob->hdr.oid.i.idx, pBlob2->hdr.oid.i.idx);
  EXPECT_EQ(pBlob->hdr.oid.i.offset, pBlob2->hdr.oid.i.offset);
  EXPECT_EQ(pBlob->hdr.version, pBlob2->hdr.version);
  EXPECT_EQ(pBlob->hdr.cmprAlg, pBlob2->hdr.cmprAlg);
  EXPECT_EQ(pBlob->hdr.reserved, pBlob2->hdr.reserved);
  EXPECT_EQ(pBlob->hdr.bodyLen, pBlob2->hdr.bodyLen);

  int32_t aSize = taosArrayGetSize(pBlob->pChunks);
  int32_t bSize = taosArrayGetSize(pBlob2->pChunks);
  EXPECT_EQ(aSize, bSize);
  for (int32_t i = 0; i < aSize; i++) {
    SBlobChunk *aChunk = (SBlobChunk *)taosArrayGet(pBlob->pChunks, i);
    SBlobChunk *bChunk = (SBlobChunk *)taosArrayGet(pBlob2->pChunks, i);
    EXPECT_EQ(aChunk->info.offset, bChunk->info.offset);
    EXPECT_EQ(aChunk->info.length, bChunk->info.length);
    for (int32_t j = 0; j < aChunk->info.length; j++) {
      EXPECT_EQ(aChunk->pData[j], bChunk->pData[j]);
    }
  }

  blFreeBlobData(pBlob);
  blFreeBlobData(pBlob2);
  taosMemoryFree(ptr);
  ptr = NULL;
}

TEST(SSubmitBlobData, putAndGet) {
  SSubmitBlobData sBlobData = {0};
  sBlobData.aBlobs = taosArrayInit(8, sizeof(SBlobData *));

  char buf[4086] = "hello world";
  int  len = sizeof(buf);

  int32_t aSize = 16;
  for (int32_t i = 0; i < aSize; i++) {
    SBlobData *pBlob = blCreateBlobData();
    pBlob->ts = 1234567890 + i;
    pBlob->hdr.oid.i.idx = 321 + i;
    pBlob->hdr.oid.i.offset = 123 + i;
    pBlob->hdr.version = 1 + i;
    pBlob->hdr.cmprAlg = 12;
    pBlob->hdr.reserved = 0;
    pBlob->hdr.bodyLen = 1000;
    int32_t code = 0;
    code = blChopDataIntoChunks(pBlob, buf, len);
    EXPECT_EQ(0, code);
    taosArrayPush(sBlobData.aBlobs, &pBlob);
  }

  int32_t  size = tPutSubmitBlobData(NULL, &sBlobData);
  uint8_t *ptr = (uint8_t *)taosMemoryMalloc(size);
  int32_t  n = tPutSubmitBlobData(ptr, &sBlobData);
  EXPECT_EQ(size, n);

  SSubmitBlobData sBlobData2 = {0};
  n = tGetSubmitBlobData(ptr, &sBlobData2);
  EXPECT_EQ(size, n);

  int32_t aSize2 = taosArrayGetSize(sBlobData.aBlobs);
  int32_t bSize2 = taosArrayGetSize(sBlobData2.aBlobs);
  EXPECT_EQ(aSize2, bSize2);
  for (int32_t i = 0; i < aSize2; i++) {
    SBlobData *aBlob = *(SBlobData **)taosArrayGet(sBlobData.aBlobs, i);
    SBlobData *bBlob = *(SBlobData **)taosArrayGet(sBlobData2.aBlobs, i);
    EXPECT_EQ(aBlob->ts, bBlob->ts);
    EXPECT_EQ(aBlob->hdr.oid.i.idx, bBlob->hdr.oid.i.idx);
    EXPECT_EQ(aBlob->hdr.oid.i.offset, bBlob->hdr.oid.i.offset);
    EXPECT_EQ(aBlob->hdr.version, bBlob->hdr.version);
    EXPECT_EQ(aBlob->hdr.cmprAlg, bBlob->hdr.cmprAlg);
    EXPECT_EQ(aBlob->hdr.reserved, bBlob->hdr.reserved);
    EXPECT_EQ(aBlob->hdr.bodyLen, bBlob->hdr.bodyLen);

    int32_t aSize = taosArrayGetSize(aBlob->pChunks);
    int32_t bSize = taosArrayGetSize(bBlob->pChunks);
    EXPECT_EQ(aSize, bSize);
    for (int32_t j = 0; j < aSize; j++) {
      SBlobChunk *aChunk = (SBlobChunk *)taosArrayGet(aBlob->pChunks, j);
      SBlobChunk *bChunk = (SBlobChunk *)taosArrayGet(bBlob->pChunks, j);
      EXPECT_EQ(aChunk->info.offset, bChunk->info.offset);
      EXPECT_EQ(aChunk->info.length, bChunk->info.length);
      for (int32_t k = 0; k < aChunk->info.length; k++) {
        EXPECT_EQ(aChunk->pData[k], bChunk->pData[k]);
      }
    }
  }

  tDestroySubmitBlobData(&sBlobData);
  tDestroySubmitBlobData(&sBlobData2);
  taosMemoryFree(ptr);
  ptr = NULL;
}

TEST(SSubmitBlobDataArray, putAndGet) {
  SSubmitReq2 sReq = {0};
  sReq.aSubmitBlobData = taosArrayInit(8, sizeof(SSubmitBlobData));

  char buf[4086] = "hello world";
  int  len = sizeof(buf);

  int32_t m = 16;
  for (int32_t i = 0; i < m; i++) {
    SSubmitBlobData sBlobData = {0};
    sBlobData.aBlobs = taosArrayInit(8, sizeof(SBlobData *));

    int32_t n = 16;
    for (int32_t j = 0; j < n; j++) {
      SBlobData *pBlob = blCreateBlobData();
      pBlob->ts = 1234567890 + j;
      pBlob->hdr.oid.i.idx = 321 + j;
      pBlob->hdr.oid.i.offset = 123 + j;
      pBlob->hdr.version = 1 + j;
      pBlob->hdr.cmprAlg = 12;
      pBlob->hdr.reserved = 0;
      pBlob->hdr.bodyLen = 1000;
      int32_t code = 0;
      code = blChopDataIntoChunks(pBlob, buf, len);
      EXPECT_EQ(0, code);
      taosArrayPush(sBlobData.aBlobs, &pBlob);
    }

    taosArrayPush(sReq.aSubmitBlobData, &sBlobData);
  }

  int32_t  size = tPutSubmitBlobDataArr(NULL, sReq.aSubmitBlobData);
  uint8_t *ptr = (uint8_t *)taosMemoryMalloc(size);
  int32_t  n = tPutSubmitBlobDataArr(ptr, sReq.aSubmitBlobData);
  EXPECT_EQ(size, n);

  SSubmitReq2 sReq2 = {0};
  sReq2.aSubmitBlobData = taosArrayInit(0, sizeof(SSubmitBlobData));
  n = tGetSubmitBlobDataArr(ptr, sReq2.aSubmitBlobData);
  EXPECT_EQ(size, n);

  int32_t aSize = taosArrayGetSize(sReq.aSubmitBlobData);
  int32_t bSize = taosArrayGetSize(sReq2.aSubmitBlobData);
  EXPECT_EQ(aSize, bSize);
  for (int32_t i = 0; i < aSize; i++) {
    SSubmitBlobData *aBlobData = (SSubmitBlobData *)taosArrayGet(sReq.aSubmitBlobData, i);
    SSubmitBlobData *bBlobData = (SSubmitBlobData *)taosArrayGet(sReq2.aSubmitBlobData, i);
    int32_t          aSize2 = taosArrayGetSize(aBlobData->aBlobs);
    int32_t          bSize2 = taosArrayGetSize(bBlobData->aBlobs);
    EXPECT_EQ(aSize2, bSize2);
    for (int32_t j = 0; j < aSize2; j++) {
      SBlobData *aBlob = *(SBlobData **)taosArrayGet(aBlobData->aBlobs, j);
      SBlobData *bBlob = *(SBlobData **)taosArrayGet(bBlobData->aBlobs, j);
      EXPECT_EQ(aBlob->ts, bBlob->ts);
      EXPECT_EQ(aBlob->hdr.oid.i.idx, bBlob->hdr.oid.i.idx);
      EXPECT_EQ(aBlob->hdr.oid.i.offset, bBlob->hdr.oid.i.offset);
      EXPECT_EQ(aBlob->hdr.version, bBlob->hdr.version);
      EXPECT_EQ(aBlob->hdr.cmprAlg, bBlob->hdr.cmprAlg);
      EXPECT_EQ(aBlob->hdr.reserved, bBlob->hdr.reserved);
      EXPECT_EQ(aBlob->hdr.bodyLen, bBlob->hdr.bodyLen);

      int32_t aSize = taosArrayGetSize(aBlob->pChunks);
      int32_t bSize = taosArrayGetSize(bBlob->pChunks);
      EXPECT_EQ(aSize, bSize);
      for (int32_t k = 0; k < aSize; k++) {
        SBlobChunk *aChunk = (SBlobChunk *)taosArrayGet(aBlob->pChunks, k);
        SBlobChunk *bChunk = (SBlobChunk *)taosArrayGet(bBlob->pChunks, k);
        EXPECT_EQ(aChunk->info.offset, bChunk->info.offset);
        EXPECT_EQ(aChunk->info.length, bChunk->info.length);
        for (int32_t l = 0; l < aChunk->info.length; l++) {
          EXPECT_EQ(aChunk->pData[l], bChunk->pData[l]);
        }
      }
    }
  }

  tDestroySubmitReq(&sReq, TSDB_MSG_FLG_ENCODE);
  tDestroySubmitReq(&sReq2, TSDB_MSG_FLG_ENCODE);
  taosMemoryFree(ptr);
  ptr = NULL;
}

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
#ifndef _TD_BLOB_H_
#define _TD_BLOB_H_

#include "tarray.h"
#include "tdataformat.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BLOB_CHUNK_LVL 11

enum EBloblEntryFlag {
  EBLOB_FLAG_DATA = 1,
  EBLOB_FLAG_OID = 2,
};

#define BLOB_OID_INVALID -1

#pragma pack(push, 1)
typedef struct SBlobEntry {
  int8_t flag;
  union {
    struct {
      uint32_t len;
      char     data[];
    } varData;
    TdOidT oid;
  };
} SBlobEntry;

typedef struct SBlobChunkOffset {
  uint32_t offset;
  uint16_t length;
} SBlobChunkOffset;

typedef struct SBlobOidOffset {
  int32_t idx;
  int32_t offset;
} SBlobOidOffset;

typedef struct SBlobDataHdr {
  union {
    TdOidT         v;
    SBlobOidOffset i;
  } oid;
  int64_t  version;
  uint32_t length;
  uint8_t  cmprAlg;
  uint8_t  chunkLvl;
  uint16_t reserved;
  uint32_t bodyLen;
} SBlobDataHdr;
#pragma pack(pop)

typedef struct SBlobOidInfo {
  TSKEY          ts;
  TdOidT         oid;
  SBlobOidOffset info;
} SBlobOidInfo;

typedef struct SBlobChunk {
  SBlobChunkOffset info;
  char            *pData;
} SBlobChunk;

typedef struct SBlobData {
  TSKEY        ts;
  SBlobDataHdr hdr;
  SArray      *pChunks;  // SArray<SBlobChunk>
} SBlobData;

// blob entry
int32_t tBlobEntrySize(SBlobEntry *pEntry);

// blob chunk
static int32_t tBlobChunkSize(uint8_t chunkLvl) { return (1 << chunkLvl); }
static int32_t tBlobChunkNum(int32_t length, int32_t chunkSize) {
  return (length / chunkSize) + (length % chunkSize != 0);
}

int32_t blCreateBlobChunk(SBlobChunk *pChunk, uint32_t offset, uint32_t length);
void    blDestroyBlobChunk(void *pChunk);

int32_t blChopDataIntoChunks(SBlobData *pBlob, const void *pData, int32_t length);
int32_t blJoinChunksIntoData(SBlobData *pBlob, void *pData, int32_t length);

// blob data
int32_t blReWriteBlobData(SColVal *pColVal, TSKEY ts, SBlobData **ppBlob);
int32_t blSeparateBlobData(const SBlobEntry *pEntry, SBlobData **ppBlob);

int32_t tPutSubmitBlobData(uint8_t *p, const SSubmitBlobData *pSubmitBlobData);
int32_t tGetSubmitBlobData(uint8_t *p, SSubmitBlobData *pSubmitBlobData);

int32_t tGetSubmitBlobDataArr(uint8_t *p, SArray *aSubmitBlobData);
int32_t tPutSubmitBlobDataArr(uint8_t *p, const SArray *aSubmitBlobData);

int32_t tEncodeSubmitOidData(SEncoder *pCoder, const SSubmitOidData *pSubmitOidData);
int32_t tDecodeSubmitOidData(SDecoder *pCoder, SSubmitOidData *pSubmitOidData);

SBlobData *blCreateBlobData();
void       blFreeBlobDataImpl(void *ptr);

#define blFreeBlobData(pBlob)  \
  do {                         \
    blFreeBlobDataImpl(pBlob); \
    pBlob = NULL;              \
  } while (0)

int32_t blApplySubmitOidData(SSubmitReq2 *pReq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_BLOB_H_*/

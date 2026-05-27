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

#include "tsdb.h"
#include "tsdbFS2.h"

#define TSDB_SNAP_MSG_VER 2

// file info for snapshot sync: (fid, ftype, level, minVer, maxVer, cid, size, isMissing)
typedef struct {
  int32_t fid;
  int32_t ftype;      // tsdb_ftype_t
  int32_t level;      // STT level (0/1/2), farr files use 0
  int64_t minVer;     // min version in file
  int64_t maxVer;     // max version in file
  int64_t cid;        // commit id
  int64_t size;       // file size
  int8_t  isMissing;  // 1=missing, 0=present
} STsdbSnapFileInfo;

// fset partition
static int32_t tsdbFSetPartCmprFn(STsdbFSetPartition* x, STsdbFSetPartition* y) {
  if (x->fid < y->fid) return -1;
  if (x->fid > y->fid) return 1;
  return 0;
}

static int32_t tVersionRangeCmprFn(SVersionRange* x, SVersionRange* y) {
  if (x->minVer < y->minVer) return -1;
  if (x->minVer > y->minVer) return 1;
  if (x->maxVer < y->maxVer) return -1;
  if (x->maxVer > y->maxVer) return 1;
  return 0;
}

static int32_t tsdbTFileSetRangeCmprFn(STFileSetRange* x, STFileSetRange* y) {
  if (x->fid < y->fid) return -1;
  if (x->fid > y->fid) return 1;
  return 0;
}

STsdbFSetPartition* tsdbFSetPartitionCreate() {
  STsdbFSetPartition* pSP = taosMemoryCalloc(1, sizeof(STsdbFSetPartition));
  if (pSP == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < TSDB_FSET_RANGE_TYP_MAX; i++) {
    TARRAY2_INIT(&pSP->verRanges[i]);
  }
  return pSP;
}

void tsdbFSetPartitionClear(STsdbFSetPartition** ppSP) {
  if (ppSP == NULL || ppSP[0] == NULL) {
    return;
  }
  for (int32_t i = 0; i < TSDB_FSET_RANGE_TYP_MAX; i++) {
    TARRAY2_DESTROY(&ppSP[0]->verRanges[i], NULL);
  }
  taosMemoryFree(ppSP[0]);
  ppSP[0] = NULL;
}

static int32_t tsdbFTypeToFRangeType(tsdb_ftype_t ftype) {
  switch (ftype) {
    case TSDB_FTYPE_HEAD:
      return TSDB_FSET_RANGE_TYP_HEAD;
    case TSDB_FTYPE_DATA:
      return TSDB_FSET_RANGE_TYP_DATA;
    case TSDB_FTYPE_SMA:
      return TSDB_FSET_RANGE_TYP_SMA;
    case TSDB_FTYPE_TOMB:
      return TSDB_FSET_RANGE_TYP_TOMB;
    case TSDB_FTYPE_STT:
      return TSDB_FSET_RANGE_TYP_STT;
  }
  return TSDB_FSET_RANGE_TYP_MAX;
}

static int32_t tsdbTFileSetToFSetPartition(STFileSet* fset, STsdbFSetPartition** ppSP) {
  STsdbFSetPartition* p = tsdbFSetPartitionCreate();
  if (p == NULL) {
    return terrno;
  }

  p->fid = fset->fid;

  int32_t code = 0;
  int32_t typ = 0;
  int32_t corrupt = false;
  int32_t count = 0;
  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] == NULL) continue;
    typ = tsdbFTypeToFRangeType(ftype);
    STFile* f = fset->farr[ftype]->f;
    if (f->maxVer > fset->maxVerValid) {
      corrupt = true;
      tsdbError("skip incomplete data file: fid:%d, maxVerValid:%" PRId64 ", minVer:%" PRId64 ", maxVer:%" PRId64
                ", ftype: %d",
                fset->fid, fset->maxVerValid, f->minVer, f->maxVer, ftype);
      continue;
    }
    count++;
    SVersionRange vr = {.minVer = f->minVer, .maxVer = f->maxVer};
    code = TARRAY2_SORT_INSERT(&p->verRanges[typ], vr, tVersionRangeCmprFn);
    if (code) {
      tsdbFSetPartitionClear(&p);
      return code;
    }
  }

  typ = TSDB_FSET_RANGE_TYP_STT;
  const SSttLvl* lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    STFileObj* fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      STFile* f = fobj->f;
      if (f->maxVer > fset->maxVerValid) {
        corrupt = true;
        tsdbError("skip incomplete stt file.fid:%d, maxVerValid:%" PRId64 ", minVer:%" PRId64 ", maxVer:%" PRId64
                  ", ftype: %d",
                  fset->fid, fset->maxVerValid, f->minVer, f->maxVer, typ);
        continue;
      }
      count++;
      SVersionRange vr = {.minVer = f->minVer, .maxVer = f->maxVer};
      code = TARRAY2_SORT_INSERT(&p->verRanges[typ], vr, tVersionRangeCmprFn);
      if (code) {
        tsdbFSetPartitionClear(&p);
        return code;
      }
    }
  }
  if (corrupt && count == 0) {
    SVersionRange vr = {.minVer = VERSION_MIN, .maxVer = fset->maxVerValid};
    code = TARRAY2_SORT_INSERT(&p->verRanges[typ], vr, tVersionRangeCmprFn);
    if (code) {
      tsdbFSetPartitionClear(&p);
      return code;
    }
  }
  ppSP[0] = p;
  return 0;
}

// fset partition list
STsdbFSetPartList* tsdbFSetPartListCreate() {
  STsdbFSetPartList* pList = taosMemoryCalloc(1, sizeof(STsdbFSetPartList));
  if (pList == NULL) {
    return NULL;
  }
  TARRAY2_INIT(pList);
  return pList;
}

void tsdbFSetPartListDestroy(STsdbFSetPartList** ppList) {
  if (ppList == NULL || ppList[0] == NULL) return;

  TARRAY2_DESTROY(ppList[0], tsdbFSetPartitionClear);
  taosMemoryFree(ppList[0]);
  ppList[0] = NULL;
}

int32_t tsdbFSetPartListToRangeDiff(STsdbFSetPartList* pList, TFileSetRangeArray** ppRanges) {
  int32_t code = 0;

  TFileSetRangeArray* pDiff = taosMemoryCalloc(1, sizeof(TFileSetRangeArray));
  if (pDiff == NULL) {
    code = terrno;
    goto _err;
  }
  TARRAY2_INIT(pDiff);

  STsdbFSetPartition* part;
  TARRAY2_FOREACH(pList, part) {
    STFileSetRange* r = taosMemoryCalloc(1, sizeof(STFileSetRange));
    if (r == NULL) {
      code = terrno;
      goto _err;
    }
    int64_t maxVerValid = -1;
    int32_t typMax = TSDB_FSET_RANGE_TYP_MAX;
    for (int32_t i = 0; i < typMax; i++) {
      SVerRangeList* iList = &part->verRanges[i];
      SVersionRange  vr = {0};
      TARRAY2_FOREACH(iList, vr) {
        if (vr.maxVer < vr.minVer) {
          continue;
        }
        maxVerValid = TMAX(maxVerValid, vr.maxVer);
      }
    }
    r->fid = part->fid;
    r->sver = maxVerValid + 1;
    r->ever = VERSION_MAX;
    tsdbDebug("range diff fid:%" PRId64 ", sver:%" PRId64 ", ever:%" PRId64, part->fid, r->sver, r->ever);
    code = TARRAY2_SORT_INSERT(pDiff, r, tsdbTFileSetRangeCmprFn);
    if (code) {
      taosMemoryFree(r);
      goto _err;
    }
  }
  ppRanges[0] = pDiff;

  tsdbInfo("pDiff size:%d", TARRAY2_SIZE(pDiff));
  return 0;

_err:
  if (pDiff) {
    tsdbTFileSetRangeArrayDestroy(&pDiff);
  }
  return code;
}

// serialization
int32_t tTsdbFSetPartListDataLenCalc(STsdbFSetPartList* pList) {
  int32_t hdrLen = sizeof(int32_t);
  int32_t datLen = 0;

  int8_t  msgVer = 1;
  int32_t len = TARRAY2_SIZE(pList);
  hdrLen += sizeof(msgVer);
  hdrLen += sizeof(len);
  datLen += hdrLen;

  for (int32_t u = 0; u < len; u++) {
    STsdbFSetPartition* p = TARRAY2_GET(pList, u);
    int32_t             typMax = TSDB_FSET_RANGE_TYP_MAX;
    int32_t             uItem = 0;
    uItem += sizeof(STsdbFSetPartition);
    uItem += sizeof(typMax);

    for (int32_t i = 0; i < typMax; i++) {
      int32_t iLen = TARRAY2_SIZE(&p->verRanges[i]);
      int32_t jItem = 0;
      jItem += sizeof(SVersionRange);
      jItem += sizeof(int64_t);
      uItem += sizeof(iLen) + jItem * iLen;
    }
    datLen += uItem;
  }
  return datLen;
}

static int32_t tSerializeTsdbFSetPartList(void* buf, int32_t bufLen, STsdbFSetPartList* pList, int32_t* encodeSize) {
  SEncoder encoder = {0};
  int8_t   reserved8 = 0;
  int16_t  reserved16 = 0;
  int64_t  reserved64 = 0;
  int8_t   msgVer = TSDB_SNAP_MSG_VER;
  int32_t  len = TARRAY2_SIZE(pList);
  int32_t  code = 0;

  tEncoderInit(&encoder, buf, bufLen);
  if ((code = tStartEncode(&encoder))) goto _exit;
  if ((code = tEncodeI8(&encoder, msgVer))) goto _exit;
  if ((code = tEncodeI32(&encoder, len))) goto _exit;

  for (int32_t u = 0; u < len; u++) {
    STsdbFSetPartition* p = TARRAY2_GET(pList, u);
    if ((code = tEncodeI64(&encoder, p->fid))) goto _exit;
    if ((code = tEncodeI8(&encoder, p->stat))) goto _exit;
    if ((code = tEncodeI8(&encoder, reserved8))) goto _exit;
    if ((code = tEncodeI16(&encoder, reserved16))) goto _exit;

    int32_t typMax = TSDB_FSET_RANGE_TYP_MAX;
    if ((code = tEncodeI32(&encoder, typMax))) goto _exit;

    for (int32_t i = 0; i < typMax; i++) {
      SVerRangeList* iList = &p->verRanges[i];
      int32_t        iLen = TARRAY2_SIZE(iList);

      if ((code = tEncodeI32(&encoder, iLen))) goto _exit;
      for (int32_t j = 0; j < iLen; j++) {
        SVersionRange r = TARRAY2_GET(iList, j);
        if ((code = tEncodeI64(&encoder, r.minVer))) goto _exit;
        if ((code = tEncodeI64(&encoder, r.maxVer))) goto _exit;
        if ((code = tEncodeI64(&encoder, reserved64))) goto _exit;
      }
    }
  }

  tEndEncode(&encoder);

  if (encodeSize) {
    encodeSize[0] = encoder.pos;
  }

_exit:
  tEncoderClear(&encoder);
  return code;
}

int32_t tDeserializeTsdbFSetPartList(void* buf, int32_t bufLen, STsdbFSetPartList* pList) {
  SDecoder decoder = {0};
  int8_t   reserved8 = 0;
  int16_t  reserved16 = 0;
  int64_t  reserved64 = 0;
  int32_t  code = 0;

  STsdbFSetPartition* p = NULL;

  tDecoderInit(&decoder, buf, bufLen);
  int8_t  msgVer = 0;
  int32_t len = 0;
  if ((code = tStartDecode(&decoder))) goto _err;
  if ((code = tDecodeI8(&decoder, &msgVer))) goto _err;
  if (msgVer != TSDB_SNAP_MSG_VER) {
    code = TSDB_CODE_INVALID_MSG;
    goto _err;
  }
  if ((code = tDecodeI32(&decoder, &len))) goto _err;

  for (int32_t u = 0; u < len; u++) {
    p = tsdbFSetPartitionCreate();
    if (p == NULL) {
      code = terrno;
      goto _err;
    }

    if ((code = tDecodeI64(&decoder, &p->fid))) goto _err;
    if ((code = tDecodeI8(&decoder, &p->stat))) goto _err;
    if ((code = tDecodeI8(&decoder, &reserved8))) goto _err;
    if ((code = tDecodeI16(&decoder, &reserved16))) goto _err;

    int32_t typMax = 0;
    if ((code = tDecodeI32(&decoder, &typMax))) goto _err;

    for (int32_t i = 0; i < typMax; i++) {
      SVerRangeList* iList = &p->verRanges[i];
      int32_t        iLen = 0;
      if ((code = tDecodeI32(&decoder, &iLen))) goto _err;
      for (int32_t j = 0; j < iLen; j++) {
        SVersionRange r = {0};
        if ((code = tDecodeI64(&decoder, &r.minVer))) goto _err;
        if ((code = tDecodeI64(&decoder, &r.maxVer))) goto _err;
        if ((code = tDecodeI64(&decoder, &reserved64))) goto _err;
        if ((code = TARRAY2_APPEND(iList, r))) goto _err;
      }
    }
    if ((code = TARRAY2_APPEND(pList, p))) goto _err;
    p = NULL;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;

_err:
  if (p) {
    tsdbFSetPartitionClear(&p);
  }
  tDecoderClear(&decoder);
  return code;
}

// fs state
static STsdbFSetPartList* tsdbSnapGetFSetPartList(STFileSystem* fs) {
  STsdbFSetPartList* pList = tsdbFSetPartListCreate();
  if (pList == NULL) {
    return NULL;
  }

  int32_t code = 0;
  (void)taosThreadMutexLock(&fs->tsdb->mutex);
  STFileSet* fset;
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    STsdbFSetPartition* pItem = NULL;
    code = tsdbTFileSetToFSetPartition(fset, &pItem);
    if (code) {
      terrno = code;
      break;
    }
    code = TARRAY2_SORT_INSERT(pList, pItem, tsdbFSetPartCmprFn);
    if (code) {
      terrno = code;
      break;
    }
  }
  (void)taosThreadMutexUnlock(&fs->tsdb->mutex);

  if (code) {
    TARRAY2_DESTROY(pList, tsdbFSetPartitionClear);
    taosMemoryFree(pList);
    pList = NULL;
  }
  return pList;
}

ETsdbFsState tsdbSnapGetFsState(SVnode* pVnode) { return pVnode->pTsdb->pFS->fsstate; }

// description
typedef struct STsdbPartitionInfo {
  int32_t            vgId;
  int32_t            tsdbMaxCnt;
  int32_t            subTyps[TSDB_RETENTION_MAX];
  STsdbFSetPartList* pLists[TSDB_RETENTION_MAX];
} STsdbPartitionInfo;

static int32_t tsdbPartitionInfoInit(SVnode* pVnode, STsdbPartitionInfo* pInfo) {
  int32_t subTyps[TSDB_RETENTION_MAX] = {SNAP_DATA_TSDB, SNAP_DATA_RSMA1, SNAP_DATA_RSMA2};
  pInfo->vgId = TD_VID(pVnode);
  pInfo->tsdbMaxCnt = 1;

  if (!(sizeof(pInfo->subTyps) == sizeof(subTyps))) {
    return TSDB_CODE_INVALID_PARA;
  }
  memcpy(pInfo->subTyps, (char*)subTyps, sizeof(subTyps));

  // fset partition list
  memset(pInfo->pLists, 0, sizeof(pInfo->pLists[0]) * TSDB_RETENTION_MAX);
  for (int32_t j = 0; j < pInfo->tsdbMaxCnt; ++j) {
    STsdb* pTsdb = SMA_RSMA_GET_TSDB(pVnode, j);
    pInfo->pLists[j] = tsdbSnapGetFSetPartList(pTsdb->pFS);
    if (pInfo->pLists[j] == NULL) {
      return terrno;
    }
  }
  return 0;
}

static void tsdbPartitionInfoClear(STsdbPartitionInfo* pInfo) {
  for (int32_t j = 0; j < pInfo->tsdbMaxCnt; ++j) {
    if (pInfo->pLists[j] == NULL) continue;
    tsdbFSetPartListDestroy(&pInfo->pLists[j]);
  }
}

static int32_t tsdbPartitionInfoEstSize(STsdbPartitionInfo* pInfo) {
  int32_t dataLen = 0;
  for (int32_t j = 0; j < pInfo->tsdbMaxCnt; ++j) {
    dataLen += sizeof(SSyncTLV);  // subTyps[j]
    dataLen += tTsdbFSetPartListDataLenCalc(pInfo->pLists[j]);
  }
  return dataLen;
}

static int32_t tsdbPartitionInfoSerialize(STsdbPartitionInfo* pInfo, uint8_t* buf, int32_t bufLen) {
  int32_t tlen = 0;
  int32_t offset = 0;
  for (int32_t j = 0; j < pInfo->tsdbMaxCnt; ++j) {
    SSyncTLV* pSubHead = (void*)((char*)buf + offset);
    int32_t   valOffset = offset + sizeof(*pSubHead);
    int32_t   code = tSerializeTsdbFSetPartList(pSubHead->val, bufLen - valOffset, pInfo->pLists[j], &tlen);
    if (code) {
      tsdbError("vgId:%d, failed to serialize fset partition list of tsdb %d since %s", pInfo->vgId, j, terrstr());
      return code;
    }
    pSubHead->typ = pInfo->subTyps[j];
    pSubHead->len = tlen;
    offset += sizeof(*pSubHead) + tlen;
  }
  return offset;
}

// tsdb replication opts
static int32_t tTsdbRepOptsDataLenCalc(STsdbRepOpts* pInfo) {
  int32_t hdrLen = sizeof(int32_t);
  int32_t datLen = 0;

  int8_t  msgVer = 0;
  int64_t reserved64 = 0;
  int16_t format = 0;
  hdrLen += sizeof(msgVer);
  datLen += hdrLen;
  datLen += sizeof(format);
  datLen += sizeof(reserved64);
  datLen += sizeof(*pInfo);
  return datLen;
}

int32_t tSerializeTsdbRepOpts(void* buf, int32_t bufLen, STsdbRepOpts* pOpts) {
  int32_t  code = 0;
  SEncoder encoder = {0};
  int64_t  reserved64 = 0;
  int8_t   msgVer = TSDB_SNAP_MSG_VER;

  tEncoderInit(&encoder, buf, bufLen);

  if ((code = tStartEncode(&encoder))) goto _err;
  if ((code = tEncodeI8(&encoder, msgVer))) goto _err;
  int16_t format = pOpts->format;
  if ((code = tEncodeI16(&encoder, format))) goto _err;
  if ((code = tEncodeI64(&encoder, reserved64))) goto _err;

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;

_err:
  tEncoderClear(&encoder);
  return code;
}

int32_t tDeserializeTsdbRepOpts(void* buf, int32_t bufLen, STsdbRepOpts* pOpts) {
  int32_t  code;
  SDecoder decoder = {0};
  int64_t  reserved64 = 0;
  int8_t   msgVer = 0;

  tDecoderInit(&decoder, buf, bufLen);

  if ((code = tStartDecode(&decoder))) goto _err;
  if ((code = tDecodeI8(&decoder, &msgVer))) goto _err;
  if (msgVer != TSDB_SNAP_MSG_VER) goto _err;
  int16_t format = 0;
  if ((code = tDecodeI16(&decoder, &format))) goto _err;
  pOpts->format = format;
  if ((code = tDecodeI64(&decoder, &reserved64))) goto _err;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;

_err:
  tDecoderClear(&decoder);
  return code;
}

int32_t tMissingFileListDataLenCalc(int32_t fileCount) {
  int32_t hdrLen = sizeof(int32_t);
  int32_t datLen = 0;

  int8_t msgVer = 0;
  hdrLen += sizeof(msgVer);
  datLen += hdrLen;
  datLen += sizeof(int32_t);  // fileCount
  // fid + ftype + level + minVer + maxVer + cid + size + isMissing = 4+4+4+8+8+8+8+1 = 45 bytes per record
  datLen += fileCount * (sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(int64_t) + sizeof(int64_t) +
                         sizeof(int64_t) + sizeof(int64_t) + sizeof(int8_t));
  return datLen;
}

int32_t tSerializeMissingFileList(void* buf, int32_t bufLen, const STsdbSnapFileInfo* files, int32_t fileCount) {
  int32_t  code = 0;
  SEncoder encoder = {0};
  int8_t   msgVer = TSDB_SNAP_MSG_VER;

  tEncoderInit(&encoder, buf, bufLen);

  if ((code = tStartEncode(&encoder))) goto _err;
  if ((code = tEncodeI8(&encoder, msgVer))) goto _err;
  if ((code = tEncodeI32(&encoder, fileCount))) goto _err;
  for (int32_t i = 0; i < fileCount; ++i) {
    if ((code = tEncodeI32(&encoder, files[i].fid))) goto _err;
    if ((code = tEncodeI32(&encoder, files[i].ftype))) goto _err;
    if ((code = tEncodeI32(&encoder, files[i].level))) goto _err;
    if ((code = tEncodeI64(&encoder, files[i].minVer))) goto _err;
    if ((code = tEncodeI64(&encoder, files[i].maxVer))) goto _err;
    if ((code = tEncodeI64(&encoder, files[i].cid))) goto _err;
    if ((code = tEncodeI64(&encoder, files[i].size))) goto _err;
    if ((code = tEncodeI8(&encoder, files[i].isMissing))) goto _err;
  }

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;

_err:
  tEncoderClear(&encoder);
  return code;
}

int32_t tDeserializeMissingFileList(void* buf, int32_t bufLen, void** ppFiles, int32_t* pFileCount, SHashObj** ppHash,
                                    SHashObj** ppSttHash, int32_t vgId) {
  int32_t            code = 0;
  SDecoder           decoder = {0};
  int8_t             msgVer = 0;
  int32_t            fileCount = 0;
  SHashObj*          pHash = NULL;
  SHashObj*          pSttHash = NULL;
  STsdbSnapFileInfo* files = NULL;

  tDecoderInit(&decoder, buf, bufLen);

  if ((code = tStartDecode(&decoder))) goto _err;
  if ((code = tDecodeI8(&decoder, &msgVer))) goto _err;
  if (msgVer != TSDB_SNAP_MSG_VER) {
    code = TSDB_CODE_INVALID_MSG;
    goto _err;
  }
  if ((code = tDecodeI32(&decoder, &fileCount))) goto _err;
  if (fileCount < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _err;
  }
  if (fileCount > 0) {
    pHash = taosHashInit(fileCount * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (pHash == NULL) {
      code = terrno;
      goto _err;
    }
    pSttHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (pSttHash == NULL) {
      code = terrno;
      goto _err;
    }
    files = taosMemoryMalloc(fileCount * sizeof(STsdbSnapFileInfo));
    if (files == NULL) {
      code = terrno;
      goto _err;
    }
    for (int32_t i = 0; i < fileCount; ++i) {
      if ((code = tDecodeI32(&decoder, &files[i].fid))) goto _err;
      if ((code = tDecodeI32(&decoder, &files[i].ftype))) goto _err;
      if ((code = tDecodeI32(&decoder, &files[i].level))) goto _err;
      if ((code = tDecodeI64(&decoder, &files[i].minVer))) goto _err;
      if ((code = tDecodeI64(&decoder, &files[i].maxVer))) goto _err;
      if ((code = tDecodeI64(&decoder, &files[i].cid))) goto _err;
      if ((code = tDecodeI64(&decoder, &files[i].size))) goto _err;
      if ((code = tDecodeI8(&decoder, &files[i].isMissing))) goto _err;

      tsdbInfo("vgId:%d, FileInfo fid:%d ftype:%d level:%d minVer:%" PRId64 " maxVer:%" PRId64 " cid:%" PRId64
               " size:%" PRId64 " isMissing:%d",
               vgId, files[i].fid, files[i].ftype, files[i].level, files[i].minVer, files[i].maxVer, files[i].cid,
               files[i].size, files[i].isMissing);

      if (files[i].isMissing) {
        char    dummy = 0;
        char    key[TSDB_SNAP_FILE_KEY_LEN];
        tsdbSnapFileKeyMake(files[i].fid, files[i].ftype, files[i].level, files[i].minVer, files[i].maxVer, key);

        if (taosHashPut(pHash, key, sizeof(key), &dummy, sizeof(dummy)) != 0) {
          code = terrno;
          goto _err;
        }
        // for STT files, also put into missingSttHash keyed by 5-tuple (fid, ftype, level, minVer, maxVer)
        if (files[i].ftype == TSDB_FTYPE_STT) {
          if (taosHashPut(pSttHash, key, sizeof(key), &dummy, sizeof(dummy)) != 0) {
            code = terrno;
            goto _err;
          }
        }
      }
    }
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);

  *ppHash = pHash;
  *ppSttHash = pSttHash;
  *ppFiles = (void*)files;
  *pFileCount = fileCount;
  return 0;

_err:
  if (pHash) taosHashCleanup(pHash);
  if (pSttHash) taosHashCleanup(pSttHash);
  taosMemoryFree(files);
  tDecoderClear(&decoder);
  return code;
}

int32_t tsdbExtractMissingFids(STsdb* pTsdb, SHashObj* missingFileHash, int32_t** ppFids, int32_t* pFidCount) {
  int32_t  code = 0;
  int32_t  fidCap = 0;
  int32_t  fidCount = 0;
  int32_t* fids = NULL;

  // extract unique fids from hash keys (key = 5-tuple binary: fid, ftype, level, minVer, maxVer)
  void* pIter = NULL;
  while ((pIter = taosHashIterate(missingFileHash, pIter)) != NULL) {
    size_t keyLen = 0;
    char*  pKey = taosHashGetKey(pIter, &keyLen);
    int32_t fid;
    memcpy(&fid, pKey, sizeof(fid));

    // check if fid already exists
    bool exists = false;
    for (int32_t i = 0; i < fidCount; ++i) {
      if (fids[i] == fid) {
        exists = true;
        break;
      }
    }
    if (exists) continue;

    if (fidCount >= fidCap) {
      int32_t  newCap = fidCap == 0 ? 16 : fidCap * 2;
      int32_t* tmp = taosMemoryRealloc(fids, newCap * sizeof(int32_t));
      if (tmp == NULL) {
        code = terrno;
        taosHashCancelIterate(missingFileHash, pIter);
        taosMemoryFree(fids);
        return code;
      }
      fids = tmp;
      fidCap = newCap;
    }
    fids[fidCount++] = fid;
  }

  // sort fids for binary search
  if (fidCount > 1) {
    for (int32_t i = 0; i < fidCount - 1; ++i) {
      for (int32_t j = i + 1; j < fidCount; ++j) {
        if (fids[i] > fids[j]) {
          int32_t tmp = fids[i];
          fids[i] = fids[j];
          fids[j] = tmp;
        }
      }
    }
  }

  *ppFids = fids;
  *pFidCount = fidCount;
  return 0;
}

int32_t tsdbDetermineFidSyncMode(STsdb* pTsdb, const void* pFileArr, int32_t fileCount, SHashObj** ppFidModeHash,
                                  int32_t** ppLeaderOnlyFids, int32_t* pLeaderOnlyFidCount) {
  int32_t                  code = 0;
  SHashObj*                pFidModeHash = NULL;
  SHashObj*                pLeaderKeyHash = NULL;
  SHashObj*                pFollowerKeyHash = NULL;
  SHashObj*                pFollowerFidSet = NULL;
  const STsdbSnapFileInfo* files = (const STsdbSnapFileInfo*)pFileArr;
  int32_t*                 leaderOnlyFids = NULL;
  int32_t                  leaderOnlyFidCount = 0;
  int32_t                  leaderOnlyFidCap = 0;

  if (fileCount <= 0 || files == NULL) {
    *ppFidModeHash = NULL;
    return 0;
  }

  pFidModeHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pFidModeHash == NULL) {
    return terrno;
  }

  // build leader file key hash: key=(fid,ftype,level,minVer,maxVer) -> (cid, size)
  pLeaderKeyHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pLeaderKeyHash == NULL) {
    code = terrno;
    goto _out;
  }

  typedef struct {
    int64_t cid;
    int64_t size;
  } SLeaderFileVal;

  (void)taosThreadMutexLock(&pTsdb->mutex);

  // populate leader key hash
  {
    STFileSet* fset;
    TARRAY2_FOREACH(pTsdb->pFS->fSetArr, fset) {
      for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
        if (fset->farr[ftype] != NULL) {
          char key[TSDB_SNAP_FILE_KEY_LEN];
          tsdbSnapFileKeyMake(fset->fid, ftype, 0, fset->farr[ftype]->f->minVer, fset->farr[ftype]->f->maxVer, key);
          SLeaderFileVal val = {.cid = fset->farr[ftype]->f->cid, .size = fset->farr[ftype]->f->size};
          tsdbInfo("vgId:%d, leader FileInfo fid:%d ftype:%d level:%d minVer:%" PRId64 " maxVer:%" PRId64
                   " cid:%" PRId64 " size:%" PRId64,
                   TD_VID(pTsdb->pVnode), fset->fid, ftype, 0, fset->farr[ftype]->f->minVer,
                   fset->farr[ftype]->f->maxVer, fset->farr[ftype]->f->cid, fset->farr[ftype]->f->size);
          if (taosHashPut(pLeaderKeyHash, key, TSDB_SNAP_FILE_KEY_LEN, &val, sizeof(val)) != 0) {
            code = terrno;
            goto _unlock;
          }
        }
      }
      SSttLvl* lvl;
      TARRAY2_FOREACH(fset->lvlArr, lvl) {
        STFileObj* fobj;
        TARRAY2_FOREACH(lvl->fobjArr, fobj) {
          char key[TSDB_SNAP_FILE_KEY_LEN];
          tsdbSnapFileKeyMake(fset->fid, TSDB_FTYPE_STT, lvl->level, fobj->f->minVer, fobj->f->maxVer, key);
          SLeaderFileVal val = {.cid = fobj->f->cid, .size = fobj->f->size};
          tsdbInfo("vgId:%d, leader FileInfo fid:%d ftype:%d level:%d minVer:%" PRId64 " maxVer:%" PRId64
                   " cid:%" PRId64 " size:%" PRId64,
                   TD_VID(pTsdb->pVnode), fset->fid, TSDB_FTYPE_STT, lvl->level, fobj->f->minVer, fobj->f->maxVer,
                   fobj->f->cid, fobj->f->size);
          if (taosHashPut(pLeaderKeyHash, key, TSDB_SNAP_FILE_KEY_LEN, &val, sizeof(val)) != 0) {
            code = terrno;
            goto _unlock;
          }
        }
      }
    }
  }

  // first pass: check each follower present file against leader
  for (int32_t i = 0; i < fileCount; ++i) {
    if (files[i].isMissing) continue;

    int32_t fid = files[i].fid;

    // skip if already FSET_LEVEL
    uint8_t* pExistMode = taosHashGet(pFidModeHash, &fid, sizeof(fid));
    if (pExistMode != NULL && *pExistMode == TSDB_SNAP_SYNC_FSET_LEVEL) {
      continue;
    }

    char key[TSDB_SNAP_FILE_KEY_LEN];
    tsdbSnapFileKeyMake(fid, files[i].ftype, files[i].level, files[i].minVer, files[i].maxVer, key);

    SLeaderFileVal* pLeaderVal = taosHashGet(pLeaderKeyHash, key, TSDB_SNAP_FILE_KEY_LEN);

    uint8_t mode = TSDB_SNAP_SYNC_FILE_LEVEL;
    if (pLeaderVal == NULL) {
      tsdbInfo("vgId:%d, snap leader no match key fid:%d ftype:%d level:%d minVer:%" PRId64 " maxVer:%" PRId64,
               TD_VID(pTsdb->pVnode), fid, files[i].ftype, files[i].level, files[i].minVer, files[i].maxVer);
      mode = TSDB_SNAP_SYNC_FSET_LEVEL;
    } else if (pLeaderVal->size != files[i].size) {
      tsdbInfo("vgId:%d, snap size mismatch fid:%d ftype:%d level:%d leader-size:%" PRId64 " follower-size:%" PRId64,
               TD_VID(pTsdb->pVnode), fid, files[i].ftype, files[i].level, pLeaderVal->size, files[i].size);
      mode = TSDB_SNAP_SYNC_FSET_LEVEL;
    } else if (llabs(pLeaderVal->cid - files[i].cid) > 10) {
      tsdbInfo("vgId:%d, snap cid diff>10 fid:%d ftype:%d level:%d leader-cid:%" PRId64 " follower-cid:%" PRId64,
               TD_VID(pTsdb->pVnode), fid, files[i].ftype, files[i].level, pLeaderVal->cid, files[i].cid);
      mode = TSDB_SNAP_SYNC_FSET_LEVEL;
    }

    if (taosHashPut(pFidModeHash, &fid, sizeof(fid), &mode, sizeof(mode)) != 0) {
      code = terrno;
      goto _unlock;
    }
    if (leaderOnlyFidCount >= leaderOnlyFidCap) {
      int32_t  newCap = leaderOnlyFidCap == 0 ? 16 : leaderOnlyFidCap * 2;
      int32_t* tmp = taosMemoryRealloc(leaderOnlyFids, newCap * sizeof(int32_t));
      if (tmp == NULL) {
        code = terrno;
        goto _unlock;
      }
      leaderOnlyFids = tmp;
      leaderOnlyFidCap = newCap;
    }
    leaderOnlyFids[leaderOnlyFidCount++] = fid;
  }

  // second pass: check leader files not known by follower (within reported fids)
  pFollowerKeyHash =
      taosHashInit(fileCount * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pFollowerKeyHash == NULL) {
    code = terrno;
    goto _unlock;
  }

  for (int32_t i = 0; i < fileCount; ++i) {
    char key[TSDB_SNAP_FILE_KEY_LEN];
    tsdbSnapFileKeyMake(files[i].fid, files[i].ftype, files[i].level, files[i].minVer, files[i].maxVer, key);
    char dummy = 0;
    if (taosHashPut(pFollowerKeyHash, key, TSDB_SNAP_FILE_KEY_LEN, &dummy, sizeof(dummy)) != 0) {
      code = terrno;
      goto _unlock;
    }
  }

  // collect fids that follower reported
  pFollowerFidSet = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pFollowerFidSet == NULL) {
    code = terrno;
    goto _unlock;
  }
  for (int32_t i = 0; i < fileCount; ++i) {
    char dummy = 0;
    (void)taosHashPut(pFollowerFidSet, &files[i].fid, sizeof(files[i].fid), &dummy, sizeof(dummy));
  }

  {
    STFileSet* fset;
    TARRAY2_FOREACH(pTsdb->pFS->fSetArr, fset) {
      int32_t fid = fset->fid;

      // skip if already FSET_LEVEL
      uint8_t* pExistMode = taosHashGet(pFidModeHash, &fid, sizeof(fid));
      if (pExistMode != NULL && *pExistMode == TSDB_SNAP_SYNC_FSET_LEVEL) continue;

      // if fid not reported by follower at all, mark FSET_LEVEL and collect as leader-only
      if (taosHashGet(pFollowerFidSet, &fid, sizeof(fid)) == NULL) {
        tsdbInfo("vgId:%d, snap leader-only fid:%d not reported by follower, mark FSET_LEVEL",
                 TD_VID(pTsdb->pVnode), fid);
        uint8_t mode = TSDB_SNAP_SYNC_FSET_LEVEL;
        if (taosHashPut(pFidModeHash, &fid, sizeof(fid), &mode, sizeof(mode)) != 0) {
          code = terrno;
          goto _unlock;
        }
        // collect leader-only fid
        if (leaderOnlyFidCount >= leaderOnlyFidCap) {
          int32_t  newCap = leaderOnlyFidCap == 0 ? 16 : leaderOnlyFidCap * 2;
          int32_t* tmp = taosMemoryRealloc(leaderOnlyFids, newCap * sizeof(int32_t));
          if (tmp == NULL) {
            code = terrno;
            goto _unlock;
          }
          leaderOnlyFids = tmp;
          leaderOnlyFidCap = newCap;
        }
        leaderOnlyFids[leaderOnlyFidCount++] = fid;
        continue;
      }

      for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
        if (fset->farr[ftype] != NULL) {
          char key[TSDB_SNAP_FILE_KEY_LEN];
          tsdbSnapFileKeyMake(fid, ftype, 0, fset->farr[ftype]->f->minVer, fset->farr[ftype]->f->maxVer, key);
          if (taosHashGet(pFollowerKeyHash, key, TSDB_SNAP_FILE_KEY_LEN) == NULL) {
            tsdbInfo("vgId:%d, snap follower missing leader file fid:%d ftype:%d minVer:%" PRId64 " maxVer:%" PRId64,
                     TD_VID(pTsdb->pVnode), fid, ftype, fset->farr[ftype]->f->minVer, fset->farr[ftype]->f->maxVer);
            uint8_t mode = TSDB_SNAP_SYNC_FSET_LEVEL;
            if (taosHashPut(pFidModeHash, &fid, sizeof(fid), &mode, sizeof(mode)) != 0) {
              code = terrno;
              goto _unlock;
            }
            if (leaderOnlyFidCount >= leaderOnlyFidCap) {
              int32_t  newCap = leaderOnlyFidCap == 0 ? 16 : leaderOnlyFidCap * 2;
              int32_t* tmp = taosMemoryRealloc(leaderOnlyFids, newCap * sizeof(int32_t));
              if (tmp == NULL) {
                code = terrno;
                goto _unlock;
              }
              leaderOnlyFids = tmp;
              leaderOnlyFidCap = newCap;
            }
            leaderOnlyFids[leaderOnlyFidCount++] = fid;
            goto _next_fset;
          }
        }
      }

      SSttLvl* lvl;
      TARRAY2_FOREACH(fset->lvlArr, lvl) {
        STFileObj* fobj;
        TARRAY2_FOREACH(lvl->fobjArr, fobj) {
          char key[TSDB_SNAP_FILE_KEY_LEN];
          tsdbSnapFileKeyMake(fid, TSDB_FTYPE_STT, lvl->level, fobj->f->minVer, fobj->f->maxVer, key);
          if (taosHashGet(pFollowerKeyHash, key, TSDB_SNAP_FILE_KEY_LEN) == NULL) {
            tsdbInfo("vgId:%d, snap follower missing leader stt fid:%d level:%d minVer:%" PRId64 " maxVer:%" PRId64,
                     TD_VID(pTsdb->pVnode), fid, lvl->level, fobj->f->minVer, fobj->f->maxVer);
            uint8_t mode = TSDB_SNAP_SYNC_FSET_LEVEL;
            if (taosHashPut(pFidModeHash, &fid, sizeof(fid), &mode, sizeof(mode)) != 0) {
              code = terrno;
              goto _unlock;
            }
            if (leaderOnlyFidCount >= leaderOnlyFidCap) {
              int32_t  newCap = leaderOnlyFidCap == 0 ? 16 : leaderOnlyFidCap * 2;
              int32_t* tmp = taosMemoryRealloc(leaderOnlyFids, newCap * sizeof(int32_t));
              if (tmp == NULL) {
                code = terrno;
                goto _unlock;
              }
              leaderOnlyFids = tmp;
              leaderOnlyFidCap = newCap;
            }
            leaderOnlyFids[leaderOnlyFidCount++] = fid;
            goto _next_fset;
          }
        }
      }
    _next_fset:;
    }
  }

_unlock:
  (void)taosThreadMutexUnlock(&pTsdb->mutex);

  // sort leader-only fids
  if (leaderOnlyFidCount > 1) {
    for (int32_t i = 0; i < leaderOnlyFidCount - 1; ++i) {
      for (int32_t j = i + 1; j < leaderOnlyFidCount; ++j) {
        if (leaderOnlyFids[i] > leaderOnlyFids[j]) {
          int32_t tmp = leaderOnlyFids[i];
          leaderOnlyFids[i] = leaderOnlyFids[j];
          leaderOnlyFids[j] = tmp;
        }
      }
    }
  }

_out:
  if (pFollowerKeyHash) taosHashCleanup(pFollowerKeyHash);
  if (pFollowerFidSet) taosHashCleanup(pFollowerFidSet);
  if (pLeaderKeyHash) taosHashCleanup(pLeaderKeyHash);

  if (code != 0) {
    taosHashCleanup(pFidModeHash);
    *ppFidModeHash = NULL;
  } else {
    tsdbInfo("vgId:%d, Fid mode count %d", TD_VID(pTsdb->pVnode), taosHashGetSize(pFidModeHash));
    if (leaderOnlyFidCount > 0) {
      tsdbInfo("vgId:%d, leader-only fid count %d", TD_VID(pTsdb->pVnode), leaderOnlyFidCount);
    }
    *ppFidModeHash = pFidModeHash;
  }
  if (code != 0) {
    taosMemoryFree(leaderOnlyFids);
    leaderOnlyFids = NULL;
    leaderOnlyFidCount = 0;
  }
  if (ppLeaderOnlyFids) *ppLeaderOnlyFids = leaderOnlyFids;
  if (pLeaderOnlyFidCount) *pLeaderOnlyFidCount = leaderOnlyFidCount;
  return code;
}

static int32_t tsdbCollectAllFileInfo(SVnode* pVnode, STsdbSnapFileInfo** ppFiles, int32_t* pFileCount) {
  int32_t            code = 0;
  STsdbSnapFileInfo* files = NULL;
  int32_t            fileCount = 0;
  int32_t            fileCap = 0;
  STsdb*             pTsdb = pVnode->pTsdb;

  *ppFiles = NULL;
  *pFileCount = 0;

  (void)taosThreadMutexLock(&pTsdb->mutex);

  STFileSet* fset;
  TARRAY2_FOREACH(pTsdb->pFS->fSetArr, fset) {
    // collect farr entries (HEAD, DATA, SMA, TOMB)
    for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
      if (fset->farr[ftype] != NULL) {
        if (fileCount >= fileCap) {
          int32_t            newCap = fileCap == 0 ? 16 : fileCap * 2;
          STsdbSnapFileInfo* tmp = taosMemoryRealloc(files, newCap * sizeof(STsdbSnapFileInfo));
          if (tmp == NULL) {
            code = terrno;
            goto _unlock;
          }
          files = tmp;
          fileCap = newCap;
        }
        files[fileCount].fid = fset->fid;
        files[fileCount].ftype = ftype;
        files[fileCount].level = 0;
        files[fileCount].minVer = fset->farr[ftype]->f->minVer;
        files[fileCount].maxVer = fset->farr[ftype]->f->maxVer;
        files[fileCount].cid = fset->farr[ftype]->f->cid;
        files[fileCount].size = fset->farr[ftype]->f->size;
        files[fileCount].isMissing = !taosCheckExistFile(fset->farr[ftype]->fname) ? 1 : 0;
        tsdbInfo("vgId:%d, collect all file info, fid:%d ftype:%d level:%d minVer:%" PRId64 " maxVer:%" PRId64
                 " cid:%" PRId64 " size:%" PRId64 " isMissing:%d",
                 TD_VID(pVnode), files[fileCount].fid, files[fileCount].ftype, files[fileCount].level,
                 files[fileCount].minVer, files[fileCount].maxVer, files[fileCount].cid, files[fileCount].size,
                 files[fileCount].isMissing);
        fileCount++;
      }
    }

    // collect STT files in lvlArr
    SSttLvl* lvl;
    TARRAY2_FOREACH(fset->lvlArr, lvl) {
      STFileObj* fobj;
      TARRAY2_FOREACH(lvl->fobjArr, fobj) {
        if (fileCount >= fileCap) {
          int32_t            newCap = fileCap == 0 ? 16 : fileCap * 2;
          STsdbSnapFileInfo* tmp = taosMemoryRealloc(files, newCap * sizeof(STsdbSnapFileInfo));
          if (tmp == NULL) {
            code = terrno;
            goto _unlock;
          }
          files = tmp;
          fileCap = newCap;
        }
        files[fileCount].fid = fset->fid;
        files[fileCount].ftype = TSDB_FTYPE_STT;
        files[fileCount].level = lvl->level;
        files[fileCount].minVer = fobj->f->minVer;
        files[fileCount].maxVer = fobj->f->maxVer;
        files[fileCount].cid = fobj->f->cid;
        files[fileCount].size = fobj->f->size;
        files[fileCount].isMissing = !taosCheckExistFile(fobj->fname) ? 1 : 0;
        tsdbInfo("vgId:%d, collect all file info, fid:%d ftype:%d level:%d minVer:%" PRId64 " maxVer:%" PRId64
                 " cid:%" PRId64 " size:%" PRId64 " isMissing:%d",
                 TD_VID(pVnode), files[fileCount].fid, files[fileCount].ftype, files[fileCount].level,
                 files[fileCount].minVer, files[fileCount].maxVer, files[fileCount].cid, files[fileCount].size,
                 files[fileCount].isMissing);
        fileCount++;
      }
    }
  }

_unlock:
  (void)taosThreadMutexUnlock(&pTsdb->mutex);

  if (code != 0) {
    taosMemoryFree(files);
    return code;
  }

  *ppFiles = files;
  *pFileCount = fileCount;
  return 0;
}

static int32_t tsdbMissingFilesEstSize(int32_t fileCount) {
  return sizeof(SSyncTLV) + tMissingFileListDataLenCalc(fileCount);
}

static int32_t tsdbMissingFilesSerialize(const STsdbSnapFileInfo* files, int32_t fileCount, void* buf, int32_t bufLen) {
  SSyncTLV* pSubHead = buf;
  int32_t   tlen = tSerializeMissingFileList(pSubHead->val, bufLen - sizeof(*pSubHead), files, fileCount);
  if (tlen < 0) return tlen;
  pSubHead->typ = SNAP_DATA_MISSING_FIDS;
  pSubHead->len = tlen;
  return sizeof(*pSubHead) + tlen;
}

static int32_t tsdbRepOptsEstSize(STsdbRepOpts* pOpts) {
  int32_t dataLen = 0;
  dataLen += sizeof(SSyncTLV);
  dataLen += tTsdbRepOptsDataLenCalc(pOpts);
  return dataLen;
}

static int32_t tsdbRepOptsSerialize(STsdbRepOpts* pOpts, void* buf, int32_t bufLen) {
  SSyncTLV* pSubHead = buf;
  int32_t   offset = 0;
  int32_t   tlen = 0;
  if ((tlen = tSerializeTsdbRepOpts(pSubHead->val, bufLen, pOpts)) < 0) {
    return tlen;
  }
  pSubHead->typ = SNAP_DATA_RAW;
  pSubHead->len = tlen;
  offset += sizeof(*pSubHead) + tlen;
  return offset;
}

// snap info
static int32_t tsdbSnapPrepDealWithSnapInfo(SVnode* pVnode, SSnapshot* pSnap, STsdbRepOpts* pInfo) {
  if (!pSnap->data) {
    return 0;
  }
  int32_t code = 0;

  SSyncTLV* pHead = (void*)pSnap->data;
  int32_t   offset = 0;

  while (offset + sizeof(*pHead) < pHead->len) {
    SSyncTLV* pField = (void*)(pHead->val + offset);
    offset += sizeof(*pField) + pField->len;
    void*   buf = pField->val;
    int32_t bufLen = pField->len;

    switch (pField->typ) {
      case SNAP_DATA_TSDB:
      case SNAP_DATA_RSMA1:
      case SNAP_DATA_RSMA2: {
      } break;
      case SNAP_DATA_RAW: {
        code = tDeserializeTsdbRepOpts(buf, bufLen, pInfo);
        if (code < 0) {
          tsdbError("vgId:%d, failed to deserialize tsdb rep opts since %s", TD_VID(pVnode), terrstr());
          return code;
        }
      } break;
      default:
        tsdbWarn("vgId:%d, unknown subfield type in snap info, skipping. typ:%d", TD_VID(pVnode), pField->typ);
        break;
    }
  }

  return code;
}

int32_t tsdbSnapPrepDescription(SVnode* pVnode, SSnapshot* pSnap) {
  STsdbPartitionInfo  partitionInfo = {0};
  int                 code = 0;
  STsdbPartitionInfo* pInfo = &partitionInfo;

  code = tsdbPartitionInfoInit(pVnode, pInfo);
  if (code) {
    goto _out;
  }

  // deal with snap info for reply
  STsdbRepOpts       opts = {.format = TSDB_SNAP_REP_FMT_RAW};
  STsdbSnapFileInfo* missingFiles = NULL;
  int32_t            missingFileCount = 0;
  if (pSnap->type == TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
    STsdbRepOpts leaderOpts = {0};
    if ((code = tsdbSnapPrepDealWithSnapInfo(pVnode, pSnap, &leaderOpts)) < 0) {
      tsdbError("vgId:%d, failed to deal with snap info for reply since %s", TD_VID(pVnode), terrstr());
      goto _out;
    }
    opts.format = TMIN(opts.format, leaderOpts.format);

    int32_t detectCode = tsdbCollectAllFileInfo(pVnode, &missingFiles, &missingFileCount);
    if (detectCode != 0) {
      tsdbWarn("vgId:%d, failed to collect file info since %s, continuing without", TD_VID(pVnode),
               tstrerror(detectCode));
      missingFileCount = 0;
    } else if (missingFileCount > 0) {
      tsdbInfo("vgId:%d, collected %d file info entries for snapshot", TD_VID(pVnode), missingFileCount);
    }
  }

  // info data realloc
  const int32_t headLen = sizeof(SSyncTLV);
  int32_t       bufLen = headLen;
  bufLen += tsdbPartitionInfoEstSize(pInfo);
  bufLen += tsdbRepOptsEstSize(&opts);
  if (missingFileCount > 0) {
    bufLen += tsdbMissingFilesEstSize(missingFileCount);
  }
  if ((code = syncSnapInfoDataRealloc(pSnap, bufLen)) != 0) {
    tsdbError("vgId:%d, failed to realloc memory for data of snap info. bytes:%d", TD_VID(pVnode), bufLen);
    goto _out;
  }

  // serialization
  char*   buf = (void*)pSnap->data;
  int32_t offset = headLen;
  int32_t tlen = 0;

  if ((tlen = tsdbPartitionInfoSerialize(pInfo, (uint8_t*)(buf + offset), bufLen - offset)) < 0) {
    code = tlen;
    tsdbError("vgId:%d, failed to serialize tsdb partition info since %s", TD_VID(pVnode), terrstr());
    goto _out;
  }
  offset += tlen;

  if ((tlen = tsdbRepOptsSerialize(&opts, buf + offset, bufLen - offset)) < 0) {
    code = tlen;
    tsdbError("vgId:%d, failed to serialize tsdb rep opts since %s", TD_VID(pVnode), terrstr());
    goto _out;
  }
  offset += tlen;

  if (missingFileCount > 0) {
    if ((tlen = tsdbMissingFilesSerialize(missingFiles, missingFileCount, buf + offset, bufLen - offset)) < 0) {
      code = tlen;
      tsdbError("vgId:%d, failed to serialize missing files since %s", TD_VID(pVnode), terrstr());
      goto _out;
    }
    offset += tlen;
  }

  // set header of info data
  SSyncTLV* pHead = pSnap->data;
  pHead->typ = pSnap->type;
  pHead->len = offset - headLen;

  tsdbInfo("vgId:%d, tsdb snap info prepared. type:%s, val length:%d", TD_VID(pVnode), TMSG_INFO(pHead->typ),
           pHead->len);

_out:
  taosMemoryFree(missingFiles);
  tsdbPartitionInfoClear(pInfo);
  return code;
}

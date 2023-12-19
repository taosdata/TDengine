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

#define TSDB_SNAP_MSG_VER 1

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
    terrno = TSDB_CODE_OUT_OF_MEMORY;
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
    goto _err;
  }

  p->fid = fset->fid;

  int32_t code = 0;
  int32_t typ = 0;
  int32_t corrupt = false;
  int32_t count = 0;
  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] == NULL) continue;
    typ = tsdbFTypeToFRangeType(ftype);
    ASSERT(typ < TSDB_FSET_RANGE_TYP_MAX);
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
    ASSERT(code == 0);
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
      ASSERT(code == 0);
    }
  }
  if (corrupt && count == 0) {
    SVersionRange vr = {.minVer = VERSION_MIN, .maxVer = fset->maxVerValid};
    code = TARRAY2_SORT_INSERT(&p->verRanges[typ], vr, tVersionRangeCmprFn);
    ASSERT(code == 0);
  }
  ppSP[0] = p;
  return 0;

_err:
  tsdbFSetPartitionClear(&p);
  return -1;
}

// fset partition list
STsdbFSetPartList* tsdbFSetPartListCreate() {
  STsdbFSetPartList* pList = taosMemoryCalloc(1, sizeof(STsdbFSetPartList));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
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
  TFileSetRangeArray* pDiff = taosMemoryCalloc(1, sizeof(TFileSetRangeArray));
  if (pDiff == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  TARRAY2_INIT(pDiff);

  STsdbFSetPartition* part;
  TARRAY2_FOREACH(pList, part) {
    STFileSetRange* r = taosMemoryCalloc(1, sizeof(STFileSetRange));
    if (r == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
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
    int32_t code = TARRAY2_SORT_INSERT(pDiff, r, tsdbTFileSetRangeCmprFn);
    ASSERT(code == 0);
  }
  ppRanges[0] = pDiff;

  tsdbInfo("pDiff size:%d", TARRAY2_SIZE(pDiff));
  return 0;

_err:
  if (pDiff) {
    tsdbTFileSetRangeArrayDestroy(&pDiff);
  }
  return -1;
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

int32_t tSerializeTsdbFSetPartList(void* buf, int32_t bufLen, STsdbFSetPartList* pList) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  int8_t  reserved8 = 0;
  int16_t reserved16 = 0;
  int64_t reserved64 = 0;

  int8_t  msgVer = TSDB_SNAP_MSG_VER;
  int32_t len = TARRAY2_SIZE(pList);

  if (tStartEncode(&encoder) < 0) goto _err;
  if (tEncodeI8(&encoder, msgVer) < 0) goto _err;
  if (tEncodeI32(&encoder, len) < 0) goto _err;

  for (int32_t u = 0; u < len; u++) {
    STsdbFSetPartition* p = TARRAY2_GET(pList, u);
    if (tEncodeI64(&encoder, p->fid) < 0) goto _err;
    if (tEncodeI8(&encoder, p->stat) < 0) goto _err;
    if (tEncodeI8(&encoder, reserved8) < 0) goto _err;
    if (tEncodeI16(&encoder, reserved16) < 0) goto _err;

    int32_t typMax = TSDB_FSET_RANGE_TYP_MAX;
    if (tEncodeI32(&encoder, typMax) < 0) goto _err;

    for (int32_t i = 0; i < typMax; i++) {
      SVerRangeList* iList = &p->verRanges[i];
      int32_t        iLen = TARRAY2_SIZE(iList);

      if (tEncodeI32(&encoder, iLen) < 0) goto _err;
      for (int32_t j = 0; j < iLen; j++) {
        SVersionRange r = TARRAY2_GET(iList, j);
        if (tEncodeI64(&encoder, r.minVer) < 0) goto _err;
        if (tEncodeI64(&encoder, r.maxVer) < 0) goto _err;
        if (tEncodeI64(&encoder, reserved64) < 0) goto _err;
      }
    }
  }

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;

_err:
  tEncoderClear(&encoder);
  return -1;
}

int32_t tDeserializeTsdbFSetPartList(void* buf, int32_t bufLen, STsdbFSetPartList* pList) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  int8_t  reserved8 = 0;
  int16_t reserved16 = 0;
  int64_t reserved64 = 0;

  STsdbFSetPartition* p = NULL;

  int8_t  msgVer = 0;
  int32_t len = 0;
  if (tStartDecode(&decoder) < 0) goto _err;
  if (tDecodeI8(&decoder, &msgVer) < 0) goto _err;
  if (msgVer != TSDB_SNAP_MSG_VER) goto _err;
  if (tDecodeI32(&decoder, &len) < 0) goto _err;

  for (int32_t u = 0; u < len; u++) {
    p = tsdbFSetPartitionCreate();
    if (p == NULL) goto _err;
    if (tDecodeI64(&decoder, &p->fid) < 0) goto _err;
    if (tDecodeI8(&decoder, &p->stat) < 0) goto _err;
    if (tDecodeI8(&decoder, &reserved8) < 0) goto _err;
    if (tDecodeI16(&decoder, &reserved16) < 0) goto _err;

    int32_t typMax = 0;
    if (tDecodeI32(&decoder, &typMax) < 0) goto _err;

    for (int32_t i = 0; i < typMax; i++) {
      SVerRangeList* iList = &p->verRanges[i];
      int32_t        iLen = 0;
      if (tDecodeI32(&decoder, &iLen) < 0) goto _err;
      for (int32_t j = 0; j < iLen; j++) {
        SVersionRange r = {0};
        if (tDecodeI64(&decoder, &r.minVer) < 0) goto _err;
        if (tDecodeI64(&decoder, &r.maxVer) < 0) goto _err;
        if (tDecodeI64(&decoder, &reserved64) < 0) goto _err;
        TARRAY2_APPEND(iList, r);
      }
    }
    TARRAY2_APPEND(pList, p);
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
  return -1;
}

// fs state
static STsdbFSetPartList* tsdbSnapGetFSetPartList(STFileSystem* fs) {
  STsdbFSetPartList* pList = tsdbFSetPartListCreate();
  if (pList == NULL) {
    return NULL;
  }

  int32_t code = 0;
  taosThreadMutexLock(&fs->tsdb->mutex);
  STFileSet* fset;
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    STsdbFSetPartition* pItem = NULL;
    if (tsdbTFileSetToFSetPartition(fset, &pItem) < 0) {
      code = -1;
      break;
    }
    ASSERT(pItem != NULL);
    code = TARRAY2_SORT_INSERT(pList, pItem, tsdbFSetPartCmprFn);
    ASSERT(code == 0);
  }
  taosThreadMutexUnlock(&fs->tsdb->mutex);

  if (code) {
    TARRAY2_DESTROY(pList, tsdbFSetPartitionClear);
    taosMemoryFree(pList);
    pList = NULL;
  }
  return pList;
}

ETsdbFsState tsdbSnapGetFsState(SVnode* pVnode) {
  if (!VND_IS_RSMA(pVnode)) {
    return pVnode->pTsdb->pFS->fsstate;
  }
  for (int32_t lvl = 0; lvl < TSDB_RETENTION_MAX; ++lvl) {
    STsdb* pTsdb = SMA_RSMA_GET_TSDB(pVnode, lvl);
    if (pTsdb && pTsdb->pFS->fsstate != TSDB_FS_STATE_NORMAL) {
      return TSDB_FS_STATE_INCOMPLETE;
    }
  }
  return TSDB_FS_STATE_NORMAL;
}

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
  pInfo->tsdbMaxCnt = (!VND_IS_RSMA(pVnode) ? 1 : TSDB_RETENTION_MAX);

  ASSERT(sizeof(pInfo->subTyps) == sizeof(subTyps));
  memcpy(pInfo->subTyps, (char*)subTyps, sizeof(subTyps));

  // fset partition list
  memset(pInfo->pLists, 0, sizeof(pInfo->pLists[0]) * TSDB_RETENTION_MAX);
  for (int32_t j = 0; j < pInfo->tsdbMaxCnt; ++j) {
    STsdb* pTsdb = SMA_RSMA_GET_TSDB(pVnode, j);
    pInfo->pLists[j] = tsdbSnapGetFSetPartList(pTsdb->pFS);
    if (pInfo->pLists[j] == NULL) return -1;
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
    ASSERT(pSubHead->val == (char*)buf + valOffset);
    if ((tlen = tSerializeTsdbFSetPartList(pSubHead->val, bufLen - valOffset, pInfo->pLists[j])) < 0) {
      tsdbError("vgId:%d, failed to serialize fset partition list of tsdb %d since %s", pInfo->vgId, j, terrstr());
      return -1;
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
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  int64_t reserved64 = 0;
  int8_t  msgVer = TSDB_SNAP_MSG_VER;

  if (tStartEncode(&encoder) < 0) goto _err;
  if (tEncodeI8(&encoder, msgVer) < 0) goto _err;
  int16_t format = pOpts->format;
  if (tEncodeI16(&encoder, format) < 0) goto _err;
  if (tEncodeI64(&encoder, reserved64) < 0) goto _err;

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;

_err:
  tEncoderClear(&encoder);
  return -1;
}

int32_t tDeserializeTsdbRepOpts(void* buf, int32_t bufLen, STsdbRepOpts* pOpts) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  int64_t reserved64 = 0;
  int8_t  msgVer = 0;

  if (tStartDecode(&decoder) < 0) goto _err;
  if (tDecodeI8(&decoder, &msgVer) < 0) goto _err;
  if (msgVer != TSDB_SNAP_MSG_VER) goto _err;
  int16_t format = 0;
  if (tDecodeI16(&decoder, &format) < 0) goto _err;
  pOpts->format = format;
  if (tDecodeI64(&decoder, &reserved64) < 0) goto _err;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;

_err:
  tDecoderClear(&decoder);
  return -1;
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
    return -1;
  }
  pSubHead->typ = SNAP_DATA_RAW;
  pSubHead->len = tlen;
  offset += sizeof(*pSubHead) + tlen;
  return offset;
}

// snap info
static int32_t tsdbSnapPrepDealWithSnapInfo(SVnode* pVnode, SSnapshot* pSnap, STsdbRepOpts* pInfo) {
  if (!pSnap->data) return 0;
  int32_t code = -1;

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
        if (tDeserializeTsdbRepOpts(buf, bufLen, pInfo) < 0) {
          terrno = TSDB_CODE_INVALID_DATA_FMT;
          tsdbError("vgId:%d, failed to deserialize tsdb rep opts since %s", TD_VID(pVnode), terrstr());
          goto _out;
        }
      } break;
      default:
        tsdbError("vgId:%d, unexpected subfield type of snap info. typ:%d", TD_VID(pVnode), pField->typ);
        goto _out;
    }
  }

  code = 0;
_out:
  return code;
}

int32_t tsdbSnapPrepDescription(SVnode* pVnode, SSnapshot* pSnap) {
  ASSERT(pSnap->type == TDMT_SYNC_PREP_SNAPSHOT || pSnap->type == TDMT_SYNC_PREP_SNAPSHOT_REPLY);
  STsdbPartitionInfo  partitionInfo = {0};
  int                 code = -1;
  STsdbPartitionInfo* pInfo = &partitionInfo;

  if (tsdbPartitionInfoInit(pVnode, pInfo) != 0) {
    goto _out;
  }

  // deal with snap info for reply
  STsdbRepOpts opts = {.format = TSDB_SNAP_REP_FMT_RAW};
  if (pSnap->type == TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
    STsdbRepOpts leaderOpts = {0};
    if (tsdbSnapPrepDealWithSnapInfo(pVnode, pSnap, &leaderOpts) < 0) {
      tsdbError("vgId:%d, failed to deal with snap info for reply since %s", TD_VID(pVnode), terrstr());
      goto _out;
    }
    opts.format = TMIN(opts.format, leaderOpts.format);
  }

  // info data realloc
  const int32_t headLen = sizeof(SSyncTLV);
  int32_t       bufLen = headLen;
  bufLen += tsdbPartitionInfoEstSize(pInfo);
  bufLen += tsdbRepOptsEstSize(&opts);
  if (syncSnapInfoDataRealloc(pSnap, bufLen) != 0) {
    tsdbError("vgId:%d, failed to realloc memory for data of snap info. bytes:%d", TD_VID(pVnode), bufLen);
    goto _out;
  }

  // serialization
  char*   buf = (void*)pSnap->data;
  int32_t offset = headLen;
  int32_t tlen = 0;

  if ((tlen = tsdbPartitionInfoSerialize(pInfo, buf + offset, bufLen - offset)) < 0) {
    tsdbError("vgId:%d, failed to serialize tsdb partition info since %s", TD_VID(pVnode), terrstr());
    goto _out;
  }
  offset += tlen;
  ASSERT(offset <= bufLen);

  if ((tlen = tsdbRepOptsSerialize(&opts, buf + offset, bufLen - offset)) < 0) {
    tsdbError("vgId:%d, failed to serialize tsdb rep opts since %s", TD_VID(pVnode), terrstr());
    goto _out;
  }
  offset += tlen;
  ASSERT(offset <= bufLen);

  // set header of info data
  SSyncTLV* pHead = pSnap->data;
  pHead->typ = pSnap->type;
  pHead->len = offset - headLen;

  tsdbInfo("vgId:%d, tsdb snap info prepared. type:%s, val length:%d", TD_VID(pVnode), TMSG_INFO(pHead->typ),
           pHead->len);
  code = 0;
_out:
  tsdbPartitionInfoClear(pInfo);
  return code;
}

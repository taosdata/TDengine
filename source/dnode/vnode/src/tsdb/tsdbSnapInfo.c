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

static int32_t tsdbFileSetRangeCmprFn(STFileSetRange* x, STFileSetRange* y) {
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
  for (int32_t i = 0; i < TSDB_SNAP_RANGE_TYP_MAX; i++) {
    TARRAY2_INIT(&pSP->verRanges[i]);
  }
  return pSP;
}

void tsdbFSetPartitionClear(STsdbFSetPartition** ppSP) {
  if (ppSP == NULL || ppSP[0] == NULL) {
    return;
  }
  for (int32_t i = 0; i < TSDB_SNAP_RANGE_TYP_MAX; i++) {
    TARRAY2_DESTROY(&ppSP[0]->verRanges[i], NULL);
  }
  taosMemoryFree(ppSP[0]);
  ppSP[0] = NULL;
}

static int32_t tsdbFTypeToSRangeTyp(tsdb_ftype_t ftype) {
  switch (ftype) {
    case TSDB_FTYPE_HEAD:
      return TSDB_SNAP_RANGE_TYP_HEAD;
    case TSDB_FTYPE_DATA:
      return TSDB_SNAP_RANGE_TYP_DATA;
    case TSDB_FTYPE_SMA:
      return TSDB_SNAP_RANGE_TYP_SMA;
    case TSDB_FTYPE_TOMB:
      return TSDB_SNAP_RANGE_TYP_TOMB;
    case TSDB_FTYPE_STT:
      return TSDB_SNAP_RANGE_TYP_STT;
  }
  return TSDB_SNAP_RANGE_TYP_MAX;
}

static int32_t tsdbTFileSetToSnapPart(STFileSet* fset, STsdbFSetPartition** ppSP) {
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
    typ = tsdbFTypeToSRangeTyp(ftype);
    ASSERT(typ < TSDB_SNAP_RANGE_TYP_MAX);
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

  typ = TSDB_SNAP_RANGE_TYP_STT;
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

static STsdbFSetPartList* tsdbGetSnapPartList(STFileSystem* fs) {
  STsdbFSetPartList* pList = tsdbFSetPartListCreate();
  if (pList == NULL) {
    return NULL;
  }

  int32_t code = 0;
  taosThreadMutexLock(&fs->tsdb->mutex);
  STFileSet* fset;
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    STsdbFSetPartition* pItem = NULL;
    if (tsdbTFileSetToSnapPart(fset, &pItem) < 0) {
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
    int32_t             typMax = TSDB_SNAP_RANGE_TYP_MAX;
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

  int8_t  msgVer = 1;
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

    int32_t typMax = TSDB_SNAP_RANGE_TYP_MAX;
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
    int32_t typMax = TSDB_SNAP_RANGE_TYP_MAX;
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
    int32_t code = TARRAY2_SORT_INSERT(pDiff, r, tsdbFileSetRangeCmprFn);
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

void tsdbFSetPartListDestroy(STsdbFSetPartList** ppList) {
  if (ppList == NULL || ppList[0] == NULL) return;

  TARRAY2_DESTROY(ppList[0], tsdbFSetPartitionClear);
  taosMemoryFree(ppList[0]);
  ppList[0] = NULL;
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

int32_t tsdbSnapGetDetails(SVnode* pVnode, SSnapshot* pSnap) {
  int                code = -1;
  int32_t            tsdbMaxCnt = (!VND_IS_RSMA(pVnode) ? 1 : TSDB_RETENTION_MAX);
  int32_t            subTyps[TSDB_RETENTION_MAX] = {SNAP_DATA_TSDB, SNAP_DATA_RSMA1, SNAP_DATA_RSMA2};
  STsdbFSetPartList* pLists[TSDB_RETENTION_MAX] = {0};

  // get part list
  for (int32_t j = 0; j < tsdbMaxCnt; ++j) {
    STsdb* pTsdb = SMA_RSMA_GET_TSDB(pVnode, j);
    pLists[j] = tsdbGetSnapPartList(pTsdb->pFS);
    if (pLists[j] == NULL) goto _out;
  }

  // estimate bufLen and prepare
  int32_t bufLen = sizeof(SSyncTLV);  // typ: TDMT_SYNC_PREP_SNAPSHOT or TDMT_SYNC_PREP_SNAPSOT_REPLY
  for (int32_t j = 0; j < tsdbMaxCnt; ++j) {
    bufLen += sizeof(SSyncTLV);  // subTyps[j]
    bufLen += tTsdbFSetPartListDataLenCalc(pLists[j]);
  }

  tsdbInfo("vgId:%d, allocate %d bytes for data of snapshot info.", TD_VID(pVnode), bufLen);

  void* data = taosMemoryRealloc(pSnap->data, bufLen);
  if (data == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tsdbError("vgId:%d, failed to realloc memory for data of snapshot info. bytes:%d", TD_VID(pVnode), bufLen);
    goto _out;
  }
  pSnap->data = data;

  // header
  SSyncTLV* head = data;
  head->len = 0;
  head->typ = pSnap->type;
  int32_t offset = sizeof(SSyncTLV);
  int32_t tlen = 0;

  // fill snapshot info
  for (int32_t j = 0; j < tsdbMaxCnt; ++j) {
    //  subHead
    SSyncTLV* subHead = (void*)((char*)data + offset);
    subHead->typ = subTyps[j];
    ASSERT(subHead->val == (char*)data + offset + sizeof(SSyncTLV));

    if ((tlen = tSerializeTsdbFSetPartList(subHead->val, bufLen - offset - sizeof(SSyncTLV), pLists[j])) < 0) {
      tsdbError("vgId:%d, failed to serialize snap partition list of tsdb %d since %s", TD_VID(pVnode), j, terrstr());
      goto _out;
    }
    subHead->len = tlen;
    offset += sizeof(SSyncTLV) + tlen;
  }

  // total length of subfields
  head->len = offset - sizeof(SSyncTLV);
  ASSERT(offset <= bufLen);
  code = 0;

_out:
  for (int32_t j = 0; j < tsdbMaxCnt; ++j) {
    if (pLists[j] == NULL) continue;
    tsdbFSetPartListDestroy(&pLists[j]);
  }

  return code;
}


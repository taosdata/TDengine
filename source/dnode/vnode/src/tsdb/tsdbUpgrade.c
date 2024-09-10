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

#include "tsdbUpgrade.h"

// old
#include "tsdb.h"
// extern void    tsdbGetCurrentFName(STsdb *pTsdb, char *current, char *current_t);

// new
#include "tsdbDataFileRW.h"
#include "tsdbFS2.h"
#include "tsdbSttFileRW.h"

static int32_t tsdbUpgradeHead(STsdb *tsdb, SDFileSet *pDFileSet, SDataFReader *reader, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  // init
  struct {
    // config
    int32_t maxRow;
    int8_t  cmprAlg;
    int32_t szPage;
    SBuffer buffers[10];
    int32_t encryptAlgorithm;
    char   *encryptKey;
    // reader
    SArray    *aBlockIdx;
    SMapData   mDataBlk[1];
    SBlockData blockData[1];
    // writer
    STsdbFD      *fd;
    SBrinBlock    brinBlock[1];
    TBrinBlkArray brinBlkArray[1];
    SHeadFooter   footer[1];
  } ctx[1] = {{
      .maxRow = tsdb->pVnode->config.tsdbCfg.maxRows,
      .cmprAlg = tsdb->pVnode->config.tsdbCfg.compression,
      .szPage = tsdb->pVnode->config.tsdbPageSize,
      .encryptAlgorithm = tsdb->pVnode->config.tsdbCfg.encryptAlgorithm,
      .encryptKey = tsdb->pVnode->config.tsdbCfg.encryptKey,
  }};

  // read SBlockIdx array
  if ((ctx->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx))) == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  TAOS_CHECK_GOTO(tsdbReadBlockIdx(reader, ctx->aBlockIdx), &lino, _exit);

  if (taosArrayGetSize(ctx->aBlockIdx) > 0) {
    // init/open file fd
    STFile file = {
        .type = TSDB_FTYPE_HEAD,
        .did = pDFileSet->diskId,
        .fid = fset->fid,
        .cid = pDFileSet->pHeadF->commitID,
        .size = pDFileSet->pHeadF->size,
        .minVer = VERSION_MAX,
        .maxVer = VERSION_MIN,
    };

    TAOS_CHECK_GOTO(tsdbTFileObjInit(tsdb, &file, &fset->farr[TSDB_FTYPE_HEAD]), &lino, _exit);

    // open fd
    char fname[TSDB_FILENAME_LEN];
    tsdbTFileName(tsdb, &file, fname);

    TAOS_CHECK_GOTO(tsdbOpenFile(fname, tsdb, TD_FILE_READ | TD_FILE_WRITE, &ctx->fd, 0), &lino, _exit);

    // convert
    for (int32_t iBlockIdx = 0; iBlockIdx < taosArrayGetSize(ctx->aBlockIdx); ++iBlockIdx) {
      SBlockIdx *pBlockIdx = taosArrayGet(ctx->aBlockIdx, iBlockIdx);

      TAOS_CHECK_GOTO(tsdbReadDataBlk(reader, pBlockIdx, ctx->mDataBlk), &lino, _exit);

      for (int32_t iDataBlk = 0; iDataBlk < ctx->mDataBlk->nItem; ++iDataBlk) {
        SDataBlk dataBlk[1];
        tMapDataGetItemByIdx(ctx->mDataBlk, iDataBlk, dataBlk, tGetDataBlk);

        SBrinRecord record = {
            .suid = pBlockIdx->suid,
            .uid = pBlockIdx->uid,
            .firstKey =
                (STsdbRowKey){
                    .key =
                        (SRowKey){
                            .ts = dataBlk->minKey.ts,
                            .numOfPKs = 0,
                        },
                    .version = dataBlk->minKey.version,
                },
            .lastKey =
                (STsdbRowKey){
                    .key =
                        (SRowKey){
                            .ts = dataBlk->maxKey.ts,
                            .numOfPKs = 0,
                        },
                    .version = dataBlk->maxKey.version,
                },
            .minVer = dataBlk->minVer,
            .maxVer = dataBlk->maxVer,
            .blockOffset = dataBlk->aSubBlock->offset,
            .smaOffset = dataBlk->smaInfo.offset,
            .blockSize = dataBlk->aSubBlock->szBlock,
            .blockKeySize = dataBlk->aSubBlock->szKey,
            .smaSize = dataBlk->smaInfo.size,
            .numRow = dataBlk->nRow,
            .count = dataBlk->nRow,
        };

        if (dataBlk->hasDup) {
          record.count = 0;
        }

        TAOS_CHECK_GOTO(tBrinBlockPut(ctx->brinBlock, &record), &lino, _exit);

        if (ctx->brinBlock->numOfRecords >= ctx->maxRow) {
          SVersionRange range = {.minVer = VERSION_MAX, .maxVer = VERSION_MIN};
          code =
              tsdbFileWriteBrinBlock(ctx->fd, ctx->brinBlock, ctx->cmprAlg, &fset->farr[TSDB_FTYPE_HEAD]->f->size,
                                     ctx->brinBlkArray, ctx->buffers, &range, ctx->encryptAlgorithm, ctx->encryptKey);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }

    if (ctx->brinBlock->numOfRecords > 0) {
      SVersionRange range = {.minVer = VERSION_MAX, .maxVer = VERSION_MIN};
      TAOS_CHECK_GOTO(
          tsdbFileWriteBrinBlock(ctx->fd, ctx->brinBlock, ctx->cmprAlg, &fset->farr[TSDB_FTYPE_HEAD]->f->size,
                                 ctx->brinBlkArray, ctx->buffers, &range, ctx->encryptAlgorithm, ctx->encryptKey),
          &lino, _exit);
    }

    TAOS_CHECK_GOTO(tsdbFileWriteBrinBlk(ctx->fd, ctx->brinBlkArray, ctx->footer->brinBlkPtr,
                                         &fset->farr[TSDB_FTYPE_HEAD]->f->size, ctx->encryptAlgorithm, ctx->encryptKey),
                    &lino, _exit);

    code = tsdbFileWriteHeadFooter(ctx->fd, &fset->farr[TSDB_FTYPE_HEAD]->f->size, ctx->footer, ctx->encryptAlgorithm,
                                   ctx->encryptKey);
    TSDB_CHECK_CODE(code, lino, _exit);

    TAOS_CHECK_GOTO(tsdbFsyncFile(ctx->fd, ctx->encryptAlgorithm, ctx->encryptKey), &lino, _exit);

    tsdbCloseFile(&ctx->fd);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  TARRAY2_DESTROY(ctx->brinBlkArray, NULL);
  tBrinBlockDestroy(ctx->brinBlock);
  tBlockDataDestroy(ctx->blockData);
  tMapDataClear(ctx->mDataBlk);
  taosArrayDestroy(ctx->aBlockIdx);
  for (int32_t i = 0; i < ARRAY_SIZE(ctx->buffers); ++i) {
    tBufferDestroy(ctx->buffers + i);
  }
  return code;
}

static int32_t tsdbUpgradeData(STsdb *tsdb, SDFileSet *pDFileSet, SDataFReader *reader, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  if (fset->farr[TSDB_FTYPE_HEAD] == NULL) {
    return 0;
  }

  STFile file = {
      .type = TSDB_FTYPE_DATA,
      .did = pDFileSet->diskId,
      .fid = fset->fid,
      .cid = pDFileSet->pDataF->commitID,
      .size = pDFileSet->pDataF->size,
      .minVer = VERSION_MAX,
      .maxVer = VERSION_MIN,
  };

  TAOS_CHECK_GOTO(tsdbTFileObjInit(tsdb, &file, &fset->farr[TSDB_FTYPE_DATA]), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbUpgradeSma(STsdb *tsdb, SDFileSet *pDFileSet, SDataFReader *reader, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  if (fset->farr[TSDB_FTYPE_HEAD] == NULL) {
    return 0;
  }

  STFile file = {
      .type = TSDB_FTYPE_SMA,
      .did = pDFileSet->diskId,
      .fid = fset->fid,
      .cid = pDFileSet->pSmaF->commitID,
      .size = pDFileSet->pSmaF->size,
      .minVer = VERSION_MAX,
      .maxVer = VERSION_MIN,
  };

  TAOS_CHECK_GOTO(tsdbTFileObjInit(tsdb, &file, &fset->farr[TSDB_FTYPE_SMA]), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbUpgradeSttFile(STsdb *tsdb, SDFileSet *pDFileSet, SDataFReader *reader, STFileSet *fset,
                                  int32_t iStt, SSttLvl *lvl) {
  int32_t code = 0;
  int32_t lino = 0;

  SArray *aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
  if (aSttBlk == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  TAOS_CHECK_GOTO(tsdbReadSttBlk(reader, iStt, aSttBlk), &lino, _exit);

  if (taosArrayGetSize(aSttBlk) > 0) {
    SSttFile  *pSttF = pDFileSet->aSttF[iStt];
    STFileObj *fobj;
    struct {
      int32_t szPage;
      int32_t encryptAlgorithm;
      char   *encryptKey;
      // writer
      STsdbFD     *fd;
      TSttBlkArray sttBlkArray[1];
      SSttFooter   footer[1];
    } ctx[1] = {{
        .szPage = tsdb->pVnode->config.tsdbPageSize,
        .encryptAlgorithm = tsdb->pVnode->config.tsdbCfg.encryptAlgorithm,
        .encryptKey = tsdb->pVnode->config.tsdbCfg.encryptKey,
    }};

    STFile file = {
        .type = TSDB_FTYPE_STT,
        .did = pDFileSet->diskId,
        .fid = fset->fid,
        .cid = pSttF->commitID,
        .size = pSttF->size,
        .minVer = VERSION_MAX,
        .maxVer = VERSION_MIN,
    };
    TAOS_CHECK_GOTO(tsdbTFileObjInit(tsdb, &file, &fobj), &lino, _exit);

    TAOS_CHECK_GOTO(tsdbOpenFile(fobj->fname, tsdb, TD_FILE_READ | TD_FILE_WRITE, &ctx->fd, 0), &lino, _exit);

    for (int32_t iSttBlk = 0; iSttBlk < taosArrayGetSize(aSttBlk); iSttBlk++) {
      TAOS_CHECK_GOTO(TARRAY2_APPEND_PTR(ctx->sttBlkArray, (SSttBlk *)taosArrayGet(aSttBlk, iSttBlk)), &lino, _exit);
    }

    code = tsdbFileWriteSttBlk(ctx->fd, ctx->sttBlkArray, ctx->footer->sttBlkPtr, &fobj->f->size, ctx->encryptAlgorithm,
                               ctx->encryptKey);
    TSDB_CHECK_CODE(code, lino, _exit1);

    TAOS_CHECK_GOTO(
        tsdbFileWriteSttFooter(ctx->fd, ctx->footer, &fobj->f->size, ctx->encryptAlgorithm, ctx->encryptKey), &lino,
        _exit);

    TAOS_CHECK_GOTO(tsdbFsyncFile(ctx->fd, ctx->encryptAlgorithm, ctx->encryptKey), &lino, _exit);

    tsdbCloseFile(&ctx->fd);

    TAOS_CHECK_GOTO(TARRAY2_APPEND(lvl->fobjArr, fobj), &lino, _exit);

  _exit1:
    TARRAY2_DESTROY(ctx->sttBlkArray, NULL);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  taosArrayDestroy(aSttBlk);
  return code;
}

static int32_t tsdbUpgradeStt(STsdb *tsdb, SDFileSet *pDFileSet, SDataFReader *reader, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pDFileSet->nSttF == 0) {
    return 0;
  }

  SSttLvl *lvl;
  TAOS_CHECK_GOTO(tsdbSttLvlInit(0, &lvl), &lino, _exit);

  for (int32_t iStt = 0; iStt < pDFileSet->nSttF; ++iStt) {
    TAOS_CHECK_GOTO(tsdbUpgradeSttFile(tsdb, pDFileSet, reader, fset, iStt, lvl), &lino, _exit);
  }

  if (TARRAY2_SIZE(lvl->fobjArr) > 0) {
    TAOS_CHECK_GOTO(TARRAY2_APPEND(fset->lvlArr, lvl), &lino, _exit);
  } else {
    tsdbSttLvlClear(&lvl);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbUpgradeFileSet(STsdb *tsdb, SDFileSet *pDFileSet, TFileSetArray *fileSetArray) {
  int32_t code = 0;
  int32_t lino = 0;

  tsdbInfo("vgId:%d upgrade file set start, fid:%d", TD_VID(tsdb->pVnode), pDFileSet->fid);

  SDataFReader *reader;
  STFileSet    *fset;

  TAOS_CHECK_GOTO(tsdbTFileSetInit(pDFileSet->fid, &fset), &lino, _exit);

  TAOS_CHECK_GOTO(tsdbDataFReaderOpen(&reader, tsdb, pDFileSet), &lino, _exit);

  // .head
  TAOS_CHECK_GOTO(tsdbUpgradeHead(tsdb, pDFileSet, reader, fset), &lino, _exit);

  // .data
  TAOS_CHECK_GOTO(tsdbUpgradeData(tsdb, pDFileSet, reader, fset), &lino, _exit);

  // .sma
  TAOS_CHECK_GOTO(tsdbUpgradeSma(tsdb, pDFileSet, reader, fset), &lino, _exit);

  // .stt
  if (pDFileSet->nSttF > 0) {
    TAOS_CHECK_GOTO(tsdbUpgradeStt(tsdb, pDFileSet, reader, fset), &lino, _exit);
  }

  (void)tsdbDataFReaderClose(&reader);

  TAOS_CHECK_GOTO(TARRAY2_APPEND(fileSetArray, fset), &lino, _exit);

  tsdbInfo("vgId:%d upgrade file set end, fid:%d", TD_VID(tsdb->pVnode), pDFileSet->fid);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbUpgradeOpenTombFile(STsdb *tsdb, STFileSet *fset, STsdbFD **fd, STFileObj **fobj, bool *toStt) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TARRAY2_SIZE(fset->lvlArr) == 0) {  // to .tomb file
    *toStt = false;

    STFile file = {
        .type = TSDB_FTYPE_TOMB,
        .did = fset->farr[TSDB_FTYPE_HEAD]->f->did,
        .fid = fset->fid,
        .cid = 0,
        .size = 0,
        .minVer = VERSION_MAX,
        .maxVer = VERSION_MIN,
    };

    TAOS_CHECK_GOTO(tsdbTFileObjInit(tsdb, &file, fobj), &lino, _exit);

    fset->farr[TSDB_FTYPE_TOMB] = *fobj;
  } else {  // to .stt file
    *toStt = true;
    SSttLvl *lvl = TARRAY2_GET(fset->lvlArr, 0);

    STFile file = {
        .type = TSDB_FTYPE_STT,
        .did = TARRAY2_GET(lvl->fobjArr, 0)->f->did,
        .fid = fset->fid,
        .cid = 0,
        .size = 0,
        .minVer = VERSION_MAX,
        .maxVer = VERSION_MIN,
    };

    TAOS_CHECK_GOTO(tsdbTFileObjInit(tsdb, &file, fobj), &lino, _exit);

    TAOS_CHECK_GOTO(TARRAY2_APPEND(lvl->fobjArr, fobj[0]), &lino, _exit);
  }

  char fname[TSDB_FILENAME_LEN] = {0};
  TAOS_CHECK_GOTO(
      tsdbOpenFile(fobj[0]->fname, tsdb, TD_FILE_READ | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_CREATE, fd, 0), &lino,
      _exit);

  uint8_t hdr[TSDB_FHDR_SIZE] = {0};
  int32_t encryptAlgorithm = tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = tsdb->pVnode->config.tsdbCfg.encryptKey;

  TAOS_CHECK_GOTO(tsdbWriteFile(fd[0], 0, hdr, TSDB_FHDR_SIZE, encryptAlgorithm, encryptKey), &lino, _exit);
  fobj[0]->f->size += TSDB_FHDR_SIZE;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbDumpTombDataToFSet(STsdb *tsdb, SDelFReader *reader, SArray *aDelIdx, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  struct {
    // context
    bool    toStt;
    int8_t  cmprAlg;
    int32_t maxRow;
    int64_t minKey;
    int64_t maxKey;
    SBuffer buffers[10];
    int32_t encryptAlgorithm;
    char   *encryptKey;
    // reader
    SArray *aDelData;
    // writer
    STsdbFD      *fd;
    STFileObj    *fobj;
    STombBlock    tombBlock[1];
    TTombBlkArray tombBlkArray[1];
    STombFooter   tombFooter[1];
    SSttFooter    sttFooter[1];
  } ctx[1] = {{
      .maxRow = tsdb->pVnode->config.tsdbCfg.maxRows,
      .cmprAlg = tsdb->pVnode->config.tsdbCfg.compression,
      .encryptAlgorithm = tsdb->pVnode->config.tsdbCfg.encryptAlgorithm,
      .encryptKey = tsdb->pVnode->config.tsdbCfg.encryptKey,
  }};

  tsdbFidKeyRange(fset->fid, tsdb->keepCfg.days, tsdb->keepCfg.precision, &ctx->minKey, &ctx->maxKey);

  if ((ctx->aDelData = taosArrayInit(0, sizeof(SDelData))) == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  for (int32_t iDelIdx = 0; iDelIdx < taosArrayGetSize(aDelIdx); iDelIdx++) {
    SDelIdx *pDelIdx = (SDelIdx *)taosArrayGet(aDelIdx, iDelIdx);

    TAOS_CHECK_GOTO(tsdbReadDelData(reader, pDelIdx, ctx->aDelData), &lino, _exit);

    for (int32_t iDelData = 0; iDelData < taosArrayGetSize(ctx->aDelData); iDelData++) {
      SDelData *pDelData = (SDelData *)taosArrayGet(ctx->aDelData, iDelData);

      STombRecord record = {
          .suid = pDelIdx->suid,
          .uid = pDelIdx->uid,
          .version = pDelData->version,
          .skey = pDelData->sKey,
          .ekey = pDelData->eKey,
      };

      TAOS_CHECK_GOTO(tTombBlockPut(ctx->tombBlock, &record), &lino, _exit);

      if (TOMB_BLOCK_SIZE(ctx->tombBlock) > ctx->maxRow) {
        if (ctx->fd == NULL) {
          TAOS_CHECK_GOTO(tsdbUpgradeOpenTombFile(tsdb, fset, &ctx->fd, &ctx->fobj, &ctx->toStt), &lino, _exit);
        }
        SVersionRange tombRange = {.minVer = VERSION_MAX, .maxVer = VERSION_MIN};
        TAOS_CHECK_GOTO(
            tsdbFileWriteTombBlock(ctx->fd, ctx->tombBlock, ctx->cmprAlg, &ctx->fobj->f->size, ctx->tombBlkArray,
                                   ctx->buffers, &tombRange, ctx->encryptAlgorithm, ctx->encryptKey),
            &lino, _exit);
      }
    }
  }

  if (TOMB_BLOCK_SIZE(ctx->tombBlock) > 0) {
    if (ctx->fd == NULL) {
      TAOS_CHECK_GOTO(tsdbUpgradeOpenTombFile(tsdb, fset, &ctx->fd, &ctx->fobj, &ctx->toStt), &lino, _exit);
    }
    SVersionRange tombRange = {.minVer = VERSION_MAX, .maxVer = VERSION_MIN};
    TAOS_CHECK_GOTO(
        tsdbFileWriteTombBlock(ctx->fd, ctx->tombBlock, ctx->cmprAlg, &ctx->fobj->f->size, ctx->tombBlkArray,
                               ctx->buffers, &tombRange, ctx->encryptAlgorithm, ctx->encryptKey),
        &lino, _exit);
  }

  if (ctx->fd != NULL) {
    if (ctx->toStt) {
      code = tsdbFileWriteTombBlk(ctx->fd, ctx->tombBlkArray, ctx->sttFooter->tombBlkPtr, &ctx->fobj->f->size,
                                  ctx->encryptAlgorithm, ctx->encryptKey);
      TSDB_CHECK_CODE(code, lino, _exit);

      code =
          tsdbFileWriteSttFooter(ctx->fd, ctx->sttFooter, &ctx->fobj->f->size, ctx->encryptAlgorithm, ctx->encryptKey);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbFileWriteTombBlk(ctx->fd, ctx->tombBlkArray, ctx->tombFooter->tombBlkPtr, &ctx->fobj->f->size,
                                  ctx->encryptAlgorithm, ctx->encryptKey);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbFileWriteTombFooter(ctx->fd, ctx->tombFooter, &ctx->fobj->f->size, ctx->encryptAlgorithm,
                                     ctx->encryptKey);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    TAOS_CHECK_GOTO(tsdbFsyncFile(ctx->fd, ctx->encryptAlgorithm, ctx->encryptKey), &lino, _exit);

    tsdbCloseFile(&ctx->fd);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  for (int32_t i = 0; i < ARRAY_SIZE(ctx->buffers); i++) {
    tBufferDestroy(ctx->buffers + i);
  }
  TARRAY2_DESTROY(ctx->tombBlkArray, NULL);
  tTombBlockDestroy(ctx->tombBlock);
  taosArrayDestroy(ctx->aDelData);
  return code;
}

static int32_t tsdbUpgradeTombFile(STsdb *tsdb, SDelFile *pDelFile, TFileSetArray *fileSetArray) {
  int32_t code = 0;
  int32_t lino = 0;

  SDelFReader *reader = NULL;
  SArray      *aDelIdx = NULL;

  if ((aDelIdx = taosArrayInit(0, sizeof(SDelIdx))) == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  TAOS_CHECK_GOTO(tsdbDelFReaderOpen(&reader, pDelFile, tsdb), &lino, _exit);

  TAOS_CHECK_GOTO(tsdbReadDelIdx(reader, aDelIdx), &lino, _exit);

  if (taosArrayGetSize(aDelIdx) > 0) {
    STFileSet *fset;
    TARRAY2_FOREACH(fileSetArray, fset) {
      TAOS_CHECK_GOTO(tsdbDumpTombDataToFSet(tsdb, reader, aDelIdx, fset), &lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  (void)tsdbDelFReaderClose(&reader);
  taosArrayDestroy(aDelIdx);
  return code;
}

static int32_t tsdbDoUpgradeFileSystem(STsdb *tsdb, TFileSetArray *fileSetArray) {
  int32_t code = 0;
  int32_t lino = 0;

  // upgrade each file set
  for (int32_t i = 0; i < taosArrayGetSize(tsdb->fs.aDFileSet); i++) {
    TAOS_CHECK_GOTO(tsdbUpgradeFileSet(tsdb, taosArrayGet(tsdb->fs.aDFileSet, i), fileSetArray), &lino, _exit);
  }

  // upgrade tomb file
  if (tsdb->fs.pDelFile != NULL) {
    TAOS_CHECK_GOTO(tsdbUpgradeTombFile(tsdb, tsdb->fs.pDelFile, fileSetArray), &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbUpgradeFileSystem(STsdb *tsdb, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;

  TFileSetArray fileSetArray[1] = {0};

  // open old file system
  TAOS_CHECK_GOTO(tsdbFSOpen(tsdb, rollback), &lino, _exit);

  TAOS_CHECK_GOTO(tsdbDoUpgradeFileSystem(tsdb, fileSetArray), &lino, _exit);

  // close file system
  TAOS_CHECK_GOTO(tsdbFSClose(tsdb), &lino, _exit);

  // save new file system
  char fname[TSDB_FILENAME_LEN];
  current_fname(tsdb, fname, TSDB_FCURRENT);
  TAOS_CHECK_GOTO(save_fs(fileSetArray, fname), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  }
  TARRAY2_DESTROY(fileSetArray, tsdbTFileSetClear);
  return code;
}

int32_t tsdbCheckAndUpgradeFileSystem(STsdb *tsdb, int8_t rollback) {
  char fname[TSDB_FILENAME_LEN];

  tsdbGetCurrentFName(tsdb, fname, NULL);
  if (!taosCheckExistFile(fname)) return 0;

  TAOS_CHECK_RETURN(tsdbUpgradeFileSystem(tsdb, rollback));

  tsdbRemoveFile(fname);
  return 0;
}

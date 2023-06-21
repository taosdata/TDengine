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
extern void tsdbGetCurrentFName(STsdb *pTsdb, char *current, char *current_t);

// new
extern int32_t save_fs(const TFileSetArray *arr, const char *fname);
extern int32_t current_fname(STsdb *pTsdb, char *fname, EFCurrentT ftype);

static int32_t tsdbUpgradeFileSet(STsdb *tsdb, SDFileSet *pDFileSet, TFileSetArray *fileSetArray) {
  int32_t code = 0;
  int32_t lino = 0;

  SDataFReader *reader;

  code = tsdbDataFReaderOpen(&reader, tsdb, pDFileSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  // .head
  {
    SArray       *aBlockIdx = NULL;
    SMapData      mDataBlk[1] = {0};
    SBrinBlock    brinBlock[1] = {0};
    TBrinBlkArray brinBlkArray[1] = {0};

    if ((aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx))) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbReadBlockIdx(reader, aBlockIdx);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (int32_t i = 0; i < taosArrayGetSize(aBlockIdx); ++i) {
      SBlockIdx *pBlockIdx = taosArrayGet(aBlockIdx, i);

      code = tsdbReadDataBlk(reader, pBlockIdx, mDataBlk);
      TSDB_CHECK_CODE(code, lino, _exit);

      for (int32_t j = 0; j < mDataBlk->nItem; ++j) {
        SDataBlk dataBlk[1];

        tMapDataGetItemByIdx(mDataBlk, j, dataBlk, tGetDataBlk);

        SBrinRecord record = {
            .suid = pBlockIdx->suid,
            .uid = pBlockIdx->uid,
            .firstKey = dataBlk->minKey.ts,
            .firstKeyVer = dataBlk->minKey.version,
            .lastKey = dataBlk->maxKey.ts,
            .lastKeyVer = dataBlk->maxKey.version,
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
          ASSERT(0);
          // TODO: need to get count
          //   record.count = 0;
        }

        code = tBrinBlockPut(brinBlock, &record);
        TSDB_CHECK_CODE(code, lino, _exit);

        if (BRIN_BLOCK_SIZE(brinBlock) >= tsdb->pVnode->config.tsdbCfg.maxRows) {
          // TODO
          tBrinBlockClear(brinBlock);
        }
      }
    }

    if (BRIN_BLOCK_SIZE(brinBlock) > 0) {
      // TODO
      ASSERT(0);
    }

    // TODO
    ASSERT(0);

    TARRAY2_DESTROY(brinBlkArray, NULL);
    tBrinBlockDestroy(brinBlock);
    taosArrayDestroy(aBlockIdx);
    tMapDataClear(mDataBlk);
  }

  // .data

  // .sma

  // .stt
  for (int32_t i = 0; i < pDFileSet->nSttF; ++i) {
    // TODO
  }

  tsdbDataFReaderClose(&reader);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDumpTombDataToFSet(STsdb *tsdb, SDelFReader *reader, SArray *aDelIdx, STFileSet *fset) {
  int32_t code = 0;
  int32_t lino = 0;

  SArray       *aDelData = NULL;
  int64_t       minKey, maxKey;
  STombBlock    tombBlock[1] = {0};
  TTombBlkArray tombBlkArray[1] = {0};
  STsdbFD      *fd = NULL;

  tsdbFidKeyRange(fset->fid, tsdb->keepCfg.days, tsdb->keepCfg.precision, &minKey, &maxKey);

  if ((aDelData = taosArrayInit(0, sizeof(SDelData))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  for (int32_t i = 0; i < taosArrayGetSize(aDelIdx); ++i) {
    SDelIdx *pDelIdx = taosArrayGet(aDelIdx, i);

    code = tsdbReadDelData(reader, pDelIdx, aDelData);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (int32_t j = 0; j < taosArrayGetSize(aDelData); ++j) {
      SDelData *pDelData = taosArrayGet(aDelData, j);

      if (pDelData->sKey > maxKey || pDelData->eKey < minKey) {
        continue;
      }

      STombRecord record = {
          .suid = pDelIdx->suid,
          .uid = pDelIdx->uid,
          .version = pDelData->version,
          .skey = pDelData->sKey,
          .ekey = pDelData->eKey,
      };

      code = tTombBlockPut(tombBlock, &record);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (TOMB_BLOCK_SIZE(tombBlock) >= tsdb->pVnode->config.tsdbCfg.maxRows) {
        if (fd == NULL) {
          STFile file = {
              .type = TSDB_FTYPE_TOMB,
              .did = {0},  // TODO
              .fid = fset->fid,
              .cid = 0,  // TODO
          };

          code = tsdbTFileObjInit(tsdb, &file, &fset->farr[TSDB_FTYPE_TOMB]);
          TSDB_CHECK_CODE(code, lino, _exit);

          code = tsdbOpenFile(fset->farr[TSDB_FTYPE_TOMB]->fname, tsdb->pVnode->config.tsdbPageSize,
                              TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC, &fd);
          TSDB_CHECK_CODE(code, lino, _exit);

          uint8_t hdr[TSDB_FHDR_SIZE] = {0};
          code = tsdbWriteFile(fd, 0, hdr, TSDB_FHDR_SIZE);
          TSDB_CHECK_CODE(code, lino, _exit);
          fset->farr[TSDB_FTYPE_TOMB]->f->size += sizeof(hdr);
        }

        // TODO
        tTombBlockClear(tombBlock);
      }
    }
  }

  if (TOMB_BLOCK_SIZE(tombBlock) > 0) {
    // TODO
    tTombBlockClear(tombBlock);
  }

  if (TARRAY2_SIZE(tombBlkArray) > 0) {
    // TODO
  }

  if (fd) {
    code = tsdbFsyncFile(fd);
    TSDB_CHECK_CODE(code, lino, _exit);

    tsdbCloseFile(&fd);
  }
  TARRAY2_DESTROY(tombBlkArray, NULL);
  tTombBlockDestroy(tombBlock);
  taosArrayDestroy(aDelData);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbUpgradeTombFile(STsdb *tsdb, SDelFile *pDelFile, TFileSetArray *fileSetArray) {
  int32_t code = 0;
  int32_t lino = 0;

  SDelFReader *reader = NULL;
  SArray      *aDelIdx = NULL;

  if ((aDelIdx = taosArrayInit(0, sizeof(SDelIdx))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbDelFReaderOpen(&reader, pDelFile, tsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadDelIdx(reader, aDelIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (taosArrayGetSize(aDelIdx) > 0) {
    STFileSet *fset;
    TARRAY2_FOREACH(fileSetArray, fset) {
      code = tsdbDumpTombDataToFSet(tsdb, reader, aDelIdx, fset);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  tsdbDelFReaderClose(&reader);
  taosArrayDestroy(aDelIdx);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDoUpgradeFileSystem(STsdb *tsdb, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;

  TFileSetArray fileSetArray[1] = {0};

  // load old file system and convert
  code = tsdbFSOpen(tsdb, rollback);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t i = 0; i < taosArrayGetSize(tsdb->fs.aDFileSet); i++) {
    SDFileSet *pDFileSet = taosArrayGet(tsdb->fs.aDFileSet, i);

    code = tsdbUpgradeFileSet(tsdb, pDFileSet, fileSetArray);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (tsdb->fs.pDelFile != NULL) {
    code = tsdbUpgradeTombFile(tsdb, tsdb->fs.pDelFile, fileSetArray);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFSClose(tsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  // save new file system
  char fname[TSDB_FILENAME_LEN];
  current_fname(tsdb, fname, TSDB_FCURRENT);

  code = save_fs(fileSetArray, fname);
  TSDB_CHECK_CODE(code, lino, _exit);

  TARRAY2_DESTROY(fileSetArray, tsdbTFileSetClear);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbCheckAndUpgradeFileSystem(STsdb *tsdb, int8_t rollback) {
  char fname[TSDB_FILENAME_LEN];

  tsdbGetCurrentFName(tsdb, fname, NULL);
  if (!taosCheckExistFile(fname)) return 0;

  int32_t code = tsdbDoUpgradeFileSystem(tsdb, rollback);
  if (code) return code;

  taosRemoveFile(fname);
  return 0;
}
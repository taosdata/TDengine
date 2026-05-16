/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 or later.
 */

/*
 * SECURE ERASE: Manual file-level overwrite of physically deleted sensitive data.
 *
 * DESIGN:
 *   After adding delete markers to the memtable, scan all on-disk TSDB files
 *   (both DATA files and STT files) and physically overwrite the blocks that
 *   contain data for the target (suid, uid) in [sKey, eKey].
 *
 * TWO STRATEGIES:
 *   1. Fully-contained blocks (block range ⊆ [sKey, eKey]):
 *      - Direct raw-bytes overwrite at block offset in the data file.
 *      - For zero mode: fill with zeros.
 *      - For random mode: fill with random bytes.
 *      - The page CRC is updated through tsdbWriteFile / tsdbFsyncFile.
 *      - Safe because delete markers guarantee TDengine will never read these rows.
 *
 *   2. Partially-overlapping blocks (block range ∩ [sKey, eKey] ≠ ∅ but not fully contained):
 *      - Must preserve non-deleted rows' data.
 *      - Decompress → zero VALUE bytes for target rows → recompress → write back.
 *      - Always use zero fill for decompressed values (random fill causes
 *        recompressed output to be larger than the original, making in-place
 *        write impossible without file reorganization).
 *
 * KNOWN PROBLEMS AND LIMITATIONS:
 *
 *   P1: SIZE CONSTRAINT WITH RANDOM FILL (recompress path only)
 *       After decompressing and zeroing VALUES, the recompressed output is
 *       guaranteed to be ≤ original because zeros are highly compressible.
 *       However, if random fill were applied, random data resists compression
 *       and the output could exceed the original block size, making in-place
 *       write impossible. Therefore, the recompress path always uses zero fill
 *       regardless of tsSecureEraseMode.
 *
 *   P2: CONCURRENT READERS
 *       While we overwrite a block, a concurrent query might be reading the
 *       same file page. The page CRC is updated atomically by tsdbFsyncFile,
 *       but there is a narrow window where a reader sees the old data before
 *       the new page is flushed. This is generally acceptable: the delete
 *       markers ensure the deleted rows won't be returned anyway; any read of
 *       the old data during the window is a transient race, not a security
 *       breach in practice.
 *
 *   P3: OS PAGE CACHE
 *       After fsync, the OS may still hold the old data in RAM (page cache).
 *       True physical erasure requires encryption-at-rest with key destruction,
 *       or hardware-level secure erase (ATA Secure Erase / NVMe Sanitize).
 *
 *   P4: WAL STILL CONTAINS ORIGINAL DATA
 *       The original write was journaled through WAL. Even after this overwrite,
 *       the WAL files may still contain the plaintext original data. WAL trimming
 *       (which happens after a checkpoint) will eventually remove old entries,
 *       but there is no immediate guarantee. For environments requiring immediate
 *       WAL erasure, WAL encryption or external WAL management is needed.
 *
 *   P5: MULTI-REPLICA CONSISTENCY
 *       This code runs only on the Raft leader. Follower replicas apply the
 *       logical DELETE via WAL replay but do NOT trigger the file-level overwrite.
 *       All replicas must independently run the overwrite to achieve full erasure.
 *       Current architecture does not support replicating physical file operations
 *       via Raft; this is a design gap for future work.
 *
 *   P6: STT MULTI-TABLE BLOCKS
 *       An STT block with uid==0 holds rows for multiple child tables. The
 *       surgical decompress→zero→recompress path handles this, but it is
 *       more expensive than a direct overwrite.
 *
 *   P7: SSD WEAR LEVELING
 *       On SSDs, the controller may map the same logical block to a different
 *       physical cell for wear leveling. The previous physical cell may retain
 *       the old data even after an overwrite. ATA Secure Erase or full-disk
 *       encryption is required for complete physical protection.
 *
 *   P8: OLD FORMAT (tsdb1, pTsdb->pFS == NULL)
 *       This implementation only handles the new tsdb2 file format. Old-format
 *       files are not yet supported and rely on eventual compaction for erasure.
 */

#include "meta.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tsdbDataFileRW.h"
#include "tsdbDef.h"
#include "tsdbFS2.h"
#include "tsdbFSet2.h"
#include "tsdbSttFileRW.h"
#include "tsdbUtil2.h"
#include "ttypes.h"

#define TSDB_SECURE_ERASE_LOG(vid, lino, code) \
  tsdbError("vgId:%d secureErase failed at %s:%d since %s", vid, __FILE__, lino, tstrerror(code))

/* ---------------------------------------------------------------------------
 * Zero (or random-fill) a byte buffer according to tsSecureEraseMode.
 * For the recompress path we always zero regardless of mode (see P1 above).
 * --------------------------------------------------------------------------*/
static void tsdbFillEraseBuf(uint8_t *buf, int32_t size, bool forceZero) {
  if (forceZero || tsSecureEraseMode == 0) {
    memset(buf, 0, size);
  } else {
    for (int32_t i = 0; i < size; i++) {
      buf[i] = (uint8_t)(taosRand() & 0xFF);
    }
  }
}

/* ---------------------------------------------------------------------------
 * Zero the VALUE bytes stored in pColData->pData for rows whose timestamp
 * falls in [sKey, eKey] AND whose uid matches (for multi-table STT blocks).
 *
 * Layout of pColData->pData:
 *   • Fixed-size types: pData[valueIdx * typeBytes .. (valueIdx+1)*typeBytes-1]
 *   • Variable-size types: pData[aOffset[valueIdx] .. aOffset[valueIdx+1]-1]
 *     (last entry: pData[aOffset[last] .. nData-1])
 *
 * Only rows with bit==2 (HAS_VALUE) have an entry in pData; NONE/NULL rows
 * do not occupy space in pData.  We walk all nVal rows and count VALUE rows
 * (valueIdx) to locate the byte range for each target row.
 * --------------------------------------------------------------------------*/
static void tsdbSecureEraseColDataRows(SColData *pColData, const int64_t *aTSKEY,
                                       const int64_t *aUid,  /* NULL for single-table blocks */
                                       tb_uid_t uid, TSKEY sKey, TSKEY eKey) {
  if (!(pColData->flag & HAS_VALUE)) return;

  bool   isVar      = IS_VAR_DATA_TYPE(pColData->type);
  int32_t typeBytes = isVar ? 0 : tDataTypes[pColData->type].bytes;
  int32_t valueIdx  = 0;

  for (int32_t iVal = 0; iVal < pColData->nVal; iVal++) {
    uint8_t bit = tColDataGetBitValue(pColData, iVal);
    if (bit != 2) continue;  /* not a VALUE row, skip */

    bool inRange  = (aTSKEY[iVal] >= sKey && aTSKEY[iVal] <= eKey);
    bool uidMatch = (aUid == NULL || aUid[iVal] == uid);

    if (inRange && uidMatch) {
      if (!isVar) {
        memset(pColData->pData + (int64_t)valueIdx * typeBytes, 0, typeBytes);
      } else {
        int32_t start = pColData->aOffset[valueIdx];
        int32_t end =
            (valueIdx + 1 < pColData->numOfValue) ? pColData->aOffset[valueIdx + 1] : pColData->nData;
        if (end > start) {
          memset(pColData->pData + start, 0, end - start);
        }
      }
    }
    valueIdx++;
  }
}

/* ---------------------------------------------------------------------------
 * Decompress a block from the file, zero column values for target rows, then
 * recompress and write back in-place.  If the recompressed block is smaller
 * than the original, zero-pad the remainder so that the original byte range
 * is fully overwritten.
 *
 * This path is used for:
 *   – Partially-overlapping DATA-file blocks.
 *   – Multi-table STT blocks (uid==0).
 * --------------------------------------------------------------------------*/
static int32_t tsdbSecureEraseBlockSurgical(STsdb *pTsdb, STsdbFD *pFD,
                                             int64_t blockOffset, int32_t blockSize,
                                             tb_uid_t suid, tb_uid_t uid,
                                             TSKEY sKey, TSKEY eKey) {
  int32_t   code = 0, lino = 0;
  int32_t   vid  = TD_VID(pTsdb->pVnode);
  SBuffer   buffer = {0}, assist = {0};
  SBlockData bData  = {0};
  SBuffer   buffers[8] = {0};

  SEncryptData *pEncryptData = &pTsdb->pVnode->config.tsdbCfg.encryptData;

  /* 1. Read compressed block bytes */
  tBufferInit(&buffer);
  TAOS_CHECK_GOTO(tsdbReadFileToBuffer(pFD, blockOffset, blockSize, &buffer, 0, pEncryptData),
                  &lino, _exit);

  /* 2. Decompress */
  SBufferReader br = BUFFER_READER_INITIALIZER(0, &buffer);
  TAOS_CHECK_GOTO(tBlockDataDecompress(&br, &bData, &assist), &lino, _exit);

  /* 3. Zero VALUE bytes for target rows (always zero fill, see P1) */
  for (int32_t iCol = 0; iCol < bData.nColData; iCol++) {
    tsdbSecureEraseColDataRows(&bData.aColData[iCol], bData.aTSKEY, bData.aUid, uid, sKey, eKey);
  }

  /* 4. Recompress – use column compression settings from meta if available */
  SColCompressInfo cmprInfo = {.defaultCmprAlg = pTsdb->pVnode->config.tsdbCfg.compression};
  (void)metaGetColCmpr(pTsdb->pVnode->pMeta, bData.suid != 0 ? bData.suid : bData.uid,
                       &cmprInfo.pColCmpr);

  for (int i = 0; i < 8; i++) tBufferInit(&buffers[i]);
  TAOS_CHECK_GOTO(tBlockDataCompress(&bData, &cmprInfo, buffers, buffers + 4), &lino, _exit);

  int32_t newSize = 0;
  for (int i = 0; i < 4; i++) newSize += (int32_t)buffers[i].size;

  /* 5. Write new compressed block back at original offset */
  int64_t writeOff = blockOffset;
  for (int i = 0; i < 4; i++) {
    if (buffers[i].size) {
      TAOS_CHECK_GOTO(tsdbWriteFile(pFD, writeOff, buffers[i].data, buffers[i].size, pEncryptData),
                      &lino, _exit);
      writeOff += buffers[i].size;
    }
  }

  /* 6. Zero-pad to fill the original block size so no original bytes remain */
  if (newSize < blockSize) {
    int32_t padSize = blockSize - newSize;
    uint8_t *zeroBuf = taosMemoryCalloc(1, padSize);
    if (zeroBuf) {
      (void)tsdbWriteFile(pFD, writeOff, zeroBuf, padSize, pEncryptData);
      taosMemoryFree(zeroBuf);
    }
  }

_exit:
  tBlockDataDestroy(&bData);
  tBufferDestroy(&buffer);
  tBufferDestroy(&assist);
  for (int i = 0; i < 8; i++) tBufferDestroy(&buffers[i]);
  if (cmprInfo.pColCmpr) taosHashCleanup(cmprInfo.pColCmpr);
  if (code) TSDB_SECURE_ERASE_LOG(vid, lino, code);
  return code;
}

/* ---------------------------------------------------------------------------
 * Process a DATA file: find all blocks for (suid, uid) in [sKey, eKey] via
 * the brin index, then overwrite them in the .data file.
 * --------------------------------------------------------------------------*/
static int32_t tsdbSecureEraseDataFile(STsdb *pTsdb, const STFileSet *fset,
                                        tb_uid_t suid, tb_uid_t uid,
                                        TSKEY sKey, TSKEY eKey) {
  if (!fset->farr[TSDB_FTYPE_HEAD] || !fset->farr[TSDB_FTYPE_DATA]) return 0;

  int32_t code = 0, lino = 0;
  int32_t vid  = TD_VID(pTsdb->pVnode);

  /* Open reader for the .head + .data files */
  SDataFileReaderConfig config = {.tsdb = pTsdb, .szPage = pTsdb->pVnode->config.tsdbPageSize};
  config.files[TSDB_FTYPE_HEAD].exist = true;
  config.files[TSDB_FTYPE_HEAD].file  = fset->farr[TSDB_FTYPE_HEAD]->f[0];
  config.files[TSDB_FTYPE_DATA].exist = true;
  config.files[TSDB_FTYPE_DATA].file  = fset->farr[TSDB_FTYPE_DATA]->f[0];

  SDataFileReader *pReader = NULL;
  TAOS_CHECK_GOTO(tsdbDataFileReaderOpen(NULL, &config, &pReader), &lino, _exit);

  /* Load brin block array */
  const TBrinBlkArray *pBrinBlkArray = NULL;
  TAOS_CHECK_GOTO(tsdbDataFileReadBrinBlk(pReader, &pBrinBlkArray), &lino, _closeReader);

  /* Collect matching SBrinRecords */
  SArray *pRecords = taosArrayInit(8, sizeof(SBrinRecord));
  if (!pRecords) { TAOS_CHECK_GOTO(terrno, &lino, _closeReader); }

  for (int32_t iBlk = 0; iBlk < TARRAY2_SIZE(pBrinBlkArray); iBlk++) {
    const SBrinBlk *pBrinBlk = TARRAY2_GET_PTR(pBrinBlkArray, iBlk);
    /* Quick suid/uid range check on the brin block */
    if (pBrinBlk->maxTbid.suid < suid || pBrinBlk->minTbid.suid > suid) continue;
    if (pBrinBlk->maxTbid.uid < uid || pBrinBlk->minTbid.uid > uid) continue;

    SBrinBlock brinBlock = {0};
    code = tsdbDataFileReadBrinBlock(pReader, pBrinBlk, &brinBlock);
    if (code) { code = 0; continue; }

    for (int32_t i = 0; i < brinBlock.numOfRecords; i++) {
      SBrinRecord record = {0};
      if (tBrinBlockGet(&brinBlock, i, &record) != 0) continue;
      if (record.suid != suid || record.uid != uid) continue;
      /* time range check using key.ts (SBrinRecord.firstKey/lastKey are STsdbRowKey) */
      if (record.lastKey.key.ts < sKey || record.firstKey.key.ts > eKey) continue;
      if (NULL == taosArrayPush(pRecords, &record)) {
        code = terrno;
        goto _exit;
      }
    }
    tBrinBlockDestroy(&brinBlock);
  }

  tsdbDataFileReaderClose(&pReader);
  pReader = NULL;

  if (taosArrayGetSize(pRecords) == 0) goto _exit;

  /* Open the .data file for read+write to overwrite blocks in-place */
  char     dataFname[TSDB_FILENAME_LEN] = {0};
  tsdbTFileName(pTsdb, &fset->farr[TSDB_FTYPE_DATA]->f[0], dataFname);

  STsdbFD *pFD  = NULL;
  int32_t  lcn  = fset->farr[TSDB_FTYPE_DATA]->f[0].lcn;
  TAOS_CHECK_GOTO(tsdbOpenFile(dataFname, pTsdb, TD_FILE_READ | TD_FILE_WRITE, &pFD, lcn),
                  &lino, _exit);

  SEncryptData *pEncryptData = &pTsdb->pVnode->config.tsdbCfg.encryptData;

  for (int32_t i = 0; i < taosArrayGetSize(pRecords); i++) {
    SBrinRecord *pRec = taosArrayGet(pRecords, i);

    bool fullyContained =
        (pRec->firstKey.key.ts >= sKey && pRec->lastKey.key.ts <= eKey);

    if (fullyContained) {
      /* Direct raw-bytes overwrite: safe because delete markers cover all rows */
      uint8_t *buf = taosMemoryMalloc(pRec->blockSize);
      if (!buf) continue;
      tsdbFillEraseBuf(buf, pRec->blockSize, false);
      (void)tsdbWriteFile(pFD, pRec->blockOffset, buf, pRec->blockSize, pEncryptData);
      taosMemoryFree(buf);
    } else {
      /* Partial overlap: surgical decompress → zero target rows → recompress */
      (void)tsdbSecureEraseBlockSurgical(pTsdb, pFD, pRec->blockOffset, pRec->blockSize,
                                         suid, uid, sKey, eKey);
    }
  }

  /* Flush with updated page CRCs */
  (void)tsdbFsyncFile(pFD, pEncryptData);
  tsdbCloseFile(&pFD);

_exit:
  taosArrayDestroy(pRecords);
  if (pReader) tsdbDataFileReaderClose(&pReader);
  if (code) TSDB_SECURE_ERASE_LOG(vid, lino, code);
  return code;

_closeReader:
  tsdbDataFileReaderClose(&pReader);
  goto _exit;
}

/* ---------------------------------------------------------------------------
 * Process a single STT file: find blocks that may contain (suid, uid) data
 * in [sKey, eKey] and overwrite them.
 *
 * STT block overwrite strategy:
 *   • If sttBlk.minUid == sttBlk.maxUid == uid (single-table block) AND
 *     the time range is fully contained → direct raw-bytes overwrite.
 *   • Otherwise → surgical decompress → zero → recompress path.
 * --------------------------------------------------------------------------*/
static int32_t tsdbSecureEraseOneSttFile(STsdb *pTsdb, const STFile *pFile,
                                          tb_uid_t suid, tb_uid_t uid,
                                          TSKEY sKey, TSKEY eKey) {
  int32_t code = 0, lino = 0;
  int32_t vid  = TD_VID(pTsdb->pVnode);

  SSttFileReaderConfig config = {
      .tsdb   = pTsdb,
      .szPage = pTsdb->pVnode->config.tsdbPageSize,
      .file   = {pFile[0]},
  };

  SSttFileReader *pReader = NULL;
  TAOS_CHECK_GOTO(tsdbSttFileReaderOpen(NULL, &config, &pReader), &lino, _exit);

  const TSttBlkArray *pSttBlkArray = NULL;
  TAOS_CHECK_GOTO(tsdbSttFileReadSttBlk(pReader, &pSttBlkArray), &lino, _closeReader);

  /* Collect matching SSttBlk entries */
  SArray *pBlks = taosArrayInit(8, sizeof(SSttBlk));
  if (!pBlks) { TAOS_CHECK_GOTO(terrno, &lino, _closeReader); }

  for (int32_t iBlk = 0; iBlk < TARRAY2_SIZE(pSttBlkArray); iBlk++) {
    const SSttBlk *pBlk = TARRAY2_GET_PTR(pSttBlkArray, iBlk);
    if (pBlk->suid != suid) continue;
    if (pBlk->minUid > uid || pBlk->maxUid < uid) continue;
    if (pBlk->maxKey < sKey || pBlk->minKey > eKey) continue;
    if (NULL == taosArrayPush(pBlks, pBlk)) {
      code = terrno;
      goto _exit;
    }
  }

  tsdbSttFileReaderClose(&pReader);
  pReader = NULL;

  if (taosArrayGetSize(pBlks) == 0) goto _exit;

  /* Open STT file for read+write */
  char    sttFname[TSDB_FILENAME_LEN] = {0};
  tsdbTFileName(pTsdb, pFile, sttFname);

  STsdbFD *pFD  = NULL;
  int32_t  lcn  = pFile->lcn;
  TAOS_CHECK_GOTO(tsdbOpenFile(sttFname, pTsdb, TD_FILE_READ | TD_FILE_WRITE, &pFD, lcn),
                  &lino, _exit);

  SEncryptData *pEncryptData = &pTsdb->pVnode->config.tsdbCfg.encryptData;

  for (int32_t i = 0; i < taosArrayGetSize(pBlks); i++) {
    SSttBlk *pSttBlk = taosArrayGet(pBlks, i);

    bool singleTable   = (pSttBlk->minUid == pSttBlk->maxUid && pSttBlk->minUid == uid);
    bool fullyContained = (pSttBlk->minKey >= sKey && pSttBlk->maxKey <= eKey);

    if (singleTable && fullyContained) {
      uint8_t *buf = taosMemoryMalloc(pSttBlk->bInfo.szBlock);
      if (!buf) continue;
      tsdbFillEraseBuf(buf, pSttBlk->bInfo.szBlock, false);
      (void)tsdbWriteFile(pFD, pSttBlk->bInfo.offset, buf, pSttBlk->bInfo.szBlock, pEncryptData);
      taosMemoryFree(buf);
    } else {
      (void)tsdbSecureEraseBlockSurgical(pTsdb, pFD,
                                         pSttBlk->bInfo.offset, pSttBlk->bInfo.szBlock,
                                         suid, uid, sKey, eKey);
    }
  }

  (void)tsdbFsyncFile(pFD, pEncryptData);
  tsdbCloseFile(&pFD);

_exit:
  taosArrayDestroy(pBlks);
  if (pReader) tsdbSttFileReaderClose(&pReader);
  if (code) TSDB_SECURE_ERASE_LOG(vid, lino, code);
  return code;

_closeReader:
  tsdbSttFileReaderClose(&pReader);
  goto _exit;
}

/* ---------------------------------------------------------------------------
 * Public entry point.
 *
 * Called from tsdbDeleteTableData when secureDelete is enabled, after the
 * delete markers have been added to the memtable.  Iterates over all on-disk
 * file sets and overwrites blocks that contain data for (suid, uid) in the
 * deleted time range [sKey, eKey].
 *
 * Only handles the new tsdb2 format (pTsdb->pFS != NULL).  Old-format files
 * are skipped silently and rely on eventual compaction for erasure.
 * --------------------------------------------------------------------------*/
int32_t tsdbSecureEraseFileRange(STsdb *pTsdb, tb_uid_t suid, tb_uid_t uid,
                                  TSKEY sKey, TSKEY eKey) {
  if (!pTsdb || !pTsdb->pFS) return 0;  /* old format: not supported yet */

  int32_t        code    = 0, lino = 0;
  int32_t        vid     = TD_VID(pTsdb->pVnode);
  TFileSetArray *fsetArr = NULL;

  /* Take a reference snapshot so file sets aren't freed under us */
  TAOS_CHECK_GOTO(tsdbFSCreateRefSnapshot(pTsdb->pFS, &fsetArr), &lino, _exit);

  const STFileSet *fset;
  TARRAY2_FOREACH(fsetArr, fset) {
    /* --- DATA file --- */
    (void)tsdbSecureEraseDataFile(pTsdb, fset, suid, uid, sKey, eKey);

    /* --- STT files (all levels) --- */
    const SSttLvl *lvl;
    TARRAY2_FOREACH(fset->lvlArr, lvl) {
      const STFileObj *fobj;
      TARRAY2_FOREACH(lvl->fobjArr, fobj) {
        (void)tsdbSecureEraseOneSttFile(pTsdb, fobj->f, suid, uid, sKey, eKey);
      }
    }
  }

_exit:
  tsdbFSDestroyRefSnapshot(&fsetArr);
  if (code) TSDB_SECURE_ERASE_LOG(vid, lino, code);
  return code;
}

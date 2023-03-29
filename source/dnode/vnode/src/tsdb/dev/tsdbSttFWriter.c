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

#include "dev.h"

extern int32_t tsdbOpenFile(const char *path, int32_t szPage, int32_t flag, STsdbFD **ppFD);
extern void    tsdbCloseFile(STsdbFD **ppFD);
extern int32_t tsdbWriteFile(STsdbFD *pFD, int64_t offset, const uint8_t *pBuf, int64_t size);
extern int32_t tsdbReadFile(STsdbFD *pFD, int64_t offset, uint8_t *pBuf, int64_t size);
extern int32_t tsdbFsyncFile(STsdbFD *pFD);

typedef struct {
  struct {
    int64_t offset;
    int64_t size;
  } dict[4];  // 0:bloom filter, 1:SSttBlk, 2:SDelBlk, 3:STbStatisBlk
  uint8_t reserved[32];
} SFSttFooter;

struct SSttFWriter {
  struct SSttFWriterConf config;
  // data
  SBlockData     bData;
  SDelBlock      dData;
  STbStatisBlock sData;
  SArray        *aSttBlk;     // SArray<SSttBlk>
  SArray        *aDelBlk;     // SArray<SDelBlk>
  SArray        *aStatisBlk;  // SArray<STbStatisBlk>
  void          *bloomFilter;
  SFSttFooter    footer;
  // helper data
  SSkmInfo skmTb;
  SSkmInfo skmRow;
  int32_t  aBufSize[5];
  uint8_t *aBuf[5];
  STsdbFD *pFd;
};

static int32_t write_timeseries_block(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  int32_t lino;

  SBlockData *pBData = &pWriter->bData;
  SSttBlk    *pSttBlk = (SSttBlk *)taosArrayReserve(pWriter->aSttBlk, 1);
  if (pSttBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pSttBlk->suid = pBData->suid;
  pSttBlk->minUid = pBData->aUid[0];
  pSttBlk->maxUid = pBData->aUid[pBData->nRow - 1];
  pSttBlk->minKey = pSttBlk->maxKey = pBData->aTSKEY[0];
  pSttBlk->minVer = pSttBlk->maxVer = pBData->aVersion[0];
  pSttBlk->nRow = pBData->nRow;
  for (int32_t iRow = 1; iRow < pBData->nRow; iRow++) {
    if (pSttBlk->minKey > pBData->aTSKEY[iRow]) pSttBlk->minKey = pBData->aTSKEY[iRow];
    if (pSttBlk->maxKey < pBData->aTSKEY[iRow]) pSttBlk->maxKey = pBData->aTSKEY[iRow];
    if (pSttBlk->minVer > pBData->aVersion[iRow]) pSttBlk->minVer = pBData->aVersion[iRow];
    if (pSttBlk->maxVer < pBData->aVersion[iRow]) pSttBlk->maxVer = pBData->aVersion[iRow];
  }

  // compress data block
  code = tCmprBlockData(pBData, pWriter->config.cmprAlg, NULL, NULL, pWriter->config.aBuf, pWriter->aBufSize);
  TSDB_CHECK_CODE(code, lino, _exit);

  pSttBlk->bInfo.offset = pWriter->config.file.size;
  pSttBlk->bInfo.szKey = pWriter->aBufSize[2] + pWriter->aBufSize[3];
  pSttBlk->bInfo.szBlock = pWriter->aBufSize[0] + pWriter->aBufSize[1] + pSttBlk->bInfo.szKey;

  for (int32_t iBuf = 3; iBuf >= 0; iBuf--) {
    if (pWriter->aBufSize[iBuf]) {
      code =
          tsdbWriteFile(pWriter->pFd, pWriter->config.file.size, pWriter->config.aBuf[iBuf], pWriter->aBufSize[iBuf]);
      TSDB_CHECK_CODE(code, lino, _exit);

      pWriter->config.file.size += pWriter->aBufSize[iBuf];
    }
  }

  tBlockDataClear(pBData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->config.pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    // tsdbTrace();
  }
  return code;
}

static int32_t write_stt_blk(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  int32_t lino;

  if (taosArrayGetSize(pWriter->aSttBlk) == 0) {
    goto _exit;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->config.pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    // tsdbDebug("vgId:%d %s done, offset:%" PRId64 " size:%" PRId64 " # of stt block:%d",
    // TD_VID(pWriter->config.pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t write_del_block(struct SSttFWriter *pWriter) {
  int32_t code = 0;

  // TODO
  return code;
}

static int32_t write_del_blk(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t create_stt_fwriter(const struct SSttFWriterConf *pConf, struct SSttFWriter **ppWriter) {
  int32_t code = 0;

  if ((ppWriter[0] = taosMemoryCalloc(1, sizeof(*ppWriter[0]))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  ppWriter[0]->config = pConf[0];
  if (pConf->pSkmTb == NULL) {
    ppWriter[0]->config.pSkmTb = &ppWriter[0]->skmTb;
  }
  if (pConf->pSkmRow == NULL) {
    ppWriter[0]->config.pSkmRow = &ppWriter[0]->skmRow;
  }
  if (pConf->aBuf == NULL) {
    ppWriter[0]->config.aBuf = ppWriter[0]->aBuf;
  }

  tBlockDataCreate(&ppWriter[0]->bData);
  ppWriter[0]->aSttBlk = taosArrayInit(64, sizeof(SSttBlk));
  if (ppWriter[0]->aSttBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  if (code && ppWriter[0]) {
    taosArrayDestroy(ppWriter[0]->aSttBlk);
    tBlockDataDestroy(&ppWriter[0]->bData);
    taosMemoryFree(ppWriter[0]);
    ppWriter[0] = NULL;
  }
  return code;
}

static int32_t destroy_stt_fwriter(struct SSttFWriter *pWriter) {
  if (pWriter) {
    tDestroyTSchema(pWriter->skmTb.pTSchema);
    tDestroyTSchema(pWriter->skmRow.pTSchema);
    for (int32_t i = 0; i < sizeof(pWriter->aBuf) / sizeof(pWriter->aBuf[0]); i++) taosMemoryFree(pWriter->aBuf[i]);
    taosArrayDestroy(pWriter->aSttBlk);
    tBlockDataDestroy(&pWriter->bData);
    taosMemoryFree(pWriter);
  }
  return 0;
}

static int32_t open_stt_fwriter(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  int32_t lino;

  int32_t flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;  // TODO

  code = tsdbOpenFile(pWriter->config.file.fname, pWriter->config.szPage, flag, &pWriter->pFd);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->config.pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t close_stt_fwriter(struct SSttFWriter *pWriter) {
  tsdbCloseFile(&pWriter->pFd);
  return 0;
}

int32_t tsdbSttFWriterOpen(const struct SSttFWriterConf *pConf, struct SSttFWriter **ppWriter) {
  int32_t code = 0;
  int32_t lino;

  code = create_stt_fwriter(pConf, ppWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = open_stt_fwriter(ppWriter[0]);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pConf->pTsdb->pVnode), __func__, lino, tstrerror(code));
    if (ppWriter[0]) {
      taosMemoryFree(ppWriter[0]);
      ppWriter[0] = NULL;
    }
  }
  return code;
}

int32_t tsdbSttFWriterClose(struct SSttFWriter **ppWriter) {
  int32_t vgId = TD_VID(ppWriter[0]->config.pTsdb->pVnode);
  int32_t code = 0;
  int32_t lino;

  if (ppWriter[0]->bData.nRow > 0) {
    code = write_timeseries_block(ppWriter[0]);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = write_stt_blk(ppWriter[0]);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = write_del_blk(ppWriter[0]);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = close_stt_fwriter(ppWriter[0]);
  TSDB_CHECK_CODE(code, lino, _exit);

  destroy_stt_fwriter(ppWriter[0]);
  ppWriter[0] = NULL;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vgId, __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriteTSData(struct SSttFWriter *pWriter, TABLEID *tbid, TSDBROW *pRow) {
  int32_t code = 0;
  int32_t lino;

  if (!TABLE_SAME_SCHEMA(pWriter->bData.suid, pWriter->bData.uid, tbid->suid, tbid->uid)) {
    if (pWriter->bData.nRow > 0) {
      code = write_timeseries_block(pWriter);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbUpdateSkmTb(pWriter->config.pTsdb, tbid, pWriter->config.pSkmTb);
    TSDB_CHECK_CODE(code, lino, _exit);

    TABLEID id = {.suid = tbid->suid, .uid = tbid->suid ? 0 : tbid->uid};
    code = tBlockDataInit(&pWriter->bData, &id, pWriter->config.pSkmTb->pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pRow->type == TSDBROW_ROW_FMT) {
    code = tsdbUpdateSkmRow(pWriter->config.pTsdb, tbid, TSDBROW_SVERSION(pRow), pWriter->config.pSkmRow);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataAppendRow(&pWriter->bData, pRow, pWriter->config.pSkmRow->pTSchema, tbid->uid);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->bData.nRow >= pWriter->config.maxRow) {
    code = write_timeseries_block(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->config.pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriteDLData(struct SSttFWriter *pWriter, TABLEID *tbid, SDelData *pDelData) {
  int32_t code = 0;
  // TODO
  return code;
}
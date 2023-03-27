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

struct SSttFWriter {
  struct SSttFWriterConf config;
  // time-series data
  SBlockData bData;
  SArray    *aSttBlk;  // SArray<SSttBlk>
  // tombstone data
  SDelBlock dData;
  SArray   *aDelBlk;  // SArray<SDelBlk>
  // helper data
  SSkmInfo skmTb;
  SSkmInfo skmRow;
  STsdbFD *pFd;
};

static int32_t write_ts_block(struct SSttFWriter *pWriter) {
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
  pSttBlk->minVer = pSttBlk->maxVer = pBData->aTSKEY[0];
  pSttBlk->nRow = pBData->nRow;
  for (int32_t iRow = 1; iRow < pBData->nRow; iRow++) {
    pSttBlk->minKey = TMIN(pSttBlk->minKey, pBData->aTSKEY[iRow]);
    pSttBlk->maxKey = TMAX(pSttBlk->maxKey, pBData->aTSKEY[iRow]);
    pSttBlk->minVer = TMIN(pSttBlk->minVer, pBData->aVersion[iRow]);
    pSttBlk->maxVer = TMAX(pSttBlk->maxVer, pBData->aVersion[iRow]);
  }

  // compress data block
  code = tCmprBlockData(pBData, pWriter->config.cmprAlg, NULL, NULL, NULL /* TODO */, NULL /* TODO */);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBlockDataClear(pBData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->config.pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t write_del_block(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t write_stt_blk(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t write_del_blk(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t stt_fwriter_create(const struct SSttFWriterConf *pConf, struct SSttFWriter **ppWriter) {
  int32_t code = 0;

  if ((ppWriter[0] = taosMemoryCalloc(1, sizeof(*ppWriter[0]))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  ppWriter[0]->config = pConf[0];
  if (pConf->pSkmRow == NULL) {
    ppWriter[0]->config.pSkmRow = &ppWriter[0]->skmRow;
  }
  if (pConf->pSkmTb == NULL) {
    ppWriter[0]->config.pSkmTb = &ppWriter[0]->skmTb;
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

static int32_t stt_fwriter_destroy(struct SSttFWriter *pWriter) {
  if (pWriter) {
    tDestroyTSchema(pWriter->skmTb.pTSchema);
    tDestroyTSchema(pWriter->skmRow.pTSchema);
    taosArrayDestroy(pWriter->aSttBlk);
    tBlockDataDestroy(&pWriter->bData);
    taosMemoryFree(pWriter);
  }
  return 0;
}

static int32_t stt_fwriter_open(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t stt_fwriter_close(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFWriterOpen(const struct SSttFWriterConf *pConf, struct SSttFWriter **ppWriter) {
  int32_t code = 0;
  int32_t lino;

  code = stt_fwriter_create(pConf, ppWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = stt_fwriter_open(ppWriter[0]);
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

  code = stt_fwriter_close(ppWriter[0]);
  TSDB_CHECK_CODE(code, lino, _exit);

  stt_fwriter_close(ppWriter[0]);

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
      code = write_ts_block(pWriter);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // TODO: code = tsdbUpdateTableSchema(pWriter->config.pTsdb, tbid->uid, tbid->suid, pWriter->config.pSkmTb);
    TSDB_CHECK_CODE(code, lino, _exit);

    TABLEID id = {.suid = tbid->suid, .uid = tbid->suid ? 0 : tbid->uid};
    code = tBlockDataInit(&pWriter->bData, &id, pWriter->config.pSkmTb->pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pRow->type == TSDBROW_ROW_FMT) {
    // TODO: code = tsdbUpdateRowSchema(pWriter->config.pTsdb, tbid->uid, tbid->suid, pRow->row,
    // pWriter->config.pSkmRow);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataAppendRow(&pWriter->bData, pRow, pWriter->config.pSkmRow->pTSchema, tbid->uid);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->bData.nRow >= pWriter->config.maxRow) {
    code = write_ts_block(pWriter);
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
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

#include "tsdbSttFWriter.h"
#include "tsdbUtil.h"

extern int32_t tsdbOpenFile(const char *path, int32_t szPage, int32_t flag, STsdbFD **ppFD);
extern void    tsdbCloseFile(STsdbFD **ppFD);
extern int32_t tsdbWriteFile(STsdbFD *pFD, int64_t offset, const uint8_t *pBuf, int64_t size);
extern int32_t tsdbReadFile(STsdbFD *pFD, int64_t offset, uint8_t *pBuf, int64_t size);
extern int32_t tsdbFsyncFile(STsdbFD *pFD);

struct SSttFWriter {
  SSttFWriterConf config;
  // time-series data
  SBlockData bData;
  SSttBlk    sttBlk;
  SArray    *aSttBlk;  // SArray<SSttBlk>
  // tombstone data
  SDelBlock dData;
  SArray   *aDelBlk;  // SArray<SDelBlk>
  // helper data
  SSkmInfo skmTb;
  SSkmInfo skmRow;
  STsdbFD *pFd;
};

static int32_t tsdbSttFWriteTSBlock(SSttFWriter *pWriter) {
  int32_t code = 0;
  int32_t lino;

  SBlockData *pBData = &pWriter->bData;

  // compress data block
  code = tCmprBlockData(pBData, pWriter->config.cmprAlg, NULL, NULL, NULL /* TODO */, NULL /* TODO */);
  TSDB_CHECK_CODE(code, lino, _exit);

  TSDB_CHECK_NULL(taosArrayPush(pWriter->aSttBlk, &pWriter->sttBlk), code, lino, _exit, TSDB_CODE_OUT_OF_MEMORY);

  tBlockDataClear(pBData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->config.pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriterOpen(const SSttFWriterConf *pConf, SSttFWriter **ppWriter) {
  int32_t code = 0;
  int32_t lino;

  if ((ppWriter[0] = taosMemoryCalloc(1, sizeof(SSttFWriter))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  ppWriter[0]->config = pConf[0];
  if (ppWriter[0]->config.pSkmTb == NULL) ppWriter[0]->config.pSkmTb = &ppWriter[0]->skmTb;
  if (ppWriter[0]->config.pSkmRow == NULL) ppWriter[0]->config.pSkmRow = &ppWriter[0]->skmRow;

  tBlockDataCreate(&ppWriter[0]->bData);
  //   tDelBlockCreate(&ppWriter[0]->dData);

  int32_t flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;  // TODO
  code = tsdbOpenFile(NULL /* TODO */, pConf->szPage, flag, &ppWriter[0]->pFd);
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

int32_t tsdbSttFWriterClose(SSttFWriter **ppWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFWriteTSData(SSttFWriter *pWriter, TABLEID *tbid, TSDBROW *pRow) {
  int32_t code = 0;
  int32_t lino;

  if (!TABLE_SAME_SCHEMA(pWriter->bData.suid, pWriter->bData.uid, tbid->suid, tbid->uid)) {
    if (pWriter->bData.nRow > 0) {
      code = tsdbSttFWriteTSBlock(pWriter);
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
    code = tsdbSttFWriteTSBlock(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->config.pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriteDLData(SSttFWriter *pWriter, TABLEID *tbid, SDelData *pDelData) {
  int32_t code = 0;
  // TODO
  return code;
}
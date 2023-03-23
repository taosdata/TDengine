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

int32_t tsdbSttFWriterOpen(const SSttFWriterConf *pConf, SSttFWriter **ppWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFWriterClose(SSttFWriter **ppWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFWriteTSData(SSttFWriter *pWriter, TABLEID *tbid, TSDBROW *pRow) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFWriteDLData(SSttFWriter *pWriter, TABLEID *tbid, SDelData *pDelData) {
  int32_t code = 0;
  // TODO
  return code;
}

#if 0
extern int32_t tsdbOpenFile(const char *path, int32_t szPage, int32_t flag, STsdbFD **ppFD);
extern void    tsdbCloseFile(STsdbFD **ppFD);
struct SSttFWriter {
  STsdb     *pTsdb;
  SSttFile   file;
  SBlockData bData;
  SArray    *aSttBlk;
  STsdbFD   *pFd;
  SSkmInfo  *pSkmTb;
  SSkmInfo  *pSkmRow;
  int32_t    maxRow;
};

static int32_t tsdbSttFWriteTSDataBlock(SSttFWriter *pWriter) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFWriterOpen(STsdb *pTsdb, const SSttFile *pSttFile, SSttFWriter **ppWriter) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t szPage = pTsdb->pVnode->config.tsdbPageSize;
  int32_t flag = TD_FILE_READ | TD_FILE_WRITE;
  char    fname[TSDB_FILENAME_LEN];

  ppWriter[0] = taosMemoryCalloc(1, sizeof(SSttFWriter));
  if (ppWriter[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  ppWriter[0]->pTsdb = pTsdb;
  ppWriter[0]->file = pSttFile[0];

  tBlockDataCreate(&ppWriter[0]->bData);
  ppWriter[0]->aSttBlk = taosArrayInit(64, sizeof(SSttBlk));
  if (ppWriter[0]->aSttBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pSttFile->size == 0) flag |= (TD_FILE_CREATE | TD_FILE_TRUNC);
  tsdbSttFileName(pTsdb, (SDiskID){0}, 0, &ppWriter[0]->file, fname);  // TODO
  code = tsdbOpenFile(fname, szPage, flag, &ppWriter[0]->pFd);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    if (ppWriter[0]) {
      tBlockDataDestroy(&ppWriter[0]->bData);
      taosArrayDestroy(ppWriter[0]->aSttBlk);
      taosMemoryFree(ppWriter[0]);
      ppWriter[0] = NULL;
    }
  }
  return 0;
}

int32_t tsdbSttFWriterClose(SSttFWriter *pWriter) {
  // TODO
  return 0;
}

int32_t tsdbSttFWriteTSData(SSttFWriter *pWriter, TABLEID *tbid, TSDBROW *pRow) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!TABLE_SAME_SCHEMA(pWriter->bData.suid, pWriter->bData.uid, tbid->suid, tbid->uid)) {
    if (pWriter->bData.nRow > 0) {
      code = tsdbSttFWriteTSDataBlock(pWriter);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataInit(&pWriter->bData, tbid, NULL /* TODO */, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataAppendRow(&pWriter->bData, pRow, NULL /* TODO */, tbid->uid);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->bData.nRow >= pWriter->maxRow) {
    code = tsdbSttFWriteTSDataBlock(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return 0;
}

int32_t tsdbSttFWriteDLData(SSttFWriter *pWriter, int64_t suid, int64_t uid, int64_t version) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO write row
  return 0;
}
#endif
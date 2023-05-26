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

typedef struct {
  int64_t   prevFooter;
  SFDataPtr dict[4];  // 0:bloom filter, 1:SSttBlk, 2:STbStatisBlk, 3:SDelBlk
  uint8_t   reserved[24];
} SFSttFooter;

// SSttFReader ============================================================
struct SSttFileReader {
  SSttFileReaderConfig *config;
  // TODO
};

// SSttFileReader
int32_t tsdbSttFReaderOpen(const SSttFileReaderConfig *config, SSttFileReader **ppReader) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFReaderClose(SSttFileReader **ppReader) {
  int32_t code = 0;
  // TODO
  return code;
}

// SSttFSegReader
int32_t tsdbSttFSegReaderOpen(SSttFileReader *pReader, SSttFSegReader **ppSegReader, int32_t nSegment) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFSegReaderClose(SSttFSegReader **ppSegReader) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFSegReadBloomFilter(SSttFSegReader *pSegReader, const void *pFilter) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFSegReadStatisBlk(SSttFSegReader *pSegReader, const SArray *pStatis) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFSegReadDelBlk(SSttFSegReader *pSegReader, const SArray *pDelBlk) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFSegReadSttBlk(SSttFSegReader *pSegReader, const SArray *pSttBlk) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFSegReadStatisBlock(SSttFSegReader *pSegReader, const void *pBlock) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFSegReadDelBlock(SSttFSegReader *pSegReader, const void *pBlock) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFSegReadSttBlock(SSttFSegReader *pSegReader, const void *pBlock) {
  int32_t code = 0;
  // TODO
  return code;
}

// SSttFWriter ============================================================
struct SSttFileWriter {
  SSttFileWriterConfig config;
  struct {
    bool opened;
  } ctx;
  // file
  STFile file;
  // data
  TARRAY2(SSttBlk) sttBlkArray;
  TARRAY2(SDelBlk) delBlkArray;
  TARRAY2(STbStatisBlk) statisBlkArray;
  void          *bloomFilter;  // TODO
  SFSttFooter    footer;
  SBlockData     bData[1];
  SDelBlock      dData[1];
  STbStatisBlock sData[1];
  // helper data
  SSkmInfo skmTb;
  SSkmInfo skmRow;
  int32_t  aBufSize[5];
  uint8_t *aBuf[5];
  STsdbFD *fd;
};

static int32_t tsdbSttFileDoWriteTSDataBlock(SSttFileWriter *writer) {
  if (writer->bData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;
  SSttBlk sttBlk[1];

  sttBlk->suid = writer->bData->suid;
  sttBlk->minUid = writer->bData->uid ? writer->bData->uid : writer->bData->aUid[0];
  sttBlk->maxUid = writer->bData->uid ? writer->bData->uid : writer->bData->aUid[writer->bData->nRow - 1];
  sttBlk->minKey = sttBlk->maxKey = writer->bData->aTSKEY[0];
  sttBlk->minVer = sttBlk->maxVer = writer->bData->aVersion[0];
  sttBlk->nRow = writer->bData->nRow;
  for (int32_t iRow = 1; iRow < writer->bData->nRow; iRow++) {
    if (sttBlk->minKey > writer->bData->aTSKEY[iRow]) sttBlk->minKey = writer->bData->aTSKEY[iRow];
    if (sttBlk->maxKey < writer->bData->aTSKEY[iRow]) sttBlk->maxKey = writer->bData->aTSKEY[iRow];
    if (sttBlk->minVer > writer->bData->aVersion[iRow]) sttBlk->minVer = writer->bData->aVersion[iRow];
    if (sttBlk->maxVer < writer->bData->aVersion[iRow]) sttBlk->maxVer = writer->bData->aVersion[iRow];
  }

  code = tCmprBlockData(writer->bData, writer->config.cmprAlg, NULL, NULL, writer->config.aBuf, writer->aBufSize);
  TSDB_CHECK_CODE(code, lino, _exit);

  sttBlk->bInfo.offset = writer->file.size;
  sttBlk->bInfo.szKey = writer->aBufSize[2] + writer->aBufSize[3];
  sttBlk->bInfo.szBlock = writer->aBufSize[0] + writer->aBufSize[1] + sttBlk->bInfo.szKey;

  for (int32_t i = 3; i >= 0; i--) {
    if (writer->aBufSize[i]) {
      code = tsdbWriteFile(writer->fd, writer->file.size, writer->config.aBuf[i], writer->aBufSize[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
      writer->file.size += writer->aBufSize[i];
    }
  }
  tBlockDataClear(writer->bData);

  code = TARRAY2_APPEND_P(&writer->sttBlkArray, sttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config.tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteStatisBlock(SSttFileWriter *writer) {
  if (writer->sData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STbStatisBlk statisBlk[1];

  statisBlk->nRow = writer->sData->nRow;
  statisBlk->minTid.suid = writer->sData->aData[0][0];
  statisBlk->minTid.uid = writer->sData->aData[1][0];
  statisBlk->maxTid.suid = writer->sData->aData[0][writer->sData->nRow - 1];
  statisBlk->maxTid.uid = writer->sData->aData[1][writer->sData->nRow - 1];
  statisBlk->minVer = statisBlk->maxVer = statisBlk->maxVer = writer->sData->aData[2][0];
  for (int32_t iRow = 1; iRow < writer->sData->nRow; iRow++) {
    if (statisBlk->minVer > writer->sData->aData[2][iRow]) statisBlk->minVer = writer->sData->aData[2][iRow];
    if (statisBlk->maxVer < writer->sData->aData[2][iRow]) statisBlk->maxVer = writer->sData->aData[2][iRow];
  }

  statisBlk->dp.offset = writer->file.size;
  statisBlk->dp.size = 0;

  // TODO: add compression here
  int64_t tsize = sizeof(int64_t) * writer->sData->nRow;
  for (int32_t i = 0; i < ARRAY_SIZE(writer->sData->aData); i++) {
    code = tsdbWriteFile(writer->fd, writer->file.size, (const uint8_t *)writer->sData->aData[i], tsize);
    TSDB_CHECK_CODE(code, lino, _exit);

    statisBlk->dp.size += tsize;
    writer->file.size += tsize;
  }
  tTbStatisBlockClear(writer->sData);

  code = TARRAY2_APPEND_P(&writer->statisBlkArray, statisBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config.tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteDelBlock(SSttFileWriter *writer) {
  if (writer->dData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino;

  SDelBlk delBlk[1];

  delBlk->nRow = writer->sData->nRow;
  delBlk->minTid.suid = writer->sData->aData[0][0];
  delBlk->minTid.uid = writer->sData->aData[1][0];
  delBlk->maxTid.suid = writer->sData->aData[0][writer->sData->nRow - 1];
  delBlk->maxTid.uid = writer->sData->aData[1][writer->sData->nRow - 1];
  delBlk->minVer = delBlk->maxVer = delBlk->maxVer = writer->sData->aData[2][0];
  for (int32_t iRow = 1; iRow < writer->sData->nRow; iRow++) {
    if (delBlk->minVer > writer->sData->aData[2][iRow]) delBlk->minVer = writer->sData->aData[2][iRow];
    if (delBlk->maxVer < writer->sData->aData[2][iRow]) delBlk->maxVer = writer->sData->aData[2][iRow];
  }

  delBlk->dp.offset = writer->file.size;
  delBlk->dp.size = 0;  // TODO

  int64_t tsize = sizeof(int64_t) * writer->dData->nRow;
  for (int32_t i = 0; i < ARRAY_SIZE(writer->dData->aData); i++) {
    code = tsdbWriteFile(writer->fd, writer->file.size, (const uint8_t *)writer->dData->aData[i], tsize);
    TSDB_CHECK_CODE(code, lino, _exit);

    delBlk->dp.size += tsize;
    writer->file.size += tsize;
  }
  tDelBlockDestroy(writer->dData);

  code = TARRAY2_APPEND_P(&writer->delBlkArray, delBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config.tsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    // tsdbTrace();
  }
  return code;
}

static int32_t tsdbSttFileDoWriteSttBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;

  writer->footer.dict[1].offset = writer->file.size;
  writer->footer.dict[1].size = sizeof(SSttBlk) * TARRAY2_SIZE(&writer->sttBlkArray);

  if (writer->footer.dict[1].size) {
    code = tsdbWriteFile(writer->fd, writer->file.size, (const uint8_t *)TARRAY2_DATA(&writer->sttBlkArray),
                         writer->footer.dict[1].size);
    TSDB_CHECK_CODE(code, lino, _exit);

    writer->file.size += writer->footer.dict[1].size;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config.tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteStatisBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;

  writer->footer.dict[2].offset = writer->file.size;
  writer->footer.dict[2].size = sizeof(STbStatisBlock) * TARRAY2_SIZE(&writer->statisBlkArray);

  if (writer->footer.dict[2].size) {
    code = tsdbWriteFile(writer->fd, writer->file.size, (const uint8_t *)TARRAY2_DATA(&writer->statisBlkArray),
                         writer->footer.dict[2].size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file.size += writer->footer.dict[2].size;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config.tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteDelBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;

  writer->footer.dict[3].offset = writer->file.size;
  writer->footer.dict[3].size = sizeof(SDelBlk) * TARRAY2_SIZE(&writer->delBlkArray);

  if (writer->footer.dict[3].size) {
    code = tsdbWriteFile(writer->fd, writer->file.size, (const uint8_t *)TARRAY2_DATA(&writer->delBlkArray),
                         writer->footer.dict[3].size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file.size += writer->footer.dict[3].size;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config.tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteFooter(SSttFileWriter *writer) {
  int32_t code = tsdbWriteFile(writer->fd, writer->file.size, (const uint8_t *)&writer->footer, sizeof(writer->footer));
  writer->file.size += sizeof(writer->footer);
  return code;
}

static int32_t tsdbSttFWriterDoOpen(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config.tsdb->pVnode);

  // set
  writer->file = writer->config.file;
  writer->file.stt.nseg++;
  if (!writer->config.skmTb) writer->config.skmTb = &writer->skmTb;
  if (!writer->config.skmRow) writer->config.skmRow = &writer->skmRow;
  if (!writer->config.aBuf) writer->config.aBuf = writer->aBuf;

  // open file
  int32_t flag;
  char    fname[TSDB_FILENAME_LEN];

  if (writer->file.size) {
    flag = TD_FILE_READ | TD_FILE_WRITE;
  } else {
    flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  }

  tsdbTFileName(writer->config.tsdb, &writer->file, fname);
  code = tsdbOpenFile(fname, writer->config.szPage, flag, &writer->fd);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (!writer->file.size) {
    uint8_t hdr[TSDB_FHDR_SIZE] = {0};

    code = tsdbWriteFile(writer->fd, 0, hdr, sizeof(hdr));
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file.size += sizeof(hdr);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    writer->ctx.opened = true;
  }
  return 0;
}

static void tsdbSttFWriterDoClose(SSttFileWriter *pWriter) {
  // TODO: do clear the struct
}

static int32_t tsdbSttFileDoWriteBloomFilter(SSttFileWriter *writer) {
  // TODO
  return 0;
}

static int32_t tsdbSttFileDoUpdateHeader(SSttFileWriter *writer) {
  // TODO
  return 0;
}

static int32_t tsdbSttFWriterCloseCommit(SSttFileWriter *writer, STFileOp *op) {
  int32_t lino;
  int32_t code;
  int32_t vid = TD_VID(writer->config.tsdb->pVnode);

  code = tsdbSttFileDoWriteTSDataBlock(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteStatisBlock(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteDelBlock(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteSttBlk(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteStatisBlk(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteDelBlk(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteBloomFilter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteFooter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoUpdateHeader(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFsyncFile(writer->fd);
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbCloseFile(&writer->fd);

  ASSERT(writer->config.file.size > writer->file.size);
  op->optype = writer->config.file.size ? TSDB_FOP_MODIFY : TSDB_FOP_CREATE;
  op->fid = writer->config.file.fid;
  op->of = writer->config.file;
  op->nf = writer->file;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFWriterCloseAbort(SSttFileWriter *writer) {
  char fname[TSDB_FILENAME_LEN];

  tsdbTFileName(writer->config.tsdb, &writer->config.file, fname);
  if (writer->config.file.size) {  // truncate the file to the original size
    ASSERT(writer->config.file.size <= writer->file.size);
    if (writer->config.file.size < writer->file.size) {
      taosFtruncateFile(writer->fd->pFD, writer->config.file.size);
      tsdbCloseFile(&writer->fd);
    }
  } else {  // remove the file
    tsdbCloseFile(&writer->fd);
    taosRemoveFile(fname);
  }

  return 0;
}

int32_t tsdbSttFWriterOpen(const SSttFileWriterConfig *config, SSttFileWriter **writer) {
  writer[0] = taosMemoryMalloc(sizeof(*writer[0]));
  if (writer[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  writer[0]->config = config[0];
  writer[0]->ctx.opened = false;
  return 0;
}

int32_t tsdbSttFWriterClose(SSttFileWriter **writer, int8_t abort, STFileOp *op) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer[0]->config.tsdb->pVnode);

  if (!writer[0]->ctx.opened) {
    op->optype = TSDB_FOP_NONE;
  } else {
    if (abort) {
      code = tsdbSttFWriterCloseAbort(writer[0]);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbSttFWriterCloseCommit(writer[0], op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tsdbSttFWriterDoClose(writer[0]);
  }
  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriteTSData(SSttFileWriter *writer, SRowInfo *row) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer->ctx.opened) {
    code = tsdbSttFWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  TABLEID *tbid = (TABLEID *)row;
  TSDBROW *pRow = &row->row;
  TSDBKEY  key = TSDBROW_KEY(pRow);
  if (!TABLE_SAME_SCHEMA(writer->bData[0].suid, writer->bData[0].uid, tbid->suid, tbid->uid)) {
    if (writer->bData[0].nRow > 0) {
      code = tsdbSttFileDoWriteTSDataBlock(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (writer->sData[0].nRow >= writer->config.maxRow) {
      code = tsdbSttFileDoWriteStatisBlock(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    writer->sData[0].aData[0][writer->sData[0].nRow] = tbid->suid;   // suid
    writer->sData[0].aData[1][writer->sData[0].nRow] = tbid->uid;    // uid
    writer->sData[0].aData[2][writer->sData[0].nRow] = key.ts;       // skey
    writer->sData[0].aData[3][writer->sData[0].nRow] = key.version;  // sver
    writer->sData[0].aData[4][writer->sData[0].nRow] = key.ts;       // ekey
    writer->sData[0].aData[5][writer->sData[0].nRow] = key.version;  // ever
    writer->sData[0].aData[6][writer->sData[0].nRow] = 1;            // count
    writer->sData[0].nRow++;

    code = tsdbUpdateSkmTb(writer->config.tsdb, tbid, writer->config.skmTb);
    TSDB_CHECK_CODE(code, lino, _exit);

    TABLEID id = {
        .suid = tbid->suid,
        .uid = tbid->uid ? 0 : tbid->uid,
    };
    code = tBlockDataInit(&writer->bData[0], &id, writer->config.skmTb->pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (row->row.type == TSDBROW_ROW_FMT) {
    code = tsdbUpdateSkmRow(writer->config.tsdb, tbid, TSDBROW_SVERSION(pRow), writer->config.skmRow);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataAppendRow(&writer->bData[0], pRow, writer->config.skmRow->pTSchema, tbid->uid);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (writer->bData[0].nRow >= writer->config.maxRow) {
    code = tsdbSttFileDoWriteTSDataBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (key.ts > writer->sData[0].aData[4][writer->sData[0].nRow - 1]) {
    writer->sData[0].aData[4][writer->sData[0].nRow - 1] = key.ts;       // ekey
    writer->sData[0].aData[5][writer->sData[0].nRow - 1] = key.version;  // ever
    writer->sData[0].aData[6][writer->sData[0].nRow - 1]++;              // count
  } else if (key.ts == writer->sData[0].aData[4][writer->sData[0].nRow - 1]) {
    writer->sData[0].aData[4][writer->sData[0].nRow - 1] = key.ts;       // ekey
    writer->sData[0].aData[5][writer->sData[0].nRow - 1] = key.version;  // ever
  } else {
    ASSERTS(0, "timestamp should be in ascending order");
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config.tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriteTSDataBlock(SSttFileWriter *writer, SBlockData *bdata) {
  int32_t code = 0;
  int32_t lino = 0;

  SRowInfo rowInfo;
  rowInfo.suid = bdata->suid;
  for (int32_t i = 0; i < bdata->nRow; i++) {
    rowInfo.uid = bdata->uid ? bdata->uid : bdata->aUid[i];
    rowInfo.row = tsdbRowFromBlockData(bdata, i);

    code = tsdbSttFWriteTSData(writer, &rowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config.tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return 0;
}

int32_t tsdbSttFWriteDLData(SSttFileWriter *writer, TABLEID *tbid, SDelData *pDelData) {
  ASSERTS(0, "TODO: Not implemented yet");

  int32_t code;
  if (!writer->ctx.opened) {
    code = tsdbSttFWriterDoOpen(writer);
    return code;
  }

  writer->dData[0].aData[0][writer->dData[0].nRow] = tbid->suid;         // suid
  writer->dData[0].aData[1][writer->dData[0].nRow] = tbid->uid;          // uid
  writer->dData[0].aData[2][writer->dData[0].nRow] = pDelData->version;  // version
  writer->dData[0].aData[3][writer->dData[0].nRow] = pDelData->sKey;     // skey
  writer->dData[0].aData[4][writer->dData[0].nRow] = pDelData->eKey;     // ekey
  writer->dData[0].nRow++;

  if (writer->dData[0].nRow >= writer->config.maxRow) {
    return tsdbSttFileDoWriteDelBlock(writer);
  } else {
    return 0;
  }
}
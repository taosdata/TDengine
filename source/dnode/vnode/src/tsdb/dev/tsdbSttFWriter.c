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
  int64_t   prevFooter;
  SFDataPtr dict[4];  // 0:bloom filter, 1:SSttBlk, 2:STbStatisBlk, 3:SDelBlk
  uint8_t   reserved[24];
} SFSttFooter;

struct SSttFWriter {
  struct SSttFWriterConf config;
  // file
  struct STFile tFile;
  // data
  SFSttFooter    footer;
  SBlockData     bData;
  SDelBlock      dData;
  STbStatisBlock sData;
  SArray        *aSttBlk;      // SArray<SSttBlk>
  SArray        *aDelBlk;      // SArray<SDelBlk>
  SArray        *aStatisBlk;   // SArray<STbStatisBlk>
  void          *bloomFilter;  // TODO
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
  pSttBlk->minUid = pBData->uid ? pBData->uid : pBData->aUid[0];
  pSttBlk->maxUid = pBData->uid ? pBData->uid : pBData->aUid[pBData->nRow - 1];
  pSttBlk->minKey = pSttBlk->maxKey = pBData->aTSKEY[0];
  pSttBlk->minVer = pSttBlk->maxVer = pBData->aVersion[0];
  pSttBlk->nRow = pBData->nRow;
  for (int32_t iRow = 1; iRow < pBData->nRow; iRow++) {
    if (pSttBlk->minKey > pBData->aTSKEY[iRow]) pSttBlk->minKey = pBData->aTSKEY[iRow];
    if (pSttBlk->maxKey < pBData->aTSKEY[iRow]) pSttBlk->maxKey = pBData->aTSKEY[iRow];
    if (pSttBlk->minVer > pBData->aVersion[iRow]) pSttBlk->minVer = pBData->aVersion[iRow];
    if (pSttBlk->maxVer < pBData->aVersion[iRow]) pSttBlk->maxVer = pBData->aVersion[iRow];
  }

  TSDB_CHECK_CODE(                  //
      code = tCmprBlockData(        //
          pBData,                   //
          pWriter->config.cmprAlg,  //
          NULL,                     //
          NULL,                     //
          pWriter->config.aBuf,     //
          pWriter->aBufSize),       //
      lino,                         //
      _exit);

  pSttBlk->bInfo.offset = pWriter->tFile.size;
  pSttBlk->bInfo.szKey = pWriter->aBufSize[2] + pWriter->aBufSize[3];
  pSttBlk->bInfo.szBlock = pWriter->aBufSize[0] + pWriter->aBufSize[1] + pSttBlk->bInfo.szKey;

  for (int32_t iBuf = 3; iBuf >= 0; iBuf--) {
    if (pWriter->aBufSize[iBuf]) {
      TSDB_CHECK_CODE(                     //
          code = tsdbWriteFile(            //
              pWriter->pFd,                //
              pWriter->tFile.size,         //
              pWriter->config.aBuf[iBuf],  //
              pWriter->aBufSize[iBuf]),    //
          lino,                            //
          _exit);

      pWriter->tFile.size += pWriter->aBufSize[iBuf];
    }
  }

  tBlockDataClear(pBData);

_exit:
  if (code) {
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pWriter->config.pTsdb->pVnode),    //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  } else {
    // tsdbTrace();
  }
  return code;
}

static int32_t write_statistics_block(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  int32_t lino;

  STbStatisBlk *pStatisBlk = (STbStatisBlk *)taosArrayReserve(pWriter->aStatisBlk, 1);
  if (pStatisBlk == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  pStatisBlk->nRow = pWriter->sData.nRow;
  pStatisBlk->minTid.suid = pWriter->sData.aData[0][0];
  pStatisBlk->minTid.uid = pWriter->sData.aData[1][0];
  pStatisBlk->maxTid.suid = pWriter->sData.aData[0][pWriter->sData.nRow - 1];
  pStatisBlk->maxTid.uid = pWriter->sData.aData[1][pWriter->sData.nRow - 1];
  pStatisBlk->minVer = pStatisBlk->maxVer = pStatisBlk->maxVer = pWriter->sData.aData[2][0];
  for (int32_t iRow = 1; iRow < pWriter->sData.nRow; iRow++) {
    if (pStatisBlk->minVer > pWriter->sData.aData[2][iRow]) pStatisBlk->minVer = pWriter->sData.aData[2][iRow];
    if (pStatisBlk->maxVer < pWriter->sData.aData[2][iRow]) pStatisBlk->maxVer = pWriter->sData.aData[2][iRow];
  }

  pStatisBlk->dp.offset = pWriter->tFile.size;
  pStatisBlk->dp.size = 0;

  // TODO: add compression here
  int64_t tsize = sizeof(int64_t) * pWriter->sData.nRow;
  for (int32_t i = 0; i < ARRAY_SIZE(pWriter->sData.aData); i++) {
    TSDB_CHECK_CODE(                                   //
        code = tsdbWriteFile(                          //
            pWriter->pFd,                              //
            pWriter->tFile.size,                       //
            (const uint8_t *)pWriter->sData.aData[i],  //
            tsize),                                    //
        lino,                                          //
        _exit);

    pStatisBlk->dp.size += tsize;
    pWriter->tFile.size += tsize;
  }

  tTbStatisBlockClear(&pWriter->sData);

_exit:
  if (code) {
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pWriter->config.pTsdb->pVnode),    //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  } else {
    // tsdbTrace();
  }
  return code;
}

static int32_t write_delete_block(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  int32_t lino;

  ASSERTS(0, "TODO: Not implemented yet");

  SDelBlk *pDelBlk = taosArrayReserve(pWriter->aDelBlk, 1);
  if (pDelBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pDelBlk->nRow = pWriter->sData.nRow;
  pDelBlk->minTid.suid = pWriter->sData.aData[0][0];
  pDelBlk->minTid.uid = pWriter->sData.aData[1][0];
  pDelBlk->maxTid.suid = pWriter->sData.aData[0][pWriter->sData.nRow - 1];
  pDelBlk->maxTid.uid = pWriter->sData.aData[1][pWriter->sData.nRow - 1];
  pDelBlk->minVer = pDelBlk->maxVer = pDelBlk->maxVer = pWriter->sData.aData[2][0];
  for (int32_t iRow = 1; iRow < pWriter->sData.nRow; iRow++) {
    if (pDelBlk->minVer > pWriter->sData.aData[2][iRow]) pDelBlk->minVer = pWriter->sData.aData[2][iRow];
    if (pDelBlk->maxVer < pWriter->sData.aData[2][iRow]) pDelBlk->maxVer = pWriter->sData.aData[2][iRow];
  }

  pDelBlk->dp.offset = pWriter->tFile.size;
  pDelBlk->dp.size = 0;  // TODO

  int64_t tsize = sizeof(int64_t) * pWriter->dData.nRow;
  for (int32_t i = 0; i < ARRAY_SIZE(pWriter->dData.aData); i++) {
    code = tsdbWriteFile(pWriter->pFd, pWriter->tFile.size, (const uint8_t *)pWriter->dData.aData[i], tsize);
    TSDB_CHECK_CODE(code, lino, _exit);

    pDelBlk->dp.size += tsize;
    pWriter->tFile.size += tsize;
  }

  tDelBlockDestroy(&pWriter->dData);

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

  pWriter->footer.dict[1].offset = pWriter->tFile.size;
  pWriter->footer.dict[1].size = sizeof(SSttBlk) * taosArrayGetSize(pWriter->aSttBlk);

  if (pWriter->footer.dict[1].size) {
    TSDB_CHECK_CODE(                        //
        code = tsdbWriteFile(               //
            pWriter->pFd,                   //
            pWriter->tFile.size,            //
            TARRAY_DATA(pWriter->aSttBlk),  //
            pWriter->footer.dict[1].size),  //
        lino,                               //
        _exit);

    pWriter->tFile.size += pWriter->footer.dict[1].size;
  }

_exit:
  if (code) {
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pWriter->config.pTsdb->pVnode),    //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  }
  return code;
}

static int32_t write_statistics_blk(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  int32_t lino;

  pWriter->footer.dict[2].offset = pWriter->tFile.size;
  pWriter->footer.dict[2].size = sizeof(STbStatisBlock) * taosArrayGetSize(pWriter->aStatisBlk);

  if (pWriter->footer.dict[2].size) {
    TSDB_CHECK_CODE(                           //
        code = tsdbWriteFile(                  //
            pWriter->pFd,                      //
            pWriter->tFile.size,               //
            TARRAY_DATA(pWriter->aStatisBlk),  //
            pWriter->footer.dict[2].size),     //
        lino,                                  //
        _exit);

    pWriter->tFile.size += pWriter->footer.dict[2].size;
  }

_exit:
  if (code) {
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pWriter->config.pTsdb->pVnode),    //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  }
  return code;
}

static int32_t write_del_blk(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  int32_t lino;

  pWriter->footer.dict[3].offset = pWriter->tFile.size;
  pWriter->footer.dict[3].size = sizeof(SDelBlk) * taosArrayGetSize(pWriter->aDelBlk);

  if (pWriter->footer.dict[3].size) {
    TSDB_CHECK_CODE(                        //
        code = tsdbWriteFile(               //
            pWriter->pFd,                   //
            pWriter->tFile.size,            //
            TARRAY_DATA(pWriter->aDelBlk),  //
            pWriter->footer.dict[3].size),  //
        lino,                               //
        _exit);

    pWriter->tFile.size += pWriter->footer.dict[3].size;
  }

_exit:
  if (code) {
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pWriter->config.pTsdb->pVnode),    //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  }
  return code;
}

static int32_t write_file_footer(struct SSttFWriter *pWriter) {
  int32_t code = tsdbWriteFile(           //
      pWriter->pFd,                       //
      pWriter->tFile.size,                //
      (const uint8_t *)&pWriter->footer,  //
      sizeof(pWriter->footer));
  pWriter->tFile.size += sizeof(pWriter->footer);
  return code;
}

static int32_t write_file_header(struct SSttFWriter *pWriter) {
  // TODO
  return 0;
}

static int32_t create_stt_fwriter(const struct SSttFWriterConf *pConf, struct SSttFWriter **ppWriter) {
  int32_t code = 0;

  // alloc
  if (((ppWriter[0] = taosMemoryCalloc(1, sizeof(*ppWriter[0]))) == NULL)                 //
      || ((ppWriter[0]->aSttBlk = taosArrayInit(64, sizeof(SSttBlk))) == NULL)            //
      || ((ppWriter[0]->aDelBlk = taosArrayInit(64, sizeof(SDelBlk))) == NULL)            //
      || ((ppWriter[0]->aStatisBlk = taosArrayInit(64, sizeof(STbStatisBlock))) == NULL)  //
  ) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if ((code = tBlockDataCreate(&ppWriter[0]->bData))                        //
      || (code = tDelBlockCreate(&ppWriter[0]->dData, pConf->maxRow))       //
      || (code = tTbStatisBlockCreate(&ppWriter[0]->sData, pConf->maxRow))  //
  ) {
    goto _exit;
  }

  // init
  ppWriter[0]->config = pConf[0];
  ppWriter[0]->tFile = pConf->file;
  ppWriter[0]->footer.prevFooter = ppWriter[0]->tFile.size;
  if (pConf->pSkmTb == NULL) {
    ppWriter[0]->config.pSkmTb = &ppWriter[0]->skmTb;
  }
  if (pConf->pSkmRow == NULL) {
    ppWriter[0]->config.pSkmRow = &ppWriter[0]->skmRow;
  }
  if (pConf->aBuf == NULL) {
    ppWriter[0]->config.aBuf = ppWriter[0]->aBuf;
  }

_exit:
  if (code && ppWriter[0]) {
    tTbStatisBlockDestroy(&ppWriter[0]->sData);
    tDelBlockDestroy(&ppWriter[0]->dData);
    tBlockDataDestroy(&ppWriter[0]->bData);
    taosArrayDestroy(ppWriter[0]->aStatisBlk);
    taosArrayDestroy(ppWriter[0]->aDelBlk);
    taosArrayDestroy(ppWriter[0]->aSttBlk);
    taosMemoryFree(ppWriter[0]);
    ppWriter[0] = NULL;
  }
  return code;
}

static int32_t destroy_stt_fwriter(struct SSttFWriter *pWriter) {
  if (pWriter) {
    for (int32_t i = 0; i < ARRAY_SIZE(pWriter->aBuf); i++) {
      tFree(pWriter->aBuf[i]);
    }
    tDestroyTSchema(pWriter->skmRow.pTSchema);
    tDestroyTSchema(pWriter->skmTb.pTSchema);

    tTbStatisBlockDestroy(&pWriter->sData);
    tDelBlockDestroy(&pWriter->dData);
    tBlockDataDestroy(&pWriter->bData);

    taosArrayDestroy(pWriter->aStatisBlk);
    taosArrayDestroy(pWriter->aDelBlk);
    taosArrayDestroy(pWriter->aSttBlk);
    taosMemoryFree(pWriter);
  }
  return 0;
}

static int32_t open_stt_fwriter(struct SSttFWriter *pWriter) {
  int32_t code = 0;
  int32_t lino;
  uint8_t hdr[TSDB_FHDR_SIZE] = {0};

  int32_t flag = TD_FILE_READ | TD_FILE_WRITE;
  if (pWriter->tFile.size == 0) {
    flag |= TD_FILE_CREATE | TD_FILE_TRUNC;
  }

  code = tsdbOpenFile(             //
      pWriter->config.file.fname,  //
      pWriter->config.szPage,      //
      flag,                        //
      &pWriter->pFd);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->tFile.size == 0) {
    code = tsdbWriteFile(  //
        pWriter->pFd,      //
        0,                 //
        hdr,               //
        sizeof(hdr));
    TSDB_CHECK_CODE(code, lino, _exit);

    pWriter->tFile.size += sizeof(hdr);
  }

_exit:
  if (code) {
    if (pWriter->pFd) {
      tsdbCloseFile(&pWriter->pFd);
    }
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pWriter->config.pTsdb->pVnode),    //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  } else {
    tsdbDebug(                                      //
        "vgId:%d %s done, fname:%s size:%" PRId64,  //
        TD_VID(pWriter->config.pTsdb->pVnode),      //
        __func__,                                   //
        pWriter->config.file.fname,                 //
        pWriter->config.file.size                   //
    );
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
    if (ppWriter[0]) {
      destroy_stt_fwriter(ppWriter[0]);
    }
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pConf->pTsdb->pVnode),             //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriterClose(struct SSttFWriter **ppWriter, int8_t abort, struct SFileOp *op) {
  int32_t vgId = TD_VID(ppWriter[0]->config.pTsdb->pVnode);
  int32_t code = 0;
  int32_t lino;

  if (!abort) {
    if (ppWriter[0]->bData.nRow > 0) {
      TSDB_CHECK_CODE(                                 //
          code = write_timeseries_block(ppWriter[0]),  //
          lino,                                        //
          _exit);
    }

    if (ppWriter[0]->sData.nRow > 0) {
      TSDB_CHECK_CODE(                                 //
          code = write_statistics_block(ppWriter[0]),  //
          lino,                                        //
          _exit);
    }

    if (ppWriter[0]->dData.nRow > 0) {
      TSDB_CHECK_CODE(                             //
          code = write_delete_block(ppWriter[0]),  //
          lino,                                    //
          _exit);
    }

    TSDB_CHECK_CODE(                        //
        code = write_stt_blk(ppWriter[0]),  //
        lino,                               //
        _exit);

    TSDB_CHECK_CODE(                               //
        code = write_statistics_blk(ppWriter[0]),  //
        lino,                                      //
        _exit);

    TSDB_CHECK_CODE(                        //
        code = write_del_blk(ppWriter[0]),  //
        lino,                               //
        _exit);

    TSDB_CHECK_CODE(                            //
        code = write_file_footer(ppWriter[0]),  //
        lino,                                   //
        _exit);

    TSDB_CHECK_CODE(                            //
        code = write_file_header(ppWriter[0]),  //
        lino,                                   //
        _exit);

    TSDB_CHECK_CODE(                             //
        code = tsdbFsyncFile(ppWriter[0]->pFd),  //
        lino,                                    //
        _exit);

    if (op) {
      op->fid = ppWriter[0]->config.file.fid;
      op->oState = ppWriter[0]->config.file;
      op->nState = ppWriter[0]->tFile;
      if (op->oState.size == 0) {
        op->op = TSDB_FOP_CREATE;
      } else {
        op->op = TSDB_FOP_EXTEND;
      }
    }
  }

  TSDB_CHECK_CODE(                            //
      code = close_stt_fwriter(ppWriter[0]),  //
      lino,                                   //
      _exit);

  destroy_stt_fwriter(ppWriter[0]);
  ppWriter[0] = NULL;

_exit:
  if (code) {
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        vgId,                                     //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  } else {
  }
  return code;
}

int32_t tsdbSttFWriteTSData(struct SSttFWriter *pWriter, TABLEID *tbid, TSDBROW *pRow) {
  int32_t code = 0;
  int32_t lino;

  TSDBKEY key = TSDBROW_KEY(pRow);

  if (!TABLE_SAME_SCHEMA(pWriter->bData.suid, pWriter->bData.uid, tbid->suid, tbid->uid)) {
    if (pWriter->bData.nRow > 0) {
      TSDB_CHECK_CODE(                             //
          code = write_timeseries_block(pWriter),  //
          lino,                                    //
          _exit);
    }

    if (pWriter->sData.nRow >= pWriter->config.maxRow) {
      TSDB_CHECK_CODE(                             //
          code = write_statistics_block(pWriter),  //
          lino,                                    //
          _exit);
    }

    pWriter->sData.aData[0][pWriter->sData.nRow] = tbid->suid;   // suid
    pWriter->sData.aData[1][pWriter->sData.nRow] = tbid->uid;    // uid
    pWriter->sData.aData[2][pWriter->sData.nRow] = key.ts;       // skey
    pWriter->sData.aData[3][pWriter->sData.nRow] = key.version;  // sver
    pWriter->sData.aData[4][pWriter->sData.nRow] = key.ts;       // ekey
    pWriter->sData.aData[5][pWriter->sData.nRow] = key.version;  // ever
    pWriter->sData.aData[6][pWriter->sData.nRow] = 1;            // count
    pWriter->sData.nRow++;

    TSDB_CHECK_CODE(                  //
        code = tsdbUpdateSkmTb(       //
            pWriter->config.pTsdb,    //
            tbid,                     //
            pWriter->config.pSkmTb),  //
        lino,                         //
        _exit);

    TABLEID id = {.suid = tbid->suid,  //
                  .uid = tbid->suid    //
                             ? 0
                             : tbid->uid};
    TSDB_CHECK_CODE(                           //
        code = tBlockDataInit(                 //
            &pWriter->bData,                   //
            &id,                               //
            pWriter->config.pSkmTb->pTSchema,  //
            NULL,                              //
            0),                                //
        lino,                                  //
        _exit);
  }

  if (pRow->type == TSDBROW_ROW_FMT) {
    TSDB_CHECK_CODE(                   //
        code = tsdbUpdateSkmRow(       //
            pWriter->config.pTsdb,     //
            tbid,                      //
            TSDBROW_SVERSION(pRow),    //
            pWriter->config.pSkmRow),  //
        lino,                          //
        _exit);
  }

  TSDB_CHECK_CODE(                            //
      code = tBlockDataAppendRow(             //
          &pWriter->bData,                    //
          pRow,                               //
          pWriter->config.pSkmRow->pTSchema,  //
          tbid->uid),                         //
      lino,                                   //
      _exit);

  if (pWriter->bData.nRow >= pWriter->config.maxRow) {
    TSDB_CHECK_CODE(                             //
        code = write_timeseries_block(pWriter),  //
        lino,                                    //
        _exit);
  }

  if (key.ts > pWriter->sData.aData[4][pWriter->sData.nRow - 1]) {
    pWriter->sData.aData[4][pWriter->sData.nRow - 1] = key.ts;       // ekey
    pWriter->sData.aData[5][pWriter->sData.nRow - 1] = key.version;  // ever
    pWriter->sData.aData[6][pWriter->sData.nRow - 1]++;              // count
  } else if (key.ts == pWriter->sData.aData[4][pWriter->sData.nRow - 1]) {
    pWriter->sData.aData[4][pWriter->sData.nRow - 1] = key.ts;       // ekey
    pWriter->sData.aData[5][pWriter->sData.nRow - 1] = key.version;  // ever
  } else {
    ASSERTS(0, "timestamp should be in ascending order");
  }

_exit:
  if (code) {
    tsdbError(                                    //
        "vgId:%d %s failed at line %d since %s",  //
        TD_VID(pWriter->config.pTsdb->pVnode),    //
        __func__,                                 //
        lino,                                     //
        tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriteDLData(struct SSttFWriter *pWriter, TABLEID *tbid, SDelData *pDelData) {
  ASSERTS(0, "TODO: Not implemented yet");

  pWriter->dData.aData[0][pWriter->dData.nRow] = tbid->suid;         // suid
  pWriter->dData.aData[1][pWriter->dData.nRow] = tbid->uid;          // uid
  pWriter->dData.aData[2][pWriter->dData.nRow] = pDelData->version;  // version
  pWriter->dData.aData[3][pWriter->dData.nRow] = pDelData->sKey;     // skey
  pWriter->dData.aData[4][pWriter->dData.nRow] = pDelData->eKey;     // ekey
  pWriter->dData.nRow++;

  if (pWriter->dData.nRow >= pWriter->config.maxRow) {
    return write_delete_block(pWriter);
  } else {
    return 0;
  }
}
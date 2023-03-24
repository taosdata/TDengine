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

#ifndef _TSDB_STT_FILE_WRITER_H
#define _TSDB_STT_FILE_WRITER_H

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSttFWriter     SSttFWriter;
typedef struct SSttFWriterConf SSttFWriterConf;

int32_t tsdbSttFWriterOpen(const SSttFWriterConf *pConf, SSttFWriter **ppWriter);
int32_t tsdbSttFWriterClose(SSttFWriter **ppWriter);
int32_t tsdbSttFWriteTSData(SSttFWriter *pWriter, TABLEID *tbid, TSDBROW *pRow);
int32_t tsdbSttFWriteDLData(SSttFWriter *pWriter, TABLEID *tbid, SDelData *pDelData);

struct SSttFWriterConf {
  STsdb    *pTsdb;
  SSkmInfo *pSkmTb;
  SSkmInfo *pSkmRow;
  int32_t   maxRow;
  int32_t   szPage;
  int8_t    cmprAlg;
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_STT_FILE_WRITER_H*/
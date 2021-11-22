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

#ifndef __INDEX_FST_COUNTING_WRITER_H__
#define __INDEX_FST_COUNTING_WRITER_H__

typedef uint32_t CheckSummer;


typedef struct FstCountingWriter {
  void*    wrt;  // wrap any writer that counts and checksum bytes written 
  uint64_t count; 
  CheckSummer summer;  
} FstCountingWriter;

uint64_t fstCountingWriterWrite(FstCountingWriter *write, uint8_t *buf, uint32_t bufLen); 

int FstCountingWriterFlush(FstCountingWriter *write);


FstCountingWriter *fstCountingWriterCreate(void *wtr);


#define FST_WRITER_COUNT(writer) (writer->count)
#define FST_WRITER_INTER_WRITER(writer) (writer->wtr)
#define FST_WRITE_CHECK_SUMMER(writer) (writer->summer)

#endif



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


FstCountingWriter *fstCountingWriterCreate(void *wrt) {
  FstCountingWriter *cw = calloc(1, sizeof(FstCountingWriter)); 
  if (cw == NULL) { return NULL; }
  cw->wrt = wrt; 
  return cw; 
}
void FstCountingWriterDestroy(FstCountingWriter *cw) {
  // free wrt object: close fd or free mem 
  free(cw);
}

uint64_t fstCountingWriterWrite(FstCountingWriter *write, uint8_t *buf, uint32_t bufLen) {
  if (write == NULL) { return 0; } 
  // update checksum 
  // write data to file/socket or mem
  
  write->count += bufLen;
  return bufLen; 
} 

int FstCountingWriterFlush(FstCountingWriter *write) {
  //write->wtr->flush
  return 1;
}




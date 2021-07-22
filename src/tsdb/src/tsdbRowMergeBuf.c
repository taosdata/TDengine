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

#include "tsdbRowMergeBuf.h"
#include "tdataformat.h"

int tsdbMergeBufMakeSureRoom(SMergeBuf *pBuf, STSchema* pSchema1, STSchema* pSchema2) {
  int len = MAX(dataRowMaxBytesFromSchema(pSchema1), dataRowMaxBytesFromSchema(pSchema2)) + sizeof(int);
  SMergeBuf buf = *pBuf;
  if(SMERGE_BUF_LEN(buf) < len) {
    buf = realloc(buf, len + sizeof(int));
    if(buf == NULL) {
      return -1;
    }
  *pBuf = buf;
  }
  return 0;
}

// row1 has higher priority
SMemRow tsdbMergeTwoRows(SMergeBuf *pBuf, SMemRow row1, SMemRow row2, STSchema *pSchema1, STSchema *pSchema2) {
  if(row2 == NULL) return row1;
  if(row1 == NULL) return row2;
  ASSERT(pSchema1->version == memRowVersion(row1)); 
  ASSERT(pSchema2->version == memRowVersion(row2));

  if(tsdbMergeBufMakeSureRoom(pBuf, pSchema1, pSchema2) < 0) {
    return NULL;
  }
  SMergeBuf buf = *pBuf;

  void *pData = SMERGE_BUF_PTR(buf);

  return mergeTwoMemRows(pData, row1, row2, pSchema1, pSchema2);

  /*
  *(uint16_t*)pData = *(uint16_t*)row1;
  *(int16_t*)POINTER_SHIFT(pData, sizeof(int16_t)) = *(int16_t*)POINTER_SHIFT(row1, sizeof(int16_t));

  int numOfColsOfRow1 = schemaNCols(pSchema1);
  int numOfColsOfRow2 = schemaNCols(pSchema2);
  int i = 0, j = 0;
  int16_t colIdOfRow1 = pSchema1->columns[i].colId;
  int16_t colIdOfRow2 = pSchema2->columns[j].colId;

  while(i < numOfColsOfRow1) {
    if(j >= numOfColsOfRow2) {
      tdCopyColOfRowBySchema(pData, pSchema1, i, row1, pSchema1, i);
      i++;
    } else {
      colIdOfRow1 = pSchema1->columns[i].colId;
      colIdOfRow2 = pSchema2->columns[j].colId;
      if(colIdOfRow1 > colIdOfRow2) {
        j++;
        continue;
      }
      if(tdIsColOfRowNullBySchema(row1, pSchema1, i)) {
        if(tdIsColOfRowNullBySchema(row2, pSchema2, j)) {
          tdSetColOfRowNullBySchema(pData, pSchema1, j);
        } else {
          tdCopyColOfRowBySchema(pData, pSchema1, i, row2, pSchema2, j);
        }
      } else {
        tdCopyColOfRowBySchema(pData, pSchema1, i, row1, pSchema1, i);
      }
      i++;
      j++;
    }
  }
  return pData;
  */
}

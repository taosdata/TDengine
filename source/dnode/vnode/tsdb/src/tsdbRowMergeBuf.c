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

// row1 has higher priority
SMemRow tsdbMergeTwoRows(SMergeBuf *pBuf, SMemRow row1, SMemRow row2, STSchema *pSchema1, STSchema *pSchema2) {
  if(row2 == NULL) return row1;
  if(row1 == NULL) return row2;
  ASSERT(pSchema1->version == memRowVersion(row1)); 
  ASSERT(pSchema2->version == memRowVersion(row2));

  if(tsdbMergeBufMakeSureRoom(pBuf, pSchema1, pSchema2) < 0) {
    return NULL;
  }
  return mergeTwoMemRows(*pBuf, row1, row2, pSchema1, pSchema2);
}

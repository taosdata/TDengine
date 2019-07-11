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

#ifndef TDENGINE_VNODEFILE_H
#define TDENGINE_VNODEFILE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tchecksum.h"

#define TSDB_VNODE_DELIMITER 0xF00AFA0F

typedef struct { int64_t compInfoOffset; } SCompHeader;

typedef struct {
  short   colId;
  short   bytes;
  int32_t numOfNullPoints;
  int32_t type : 8;
  int32_t offset : 24;
  int32_t len;  // data length
  int64_t sum;
  int64_t max;
  int64_t min;
  int64_t wsum;
  char    reserved[16];
} SField;

typedef struct {
  int64_t  last : 1;
  int64_t  offset : 63;
  int32_t  algorithm : 8;     // compression algorithm can be changed
  int32_t  numOfPoints : 24;  // how many points have been written into this block
  int32_t  sversion;
  int32_t  len;  // total length of this data block
  uint16_t numOfCols;
  char     reserved[16];
  TSKEY    keyFirst;  // time stamp for the first point
  TSKEY    keyLast;   // time stamp for the last point
} SCompBlock;

typedef struct {
  SCompBlock *compBlock;
  SField *    fields;
} SCompBlockFields;

typedef struct {
  uint64_t   uid;
  int64_t    last : 1;
  int64_t    numOfBlocks : 62;
  uint32_t   delimiter;  // delimiter for recovery
  TSCKSUM    checksum;
  SCompBlock compBlocks[];  // comp block list
} SCompInfo;

typedef struct {
  long tempHeadOffset;
  long compInfoOffset;
  long oldCompBlockOffset;

  long oldNumOfBlocks;
  long newNumOfBlocks;
  long finalNumOfBlocks;

  long oldCompBlockLen;
  long newCompBlockLen;
  long finalCompBlockLen;

  long       committedPoints;
  int        commitSlot;
  int32_t    last : 1;
  int32_t    changed : 1;
  int32_t    commitPos : 30;
  int64_t    commitCount;
  SCompBlock lastBlock;
} SMeterInfo;

typedef struct { int64_t totalStorage; } SVnodeHeadInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODEFILE_H

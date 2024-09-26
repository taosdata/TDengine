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

#ifndef TDENGINE_FUNCTIONRESINFO_H
#define TDENGINE_FUNCTIONRESINFO_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tcommon.h"

typedef struct SFirstLastRes {
  bool hasResult;
  // used for last_row function only, isNullRes in SResultRowEntry can not be passed to downstream.So,
  // this attribute is required
  bool      isNull;
  int32_t   bytes;
  int64_t   ts;
  char*     pkData;
  int32_t   pkBytes;
  int8_t    pkType;
  STuplePos pos;
  STuplePos nullTuplePos;
  bool      nullTupleSaved;
  char      buf[];
} SFirstLastRes;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FUNCTIONRESINFO_H

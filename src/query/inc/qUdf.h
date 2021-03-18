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

#ifndef TDENGINE_QUDF_H
#define TDENGINE_QUDF_H

typedef struct SUdfInfo {
  int32_t functionId;  // system assigned function id
  int8_t  funcType;    // scalar function or aggregate function
  int8_t  resType;     // result type
  int16_t resBytes;    // result byte
  int32_t contLen;     // content length
  char   *name;        // function name
  union {              // file path or [in memory] binary content
    char *content;
    char *path;
  };
} SUdfInfo;

#endif  // TDENGINE_QUDF_H

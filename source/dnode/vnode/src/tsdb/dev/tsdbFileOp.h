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

#ifndef _TSDB_FILE_OP_H
#define _TSDB_FILE_OP_H

#ifdef __cplusplus
extern "C" {
#endif

/* Exposed Handle */
typedef enum {
  TSDB_FOP_EXTEND = -2,
  TSDB_FOP_CREATE,
  TSDB_FOP_NONE,
  TSDB_FOP_DELETE,
  TSDB_FOP_TRUNCATE,
} EFileOpType;

struct SFileOp {
  EFileOpType op;
  // TODO
};

/* Exposed APIs */

/* Exposed Structs */

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_OP_H*/